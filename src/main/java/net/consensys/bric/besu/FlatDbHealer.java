package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Reconciles a Bonsai database's flat account/storage tables against the canonical
 * account and storage tries, driving Besu's own trie/storage classes directly rather
 * than Besu's peer-network-driven snap-sync healing (which isn't reusable outside a
 * running node — see docs/superpowers/specs/2026-07-24-flatdb-heal-design.md).
 */
public class FlatDbHealer {

    private static final Logger LOG = LoggerFactory.getLogger(FlatDbHealer.class);

    private static final int RANGE_COUNT = 16;
    /**
     * Upper bound on how many trie leaves a single {@link #healAccountRange}/{@link #healAccountStorage}
     * batch pulls into memory at once. The trie walk resumes from the last key of each batch until the
     * range is exhausted, so a whole 1/16 account range (or one contract's entire storage) is never
     * materialised in a single {@code TreeMap} — that unbounded whole-range load was what OOM'd on
     * mainnet-size state. Chosen to keep a batch's in-memory footprint well under a JVM's default heap
     * (~a few MB per batch) while staying large enough that batching overhead is negligible.
     */
    private static final int DEFAULT_BATCH_LIMIT = 50_000;
    private static final byte[] CHECKPOINT_KEY = "bricFlatDbHealCheckpoint".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PENDING_STORAGE_ACCOUNTS_KEY =
        "bricFlatDbHealPendingStorageAccounts".getBytes(StandardCharsets.UTF_8);

    private final RocksDBSegmentedStorage storage;
    private final BonsaiWorldStateKeyValueStorage worldState;

    public FlatDbHealer(BesuDatabaseManager dbManager) {
        this.storage = new RocksDBSegmentedStorage(dbManager);
        DataStorageConfiguration dataStorageConfiguration =
            dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE
                ? DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG
                : DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;
        BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
            new NoOpMetricsSystem(), dataStorageConfiguration);
        try {
            flatDbStrategyProvider.loadFlatDbStrategy(storage);
        } catch (UnsupportedOperationException e) {
            // Besu's loadFlatDbStrategy() persists the flat DB mode byte via
            // storage.startTransaction() whenever that byte isn't already on disk (e.g. a
            // database created before this metadata key existed) — which RocksDBSegmentedStorage
            // correctly rejects when the underlying database was opened read-only. Skipping it
            // is harmless here: dry-run callers only need to read the derived strategy, not
            // persist it, and bric's own heal logic doesn't depend on this write having happened.
            LOG.debug("Skipping flat DB metadata write-back; database is read-only.", e);
        }
        this.worldState = new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider, storage, new NoOpKeyValueStorage());
    }

    public Bytes32 getTargetStateRoot() {
        return worldState.getWorldStateRootHash()
            .map(Bytes32::wrap)
            .orElseThrow(() -> new IllegalStateException(
                "No world state root found; database may be empty or not yet synced."));
    }

    public FlatDbHealResult heal(boolean dryRun, FlatDbHealProgressListener listener) {
        Bytes32 stateRoot = getTargetStateRoot();

        int startRangeIndex = 0;
        FlatDbHealCheckpoint.Phase resumePhase = FlatDbHealCheckpoint.Phase.ACCOUNTS;
        List<Hash> divergentAccounts = new ArrayList<>();
        // Trie-derived RLP for every divergent account found *in this invocation's* account
        // phase, keyed so the storage phase can use the already-computed correct value instead
        // of re-reading the flat table (see heal()'s storage-phase loop below). Only populated
        // for ranges actually processed by this call, never reloaded from the checkpoint.
        Map<Hash, Bytes> divergentAccountValues = new HashMap<>();

        if (!dryRun) {
            Optional<FlatDbHealCheckpoint> checkpoint = readCheckpoint();
            if (checkpoint.isPresent() && checkpoint.get().stateRoot().equals(stateRoot)) {
                startRangeIndex = checkpoint.get().nextRangeIndex();
                resumePhase = checkpoint.get().phase();
                divergentAccounts.addAll(readPendingStorageAccounts());
            } else if (checkpoint.isPresent()) {
                LOG.info("Chain has advanced since the last interrupted run; "
                    + "restarting heal from the beginning.");
            }
        }

        long accountsAdded = 0;
        long accountsUpdated = 0;
        long accountsRemoved = 0;

        if (resumePhase == FlatDbHealCheckpoint.Phase.ACCOUNTS) {
            List<Map.Entry<Bytes32, Bytes32>> ranges =
                new ArrayList<>(RangeManager.generateAllRanges(RANGE_COUNT).entrySet());
            for (int i = startRangeIndex; i < ranges.size(); i++) {
                Map.Entry<Bytes32, Bytes32> range = ranges.get(i);
                AccountRangeOutcome outcome = healAccountRange(stateRoot, range.getKey(), range.getValue(), dryRun);
                accountsAdded += outcome.added;
                accountsUpdated += outcome.updated;
                accountsRemoved += outcome.removed;
                divergentAccounts.addAll(outcome.divergentAccounts);
                divergentAccountValues.putAll(outcome.divergentAccountValues);
                listener.onRangeComplete(
                    i + 1, ranges.size(), outcome.accountsChecked, outcome.added + outcome.updated + outcome.removed);
                if (!dryRun) {
                    // Persist the accumulated divergent-accounts list alongside every per-range
                    // checkpoint write (not just once after the whole loop) so an interruption
                    // right after this range's checkpoint commits can never strand accounts found
                    // divergent in this or any earlier range: their flat entry is already durably
                    // fixed, so they'd never be re-flagged as divergent on a resumed run.
                    persistPendingStorageAccounts(divergentAccounts);
                    persistCheckpoint(new FlatDbHealCheckpoint(stateRoot, FlatDbHealCheckpoint.Phase.ACCOUNTS, i + 1));
                }
            }
            if (!dryRun) {
                persistCheckpoint(new FlatDbHealCheckpoint(stateRoot, FlatDbHealCheckpoint.Phase.STORAGE, RANGE_COUNT));
            }
        }

        long slotsAdded = 0;
        long slotsUpdated = 0;
        long slotsRemoved = 0;
        int totalAccountsToHeal = divergentAccounts.size();
        List<Hash> remainingAccounts = new ArrayList<>(divergentAccounts);
        int accountsHealed = 0;

        while (!remainingAccounts.isEmpty()) {
            Hash accountHash = remainingAccounts.remove(0);
            // Prefer the trie-derived value already computed by this invocation's account phase
            // (correct even for "updated" accounts, whose flat entry may still be stale at this
            // point) over re-reading the flat table, which Besu's BonsaiPartialFlatDbStrategy
            // only falls back to the trie for when the flat entry is entirely *missing* — not
            // when it's merely stale. The fallback below only fires when resuming a non-dry-run
            // run in a fresh process (no in-memory map), at which point the account phase's
            // writes are already durably committed, so getAccount() is safe there.
            Optional<Bytes> accountValue = Optional.ofNullable(divergentAccountValues.get(accountHash))
                .or(() -> worldState.getAccount(accountHash));
            if (accountValue.isPresent()) {
                Hash storageRoot = PmtStateTrieAccountValue.readFrom(RLP.input(accountValue.get())).getStorageRoot();
                StorageRangeOutcome outcome = healAccountStorage(accountHash, storageRoot, dryRun);
                slotsAdded += outcome.added;
                slotsUpdated += outcome.updated;
                slotsRemoved += outcome.removed;
                accountsHealed++;
                listener.onStorageAccountComplete(
                    accountsHealed, totalAccountsToHeal, outcome.slotsChecked,
                    outcome.added + outcome.updated + outcome.removed);
            }
            if (!dryRun) {
                persistPendingStorageAccounts(remainingAccounts);
            }
        }

        if (!dryRun) {
            worldState.upgradeToFullFlatDbMode();
            clearCheckpoint();
        }

        return new FlatDbHealResult(
            accountsAdded, accountsUpdated, accountsRemoved, slotsAdded, slotsUpdated, slotsRemoved, dryRun);
    }

    private Optional<FlatDbHealCheckpoint> readCheckpoint() {
        return storage.get(KeyValueSegmentIdentifier.VARIABLES, CHECKPOINT_KEY)
            .map(FlatDbHealCheckpoint::decode);
    }

    private List<Hash> readPendingStorageAccounts() {
        return storage.get(KeyValueSegmentIdentifier.VARIABLES, PENDING_STORAGE_ACCOUNTS_KEY)
            .map(FlatDbHealer::decodeAccountList)
            .orElseGet(ArrayList::new);
    }

    private void persistCheckpoint(FlatDbHealCheckpoint checkpoint) {
        var transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.VARIABLES, CHECKPOINT_KEY, checkpoint.encode());
        transaction.commit();
        transaction.close();
    }

    private void persistPendingStorageAccounts(List<Hash> accounts) {
        var transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.VARIABLES, PENDING_STORAGE_ACCOUNTS_KEY, encodeAccountList(accounts));
        transaction.commit();
        transaction.close();
    }

    private void clearCheckpoint() {
        var transaction = storage.startTransaction();
        transaction.remove(KeyValueSegmentIdentifier.VARIABLES, CHECKPOINT_KEY);
        transaction.remove(KeyValueSegmentIdentifier.VARIABLES, PENDING_STORAGE_ACCOUNTS_KEY);
        transaction.commit();
        transaction.close();
    }

    private static byte[] encodeAccountList(List<Hash> accounts) {
        ByteBuffer buffer = ByteBuffer.allocate(accounts.size() * 32);
        for (Hash account : accounts) {
            buffer.put(account.toArrayUnsafe());
        }
        return buffer.array();
    }

    private static List<Hash> decodeAccountList(byte[] bytes) {
        List<Hash> accounts = new ArrayList<>();
        for (int offset = 0; offset < bytes.length; offset += 32) {
            accounts.add(Hash.wrap(Bytes32.wrap(bytes, offset)));
        }
        return accounts;
    }

    static final class AccountRangeOutcome {
        final long accountsChecked;
        final long added;
        final long updated;
        final long removed;
        final List<Hash> divergentAccounts;
        /**
         * Trie-derived RLP value for every account in {@link #divergentAccounts} (both added and
         * updated). Lets callers use the already-computed, correct value directly instead of
         * re-reading it from the flat table, which is wrong for "updated" accounts whose flat
         * entry is present but stale.
         */
        final Map<Hash, Bytes> divergentAccountValues;

        AccountRangeOutcome(
                long accountsChecked, long added, long updated, long removed,
                List<Hash> divergentAccounts, Map<Hash, Bytes> divergentAccountValues) {
            this.accountsChecked = accountsChecked;
            this.added = added;
            this.updated = updated;
            this.removed = removed;
            this.divergentAccounts = divergentAccounts;
            this.divergentAccountValues = divergentAccountValues;
        }
    }

    AccountRangeOutcome healAccountRange(
            Bytes32 stateRoot, Bytes32 startKeyHash, Bytes32 endKeyHash, boolean dryRun) {
        return healAccountRange(stateRoot, startKeyHash, endKeyHash, dryRun, DEFAULT_BATCH_LIMIT);
    }

    /**
     * Walks the account trie for {@code [startKeyHash, endKeyHash]} in batches of at most
     * {@code batchLimit} leaves, diffing each batch against the matching flat-table slice and
     * writing corrections as it goes, rather than materialising the whole range at once (which
     * OOM'd on mainnet-size state). Consecutive batches tile the key space with no gaps — batch
     * <em>n</em> resumes from the successor of batch <em>n-1</em>'s last key — so orphan flat
     * entries between trie leaves are still detected, and the final batch extends its flat slice to
     * {@code endKeyHash} so orphans past the last trie leaf are caught too. Aggregate counts and the
     * divergent-account set are identical to a single whole-range pass; only the peak memory differs.
     */
    AccountRangeOutcome healAccountRange(
            Bytes32 stateRoot, Bytes32 startKeyHash, Bytes32 endKeyHash, boolean dryRun, int batchLimit) {
        MerkleTrie<Bytes, Bytes> accountTrie = new StoredMerklePatriciaTrie<>(
            worldState::getAccountStateTrieNode, stateRoot, Function.identity(), Function.identity());

        long accountsChecked = 0;
        long added = 0;
        long updated = 0;
        long removed = 0;
        List<Hash> divergentAccounts = new ArrayList<>();
        Map<Hash, Bytes> divergentAccountValues = new HashMap<>();

        Bytes32 batchStart = startKeyHash;
        while (true) {
            NavigableMap<Bytes32, Bytes> trieBatch = collectTrieBatch(
                accountTrie, batchStart, endKeyHash, batchLimit);
            if (trieBatch.isEmpty()) {
                break;
            }

            boolean lastBatch = trieBatch.size() < batchLimit || trieBatch.lastKey().compareTo(endKeyHash) >= 0;
            Bytes32 flatSliceEnd = lastBatch ? endKeyHash : trieBatch.lastKey();
            NavigableMap<Bytes32, Bytes> flatBatch = readFlatRange(
                KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE,
                batchStart.toArrayUnsafe(), flatSliceEnd.toArrayUnsafe());

            List<Bytes32> toAdd = new ArrayList<>();
            List<Bytes32> toUpdate = new ArrayList<>();
            List<Bytes32> toRemove = new ArrayList<>();

            for (Map.Entry<Bytes32, Bytes> entry : trieBatch.entrySet()) {
                Bytes flatValue = flatBatch.get(entry.getKey());
                if (flatValue == null) {
                    toAdd.add(entry.getKey());
                } else if (!flatValue.equals(entry.getValue())) {
                    toUpdate.add(entry.getKey());
                }
            }
            for (Bytes32 accountHash : flatBatch.keySet()) {
                if (!trieBatch.containsKey(accountHash)) {
                    toRemove.add(accountHash);
                }
            }

            if (!dryRun && !(toAdd.isEmpty() && toUpdate.isEmpty() && toRemove.isEmpty())) {
                BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
                for (Bytes32 accountHash : toAdd) {
                    updater.putAccountInfoState(Hash.wrap(accountHash), trieBatch.get(accountHash));
                }
                for (Bytes32 accountHash : toUpdate) {
                    updater.putAccountInfoState(Hash.wrap(accountHash), trieBatch.get(accountHash));
                }
                for (Bytes32 accountHash : toRemove) {
                    updater.removeAccountInfoState(Hash.wrap(accountHash));
                }
                updater.commit();
            }

            toAdd.forEach(hash -> {
                divergentAccounts.add(Hash.wrap(hash));
                divergentAccountValues.put(Hash.wrap(hash), trieBatch.get(hash));
            });
            toUpdate.forEach(hash -> {
                divergentAccounts.add(Hash.wrap(hash));
                divergentAccountValues.put(Hash.wrap(hash), trieBatch.get(hash));
            });

            accountsChecked += trieBatch.size();
            added += toAdd.size();
            updated += toUpdate.size();
            removed += toRemove.size();

            if (lastBatch) {
                break;
            }
            batchStart = nextKey(trieBatch.lastKey());
        }

        return new AccountRangeOutcome(
            accountsChecked, added, updated, removed, divergentAccounts, divergentAccountValues);
    }

    /**
     * Collects up to {@code batchLimit} trie leaves in {@code [batchStart, endKeyHash]}, starting the
     * walk at {@code batchStart}. Shared by the account and storage phases, which differ only in the
     * {@code trie} being walked.
     */
    private static NavigableMap<Bytes32, Bytes> collectTrieBatch(
            MerkleTrie<Bytes, Bytes> trie, Bytes32 batchStart, Bytes32 endKeyHash, int batchLimit) {
        RangeStorageEntriesCollector collector = RangeStorageEntriesCollector.createCollector(
            batchStart, endKeyHash, batchLimit, Integer.MAX_VALUE);
        TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
        return new TreeMap<>(trie.entriesFrom(
            root -> RangeStorageEntriesCollector.collectEntries(collector, visitor, root, batchStart)));
    }

    /** The next 32-byte key after {@code key} (i.e. {@code key + 1}), used to resume a paged walk. */
    private static Bytes32 nextKey(Bytes32 key) {
        return UInt256.fromBytes(key).add(UInt256.ONE).toBytes();
    }

    private NavigableMap<Bytes32, Bytes> readFlatRange(
            KeyValueSegmentIdentifier segment, byte[] startKey, byte[] endKey) {
        NavigableMap<Bytes32, Bytes> result = new TreeMap<>();
        for (Pair<byte[], byte[]> entry : storage.streamFromKey(segment, startKey, endKey).toList()) {
            result.put(Bytes32.wrap(entry.getLeft()), Bytes.wrap(entry.getRight()));
        }
        return result;
    }

    static final class StorageRangeOutcome {
        final long slotsChecked;
        final long added;
        final long updated;
        final long removed;

        StorageRangeOutcome(long slotsChecked, long added, long updated, long removed) {
            this.slotsChecked = slotsChecked;
            this.added = added;
            this.updated = updated;
            this.removed = removed;
        }
    }

    StorageRangeOutcome healAccountStorage(Hash accountHash, Hash storageRoot, boolean dryRun) {
        return healAccountStorage(accountHash, storageRoot, dryRun, DEFAULT_BATCH_LIMIT);
    }

    /**
     * Storage-phase counterpart of {@link #healAccountRange}: walks one account's storage trie in
     * batches of at most {@code batchLimit} slots so a contract with millions of slots never
     * materialises its entire storage in memory at once. Same paging/tiling guarantees as the
     * account phase.
     */
    StorageRangeOutcome healAccountStorage(Hash accountHash, Hash storageRoot, boolean dryRun, int batchLimit) {
        MerkleTrie<Bytes, Bytes> storageTrie = new StoredMerklePatriciaTrie<>(
            (location, hash) -> worldState.getAccountStorageTrieNode(accountHash, location, hash),
            storageRoot, Function.identity(), Function.identity());

        long slotsChecked = 0;
        long added = 0;
        long updated = 0;
        long removed = 0;

        Bytes32 batchStart = RangeManager.MIN_RANGE;
        while (true) {
            NavigableMap<Bytes32, Bytes> trieSlots = collectTrieBatch(
                storageTrie, batchStart, RangeManager.MAX_RANGE, batchLimit);
            if (trieSlots.isEmpty()) {
                break;
            }

            boolean lastBatch =
                trieSlots.size() < batchLimit || trieSlots.lastKey().compareTo(RangeManager.MAX_RANGE) >= 0;
            Bytes32 flatSliceEnd = lastBatch ? RangeManager.MAX_RANGE : trieSlots.lastKey();
            NavigableMap<Bytes32, Bytes> flatSlots = readFlatStorageRange(accountHash, batchStart, flatSliceEnd);

            List<Bytes32> toAdd = new ArrayList<>();
            List<Bytes32> toUpdate = new ArrayList<>();
            List<Bytes32> toRemove = new ArrayList<>();

            for (Map.Entry<Bytes32, Bytes> entry : trieSlots.entrySet()) {
                Bytes flatValue = flatSlots.get(entry.getKey());
                if (flatValue == null) {
                    toAdd.add(entry.getKey());
                } else if (!flatValue.equals(entry.getValue())) {
                    toUpdate.add(entry.getKey());
                }
            }
            for (Bytes32 slotHash : flatSlots.keySet()) {
                if (!trieSlots.containsKey(slotHash)) {
                    toRemove.add(slotHash);
                }
            }

            if (!dryRun && !(toAdd.isEmpty() && toUpdate.isEmpty() && toRemove.isEmpty())) {
                BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
                for (Bytes32 slotHash : toAdd) {
                    updater.putStorageValueBySlotHash(accountHash, Hash.wrap(slotHash), trieSlots.get(slotHash));
                }
                for (Bytes32 slotHash : toUpdate) {
                    updater.putStorageValueBySlotHash(accountHash, Hash.wrap(slotHash), trieSlots.get(slotHash));
                }
                for (Bytes32 slotHash : toRemove) {
                    updater.removeStorageValueBySlotHash(accountHash, Hash.wrap(slotHash));
                }
                updater.commit();
            }

            slotsChecked += trieSlots.size();
            added += toAdd.size();
            updated += toUpdate.size();
            removed += toRemove.size();

            if (lastBatch) {
                break;
            }
            batchStart = nextKey(trieSlots.lastKey());
        }

        return new StorageRangeOutcome(slotsChecked, added, updated, removed);
    }

    private NavigableMap<Bytes32, Bytes> readFlatStorageRange(
            Hash accountHash, Bytes32 startSlotHash, Bytes32 endSlotHash) {
        byte[] startKey = Bytes.concatenate(accountHash, startSlotHash).toArrayUnsafe();
        byte[] endKey = Bytes.concatenate(accountHash, endSlotHash).toArrayUnsafe();

        NavigableMap<Bytes32, Bytes> result = new TreeMap<>();
        for (Pair<byte[], byte[]> entry : storage.streamFromKey(
                KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE, startKey, endKey).toList()) {
            byte[] key = entry.getLeft();
            Bytes32 slotHash = Bytes32.wrap(key, 32);
            result.put(slotHash, Bytes.wrap(entry.getRight()));
        }
        return result;
    }
}
