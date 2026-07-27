package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
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
    private static final byte[] CHECKPOINT_KEY = "bricFlatDbHealCheckpoint".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PENDING_STORAGE_ACCOUNTS_KEY =
        "bricFlatDbHealPendingStorageAccounts".getBytes(StandardCharsets.UTF_8);

    private final RocksDBSegmentedStorage storage;
    private final BonsaiWorldStateKeyValueStorage worldState;

    public FlatDbHealer(BesuDatabaseManager dbManager) {
        this.storage = new RocksDBSegmentedStorage(dbManager);
        BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
            new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
        try {
            flatDbStrategyProvider.loadFlatDbStrategy(storage);
        } catch (UnsupportedOperationException e) {
            // Besu's loadFlatDbStrategy() always attempts to persist a metadata write-back
            // (the flat DB mode byte and a "use code storage by hash" flag) on first load,
            // because its in-memory flatDbMode field starts null and therefore never equals
            // the freshly-derived mode. That write-back calls storage.startTransaction(),
            // which RocksDBSegmentedStorage correctly rejects when the underlying database
            // was opened read-only. Skipping it is harmless here: bric's own heal logic never
            // depends on the persisted "use code storage by hash" flag, and dry-run callers
            // only need to read the strategy, not persist it.
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
            Optional<Bytes> accountValue = worldState.getAccount(accountHash);
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
            clearCheckpoint();
            worldState.upgradeToFullFlatDbMode();
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

        AccountRangeOutcome(
                long accountsChecked, long added, long updated, long removed, List<Hash> divergentAccounts) {
            this.accountsChecked = accountsChecked;
            this.added = added;
            this.updated = updated;
            this.removed = removed;
            this.divergentAccounts = divergentAccounts;
        }
    }

    AccountRangeOutcome healAccountRange(
            Bytes32 stateRoot, Bytes32 startKeyHash, Bytes32 endKeyHash, boolean dryRun) {
        MerkleTrie<Bytes, Bytes> accountTrie = new StoredMerklePatriciaTrie<>(
            worldState::getAccountStateTrieNode, stateRoot, Function.identity(), Function.identity());

        RangeStorageEntriesCollector collector = RangeStorageEntriesCollector.createCollector(
            startKeyHash, endKeyHash, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
        NavigableMap<Bytes32, Bytes> trieAccounts = new TreeMap<>(accountTrie.entriesFrom(
            root -> RangeStorageEntriesCollector.collectEntries(collector, visitor, root, startKeyHash)));

        NavigableMap<Bytes32, Bytes> flatAccounts = readFlatRange(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, startKeyHash.toArrayUnsafe(), endKeyHash.toArrayUnsafe());

        List<Bytes32> toAdd = new ArrayList<>();
        List<Bytes32> toUpdate = new ArrayList<>();
        List<Bytes32> toRemove = new ArrayList<>();

        for (Map.Entry<Bytes32, Bytes> entry : trieAccounts.entrySet()) {
            Bytes flatValue = flatAccounts.get(entry.getKey());
            if (flatValue == null) {
                toAdd.add(entry.getKey());
            } else if (!flatValue.equals(entry.getValue())) {
                toUpdate.add(entry.getKey());
            }
        }
        for (Bytes32 accountHash : flatAccounts.keySet()) {
            if (!trieAccounts.containsKey(accountHash)) {
                toRemove.add(accountHash);
            }
        }

        if (!dryRun && !(toAdd.isEmpty() && toUpdate.isEmpty() && toRemove.isEmpty())) {
            BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
            for (Bytes32 accountHash : toAdd) {
                updater.putAccountInfoState(Hash.wrap(accountHash), trieAccounts.get(accountHash));
            }
            for (Bytes32 accountHash : toUpdate) {
                updater.putAccountInfoState(Hash.wrap(accountHash), trieAccounts.get(accountHash));
            }
            for (Bytes32 accountHash : toRemove) {
                updater.removeAccountInfoState(Hash.wrap(accountHash));
            }
            updater.commit();
        }

        List<Hash> divergentAccounts = new ArrayList<>();
        toAdd.forEach(hash -> divergentAccounts.add(Hash.wrap(hash)));
        toUpdate.forEach(hash -> divergentAccounts.add(Hash.wrap(hash)));

        return new AccountRangeOutcome(
            trieAccounts.size(), toAdd.size(), toUpdate.size(), toRemove.size(), divergentAccounts);
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
        MerkleTrie<Bytes, Bytes> storageTrie = new StoredMerklePatriciaTrie<>(
            (location, hash) -> worldState.getAccountStorageTrieNode(accountHash, location, hash),
            storageRoot, Function.identity(), Function.identity());

        RangeStorageEntriesCollector collector = RangeStorageEntriesCollector.createCollector(
            RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
        NavigableMap<Bytes32, Bytes> trieSlots = new TreeMap<>(storageTrie.entriesFrom(
            root -> RangeStorageEntriesCollector.collectEntries(collector, visitor, root, RangeManager.MIN_RANGE)));

        NavigableMap<Bytes32, Bytes> flatSlots = readFlatStorageRange(accountHash);

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

        return new StorageRangeOutcome(trieSlots.size(), toAdd.size(), toUpdate.size(), toRemove.size());
    }

    private NavigableMap<Bytes32, Bytes> readFlatStorageRange(Hash accountHash) {
        byte[] startKey = Bytes.concatenate(accountHash, RangeManager.MIN_RANGE).toArrayUnsafe();
        byte[] endKey = Bytes.concatenate(accountHash, RangeManager.MAX_RANGE).toArrayUnsafe();

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
