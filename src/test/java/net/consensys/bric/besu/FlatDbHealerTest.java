package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlatDbHealerTest {

    @TempDir
    Path tempDir;

    private BesuDatabaseManager dbManager;

    @BeforeEach
    void setUp() {
        RocksDB.loadLibrary();
        dbManager = new BesuDatabaseManager();
    }

    @AfterEach
    void tearDown() {
        if (dbManager.isOpen()) {
            dbManager.closeDatabase();
        }
    }

    /**
     * Creates the column families a real Bonsai database has, without opening it via
     * BesuDatabaseManager or ever touching its flat DB mode metadata. Used as the shared
     * base for both the writable fixture helper and the "never-before-loaded, read-only"
     * regression fixture below.
     */
    private void createFixtureDatabaseSchema() throws Exception {
        List<ColumnFamilyDescriptor> descriptors = List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.CODE_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.VARIABLES.getId()));
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        try (RocksDB db = RocksDB.open(options, tempDir.toString(), descriptors, handles)) {
            for (ColumnFamilyHandle handle : handles) {
                handle.close();
            }
        }
    }

    /**
     * Creates the column families a real Bonsai database has, opens it via
     * BesuDatabaseManager in write mode, and returns a BonsaiWorldStateKeyValueStorage
     * built the same way FlatDbHealer builds its own — used only to seed fixture data
     * (trie nodes, flat entries, the worldRoot key) that FlatDbHealer will later read.
     */
    private BonsaiWorldStateKeyValueStorage openWritableFixtureDatabase() throws Exception {
        createFixtureDatabaseSchema();

        dbManager.openDatabase(tempDir.toString(), true);
        return buildWorldState(dbManager);
    }

    static BonsaiWorldStateKeyValueStorage buildWorldState(BesuDatabaseManager dbManager) {
        RocksDBSegmentedStorage storage = new RocksDBSegmentedStorage(dbManager);
        BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
            new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
        flatDbStrategyProvider.loadFlatDbStrategy(storage);
        return new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider, storage, new NoOpKeyValueStorage());
    }

    /**
     * Regression test for the bug where FlatDbHealer's constructor could never succeed
     * against a read-only-opened database: Besu's BonsaiFlatDbStrategyProvider.loadFlatDbStrategy()
     * always attempts a metadata write-back on first load (its in-memory flatDbMode field starts
     * null, so it never matches the freshly-derived mode), which calls storage.startTransaction()
     * and RocksDBSegmentedStorage correctly rejects that when the database isn't writable. This
     * made `db upgrade-flatdb --dry-run` unusable against a database opened read-only, which is
     * exactly how dry-run is meant to be used.
     *
     * <p>Crucially, the schema is created directly (never opened in write mode, never run through
     * buildWorldState/loadFlatDbStrategy first): once the flat DB mode metadata has been persisted
     * once, Besu's loadFlatDbStrategy() reads it back directly without attempting the write-back,
     * so re-using openWritableFixtureDatabase() here would not actually exercise the bug.
     */
    @Test
    void constructor_succeedsAgainstNeverBeforeLoadedReadOnlyDatabase() throws Exception {
        createFixtureDatabaseSchema();

        dbManager.openDatabase(tempDir.toString(), false);

        assertThatCode(() -> new FlatDbHealer(dbManager)).doesNotThrowAnyException();
    }

    @Test
    void getTargetStateRoot_returnsPersistedWorldRoot() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Bytes32 expectedRoot = Bytes32.fromHexString(
            "0x2222222222222222222222222222222222222222222222222222222222222222");
        BonsaiWorldStateKeyValueStorage.Updater seedUpdater = fixtureWorldState.updater();
        seedUpdater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY,
            expectedRoot.toArrayUnsafe());
        seedUpdater.commit();

        FlatDbHealer healer = new FlatDbHealer(dbManager);

        assertThat(healer.getTargetStateRoot()).isEqualTo(expectedRoot);
    }

    @Test
    void getTargetStateRoot_throwsWhenNoWorldRootPersisted() throws Exception {
        openWritableFixtureDatabase();

        FlatDbHealer healer = new FlatDbHealer(dbManager);

        assertThatThrownBy(healer::getTargetStateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No world state root found");
    }

    private static byte[] accountRlp(long nonce, Hash storageRoot) {
        return new BonsaiAccount(
                null, Address.ZERO, Hash.ZERO, nonce, Wei.ZERO, storageRoot, Hash.EMPTY, false, null)
            .serializeAccount().toArrayUnsafe();
    }

    /** Builds a real, persisted account trie with the given (accountHash -> accountRlp) leaves. */
    private static Bytes32 seedAccountTrie(
            BonsaiWorldStateKeyValueStorage worldState, Map<Hash, byte[]> accounts) {
        StoredMerklePatriciaTrie<Bytes, Bytes> trie = new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(), Function.identity(), Function.identity());
        accounts.forEach((accountHash, rlp) -> trie.put(accountHash, Bytes.wrap(rlp)));

        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        trie.commit((location, hash, node) -> updater.putAccountStateTrieNode(location, hash, node));
        Bytes32 rootHash = trie.getRootHash();
        updater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY,
            rootHash.toArrayUnsafe());
        updater.commit();
        return rootHash;
    }

    private static void seedFlatAccount(
            BonsaiWorldStateKeyValueStorage worldState, Hash accountHash, byte[] rlp) {
        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        updater.putAccountInfoState(accountHash, Bytes.wrap(rlp));
        updater.commit();
    }

    @Test
    void healAccountRange_addsMissingUpdatesStaleAndRemovesOrphan() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();

        Hash matchingHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        Hash missingHash = Hash.wrap(Bytes32.leftPad(Bytes.of(2)));
        Hash staleHash = Hash.wrap(Bytes32.leftPad(Bytes.of(3)));
        Hash orphanHash = Hash.wrap(Bytes32.leftPad(Bytes.of(4)));

        byte[] matchingRlp = accountRlp(1, Hash.EMPTY);
        byte[] missingRlp = accountRlp(2, Hash.EMPTY);
        byte[] staleRlpInTrie = accountRlp(3, Hash.EMPTY);
        byte[] staleRlpInFlat = accountRlp(999, Hash.EMPTY);
        byte[] orphanRlp = accountRlp(4, Hash.EMPTY);

        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(
            matchingHash, matchingRlp,
            missingHash, missingRlp,
            staleHash, staleRlpInTrie));

        seedFlatAccount(fixtureWorldState, matchingHash, matchingRlp);
        seedFlatAccount(fixtureWorldState, staleHash, staleRlpInFlat);
        seedFlatAccount(fixtureWorldState, orphanHash, orphanRlp);

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealer.AccountRangeOutcome outcome = healer.healAccountRange(
            stateRoot, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, false);

        assertThat(outcome.accountsChecked).isEqualTo(3);
        assertThat(outcome.added).isEqualTo(1);
        assertThat(outcome.updated).isEqualTo(1);
        assertThat(outcome.removed).isEqualTo(1);
        assertThat(outcome.divergentAccounts).containsExactlyInAnyOrder(missingHash, staleHash);

        // Re-open a fresh reader-side world state and verify the flat table now matches the trie.
        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(matchingHash)).contains(Bytes.wrap(matchingRlp));
        assertThat(verifyWorldState.getAccount(missingHash)).contains(Bytes.wrap(missingRlp));
        assertThat(verifyWorldState.getAccount(staleHash)).contains(Bytes.wrap(staleRlpInTrie));
        assertThat(verifyWorldState.getAccount(orphanHash)).isEmpty();
    }

    @Test
    void healAccountRange_dryRun_reportsWithoutWriting() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash missingHash = Hash.wrap(Bytes32.leftPad(Bytes.of(2)));
        byte[] missingRlp = accountRlp(2, Hash.EMPTY);

        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(missingHash, missingRlp));

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealer.AccountRangeOutcome outcome = healer.healAccountRange(
            stateRoot, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, true);

        assertThat(outcome.added).isEqualTo(1);

        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(missingHash)).isEmpty();
    }

    /** Builds a real, persisted storage trie for one account and returns its root. */
    private static Hash seedStorageTrie(
            BonsaiWorldStateKeyValueStorage worldState, Hash accountHash, Map<Bytes32, Bytes> slots) {
        StoredMerklePatriciaTrie<Bytes, Bytes> trie = new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(), Function.identity(), Function.identity());
        slots.forEach(trie::put);

        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        trie.commit((location, hash, node) ->
            updater.putAccountStorageTrieNode(accountHash, location, hash, node));
        Bytes32 rootHash = trie.getRootHash();
        updater.commit();
        return Hash.wrap(rootHash);
    }

    private static void seedFlatStorage(
            BonsaiWorldStateKeyValueStorage worldState, Hash accountHash, Hash slotHash, Bytes value) {
        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        updater.putStorageValueBySlotHash(accountHash, slotHash, value);
        updater.commit();
    }

    @Test
    void healAccountStorage_addsMissingUpdatesStaleAndRemovesOrphan() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));

        Hash matchingSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(10)));
        Hash missingSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(20)));
        Hash staleSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(30)));
        Hash orphanSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(40)));

        Bytes matchingValue = Bytes.of(1);
        Bytes missingValue = Bytes.of(2);
        Bytes staleValueInTrie = Bytes.of(3);
        Bytes staleValueInFlat = Bytes.of(99);
        Bytes orphanValue = Bytes.of(4);

        Hash storageRoot = seedStorageTrie(fixtureWorldState, accountHash, Map.of(
            matchingSlot, matchingValue,
            missingSlot, missingValue,
            staleSlot, staleValueInTrie));

        seedFlatStorage(fixtureWorldState, accountHash, matchingSlot, matchingValue);
        seedFlatStorage(fixtureWorldState, accountHash, staleSlot, staleValueInFlat);
        seedFlatStorage(fixtureWorldState, accountHash, orphanSlot, orphanValue);

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealer.StorageRangeOutcome outcome =
            healer.healAccountStorage(accountHash, storageRoot, false);

        assertThat(outcome.slotsChecked).isEqualTo(3);
        assertThat(outcome.added).isEqualTo(1);
        assertThat(outcome.updated).isEqualTo(1);
        assertThat(outcome.removed).isEqualTo(1);

        // Read the flat table directly (not via getStorageValueByStorageSlotKey, which derives
        // storageRoot from getAccount(accountHash) — this test never seeds a flat account entry,
        // since it's testing storage healing in isolation from account healing).
        RocksDBSegmentedStorage verifyStorage = new RocksDBSegmentedStorage(dbManager);
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, matchingSlot).toArrayUnsafe()))
            .contains(matchingValue.toArrayUnsafe());
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, missingSlot).toArrayUnsafe()))
            .contains(missingValue.toArrayUnsafe());
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, staleSlot).toArrayUnsafe()))
            .contains(staleValueInTrie.toArrayUnsafe());
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, orphanSlot).toArrayUnsafe()))
            .isEmpty();
    }

    @Test
    void heal_reconcilesAccountsAndTheirStorageThenUpgradesToFull() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();

        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        Hash missingSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(20)));
        Bytes missingSlotValue = Bytes.of(7);

        Hash storageRoot = seedStorageTrie(fixtureWorldState, accountHash, Map.of(missingSlot, missingSlotValue));
        byte[] accountRlp = accountRlp(1, storageRoot);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));
        // No flat account entry seeded at all: account is "missing", so its storage must be healed too.

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(false, FlatDbHealProgressListener.NO_OP);

        assertThat(result.accountsAdded).isEqualTo(1);
        assertThat(result.slotsAdded).isEqualTo(1);
        assertThat(result.dryRun).isFalse();

        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountHash)).contains(Bytes.wrap(accountRlp));
        assertThat(verifyWorldState.getStorageValueByStorageSlotKey(
                accountHash, new org.hyperledger.besu.datatypes.StorageSlotKey(missingSlot, Optional.empty())))
            .contains(missingSlotValue);

        Optional<byte[]> flatDbMode = new net.consensys.bric.db.SegmentReader(dbManager)
            .get(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, "flatDbStatus".getBytes());
        assertThat(flatDbMode).isPresent();
        assertThat(flatDbMode.get()[0]).isEqualTo((byte) 0x01); // FULL, per FlatDbMode encoding
    }

    @Test
    void heal_dryRun_reportsWithoutTouchingCheckpointOrData() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        byte[] accountRlp = accountRlp(1, Hash.EMPTY_TRIE_HASH);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(true, FlatDbHealProgressListener.NO_OP);

        assertThat(result.accountsAdded).isEqualTo(1);
        assertThat(result.dryRun).isTrue();

        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountHash)).isEmpty();
        assertThat(new net.consensys.bric.db.SegmentReader(dbManager)
            .get(KeyValueSegmentIdentifier.VARIABLES, "bricFlatDbHealCheckpoint".getBytes())).isEmpty();
    }

    /**
     * Regression test for the bug where heal()'s storage phase derived a divergent account's
     * storage root from the *flat* table (via worldState.getAccount(accountHash)) rather than
     * from the trie-derived value healAccountRange already computed. For an "updated" account
     * (flat entry present but stale), Besu's BonsaiPartialFlatDbStrategy.getFlatAccount() only
     * falls back to the trie when the flat entry is entirely *missing* — a present-but-stale
     * entry is returned as-is, uncorrected. In dry-run (which never writes, so the account
     * phase's fix is never durably committed before the storage phase runs), this meant
     * healAccountStorage would walk the account's *stale* storage root instead of its real one.
     * Here the stale flat RLP's storage root points at a hash with no nodes in
     * TRIE_BRANCH_STORAGE at all, so the pre-fix code throws a MerkleTrieException instead of
     * cleanly reporting the account's real storage drift.
     */
    @Test
    void heal_dryRun_usesTrieDerivedStorageRootForAccountWithStaleFlatEntry() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        Hash realSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(50)));
        Bytes realSlotValue = Bytes.of(9);

        // The account's real, persisted storage trie (per the account trie's RLP below).
        Hash realStorageRoot = seedStorageTrie(fixtureWorldState, accountHash, Map.of(realSlot, realSlotValue));

        // A storage root with no nodes in TRIE_BRANCH_STORAGE at all — stands in for the stale
        // flat RLP's storage root, which the bug would incorrectly use instead of realStorageRoot.
        Hash staleStorageRoot = Hash.wrap(Bytes32.leftPad(Bytes.of(0x77)));

        byte[] trieAccountRlp = accountRlp(1, realStorageRoot);
        byte[] staleFlatAccountRlp = accountRlp(999, staleStorageRoot);

        seedAccountTrie(fixtureWorldState, Map.of(accountHash, trieAccountRlp));
        seedFlatAccount(fixtureWorldState, accountHash, staleFlatAccountRlp);

        FlatDbHealer healer = new FlatDbHealer(dbManager);

        FlatDbHealResult[] resultHolder = new FlatDbHealResult[1];
        assertThatCode(() -> resultHolder[0] = healer.heal(true, FlatDbHealProgressListener.NO_OP))
            .doesNotThrowAnyException();

        FlatDbHealResult result = resultHolder[0];
        assertThat(result.accountsUpdated).isEqualTo(1);
        // Must reflect the account's real storage drift (realSlot, found via the correct
        // trie-derived storage root) rather than crashing or reporting against the stale root.
        assertThat(result.slotsAdded).isEqualTo(1);

        // Dry-run: nothing actually written.
        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountHash)).contains(Bytes.wrap(staleFlatAccountRlp));
    }

    @Test
    void heal_resumesFromCheckpointAfterSimulatedInterruption() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        byte[] accountRlp = accountRlp(1, Hash.EMPTY_TRIE_HASH);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));

        // Simulate an interruption after range 0 of 16 completed by writing the checkpoint directly,
        // without ever writing accountHash's flat entry (as if the process died mid-range-1).
        BonsaiWorldStateKeyValueStorage.Updater updater = fixtureWorldState.updater();
        updater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.VARIABLES, "bricFlatDbHealCheckpoint".getBytes(),
            new FlatDbHealCheckpoint(stateRoot, FlatDbHealCheckpoint.Phase.ACCOUNTS, 1).encode());
        updater.commit();

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(false, FlatDbHealProgressListener.NO_OP);

        // accountHash falls in range 0 (its hash starts with 0x00...01, the very first range),
        // which the simulated checkpoint marks already-done, so it must NOT be healed.
        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountHash)).isEmpty();
        assertThat(result.accountsAdded).isEqualTo(0);
    }

    @Test
    void heal_doesNotLosePendingStorageAccountsFromRangesBeforeASimulatedInterruption() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();

        // Pick accountA's hash to be the start of range 0 and accountB's hash to be the start of
        // range 1 (per RangeManager.generateAllRanges(16)), so they fall into different
        // account-range-loop iterations of heal().
        List<Map.Entry<Bytes32, Bytes32>> ranges =
            new ArrayList<>(RangeManager.generateAllRanges(16).entrySet());
        Hash accountAHash = Hash.wrap(ranges.get(0).getKey());
        Hash accountBHash = Hash.wrap(ranges.get(1).getKey());

        Bytes accountASlotValue = Bytes.of(11);
        Bytes accountBSlotValue = Bytes.of(22);
        Hash accountASlot = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        Hash accountBSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(2)));

        Hash accountAStorageRoot = seedStorageTrie(
            fixtureWorldState, accountAHash, Map.of(accountASlot, accountASlotValue));
        Hash accountBStorageRoot = seedStorageTrie(
            fixtureWorldState, accountBHash, Map.of(accountBSlot, accountBSlotValue));

        byte[] accountARlp = accountRlp(1, accountAStorageRoot);
        byte[] accountBRlp = accountRlp(2, accountBStorageRoot);
        seedAccountTrie(fixtureWorldState, Map.of(accountAHash, accountARlp, accountBHash, accountBRlp));
        // Neither account's flat entry is seeded, so both are "missing" and thus divergent —
        // each one's storage must be healed too.

        // Simulate a process kill partway through the accounts phase: abort while processing
        // range index 2 (rangeIndex == 3, 1-indexed) — i.e. only *after* both range 0 (accountA)
        // and range 1 (accountB) have each fully completed, including their own per-range
        // checkpoint + pending-storage-accounts writes. Range 2 itself has no seeded accounts, so
        // interrupting mid-range-2 doesn't confound the result with a partially-processed range.
        FlatDbHealProgressListener interruptingListener = new FlatDbHealProgressListener() {
            @Override
            public void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed) {
                if (rangeIndex == 3) {
                    throw new SimulatedInterruption();
                }
            }

            @Override
            public void onStorageAccountComplete(
                    int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed) {
            }
        };

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        assertThatThrownBy(() -> healer.heal(false, interruptingListener))
            .isInstanceOf(SimulatedInterruption.class);

        // After the simulated crash, both accounts' flat entries are already durably fixed (their
        // ranges' healAccountRange writes are unconditional and both ranges fully completed and
        // checkpointed before the interruption), so the "resume" checkpoint reflects range 2 as
        // the next range to process — ranges 0 and 1 will NOT be reprocessed.
        BonsaiWorldStateKeyValueStorage afterCrashWorldState = buildWorldState(dbManager);
        assertThat(afterCrashWorldState.getAccount(accountAHash)).contains(Bytes.wrap(accountARlp));
        assertThat(afterCrashWorldState.getAccount(accountBHash)).contains(Bytes.wrap(accountBRlp));

        // Resume to completion.
        FlatDbHealer resumedHealer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = resumedHealer.heal(false, FlatDbHealProgressListener.NO_OP);
        assertThat(result.dryRun).isFalse();

        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountAHash)).contains(Bytes.wrap(accountARlp));
        assertThat(verifyWorldState.getAccount(accountBHash)).contains(Bytes.wrap(accountBRlp));

        // The key assertion: accountA's storage slot — divergent in the range that was already
        // checkpointed before the simulated interruption — must still get healed on resume,
        // not silently dropped forever.
        RocksDBSegmentedStorage verifyStorage = new RocksDBSegmentedStorage(dbManager);
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountAHash, accountASlot).toArrayUnsafe()))
            .contains(accountASlotValue.toArrayUnsafe());
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountBHash, accountBSlot).toArrayUnsafe()))
            .contains(accountBSlotValue.toArrayUnsafe());
    }

    /** Marker exception used only to simulate an interrupted heal() run without failing the test. */
    private static final class SimulatedInterruption extends RuntimeException {
    }

    @Test
    void heal_discardsStaleCheckpointWhenStateRootHasMoved() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        byte[] accountRlp = accountRlp(1, Hash.EMPTY_TRIE_HASH);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));

        Bytes32 staleRoot = Bytes32.leftPad(Bytes.of(0x7f));
        BonsaiWorldStateKeyValueStorage.Updater updater = fixtureWorldState.updater();
        updater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.VARIABLES, "bricFlatDbHealCheckpoint".getBytes(),
            new FlatDbHealCheckpoint(staleRoot, FlatDbHealCheckpoint.Phase.ACCOUNTS, 1).encode());
        updater.commit();

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(false, FlatDbHealProgressListener.NO_OP);

        // Stale checkpoint discarded, so the full walk runs and finds accountHash missing.
        assertThat(result.accountsAdded).isEqualTo(1);
    }
}
