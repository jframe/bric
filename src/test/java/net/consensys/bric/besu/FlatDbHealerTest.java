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
     * Creates the column families a real Bonsai database has, opens it via
     * BesuDatabaseManager in write mode, and returns a BonsaiWorldStateKeyValueStorage
     * built the same way FlatDbHealer builds its own — used only to seed fixture data
     * (trie nodes, flat entries, the worldRoot key) that FlatDbHealer will later read.
     */
    private BonsaiWorldStateKeyValueStorage openWritableFixtureDatabase() throws Exception {
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
}
