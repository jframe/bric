package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes32;
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
}
