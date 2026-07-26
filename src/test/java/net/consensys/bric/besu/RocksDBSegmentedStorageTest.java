package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
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

class RocksDBSegmentedStorageTest {

    @TempDir
    Path tempDir;

    private BesuDatabaseManager dbManager;
    private RocksDBSegmentedStorage storage;

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

    private void createTestDatabase() throws Exception {
        List<ColumnFamilyDescriptor> descriptors = List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()));
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

    @Test
    void startTransaction_putAndCommit_persistsValue() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        transaction.commit();
        transaction.close();

        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes()))
            .contains("value".getBytes());
    }

    @Test
    void startTransaction_putAndRollback_doesNotPersist() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        transaction.rollback();
        transaction.close();

        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes())).isEmpty();
    }

    @Test
    void startTransaction_removeAndCommit_deletesValue() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction seed = storage.startTransaction();
        seed.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        seed.commit();
        seed.close();

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.remove(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes());
        transaction.commit();
        transaction.close();

        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes())).isEmpty();
    }

    @Test
    void startTransaction_throwsWhenReadOnly() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), false);
        storage = new RocksDBSegmentedStorage(dbManager);

        assertThatThrownBy(() -> storage.startTransaction())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("read-only");
    }

    @Test
    void tryDelete_removesKeyWhenWritable() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction seed = storage.startTransaction();
        seed.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        seed.commit();
        seed.close();

        boolean deleted = storage.tryDelete(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes());

        assertThat(deleted).isTrue();
        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes())).isEmpty();
    }

    @Test
    void tryDelete_throwsWhenReadOnly() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), false);
        storage = new RocksDBSegmentedStorage(dbManager);

        assertThatThrownBy(() -> storage.tryDelete(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes()))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("read-only");
    }

    @Test
    void streamFromKey_returnsOnlyEntriesWithinRange() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x01}, "a".getBytes());
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x05}, "b".getBytes());
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x09}, "c".getBytes());
        transaction.commit();
        transaction.close();

        List<Pair<byte[], byte[]>> results = storage.streamFromKey(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x02}, new byte[]{0x08}).toList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).getLeft()).isEqualTo(new byte[]{0x05});
        assertThat(results.get(0).getRight()).isEqualTo("b".getBytes());
    }

    @Test
    void transaction_put_throwsStorageExceptionForUnknownSegment() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();

        assertThatThrownBy(() -> transaction.put(
                KeyValueSegmentIdentifier.CODE_STORAGE, "key".getBytes(), "value".getBytes()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("CODE_STORAGE");

        transaction.close();
    }

    @Test
    void transaction_remove_throwsStorageExceptionForUnknownSegment() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();

        assertThatThrownBy(() -> transaction.remove(KeyValueSegmentIdentifier.CODE_STORAGE, "key".getBytes()))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("CODE_STORAGE");

        transaction.close();
    }

    @Test
    void streamFromKey_returnsEmptyForUnknownSegment() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), false);
        storage = new RocksDBSegmentedStorage(dbManager);

        List<Pair<byte[], byte[]>> results = storage.streamFromKey(
            KeyValueSegmentIdentifier.TRIE_LOG_STORAGE, new byte[]{0x00}, new byte[]{(byte) 0xff}).toList();

        assertThat(results).isEmpty();
    }
}
