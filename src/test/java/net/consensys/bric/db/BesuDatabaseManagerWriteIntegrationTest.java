package net.consensys.bric.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for BesuDatabaseManager write mode using a real RocksDB instance.
 */
class BesuDatabaseManagerWriteIntegrationTest {

    @TempDir
    Path tempDir;

    private BesuDatabaseManager manager;

    @BeforeEach
    void setUp() {
        manager = new BesuDatabaseManager();
    }

    @AfterEach
    void tearDown() {
        if (manager.isOpen()) {
            manager.closeDatabase();
        }
    }

    /**
     * Create a minimal RocksDB database with the default column family and
     * one custom column family using non-default options (to simulate Besu's setup),
     * then close it so tests can reopen it.
     */
    private String createTestDatabase() throws Exception {
        RocksDB.loadLibrary();
        String path = tempDir.toString();

        // Use non-default ColumnFamilyOptions to simulate Besu's configuration
        ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
            .setCompressionType(CompressionType.LZ4_COMPRESSION)
            .setWriteBufferSize(64 * 1024 * 1024);

        List<ColumnFamilyDescriptor> cfDescriptors = List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
            new ColumnFamilyDescriptor("ACCOUNT_INFO_STATE".getBytes(), cfOpts)
        );
        List<ColumnFamilyHandle> handles = new ArrayList<>();

        try (DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);
             RocksDB db = RocksDB.open(options, path, cfDescriptors, handles)) {
            // Write a seed value so the DB is non-empty
            db.put(handles.get(1), "seedkey".getBytes(), "seedvalue".getBytes());
            for (ColumnFamilyHandle h : handles) h.close();
        }

        cfOpts.close();
        return path;
    }

    @Test
    void openInWriteMode_succeeds() throws Exception {
        String path = createTestDatabase();

        manager.openDatabase(path, true);

        assertThat(manager.isOpen()).isTrue();
        assertThat(manager.isWritable()).isTrue();
    }

    @Test
    void openInReadOnlyMode_isNotWritable() throws Exception {
        String path = createTestDatabase();

        manager.openDatabase(path, false);

        assertThat(manager.isOpen()).isTrue();
        assertThat(manager.isWritable()).isFalse();
    }

    @Test
    void put_writesValueAndGetReadsItBack() throws Exception {
        String path = createTestDatabase();
        manager.openDatabase(path, true);

        ColumnFamilyHandle handle = manager.getColumnFamilyByName("ACCOUNT_INFO_STATE");
        byte[] key = "testkey".getBytes();
        byte[] value = "testvalue".getBytes();

        manager.put(handle, key, value);

        byte[] result = manager.getDatabase().get(handle, key);
        assertThat(result).isEqualTo(value);
    }

    @Test
    void put_throwsWhenOpenedReadOnly() throws Exception {
        String path = createTestDatabase();
        manager.openDatabase(path, false);

        ColumnFamilyHandle handle = manager.getColumnFamilyByName("ACCOUNT_INFO_STATE");

        assertThatThrownBy(() -> manager.put(handle, "key".getBytes(), "value".getBytes()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("read-only");
    }

    @Test
    void put_throwsWhenDatabaseNotOpen() throws Exception {
        assertThatThrownBy(() -> manager.put(null, "key".getBytes(), "value".getBytes()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No database is open");
    }

    @Test
    void closeAndReopenInWriteMode_retainsWrittenValue() throws Exception {
        String path = createTestDatabase();
        byte[] key = "persistkey".getBytes();
        byte[] value = "persistvalue".getBytes();

        // Write
        manager.openDatabase(path, true);
        ColumnFamilyHandle handle = manager.getColumnFamilyByName("ACCOUNT_INFO_STATE");
        manager.put(handle, key, value);
        manager.closeDatabase();

        // Reopen read-only and verify value persisted
        manager.openDatabase(path, false);
        ColumnFamilyHandle readHandle = manager.getColumnFamilyByName("ACCOUNT_INFO_STATE");
        byte[] result = manager.getDatabase().get(readHandle, key);
        assertThat(result).isEqualTo(value);
    }
}
