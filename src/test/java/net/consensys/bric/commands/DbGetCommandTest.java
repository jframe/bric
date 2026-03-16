package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DbGetCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbGetCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbGetCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testNoDatabaseOpen() {
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xabcd"});

        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void testMissingArgs() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{});

        assertThat(errorStream.toString()).contains("Error: Missing segment and/or key");
    }

    @Test
    void testMissingKey() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"ACCOUNT_INFO_STATE"});

        assertThat(errorStream.toString()).contains("Error: Missing segment and/or key");
    }

    @Test
    void testUnknownSegment() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"NONEXISTENT_SEGMENT", "0xabcd"});

        assertThat(errorStream.toString()).contains("Error: Column family not found");
    }

    @Test
    void testSegmentNotInDatabase() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"NONEXISTENT", "0xabcd"});

        assertThat(errorStream.toString()).contains("Error: Column family not found");
    }

    @Test
    void testInvalidHexKey() {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("ACCOUNT_INFO_STATE")).thenReturn(mockHandle);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xZZ"});

        assertThat(errorStream.toString()).contains("Error: Invalid key format");
    }

    @Test
    void testKeyNotFound() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(null);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef"});

        assertThat(outputStream.toString()).contains("Key not found");
    }

    @Test
    void testKeyFound() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{0x01, 0x02, 0x03});

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef"});

        String output = outputStream.toString();
        assertThat(output).contains("Key:");
        assertThat(output).contains("Value:");
        assertThat(output).contains("0x010203");
    }

    @Test
    void testSegmentNameCaseInsensitive() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{0x01});

        command.execute(new String[]{"account_info_state", "0xdeadbeef"});

        assertThat(outputStream.toString()).contains("Value:");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testKeyWithoutOxPrefix() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{0x42});

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "deadbeef"});

        assertThat(outputStream.toString()).contains("Value:");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("raw");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db get");
        assertThat(usage).contains("<segment|name|hex>");
        assertThat(usage).contains("<key-hex>");
    }

    @Test
    void testGetFromArbitraryUTF8CF() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{(byte) 0xaa, (byte) 0xbb, (byte) 0xcc});

        command.execute(new String[]{"CUSTOM_CF", "0xabcdef"});

        String output = outputStream.toString();
        assertThat(output).contains("Key:");
        assertThat(output).contains("Value:");
        assertThat(output).contains("0xaabbcc");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testGetFromArbitraryHexCF() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);

        KeyValueSegmentIdentifier segment = KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;
        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(segment)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{(byte) 0x11, (byte) 0x22, (byte) 0x33});

        // Use hex representation of TRIE_LOG_STORAGE ID (0x0a)
        command.execute(new String[]{"0x0a", "0xdeadbeef"});

        String output = outputStream.toString();
        assertThat(output).contains("Key:");
        assertThat(output).contains("Value:");
        assertThat(output).contains("0x112233");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testGetKeyNotFoundWithArbitraryName() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(null);

        command.execute(new String[]{"CUSTOM_CF", "0x0102"});

        assertThat(outputStream.toString()).contains("Key not found");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testGetInvalidKeyHex() {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xZZ"});

        assertThat(errorStream.toString()).contains("Error").contains("Invalid key format");
    }
}
