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

        assertThat(errorStream.toString()).contains("Error: Missing arguments");
    }

    @Test
    void testMissingKey() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"ACCOUNT_INFO_STATE"});

        assertThat(errorStream.toString()).contains("Error: Missing arguments");
    }

    @Test
    void testUnknownSegment() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"NONEXISTENT_SEGMENT", "0xabcd"});

        assertThat(errorStream.toString()).contains("Error: Unknown segment 'NONEXISTENT_SEGMENT'");
    }

    @Test
    void testSegmentNotInDatabase() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("BLOCKCHAIN"));

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xabcd"});

        assertThat(errorStream.toString()).contains("Error: Segment 'ACCOUNT_INFO_STATE' is not present in this database");
    }

    @Test
    void testInvalidHexKey() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "notvalidhex"});

        assertThat(errorStream.toString()).contains("Error: Invalid hex key");
    }

    @Test
    void testKeyNotFound() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(null);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef"});

        assertThat(outputStream.toString()).contains("Not found");
    }

    @Test
    void testKeyFound() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{0x01, 0x02, 0x03});

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef"});

        String output = outputStream.toString();
        assertThat(output).contains("Key");
        assertThat(output).contains("Value");
        assertThat(output).contains("0x010203");
        assertThat(output).contains("3 bytes");
    }

    @Test
    void testSegmentNameCaseInsensitive() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{0x01});

        command.execute(new String[]{"account_info_state", "0xdeadbeef"});

        assertThat(outputStream.toString()).contains("Value");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testKeyWithoutOxPrefix() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.get(eq(mockHandle), any(byte[].class))).thenReturn(new byte[]{0x42});

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "deadbeef"});

        assertThat(outputStream.toString()).contains("Value");
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
        assertThat(usage).contains("<segment>");
        assertThat(usage).contains("<hex-key>");
    }
}
