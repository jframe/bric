package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DbPutCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbPutCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbPutCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testNoDatabaseOpen() {
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "0x01"});

        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void testReadOnlyDatabase() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(false);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "0x01"});

        assertThat(errorStream.toString()).contains("Error: Database is open in read-only mode");
        assertThat(errorStream.toString()).contains("--write");
    }

    @Test
    void testMissingArgs() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        command.execute(new String[]{});

        assertThat(errorStream.toString()).contains("Error: Missing arguments");
    }

    @Test
    void testMissingValue() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef"});

        assertThat(errorStream.toString()).contains("Error: Missing arguments");
    }

    @Test
    void testUnknownSegment() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        command.execute(new String[]{"NONEXISTENT_SEGMENT", "0xdeadbeef", "0x01"});

        assertThat(errorStream.toString()).contains("Error: Unknown segment 'NONEXISTENT_SEGMENT'");
    }

    @Test
    void testSegmentNotInDatabase() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("BLOCKCHAIN"));

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "0x01"});

        assertThat(errorStream.toString()).contains("Error: Segment 'ACCOUNT_INFO_STATE' is not present in this database");
    }

    @Test
    void testInvalidHexKey() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "notvalidhex", "0x01"});

        assertThat(errorStream.toString()).contains("Error: Invalid hex key");
    }

    @Test
    void testInvalidHexValue() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "notvalidhex"});

        assertThat(errorStream.toString()).contains("Error: Invalid hex value");
    }

    @Test
    void testSuccessfulPut() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "0x010203"});

        verify(mockDbManager).put(eq(mockHandle), eq(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}),
                                                  eq(new byte[]{0x01, 0x02, 0x03}));
        String output = outputStream.toString();
        assertThat(output).contains("OK");
        assertThat(output).contains("0xdeadbeef");
        assertThat(output).contains("0x010203");
        assertThat(output).contains("4 bytes");
        assertThat(output).contains("3 bytes");
    }

    @Test
    void testPutWithRocksDBException() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        doThrow(new RocksDBException("disk full")).when(mockDbManager).put(any(), any(), any());

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "0x01"});

        assertThat(errorStream.toString()).contains("Error writing to database");
    }

    @Test
    void testSegmentNameCaseInsensitive() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);

        command.execute(new String[]{"account_info_state", "0xdeadbeef", "0x01"});

        assertThat(outputStream.toString()).contains("OK");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("write");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db put");
        assertThat(usage).contains("<segment>");
        assertThat(usage).contains("<hex-key>");
        assertThat(usage).contains("<hex-value>");
    }
}
