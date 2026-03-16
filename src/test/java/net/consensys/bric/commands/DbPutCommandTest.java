package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DbPutCommandTest {

    private BesuDatabaseManager mockDbManager;
    private RocksDB mockRocksDB;
    private DbPutCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        mockRocksDB = Mockito.mock(RocksDB.class);
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

        assertThat(errorStream.toString()).contains("Error: Missing segment, key, and/or value");
    }

    @Test
    void testMissingValue() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef"});

        assertThat(errorStream.toString()).contains("Error: Missing segment, key, and/or value");
    }

    @Test
    void testColumnFamilyNotFound() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "NONEXISTENT"))
                    .thenReturn(null);

            command.execute(new String[]{"NONEXISTENT", "0xdeadbeef", "0x01"});

            assertThat(errorStream.toString()).contains("Error: Column family not found");
        }
    }

    @Test
    void testInvalidHexKey() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "ACCOUNT_INFO_STATE"))
                    .thenReturn(mockHandle);
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("notvalidhex"))
                    .thenThrow(new IllegalArgumentException("invalid hex characters"));

            command.execute(new String[]{"ACCOUNT_INFO_STATE", "notvalidhex", "0x01"});

            assertThat(errorStream.toString()).contains("Error: Invalid key or value format");
        }
    }

    @Test
    void testInvalidHexValue() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "ACCOUNT_INFO_STATE"))
                    .thenReturn(mockHandle);
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xdeadbeef"))
                    .thenReturn(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef});
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("notvalidhex"))
                    .thenThrow(new IllegalArgumentException("invalid hex characters"));

            command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "notvalidhex"});

            assertThat(errorStream.toString()).contains("Error: Invalid key or value format");
        }
    }

    @Test
    void testSuccessfulPut() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDB);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "ACCOUNT_INFO_STATE"))
                    .thenReturn(mockHandle);
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xdeadbeef"))
                    .thenReturn(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef});
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0x010203"))
                    .thenReturn(new byte[]{0x01, 0x02, 0x03});

            command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "0x010203"});

            verify(mockRocksDB).put(eq(mockHandle), eq(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}),
                                                       eq(new byte[]{0x01, 0x02, 0x03}));
            String output = outputStream.toString();
            assertThat(output).contains("Wrote to column family: ACCOUNT_INFO_STATE");
            assertThat(output).contains("0xdeadbeef");
            assertThat(output).contains("0x010203");
        }
    }

    @Test
    void testPutWithRocksDBException() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDB);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        doThrow(new RocksDBException("disk full")).when(mockRocksDB).put(
                any(ColumnFamilyHandle.class), any(byte[].class), any(byte[].class));

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "ACCOUNT_INFO_STATE"))
                    .thenReturn(mockHandle);
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xdeadbeef"))
                    .thenReturn(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef});
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0x01"))
                    .thenReturn(new byte[]{0x01});

            command.execute(new String[]{"ACCOUNT_INFO_STATE", "0xdeadbeef", "0x01"});

            assertThat(errorStream.toString()).contains("Error writing to database");
        }
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
        assertThat(usage).contains("segment|name|hex");
    }

    @Test
    void testPutToArbitraryUTF8CF() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDB);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "CUSTOM_CF"))
                    .thenReturn(mockHandle);
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xabcd"))
                    .thenReturn(new byte[]{(byte)0xab, (byte)0xcd});
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xdeadbeef"))
                    .thenReturn(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef});

            command.execute(new String[]{"CUSTOM_CF", "0xabcd", "0xdeadbeef"});

            verify(mockRocksDB).put(eq(mockHandle), eq(new byte[]{(byte)0xab, (byte)0xcd}),
                                                       eq(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}));
            assertThat(outputStream.toString()).contains("Wrote to column family: CUSTOM_CF");
            assertThat(outputStream.toString()).contains("0xabcd");
            assertThat(outputStream.toString()).contains("0xdeadbeef");
        }
    }

    @Test
    void testPutToArbitraryHexCF() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDB);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "0x0a"))
                    .thenReturn(mockHandle);
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xabcd"))
                    .thenReturn(new byte[]{(byte)0xab, (byte)0xcd});
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xdeadbeef"))
                    .thenReturn(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef});

            command.execute(new String[]{"0x0a", "0xabcd", "0xdeadbeef"});

            verify(mockRocksDB).put(eq(mockHandle), eq(new byte[]{(byte)0xab, (byte)0xcd}),
                                                       eq(new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}));
            assertThat(outputStream.toString()).contains("Wrote to column family: 0x0a");
        }
    }

    @Test
    void testPutReadOnlyError() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(false);

        command.execute(new String[]{"CUSTOM_CF", "0xabcd", "0xdeadbeef"});

        assertThat(errorStream.toString()).contains("Error: Database is open in read-only mode");
        assertThat(errorStream.toString()).contains("--write");
    }

    @Test
    void testPutInvalidKeyOrValueHex() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);

        try (MockedStatic<net.consensys.bric.db.ColumnFamilyResolver> mockedResolver =
                mockStatic(net.consensys.bric.db.ColumnFamilyResolver.class)) {
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.resolveColumnFamily(mockDbManager, "CUSTOM_CF"))
                    .thenReturn(mockHandle);
            mockedResolver.when(() -> net.consensys.bric.db.ColumnFamilyResolver.parseInput("0xZZ"))
                    .thenThrow(new IllegalArgumentException("invalid hex characters in input: ZZ"));

            command.execute(new String[]{"CUSTOM_CF", "0xZZ", "0xdeadbeef"});

            assertThat(errorStream.toString()).contains("Error: Invalid key or value format");
        }
    }
}
