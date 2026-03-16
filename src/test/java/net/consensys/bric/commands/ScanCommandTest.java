package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ScanCommandTest {

    private BesuDatabaseManager mockDbManager;
    private RocksDB mockRocksDb;
    private ScanCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        mockRocksDb = Mockito.mock(RocksDB.class);
        command = new ScanCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testNoDatabaseOpen() {
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{"ACCOUNT_INFO_STATE"});

        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void testMissingSegment() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{});

        assertThat(errorStream.toString()).contains("Error: Missing segment");
    }

    @Test
    void testScanArbitraryUTF8CF() {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksIterator mockIterator = mock(RocksIterator.class);

        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockDbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
        when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);
        when(mockIterator.isValid()).thenReturn(false); // No entries

        command.execute(new String[]{"CUSTOM_CF"});

        verify(mockRocksDb).newIterator(mockHandle);
        verify(mockIterator).seekToFirst();
        String output = outputStream.toString();
        assertThat(output).contains("(no entries found)");
    }

    @Test
    void testScanArbitraryHexCF() {
        when(mockDbManager.isOpen()).thenReturn(true);

        KeyValueSegmentIdentifier segment = KeyValueSegmentIdentifier.TRIE_LOG_STORAGE;
        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksIterator mockIterator = mock(RocksIterator.class);

        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockDbManager.getColumnFamily(segment)).thenReturn(mockHandle);
        when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);
        when(mockIterator.isValid())
            .thenReturn(true)
            .thenReturn(false); // One entry then done
        when(mockIterator.key()).thenReturn(new byte[]{(byte) 0xaa, (byte) 0xbb});
        when(mockIterator.value()).thenReturn(new byte[]{(byte) 0xcc, (byte) 0xdd});

        command.execute(new String[]{"0x0a"}); // Hex for TRIE_LOG_STORAGE

        verify(mockRocksDb).newIterator(mockHandle);
        String output = outputStream.toString();
        assertThat(output).contains("0xaabb");
        assertThat(output).contains("0xccdd");
    }

    @Test
    void testScanWithLimitOption() {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksIterator mockIterator = mock(RocksIterator.class);

        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);

        // Return 3 entries, but limit is 2
        when(mockIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(true); // Will reach limit before this
        when(mockIterator.key()).thenReturn(new byte[]{0x01});
        when(mockIterator.value()).thenReturn(new byte[]{0x02});

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "--limit", "2"});

        verify(mockIterator, times(4)).isValid(); // seekToFirst, then 2x next loop iteration + check for 3rd
        String output = outputStream.toString();
        assertThat(output).contains("Limited to 2 entries");
    }

    @Test
    void testScanEmptyColumnFamily() {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksIterator mockIterator = mock(RocksIterator.class);

        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.CODE_STORAGE)).thenReturn(mockHandle);
        when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);
        when(mockIterator.isValid()).thenReturn(false); // Empty

        command.execute(new String[]{"CODE_STORAGE"});

        String output = outputStream.toString();
        assertThat(output).contains("(no entries found)");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testScanInvalidLimitValue() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "--limit", "notanumber"});

        assertThat(errorStream.toString()).contains("Error: Invalid limit value");
    }

    @Test
    void testScanLimitMustBePositive() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "--limit", "0"});

        assertThat(errorStream.toString()).contains("Error: Invalid limit value");
    }

    @Test
    void testScanWithMultipleEntries() {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksIterator mockIterator = mock(RocksIterator.class);

        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE)).thenReturn(mockHandle);
        when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);

        when(mockIterator.isValid())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false); // Two entries
        when(mockIterator.key())
            .thenReturn(new byte[]{0x11})
            .thenReturn(new byte[]{0x22});
        when(mockIterator.value())
            .thenReturn(new byte[]{0x33})
            .thenReturn(new byte[]{0x44});

        command.execute(new String[]{"ACCOUNT_INFO_STATE"});

        String output = outputStream.toString();
        assertThat(output).contains("0x11");
        assertThat(output).contains("0x33");
        assertThat(output).contains("0x22");
        assertThat(output).contains("0x44");
        assertThat(output).contains("Displayed 2 entries");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("scan");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db scan");
        assertThat(usage).contains("<segment|name|hex>");
        assertThat(usage).contains("--limit");
    }

    @Test
    void testUnknownSegment() {
        when(mockDbManager.isOpen()).thenReturn(true);

        command.execute(new String[]{"NONEXISTENT_SEGMENT"});

        assertThat(errorStream.toString()).contains("Error");
    }

    @Test
    void testScanWithEnumSegmentName() {
        when(mockDbManager.isOpen()).thenReturn(true);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksIterator mockIterator = mock(RocksIterator.class);

        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE)).thenReturn(mockHandle);
        when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);
        when(mockIterator.isValid()).thenReturn(false);

        command.execute(new String[]{"TRIE_LOG_STORAGE"});

        verify(mockRocksDb).newIterator(mockHandle);
        assertThat(errorStream.toString()).doesNotContain("Error");
    }
}
