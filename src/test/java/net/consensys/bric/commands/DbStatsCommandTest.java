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
import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class DbStatsCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbStatsCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbStatsCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testNoDatabaseOpen() {
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{});

        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void testDbLevelSummaryShowsOpenFileCount() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getMaxOpenFiles()).thenReturn(5000);

        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.getLiveFilesMetaData()).thenReturn(Collections.emptyList());
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Collections.emptySet());

        command.execute(new String[]{});

        assertThat(outputStream.toString()).contains("Open SST files: 0 / 5000");
    }

    @Test
    void testDbLevelSummaryShowsUnlimitedWhenMinusOne() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getMaxOpenFiles()).thenReturn(-1);

        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.getLiveFilesMetaData()).thenReturn(Collections.emptyList());
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Collections.emptySet());

        command.execute(new String[]{});

        assertThat(outputStream.toString()).contains("/ unlimited");
    }

    @Test
    void testSingleCfModePrintsAllSections() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getMaxOpenFiles()).thenReturn(-1);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);

        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE))
            .thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.getLiveFilesMetaData()).thenReturn(Collections.emptyList());
        when(mockRocksDb.getProperty(eq(mockHandle), eq("rocksdb.stats")))
            .thenReturn("compaction stats here");
        when(mockRocksDb.getProperty(eq(mockHandle), eq("rocksdb.levelstats")))
            .thenReturn("level stats here");
        when(mockRocksDb.getProperty(eq(mockHandle), eq("rocksdb.sstables")))
            .thenReturn("sstable listing here");

        command.execute(new String[]{"ACCOUNT_INFO_STATE"});

        String output = outputStream.toString();
        assertThat(output).contains("Column Family: ACCOUNT_INFO_STATE");
        assertThat(output).contains("compaction stats here");
        assertThat(output).contains("level stats here");
        assertThat(output).contains("sstable listing here");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testUnknownCfNamePrintsErrorWithAvailableCfs() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getMaxOpenFiles()).thenReturn(-1);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.getLiveFilesMetaData()).thenReturn(Collections.emptyList());

        command.execute(new String[]{"NONEXISTENT_CF"});

        assertThat(errorStream.toString()).contains("Error: Column family not found");
        assertThat(errorStream.toString()).contains("ACCOUNT_INFO_STATE");
    }

    @Test
    void testAllCfsModeSkipsEmptyCfs() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getMaxOpenFiles()).thenReturn(-1);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.getLiveFilesMetaData()).thenReturn(Collections.emptyList());
        when(mockDbManager.getColumnFamilyNames())
            .thenReturn(Set.of("ACCOUNT_INFO_STATE", "EMPTY_CF"));

        BesuDatabaseManager.DatabaseStats statsWithData = new BesuDatabaseManager.DatabaseStats();
        statsWithData.estimatedKeys = 1000;
        statsWithData.totalSstSize = 1024;
        statsWithData.totalBlobSize = 0;
        when(mockDbManager.getStats("ACCOUNT_INFO_STATE")).thenReturn(statsWithData);
        when(mockDbManager.getColumnFamilyByName("ACCOUNT_INFO_STATE")).thenReturn(mockHandle);
        when(mockRocksDb.getProperty(eq(mockHandle), anyString())).thenReturn("stats");

        BesuDatabaseManager.DatabaseStats emptyStats = new BesuDatabaseManager.DatabaseStats();
        emptyStats.estimatedKeys = 0;
        emptyStats.totalSstSize = 0;
        emptyStats.totalBlobSize = 0;
        when(mockDbManager.getStats("EMPTY_CF")).thenReturn(emptyStats);

        command.execute(new String[]{});

        String output = outputStream.toString();
        assertThat(output).contains("Column Family: ACCOUNT_INFO_STATE");
        assertThat(output).doesNotContain("Column Family: EMPTY_CF");
    }

    @Test
    void testNullPropertySkipsSection() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getMaxOpenFiles()).thenReturn(-1);

        ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
        RocksDB mockRocksDb = mock(RocksDB.class);

        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE))
            .thenReturn(mockHandle);
        when(mockDbManager.getDatabase()).thenReturn(mockRocksDb);
        when(mockRocksDb.getLiveFilesMetaData()).thenReturn(Collections.emptyList());
        when(mockRocksDb.getProperty(eq(mockHandle), eq("rocksdb.stats"))).thenReturn(null);
        when(mockRocksDb.getProperty(eq(mockHandle), eq("rocksdb.levelstats"))).thenReturn(null);
        when(mockRocksDb.getProperty(eq(mockHandle), eq("rocksdb.sstables"))).thenReturn(null);

        command.execute(new String[]{"ACCOUNT_INFO_STATE"});

        assertThat(outputStream.toString()).contains("Column Family: ACCOUNT_INFO_STATE");
        assertThat(errorStream.toString()).doesNotContain("Exception");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("stats");
    }

    @Test
    void testGetUsage() {
        assertThat(command.getUsage()).contains("db stats");
    }
}
