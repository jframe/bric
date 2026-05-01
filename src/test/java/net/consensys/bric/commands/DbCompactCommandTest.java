package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.CompactionJobManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DbCompactCommandTest {

    private BesuDatabaseManager mockDbManager;
    private CompactionJobManager mockJobManager;
    private DbCompactCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = mock(BesuDatabaseManager.class);
        mockJobManager = mock(CompactionJobManager.class);
        when(mockDbManager.getCompactionJobManager()).thenReturn(mockJobManager);
        command = new DbCompactCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void refusesWhenDbClosed() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{"ACCOUNT_INFO_STATE"});
        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void refusesWhenReadOnly() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{"ACCOUNT_INFO_STATE"});
        assertThat(errorStream.toString()).contains("read-only mode");
    }

    @Test
    void refusesWhenNoArgs() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("Error: Missing");
        assertThat(errorStream.toString()).contains("Usage:");
    }

    @Test
    void submitsOneJobPerCfAndPrintsIds() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        ColumnFamilyHandle h1 = mock(ColumnFamilyHandle.class);
        ColumnFamilyHandle h2 = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("ACCOUNT_INFO_STATE")).thenReturn(h1);
        when(mockDbManager.getColumnFamilyByName("CODE_STORAGE")).thenReturn(h2);
        // resolveCfName iterates this set looking up handles by name.
        when(mockDbManager.getColumnFamilyNames())
            .thenReturn(Set.of("ACCOUNT_INFO_STATE", "CODE_STORAGE"));
        when(mockJobManager.submit("ACCOUNT_INFO_STATE", h1)).thenReturn(1);
        when(mockJobManager.submit("CODE_STORAGE", h2)).thenReturn(2);

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "CODE_STORAGE"});

        verify(mockJobManager).submit("ACCOUNT_INFO_STATE", h1);
        verify(mockJobManager).submit("CODE_STORAGE", h2);
        String out = outputStream.toString();
        assertThat(out).contains("Submitted job 1: ACCOUNT_INFO_STATE");
        assertThat(out).contains("Submitted job 2: CODE_STORAGE");
    }

    @Test
    void unknownCfAbortsBeforeAnySubmission() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        ColumnFamilyHandle h1 = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("ACCOUNT_INFO_STATE")).thenReturn(h1);
        when(mockDbManager.getColumnFamilyByName("NOPE")).thenReturn(null);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("ACCOUNT_INFO_STATE"));

        command.execute(new String[]{"ACCOUNT_INFO_STATE", "NOPE"});

        verify(mockJobManager, never()).submit(anyString(), any());
        assertThat(errorStream.toString()).contains("Column family not found: NOPE");
    }

    @Test
    void allFlagSubmitsForEveryNonEmptyCf() throws Exception {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames())
            .thenReturn(Set.of("CF_FULL", "CF_EMPTY"));

        BesuDatabaseManager.DatabaseStats fullStats = new BesuDatabaseManager.DatabaseStats();
        fullStats.estimatedKeys = 100;
        fullStats.totalSstSize = 1000;
        BesuDatabaseManager.DatabaseStats emptyStats = new BesuDatabaseManager.DatabaseStats();
        emptyStats.estimatedKeys = 0;
        emptyStats.totalSstSize = 0;
        when(mockDbManager.getStats("CF_FULL")).thenReturn(fullStats);
        when(mockDbManager.getStats("CF_EMPTY")).thenReturn(emptyStats);

        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("CF_FULL")).thenReturn(handle);
        when(mockJobManager.submit("CF_FULL", handle)).thenReturn(1);

        command.execute(new String[]{"--all"});

        verify(mockJobManager).submit("CF_FULL", handle);
        verify(mockJobManager, never()).submit(eq("CF_EMPTY"), any());
        assertThat(outputStream.toString()).contains("Submitted job 1: CF_FULL");
    }

    @Test
    void allFlagWithNoNonEmptyCfsPrintsInfoMessage() throws Exception {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("CF_EMPTY"));
        BesuDatabaseManager.DatabaseStats empty = new BesuDatabaseManager.DatabaseStats();
        empty.estimatedKeys = 0;
        empty.totalSstSize = 0;
        when(mockDbManager.getStats("CF_EMPTY")).thenReturn(empty);

        command.execute(new String[]{"--all"});

        verify(mockJobManager, never()).submit(anyString(), any());
        assertThat(outputStream.toString())
            .contains("No non-empty column families to compact.");
    }

    @Test
    void getUsageMentionsCompact() {
        assertThat(command.getUsage()).contains("db compact");
        assertThat(command.getUsage()).contains("--all");
    }
}
