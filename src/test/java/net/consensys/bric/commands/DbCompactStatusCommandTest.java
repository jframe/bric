package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.CompactionJob;
import net.consensys.bric.db.CompactionJobManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.RocksDB;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DbCompactStatusCommandTest {

    private BesuDatabaseManager mockDbManager;
    private CompactionJobManager mockJobManager;
    private RocksDB mockDb;
    private DbCompactStatusCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = mock(BesuDatabaseManager.class);
        mockJobManager = mock(CompactionJobManager.class);
        mockDb = mock(RocksDB.class);
        when(mockDbManager.getCompactionJobManager()).thenReturn(mockJobManager);
        when(mockDbManager.getDatabase()).thenReturn(mockDb);
        command = new DbCompactStatusCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    private CompactionJob runningJob(int id, String cf) {
        return new CompactionJob(id, cf, mock(CompactRangeOptions.class));
    }

    private CompactionJob doneJob(int id, String cf) {
        CompactionJob j = runningJob(id, cf);
        j.markDone();
        return j;
    }

    private CompactionJob failedJob(int id, String cf, String err) {
        CompactionJob j = runningJob(id, cf);
        j.markFailed(err);
        return j;
    }

    @Test
    void refusesWhenDbClosed() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void noArgListsAllJobsInIdOrder() throws Exception {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockJobManager.list()).thenReturn(List.of(
            runningJob(1, "CF_A"),
            doneJob(2, "CF_B")
        ));
        ColumnFamilyHandle handleA = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("CF_A")).thenReturn(handleA);
        when(mockDb.getProperty(handleA, "rocksdb.estimate-pending-compaction-bytes"))
            .thenReturn("1234567890");

        command.execute(new String[]{});

        String out = outputStream.toString();
        assertThat(out).contains("JOB").contains("CF").contains("STATE");
        assertThat(out).contains("CF_A").contains("RUNNING");
        assertThat(out).contains("CF_B").contains("DONE");
        assertThat(out.indexOf("CF_A")).isLessThan(out.indexOf("CF_B"));
    }

    @Test
    void noArgEmptyJobListPrintsInfoMessage() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockJobManager.list()).thenReturn(List.of());
        command.execute(new String[]{});
        assertThat(outputStream.toString()).contains("No compaction jobs.");
    }

    @Test
    void withIdPrintsDetailIncludingCompactionStats() throws Exception {
        when(mockDbManager.isOpen()).thenReturn(true);
        CompactionJob j = runningJob(7, "ACCOUNT_INFO_STATE");
        when(mockJobManager.get(7)).thenReturn(Optional.of(j));
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("ACCOUNT_INFO_STATE")).thenReturn(handle);
        when(mockDb.getProperty(handle, "rocksdb.compaction-stats"))
            .thenReturn("** Compaction Stats Level 0 **\nLevel0 -> Level1 ...");
        when(mockDb.getProperty(handle, "rocksdb.estimate-pending-compaction-bytes"))
            .thenReturn("1024");

        command.execute(new String[]{"7"});

        String out = outputStream.toString();
        assertThat(out).contains("ACCOUNT_INFO_STATE").contains("RUNNING");
        assertThat(out).contains("Compaction Stats");
        assertThat(out).contains("Level0 -> Level1");
    }

    @Test
    void withIdOnFailedJobIncludesErrorMessage() {
        when(mockDbManager.isOpen()).thenReturn(true);
        CompactionJob j = failedJob(3, "CF", "io error: corrupt sst");
        when(mockJobManager.get(3)).thenReturn(Optional.of(j));
        ColumnFamilyHandle handle = mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("CF")).thenReturn(handle);

        command.execute(new String[]{"3"});

        assertThat(outputStream.toString()).contains("io error: corrupt sst");
    }

    @Test
    void withUnknownIdPrintsError() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockJobManager.get(42)).thenReturn(Optional.empty());

        command.execute(new String[]{"42"});

        assertThat(errorStream.toString()).contains("Error: No such job: 42");
    }

    @Test
    void nonNumericJobIdPrintsError() {
        when(mockDbManager.isOpen()).thenReturn(true);
        command.execute(new String[]{"abc"});
        assertThat(errorStream.toString()).contains("Error: Invalid job id");
    }
}
