package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.CompactionJob;
import net.consensys.bric.db.CompactionJobManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.CompactRangeOptions;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DbCompactCancelCommandTest {

    private BesuDatabaseManager mockDbManager;
    private CompactionJobManager mockJobManager;
    private DbCompactCancelCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = mock(BesuDatabaseManager.class);
        mockJobManager = mock(CompactionJobManager.class);
        when(mockDbManager.getCompactionJobManager()).thenReturn(mockJobManager);
        command = new DbCompactCancelCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    private CompactionJob job(int id, String cf, CompactionJob.State state) {
        CompactionJob j = new CompactionJob(id, cf, mock(CompactRangeOptions.class));
        switch (state) {
            case DONE: j.markDone(); break;
            case FAILED: j.markFailed("x"); break;
            case CANCELLED: j.markCancelled(); break;
            case RUNNING: break;
        }
        return j;
    }

    @Test
    void refusesWhenDbClosed() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{"1"});
        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void refusesWhenReadOnly() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{"1"});
        assertThat(errorStream.toString()).contains("read-only mode");
    }

    @Test
    void refusesWhenNoArgs() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("Error: Missing job id");
    }

    @Test
    void refusesNonNumericJobId() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        command.execute(new String[]{"abc"});
        assertThat(errorStream.toString()).contains("Error: Invalid job id");
    }

    @Test
    void unknownJobIdPrintsError() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockJobManager.get(42)).thenReturn(Optional.empty());
        command.execute(new String[]{"42"});
        assertThat(errorStream.toString()).contains("Error: No such job: 42");
        verify(mockJobManager, never()).cancel(anyInt());
    }

    @Test
    void alreadyFinishedJobReportsError() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockJobManager.get(1)).thenReturn(Optional.of(
            job(1, "CF", CompactionJob.State.DONE)));
        command.execute(new String[]{"1"});
        assertThat(errorStream.toString()).contains("already").contains("DONE");
        verify(mockJobManager, never()).cancel(anyInt());
    }

    @Test
    void runningJobIsCancelledAndPrintsConfirmation() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockJobManager.get(1)).thenReturn(Optional.of(
            job(1, "CF", CompactionJob.State.RUNNING)));
        when(mockJobManager.cancel(1)).thenReturn(true);

        command.execute(new String[]{"1"});

        verify(mockJobManager).cancel(1);
        assertThat(outputStream.toString()).contains("Cancelled job 1");
    }

    @Test
    void timeoutPrintsTimeoutMessageNotConfirmation() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockJobManager.get(1)).thenReturn(Optional.of(
            job(1, "CF", CompactionJob.State.RUNNING)));
        when(mockJobManager.cancel(1)).thenReturn(false);

        command.execute(new String[]{"1"});

        verify(mockJobManager).cancel(1);
        assertThat(outputStream.toString())
            .contains("Cancel signalled for job 1")
            .contains("did not transition");
    }
}
