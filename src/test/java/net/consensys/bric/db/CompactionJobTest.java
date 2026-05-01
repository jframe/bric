package net.consensys.bric.db;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.CompactRangeOptions;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompactionJobTest {

    private CompactRangeOptions mockOptions() {
        return Mockito.mock(CompactRangeOptions.class);
    }

    @Test
    void newJobIsRunningWithStartedAtSet() {
        CompactRangeOptions opts = mockOptions();
        Instant before = Instant.now();
        CompactionJob job = new CompactionJob(7, "ACCOUNT_INFO_STATE", opts);
        Instant after = Instant.now();

        assertThat(job.getId()).isEqualTo(7);
        assertThat(job.getCfName()).isEqualTo("ACCOUNT_INFO_STATE");
        assertThat(job.getState()).isEqualTo(CompactionJob.State.RUNNING);
        assertThat(job.getOptions()).isSameAs(opts);
        assertThat(job.getStartedAt()).isBetween(before, after);
        assertThat(job.getFinishedAt()).isNull();
        assertThat(job.getError()).isNull();
    }

    @Test
    void markDoneSetsStateAndFinishedAt() {
        CompactionJob job = new CompactionJob(1, "CF", mockOptions());
        Instant before = Instant.now();
        job.markDone();
        Instant after = Instant.now();

        assertThat(job.getState()).isEqualTo(CompactionJob.State.DONE);
        assertThat(job.getFinishedAt()).isBetween(before, after);
        assertThat(job.getError()).isNull();
    }

    @Test
    void markFailedSetsStateErrorAndFinishedAt() {
        CompactionJob job = new CompactionJob(1, "CF", mockOptions());
        job.markFailed("io error");

        assertThat(job.getState()).isEqualTo(CompactionJob.State.FAILED);
        assertThat(job.getError()).isEqualTo("io error");
        assertThat(job.getFinishedAt()).isNotNull();
    }

    @Test
    void markCancelledSetsState() {
        CompactionJob job = new CompactionJob(1, "CF", mockOptions());
        job.markCancelled();

        assertThat(job.getState()).isEqualTo(CompactionJob.State.CANCELLED);
        assertThat(job.getFinishedAt()).isNotNull();
        assertThat(job.getError()).isNull();
    }

    @Test
    void terminalDoneCannotBeChanged() {
        CompactionJob job = new CompactionJob(1, "CF", mockOptions());
        job.markDone();

        assertThatThrownBy(job::markDone)
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> job.markFailed("x"))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(job::markCancelled)
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void terminalFailedCannotBeChanged() {
        CompactionJob job = new CompactionJob(2, "CF", mockOptions());
        job.markFailed("boom");

        assertThatThrownBy(job::markDone)
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> job.markFailed("y"))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(job::markCancelled)
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void terminalCancelledCannotBeChanged() {
        CompactionJob job = new CompactionJob(3, "CF", mockOptions());
        job.markCancelled();

        assertThatThrownBy(job::markDone)
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> job.markFailed("z"))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(job::markCancelled)
            .isInstanceOf(IllegalStateException.class);
    }
}
