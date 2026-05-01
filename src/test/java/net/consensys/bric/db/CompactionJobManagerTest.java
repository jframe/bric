package net.consensys.bric.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.RocksDB;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class CompactionJobManagerTest {

    private RocksDB db;
    private ColumnFamilyHandle cf;
    private CompactionJobManager manager;
    private CountDownLatch workerGate;

    @BeforeEach
    void setUp() {
        db = Mockito.mock(RocksDB.class);
        cf = Mockito.mock(ColumnFamilyHandle.class);
        workerGate = new CountDownLatch(1);
        // Worker blocks inside compactRange until the gate opens.
        Supplier<CompactRangeOptions> optionsFactory =
            () -> Mockito.mock(CompactRangeOptions.class);
        manager = new CompactionJobManager(db, optionsFactory);
    }

    @AfterEach
    void tearDown() {
        workerGate.countDown();
        manager.shutdownAndClear();
    }

    @Test
    void submitReturnsMonotonicIds() throws Exception {
        // Block all workers so jobs stay RUNNING.
        Mockito.doAnswer(inv -> {
            workerGate.await();
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id1 = manager.submit("CF_A", cf);
        int id2 = manager.submit("CF_B", cf);
        int id3 = manager.submit("CF_C", cf);

        assertThat(id1).isEqualTo(1);
        assertThat(id2).isEqualTo(2);
        assertThat(id3).isEqualTo(3);
    }

    @Test
    void submitRecordsRunningJob() throws Exception {
        Mockito.doAnswer(inv -> {
            workerGate.await();
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("ACCOUNT_INFO_STATE", cf);

        // Wait until the worker has actually entered compactRange — manager.submit
        // returns before the worker thread picks up the task.
        await().atMost(2, TimeUnit.SECONDS).untilAsserted(() ->
            Mockito.verify(db).compactRange(
                Mockito.eq(cf), Mockito.isNull(), Mockito.isNull(),
                Mockito.any(CompactRangeOptions.class))
        );

        CompactionJob job = manager.get(id).orElseThrow();
        assertThat(job.getCfName()).isEqualTo("ACCOUNT_INFO_STATE");
        assertThat(job.getState()).isEqualTo(CompactionJob.State.RUNNING);
        assertThat(job.getStartedAt()).isNotNull();
        assertThat(job.getFinishedAt()).isNull();
    }

    @Test
    void hasRunningAndRunningJobIdsReflectLiveState() throws Exception {
        Mockito.doAnswer(inv -> {
            workerGate.await();
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id1 = manager.submit("CF1", cf);
        int id2 = manager.submit("CF2", cf);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.runningJobIds().size() == 2);

        assertThat(manager.hasRunning()).isTrue();
        assertThat(manager.runningJobIds()).containsExactlyInAnyOrder(id1, id2);
    }

    @Test
    void listReturnsJobsSortedById() throws Exception {
        Mockito.doAnswer(inv -> {
            workerGate.await();
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        manager.submit("CF1", cf);
        manager.submit("CF2", cf);
        manager.submit("CF3", cf);

        List<CompactionJob> jobs = manager.list();
        assertThat(jobs).hasSize(3);
        assertThat(jobs.get(0).getId()).isEqualTo(1);
        assertThat(jobs.get(1).getId()).isEqualTo(2);
        assertThat(jobs.get(2).getId()).isEqualTo(3);
    }

    @Test
    void getReturnsEmptyForUnknownId() {
        assertThat(manager.get(999)).isEmpty();
    }

    @Test
    void hasRunningIsFalseInitially() {
        assertThat(manager.hasRunning()).isFalse();
        assertThat(manager.runningJobIds()).isEmpty();
    }

    @Test
    void workerCompletingCleanlyMarksJobDone() throws Exception {
        // compactRange returns normally, options.canceled() returns false.
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(false);
        manager = new CompactionJobManager(db, () -> opts);

        Mockito.doNothing().when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().getState() == CompactionJob.State.DONE);

        CompactionJob job = manager.get(id).orElseThrow();
        assertThat(job.getFinishedAt()).isNotNull();
        assertThat(job.getError()).isNull();
    }

    @Test
    void workerThrowingRocksDBExceptionMarksJobFailed() throws Exception {
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(false);
        manager = new CompactionJobManager(db, () -> opts);

        Mockito.doThrow(new org.rocksdb.RocksDBException("io error"))
            .when(db).compactRange(
                Mockito.any(), Mockito.isNull(), Mockito.isNull(),
                Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().getState() == CompactionJob.State.FAILED);

        CompactionJob job = manager.get(id).orElseThrow();
        assertThat(job.getError()).contains("io error");
    }

    @Test
    void workerThrowingIncompleteMarksJobCancelled() throws Exception {
        // canceled() returns false so the Status.Incomplete path is exercised in isolation.
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(false);
        manager = new CompactionJobManager(db, () -> opts);

        org.rocksdb.Status incomplete = new org.rocksdb.Status(
            org.rocksdb.Status.Code.Incomplete, org.rocksdb.Status.SubCode.None, "");
        Mockito.doThrow(new org.rocksdb.RocksDBException("manual compaction paused", incomplete))
            .when(db).compactRange(
                Mockito.any(), Mockito.isNull(), Mockito.isNull(),
                Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().getState() == CompactionJob.State.CANCELLED);
    }

    @Test
    void workerReturningNormallyWithCanceledFlagMarksCancelled() throws Exception {
        // RocksDB sometimes returns cleanly when cancellation is observed —
        // we still want to record that as CANCELLED, not DONE.
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(true);
        manager = new CompactionJobManager(db, () -> opts);

        Mockito.doNothing().when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().getState() == CompactionJob.State.CANCELLED);
    }

    @Test
    void workerThrowingUnexpectedThrowableMarksFailed() throws Exception {
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(false);
        manager = new CompactionJobManager(db, () -> opts);

        Mockito.doThrow(new RuntimeException("boom"))
            .when(db).compactRange(
                Mockito.any(), Mockito.isNull(), Mockito.isNull(),
                Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().getState() == CompactionJob.State.FAILED);

        assertThat(manager.get(id).orElseThrow().getError()).contains("boom");
    }

    @Test
    void workerClosesOptionsOnCompletion() throws Exception {
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(false);
        manager = new CompactionJobManager(db, () -> opts);

        Mockito.doNothing().when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().getState() == CompactionJob.State.DONE);

        Mockito.verify(opts).close();
    }

    @Test
    void cancelSetsCanceledOnJobOptionsAndReturnsTrueOnTransition() throws Exception {
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        // Worker simulates real behavior: once setCanceled(true) is called, return.
        // Simulated by checking canceled() in a loop on the mock.
        java.util.concurrent.atomic.AtomicBoolean cancelled =
            new java.util.concurrent.atomic.AtomicBoolean(false);
        Mockito.when(opts.canceled()).thenAnswer(inv -> cancelled.get());
        Mockito.doAnswer(inv -> {
            cancelled.set(true);
            return opts;
        }).when(opts).setCanceled(true);

        manager = new CompactionJobManager(db, () -> opts);

        // Worker spins until canceled() returns true.
        Mockito.doAnswer(inv -> {
            while (!cancelled.get()) {
                Thread.sleep(20);
            }
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);
        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().isRunning());

        boolean transitioned = manager.cancel(id);

        assertThat(transitioned).isTrue();
        Mockito.verify(opts).setCanceled(true);
        assertThat(manager.get(id).orElseThrow().getState())
            .isEqualTo(CompactionJob.State.CANCELLED);
    }

    @Test
    void cancelReturnsFalseWhenWorkerDoesNotTransitionInTime() throws Exception {
        // Worker is held by a latch and ignores cancellation.
        CountDownLatch hold = new CountDownLatch(1);
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(false);
        manager = new CompactionJobManager(db, () -> opts);

        Mockito.doAnswer(inv -> {
            hold.await();
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);
        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().isRunning());

        boolean transitioned = manager.cancel(id, java.time.Duration.ofMillis(200));

        assertThat(transitioned).isFalse();
        Mockito.verify(opts).setCanceled(true);
        // Job is still RUNNING (worker held by latch).
        assertThat(manager.get(id).orElseThrow().isRunning()).isTrue();

        hold.countDown();
    }

    @Test
    void cancelOnlyAffectsTargetedJob() throws Exception {
        // Two jobs, distinct options instances.
        CompactRangeOptions opts1 = Mockito.mock(CompactRangeOptions.class);
        CompactRangeOptions opts2 = Mockito.mock(CompactRangeOptions.class);
        java.util.concurrent.atomic.AtomicBoolean cancelled1 =
            new java.util.concurrent.atomic.AtomicBoolean(false);
        Mockito.when(opts1.canceled()).thenAnswer(inv -> cancelled1.get());
        Mockito.doAnswer(inv -> { cancelled1.set(true); return opts1; })
            .when(opts1).setCanceled(true);
        Mockito.when(opts2.canceled()).thenReturn(false);

        java.util.Iterator<CompactRangeOptions> seq =
            java.util.List.of(opts1, opts2).iterator();
        manager = new CompactionJobManager(db, seq::next);

        Mockito.doAnswer(inv -> {
            CompactRangeOptions o = inv.getArgument(3);
            while (!o.canceled()) {
                Thread.sleep(20);
            }
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id1 = manager.submit("CF1", cf);
        int id2 = manager.submit("CF2", cf);
        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id1).orElseThrow().isRunning() &&
            manager.get(id2).orElseThrow().isRunning());

        manager.cancel(id1);

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id1).orElseThrow().getState() == CompactionJob.State.CANCELLED);

        // Job 2 must still be RUNNING — its setCanceled was never called.
        Mockito.verify(opts2, Mockito.never()).setCanceled(Mockito.anyBoolean());
        assertThat(manager.get(id2).orElseThrow().isRunning()).isTrue();
    }

    @Test
    void cancelOnUnknownIdReturnsFalse() {
        assertThat(manager.cancel(999)).isFalse();
    }

    @Test
    void cancelOnFinishedJobReturnsTrueAndIsNoOp() throws Exception {
        // A DONE job shouldn't have its options touched.
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(false);
        manager = new CompactionJobManager(db, () -> opts);

        Mockito.doNothing().when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        int id = manager.submit("CF", cf);
        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.get(id).orElseThrow().getState() == CompactionJob.State.DONE);

        boolean result = manager.cancel(id);

        assertThat(result).isTrue();
        Mockito.verify(opts, Mockito.never()).setCanceled(Mockito.anyBoolean());
    }

    @Test
    void cancelAllSignalsEveryRunningJob() throws Exception {
        CompactRangeOptions opts1 = Mockito.mock(CompactRangeOptions.class);
        CompactRangeOptions opts2 = Mockito.mock(CompactRangeOptions.class);
        java.util.concurrent.atomic.AtomicBoolean c1 =
            new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicBoolean c2 =
            new java.util.concurrent.atomic.AtomicBoolean(false);
        Mockito.when(opts1.canceled()).thenAnswer(inv -> c1.get());
        Mockito.when(opts2.canceled()).thenAnswer(inv -> c2.get());
        Mockito.doAnswer(inv -> { c1.set(true); return opts1; })
            .when(opts1).setCanceled(true);
        Mockito.doAnswer(inv -> { c2.set(true); return opts2; })
            .when(opts2).setCanceled(true);

        java.util.Iterator<CompactRangeOptions> seq =
            java.util.List.of(opts1, opts2).iterator();
        manager = new CompactionJobManager(db, seq::next);

        Mockito.doAnswer(inv -> {
            CompactRangeOptions o = inv.getArgument(3);
            while (!o.canceled()) Thread.sleep(20);
            return null;
        }).when(db).compactRange(
            Mockito.any(), Mockito.isNull(), Mockito.isNull(),
            Mockito.any(CompactRangeOptions.class));

        manager.submit("CF1", cf);
        manager.submit("CF2", cf);
        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.runningJobIds().size() == 2);

        manager.cancelAll();

        await().atMost(2, TimeUnit.SECONDS).until(() ->
            manager.runningJobIds().isEmpty());

        Mockito.verify(opts1).setCanceled(true);
        Mockito.verify(opts2).setCanceled(true);
    }

    @Test
    void cancelAllOnEmptyManagerIsNoOp() {
        // Should not throw and should not block.
        manager.cancelAll();
        assertThat(manager.hasRunning()).isFalse();
    }
}
