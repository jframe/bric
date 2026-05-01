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
}
