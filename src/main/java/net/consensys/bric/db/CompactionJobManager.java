package net.consensys.bric.db;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Owns the executor and job map for asynchronous RocksDB manual compactions.
 * One instance per BesuDatabaseManager open-session; reset on close.
 */
public class CompactionJobManager {

    private static final Logger LOG = LoggerFactory.getLogger(CompactionJobManager.class);

    private final RocksDB db;
    private final Supplier<CompactRangeOptions> optionsFactory;
    private final AtomicInteger nextId = new AtomicInteger(1);
    private final ConcurrentMap<Integer, CompactionJob> jobs = new ConcurrentHashMap<>();
    private final ExecutorService executor;

    public CompactionJobManager(RocksDB db) {
        this(db, CompactRangeOptions::new);
    }

    /** Test-friendly constructor: lets tests substitute a mock options factory. */
    CompactionJobManager(RocksDB db, Supplier<CompactRangeOptions> optionsFactory) {
        this.db = db;
        this.optionsFactory = optionsFactory;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "bric-compaction");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Submit a manual compaction for {@code handle}. Returns immediately; the
     * worker runs on the executor.
     *
     * <p>The job is registered in {@link CompactionJob.State#RUNNING} state
     * before the worker thread has actually started executing. Callers that
     * need to observe state transitions should poll {@link #get(int)} rather
     * than assuming the worker is mid-flight.
     */
    public int submit(String cfName, ColumnFamilyHandle handle) {
        int id = nextId.getAndIncrement();
        CompactRangeOptions options = optionsFactory.get();
        CompactionJob job = new CompactionJob(id, cfName, options);
        jobs.put(id, job);
        executor.submit(() -> runWorker(job, handle));
        LOG.info("Submitted compaction job {} for CF {}", id, cfName);
        return id;
    }

    private void runWorker(CompactionJob job, ColumnFamilyHandle handle) {
        // Outcome handling lands in Task 3. For now, just call compactRange and
        // let any exception escape (caught by ExecutorService).
        try {
            db.compactRange(handle, null, null, job.getOptions());
        } catch (Exception e) {
            LOG.warn("Compaction job {} threw: {}", job.getId(), e.toString());
        }
    }

    public Optional<CompactionJob> get(int jobId) {
        return Optional.ofNullable(jobs.get(jobId));
    }

    public List<CompactionJob> list() {
        return jobs.values().stream()
            .sorted(Comparator.comparingInt(CompactionJob::getId))
            .collect(Collectors.toList());
    }

    public boolean hasRunning() {
        return jobs.values().stream().anyMatch(CompactionJob::isRunning);
    }

    public List<Integer> runningJobIds() {
        return jobs.values().stream()
            .filter(CompactionJob::isRunning)
            .map(CompactionJob::getId)
            .sorted()
            .collect(Collectors.toList());
    }

    /**
     * Shutdown the executor and clear the job map. Called on db close.
     *
     * <p>Caller precondition: there must be no RUNNING jobs. {@code
     * BesuDatabaseManager.closeDatabase()} enforces this by checking
     * {@link #hasRunning()} before invoking this method. Calling this while
     * workers are still inside {@code compactRange} can leave them updating
     * job objects that are no longer reachable through the map.
     */
    public void shutdownAndClear() {
        executor.shutdownNow();
        jobs.clear();
    }
}
