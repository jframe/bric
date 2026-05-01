# RocksDB Compaction Subcommands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `db compact`, `db compact-status`, and `db compact-cancel` subcommands that run RocksDB manual compactions asynchronously, expose live and historical job status, and let the user cancel a specific running job.

**Architecture:** A new `CompactionJobManager` (owned by `BesuDatabaseManager`) holds an `ExecutorService` and a `Map<Integer, CompactionJob>`. Each job carries its own `CompactRangeOptions`, so `setCanceled(true)` cancels exactly one compaction. Three new flat subcommands wire into `DbCommand` exactly like `db drop-cf` / `db stats`. `BesuDatabaseManager.closeDatabase()` refuses to close while jobs are running; `BricApplication` REPL-exit auto-cancels.

**Tech Stack:** Java 17, RocksDB Java (`org.rocksdb` 10.6.2), JUnit 5, AssertJ, Mockito.

**Spec:** `docs/superpowers/specs/2026-05-01-rocksdb-compaction-design.md`

**File map (everything under `src/main/java/net/consensys/bric` unless noted):**
- Create `db/CompactionJob.java` — value class with state machine.
- Create `db/CompactionJobManager.java` — executor, job map, submit/cancel/shutdown.
- Create `commands/DbCompactCommand.java` — `db compact <cf...|--all>`.
- Create `commands/DbCompactStatusCommand.java` — `db compact-status [<job-id>]`.
- Create `commands/DbCompactCancelCommand.java` — `db compact-cancel <job-id>`.
- Modify `db/BesuDatabaseManager.java` — own/expose `CompactionJobManager`; refuse close while jobs running.
- Modify `commands/DbCommand.java` — three new switch cases, updated usage text.
- Modify `BricApplication.java` — REPL-exit auto-cancel before close.
- Modify `completion/BricCompleter.java` — three new subcommand entries.
- Test files mirror the new sources.

---

### Task 1: `CompactionJob` value class

**Files:**
- Create: `src/main/java/net/consensys/bric/db/CompactionJob.java`
- Create: `src/test/java/net/consensys/bric/db/CompactionJobTest.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/net/consensys/bric/db/CompactionJobTest.java`:

```java
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
    void terminalStateCannotBeChanged() {
        CompactionJob job = new CompactionJob(1, "CF", mockOptions());
        job.markDone();

        assertThatThrownBy(job::markDone)
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> job.markFailed("x"))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(job::markCancelled)
            .isInstanceOf(IllegalStateException.class);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail with compilation errors**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobTest"`
Expected: Compilation failure — `CompactionJob` does not exist.

- [ ] **Step 3: Implement `CompactionJob`**

Create `src/main/java/net/consensys/bric/db/CompactionJob.java`:

```java
package net.consensys.bric.db;

import org.rocksdb.CompactRangeOptions;

import java.time.Instant;

/**
 * State for a single manual compaction job. Each job owns a CompactRangeOptions
 * so cancellation (setCanceled(true)) targets only this job, not siblings.
 */
public class CompactionJob {

    public enum State { RUNNING, DONE, FAILED, CANCELLED }

    private final int id;
    private final String cfName;
    private final CompactRangeOptions options;
    private final Instant startedAt;
    private volatile State state;
    private volatile Instant finishedAt;
    private volatile String error;

    public CompactionJob(int id, String cfName, CompactRangeOptions options) {
        this.id = id;
        this.cfName = cfName;
        this.options = options;
        this.startedAt = Instant.now();
        this.state = State.RUNNING;
    }

    public int getId() { return id; }
    public String getCfName() { return cfName; }
    public CompactRangeOptions getOptions() { return options; }
    public Instant getStartedAt() { return startedAt; }
    public State getState() { return state; }
    public Instant getFinishedAt() { return finishedAt; }
    public String getError() { return error; }

    public synchronized void markDone() {
        requireRunning();
        this.state = State.DONE;
        this.finishedAt = Instant.now();
    }

    public synchronized void markFailed(String message) {
        requireRunning();
        this.state = State.FAILED;
        this.error = message;
        this.finishedAt = Instant.now();
    }

    public synchronized void markCancelled() {
        requireRunning();
        this.state = State.CANCELLED;
        this.finishedAt = Instant.now();
    }

    public boolean isRunning() {
        return state == State.RUNNING;
    }

    private void requireRunning() {
        if (state != State.RUNNING) {
            throw new IllegalStateException(
                "Job " + id + " is already in terminal state: " + state);
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobTest"`
Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/db/CompactionJob.java \
        src/test/java/net/consensys/bric/db/CompactionJobTest.java
git commit -m "$(cat <<'EOF'
feat: add CompactionJob value class for tracking manual compactions

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: `CompactionJobManager` — submit + state observation

**Files:**
- Create: `src/main/java/net/consensys/bric/db/CompactionJobManager.java`
- Create: `src/test/java/net/consensys/bric/db/CompactionJobManagerTest.java`

This task adds `submit`, `get`, `list`, `hasRunning`, `runningJobIds`. Worker outcome handling (DONE/FAILED/CANCELLED transitions on real RocksDB calls) lands in Task 3. Cancellation lands in Task 4.

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/net/consensys/bric/db/CompactionJobManagerTest.java`:

```java
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
```

- [ ] **Step 2: Add Awaitility dependency**

Awaitility is needed for the polling waits. Open `build.gradle`. Find the `dependencies { ... }` block (search for `testImplementation` to locate). Add inside it (near the other `testImplementation` lines):

```groovy
testImplementation 'org.awaitility:awaitility:4.2.2'
```

Run: `./gradlew dependencies --configuration testRuntimeClasspath | grep awaitility`
Expected: Output shows `org.awaitility:awaitility:4.2.2`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobManagerTest"`
Expected: Compilation failure — `CompactionJobManager` does not exist.

- [ ] **Step 4: Implement `CompactionJobManager` (submit-only)**

Create `src/main/java/net/consensys/bric/db/CompactionJobManager.java`:

```java
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
     * Submit a manual compaction for {@code handle}. Returns immediately;
     * the worker runs on the executor.
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

    /** Shutdown the executor and clear the job map. Called on db close. */
    public void shutdownAndClear() {
        executor.shutdownNow();
        jobs.clear();
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobManagerTest"`
Expected: All 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/net/consensys/bric/db/CompactionJobManager.java \
        src/test/java/net/consensys/bric/db/CompactionJobManagerTest.java \
        build.gradle
git commit -m "$(cat <<'EOF'
feat: add CompactionJobManager with submit and state observation

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Worker outcome handling — DONE / FAILED / CANCELLED

**Files:**
- Modify: `src/main/java/net/consensys/bric/db/CompactionJobManager.java`
- Modify: `src/test/java/net/consensys/bric/db/CompactionJobManagerTest.java`

- [ ] **Step 1: Add the failing tests**

Add these methods to `CompactionJobManagerTest` (paste before the closing `}` of the class):

```java
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
        CompactRangeOptions opts = Mockito.mock(CompactRangeOptions.class);
        Mockito.when(opts.canceled()).thenReturn(true);
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobManagerTest"`
Expected: The 6 new tests FAIL — current `runWorker` doesn't update job state or close options.

- [ ] **Step 3: Replace `runWorker` with the full outcome-handling version**

In `CompactionJobManager.java`, replace the existing `runWorker` method:

```java
    private void runWorker(CompactionJob job, ColumnFamilyHandle handle) {
        try {
            try {
                db.compactRange(handle, null, null, job.getOptions());
                if (job.getOptions().canceled()) {
                    job.markCancelled();
                } else {
                    job.markDone();
                }
            } catch (org.rocksdb.RocksDBException e) {
                if (isCancellation(e, job.getOptions())) {
                    job.markCancelled();
                } else {
                    job.markFailed(e.getMessage() != null ? e.getMessage() : e.toString());
                }
            } catch (Throwable t) {
                job.markFailed(t.toString());
            }
        } finally {
            try {
                job.getOptions().close();
            } catch (Exception e) {
                LOG.warn("Failed to close CompactRangeOptions for job {}: {}",
                    job.getId(), e.toString());
            }
        }
    }

    private static boolean isCancellation(
            org.rocksdb.RocksDBException e, CompactRangeOptions options) {
        if (options.canceled()) {
            return true;
        }
        org.rocksdb.Status status = e.getStatus();
        return status != null && status.getCode() == org.rocksdb.Status.Code.Incomplete;
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobManagerTest"`
Expected: All 12 tests in the class PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/db/CompactionJobManager.java \
        src/test/java/net/consensys/bric/db/CompactionJobManagerTest.java
git commit -m "$(cat <<'EOF'
feat: track compaction worker outcome (done, failed, cancelled)

Closes the per-job CompactRangeOptions in a finally block to release
the native handle.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: `CompactionJobManager.cancel` and `cancelAll`

**Files:**
- Modify: `src/main/java/net/consensys/bric/db/CompactionJobManager.java`
- Modify: `src/test/java/net/consensys/bric/db/CompactionJobManagerTest.java`

- [ ] **Step 1: Write the failing tests**

Append to `CompactionJobManagerTest`:

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobManagerTest"`
Expected: The 7 new tests FAIL — `cancel` / `cancelAll` not implemented.

- [ ] **Step 3: Add `cancel` and `cancelAll` to `CompactionJobManager`**

Add these methods to `CompactionJobManager.java` (after `runningJobIds()`):

```java
    private static final java.time.Duration DEFAULT_CANCEL_WAIT =
        java.time.Duration.ofSeconds(5);

    /**
     * Signal cancellation on a single job. Returns true if the job is no
     * longer RUNNING by the time the wait elapses (CANCELLED, DONE, or FAILED),
     * false if the worker did not transition within the timeout.
     */
    public boolean cancel(int jobId) {
        return cancel(jobId, DEFAULT_CANCEL_WAIT);
    }

    /** Test-friendly variant with a custom timeout. */
    boolean cancel(int jobId, java.time.Duration timeout) {
        CompactionJob job = jobs.get(jobId);
        if (job == null) {
            return false;
        }
        if (!job.isRunning()) {
            return true;
        }
        try {
            job.getOptions().setCanceled(true);
        } catch (Exception e) {
            LOG.warn("setCanceled failed on job {}: {}", jobId, e.toString());
        }
        return waitForTerminal(job, timeout);
    }

    /**
     * Signal cancellation on every RUNNING job and wait for them to settle.
     * Idempotent: no-op when nothing is running.
     */
    public void cancelAll() {
        cancelAll(DEFAULT_CANCEL_WAIT);
    }

    void cancelAll(java.time.Duration timeout) {
        List<CompactionJob> running = jobs.values().stream()
            .filter(CompactionJob::isRunning)
            .collect(Collectors.toList());
        for (CompactionJob job : running) {
            try {
                job.getOptions().setCanceled(true);
            } catch (Exception e) {
                LOG.warn("setCanceled failed on job {}: {}",
                    job.getId(), e.toString());
            }
        }
        long deadline = System.nanoTime() + timeout.toNanos();
        for (CompactionJob job : running) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) break;
            waitForTerminal(job, java.time.Duration.ofNanos(remaining));
        }
    }

    private boolean waitForTerminal(CompactionJob job, java.time.Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (job.isRunning()) {
            if (System.nanoTime() >= deadline) {
                return false;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.db.CompactionJobManagerTest"`
Expected: All tests PASS (19 total in this class).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/db/CompactionJobManager.java \
        src/test/java/net/consensys/bric/db/CompactionJobManagerTest.java
git commit -m "$(cat <<'EOF'
feat: per-job and bulk cancellation for compaction jobs

cancel(id) and cancelAll() use CompactRangeOptions.setCanceled(true);
each job has its own options instance so cancellation is isolated.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Wire `CompactionJobManager` into `BesuDatabaseManager`

**Files:**
- Modify: `src/main/java/net/consensys/bric/db/BesuDatabaseManager.java`
- Modify: `src/test/java/net/consensys/bric/db/` (new test class)

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/net/consensys/bric/db/BesuDatabaseManagerCompactionTest.java`:

```java
package net.consensys.bric.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BesuDatabaseManagerCompactionTest {

    private Path tempDbDir;
    private BesuDatabaseManager manager;

    @BeforeEach
    void setUp() throws Exception {
        tempDbDir = Files.createTempDirectory("bric-cfmgr-test-");
        // Bootstrap an empty RocksDB so BesuDatabaseManager (which uses
        // setCreateIfMissing(false)) can open it.
        RocksDB.loadLibrary();
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB initDb = RocksDB.open(opts, tempDbDir.toString())) {
            // Empty DB created.
        }
        manager = new BesuDatabaseManager();
        manager.openDatabase(tempDbDir.toString(), true);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager.isOpen()) {
            // Replace any stub job manager with a real one so close succeeds.
            manager.setCompactionJobManagerForTesting(
                new CompactionJobManager(manager.getDatabase()));
            manager.closeDatabase();
        }
        deleteRecursively(tempDbDir);
    }

    @Test
    void getCompactionJobManagerIsNonNullWhileOpen() {
        assertThat(manager.getCompactionJobManager()).isNotNull();
    }

    @Test
    void getCompactionJobManagerThrowsWhenClosed() {
        manager.setCompactionJobManagerForTesting(
            new CompactionJobManager(manager.getDatabase()));
        manager.closeDatabase();
        assertThatThrownBy(() -> manager.getCompactionJobManager())
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeDatabaseRefusesWhenJobsRunning() {
        CompactionJobManager stub = new CompactionJobManager(manager.getDatabase()) {
            @Override
            public boolean hasRunning() { return true; }
            @Override
            public List<Integer> runningJobIds() { return List.of(3, 5); }
        };
        manager.setCompactionJobManagerForTesting(stub);

        assertThatThrownBy(() -> manager.closeDatabase())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("compaction jobs still running")
            .hasMessageContaining("[3, 5]");
    }

    private static void deleteRecursively(Path dir) throws Exception {
        if (!Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(p -> {
                      try { Files.delete(p); }
                      catch (Exception ignored) {}
                  });
        }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.db.BesuDatabaseManagerCompactionTest"`
Expected: Compilation failure — `getCompactionJobManager`, `setCompactionJobManagerForTesting` don't exist.

- [ ] **Step 3: Modify `BesuDatabaseManager` to own a `CompactionJobManager`**

In `BesuDatabaseManager.java`:

3a. Add field after `private boolean writable = false;` (around line 29):

```java
    private CompactionJobManager jobManager;
```

3b. At the end of `openDatabase` (after `LOG.info("Database opened successfully. Format: {}", format);`, line 164), add:

```java
        this.jobManager = new CompactionJobManager(db);
```

3c. At the top of `closeDatabase()` (immediately after the `if (!isOpen)` early return, around line 175), add:

```java
        if (jobManager != null && jobManager.hasRunning()) {
            throw new IllegalStateException(
                "Cannot close: compaction jobs still running: "
                + jobManager.runningJobIds()
                + ". Cancel them first with 'db compact-cancel <job-id>'.");
        }
```

3d. Inside `closeDatabase()`, after `db.close()` and the `currentPath = null;` block (so it runs when we're actually shutting down), add:

```java
        if (jobManager != null) {
            jobManager.shutdownAndClear();
            jobManager = null;
        }
```

Place this just before the final `LOG.info("Database closed successfully");` line.

3e. Add public accessor and test seam (after `getDatabase()`, around line 259):

```java
    /** Get the per-session compaction job manager. */
    public CompactionJobManager getCompactionJobManager() {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        return jobManager;
    }

    /** Test seam: replace the job manager. Package-private. */
    void setCompactionJobManagerForTesting(CompactionJobManager manager) {
        this.jobManager = manager;
    }
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.db.BesuDatabaseManagerCompactionTest"`
Expected: All 3 tests PASS.

- [ ] **Step 5: Run the full test suite to verify no regressions**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL with all tests passing.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/net/consensys/bric/db/BesuDatabaseManager.java \
        src/test/java/net/consensys/bric/db/BesuDatabaseManagerCompactionTest.java
git commit -m "$(cat <<'EOF'
feat: own CompactionJobManager in BesuDatabaseManager lifecycle

closeDatabase() refuses while jobs are running and shuts down the
manager on a clean close.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: `DbCompactCommand`

**Files:**
- Create: `src/main/java/net/consensys/bric/commands/DbCompactCommand.java`
- Create: `src/test/java/net/consensys/bric/commands/DbCompactCommandTest.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/net/consensys/bric/commands/DbCompactCommandTest.java`:

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCompactCommandTest"`
Expected: Compilation failure — `DbCompactCommand` does not exist.

- [ ] **Step 3: Implement `DbCompactCommand`**

Create `src/main/java/net/consensys/bric/commands/DbCompactCommand.java`:

```java
package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.ColumnFamilyResolver;
import net.consensys.bric.db.CompactionJobManager;
import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Submit asynchronous manual compactions for one or more column families.
 * Use {@code db compact-status} to monitor and {@code db compact-cancel} to abort.
 */
public class DbCompactCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbCompactCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }
        if (!dbManager.isWritable()) {
            System.err.println(
                "Error: Database is open in read-only mode. "
                + "Reopen with 'db open <path> --write'.");
            return;
        }
        if (args.length == 0) {
            System.err.println("Error: Missing column family argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        // Resolve targets atomically: build the full list before submitting any job.
        Map<String, ColumnFamilyHandle> targets = new LinkedHashMap<>();
        if (args.length == 1 && "--all".equals(args[0])) {
            if (!collectAllNonEmptyCfs(targets)) {
                return;
            }
            if (targets.isEmpty()) {
                System.out.println("No non-empty column families to compact.");
                return;
            }
        } else {
            for (String input : args) {
                if ("--all".equals(input)) {
                    System.err.println(
                        "Error: --all cannot be combined with other arguments");
                    return;
                }
                ColumnFamilyHandle handle;
                try {
                    handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, input);
                } catch (IllegalArgumentException e) {
                    System.err.println("Error: " + e.getMessage());
                    return;
                }
                if (handle == null) {
                    System.err.println("Error: Column family not found: " + input);
                    System.err.println("Available in this database: "
                        + dbManager.getColumnFamilyNames().stream()
                            .sorted().collect(Collectors.joining(", ")));
                    return;
                }
                String cfName = resolveCfName(handle);
                if (cfName == null) {
                    System.err.println("Error: Could not identify column family name");
                    return;
                }
                targets.put(cfName, handle);
            }
        }

        CompactionJobManager jobManager = dbManager.getCompactionJobManager();
        for (Map.Entry<String, ColumnFamilyHandle> entry : targets.entrySet()) {
            int id = jobManager.submit(entry.getKey(), entry.getValue());
            System.out.println("Submitted job " + id + ": " + entry.getKey());
        }
    }

    /** Populate {@code targets} with all CFs whose stats are non-empty. */
    private boolean collectAllNonEmptyCfs(Map<String, ColumnFamilyHandle> targets) {
        List<String> cfNames = new ArrayList<>(dbManager.getColumnFamilyNames());
        cfNames.sort(String::compareTo);
        for (String cfName : cfNames) {
            try {
                BesuDatabaseManager.DatabaseStats stats = dbManager.getStats(cfName);
                if (stats.estimatedKeys == 0 && stats.getTotalSize() == 0) {
                    continue;
                }
            } catch (Exception e) {
                System.err.println("Warning: skipping " + cfName
                    + " (could not read stats: " + e.getMessage() + ")");
                continue;
            }
            ColumnFamilyHandle handle = dbManager.getColumnFamilyByName(cfName);
            if (handle != null) {
                targets.put(cfName, handle);
            }
        }
        return true;
    }

    /** Reverse-look-up the stored CF name for a handle. */
    private String resolveCfName(ColumnFamilyHandle handle) {
        for (String name : dbManager.getColumnFamilyNames()) {
            if (dbManager.getColumnFamilyByName(name) == handle) {
                return name;
            }
        }
        return null;
    }

    @Override
    public String getHelp() {
        return "Submit async manual compactions for one or more column families";
    }

    @Override
    public String getUsage() {
        return "db compact <segment...|--all>\n"
             + "                               Submit async compactions; use 'db compact-status'\n"
             + "                               to monitor, 'db compact-cancel <id>' to abort.\n"
             + "                               Requires --write mode.";
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCompactCommandTest"`
Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbCompactCommand.java \
        src/test/java/net/consensys/bric/commands/DbCompactCommandTest.java
git commit -m "$(cat <<'EOF'
feat: add DbCompactCommand for async manual compactions

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: `DbCompactStatusCommand`

**Files:**
- Create: `src/main/java/net/consensys/bric/commands/DbCompactStatusCommand.java`
- Create: `src/test/java/net/consensys/bric/commands/DbCompactStatusCommandTest.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/net/consensys/bric/commands/DbCompactStatusCommandTest.java`:

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCompactStatusCommandTest"`
Expected: Compilation failure — `DbCompactStatusCommand` does not exist.

- [ ] **Step 3: Implement `DbCompactStatusCommand`**

Create `src/main/java/net/consensys/bric/commands/DbCompactStatusCommand.java`:

```java
package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.CompactionJob;
import net.consensys.bric.db.CompactionJobManager;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

/**
 * List compaction jobs (no arg) or print full detail for a single job.
 */
public class DbCompactStatusCommand implements Command {

    private static final DateTimeFormatter STARTED_FMT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private final BesuDatabaseManager dbManager;

    public DbCompactStatusCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        CompactionJobManager jobManager = dbManager.getCompactionJobManager();

        if (args.length == 0) {
            listAll(jobManager);
            return;
        }

        int id;
        try {
            id = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid job id: " + args[0]);
            return;
        }

        Optional<CompactionJob> opt = jobManager.get(id);
        if (opt.isEmpty()) {
            System.err.println("Error: No such job: " + id);
            return;
        }
        printDetail(opt.get());
    }

    private void listAll(CompactionJobManager jobManager) {
        List<CompactionJob> jobs = jobManager.list();
        if (jobs.isEmpty()) {
            System.out.println("No compaction jobs.");
            return;
        }
        System.out.printf("%-4s  %-30s  %-9s  %-19s  %-9s  %s%n",
            "JOB", "CF", "STATE", "STARTED", "ELAPSED", "PENDING_BYTES");
        for (CompactionJob job : jobs) {
            System.out.println(formatRow(job));
        }
    }

    private void printDetail(CompactionJob job) {
        System.out.printf("%-4s  %-30s  %-9s  %-19s  %-9s  %s%n",
            "JOB", "CF", "STATE", "STARTED", "ELAPSED", "PENDING_BYTES");
        System.out.println(formatRow(job));

        ColumnFamilyHandle handle = dbManager.getColumnFamilyByName(job.getCfName());
        if (handle != null) {
            try {
                String stats = dbManager.getDatabase()
                    .getProperty(handle, "rocksdb.compaction-stats");
                if (stats != null && !stats.isBlank()) {
                    System.out.println();
                    System.out.println("--- Compaction Stats ---");
                    System.out.println(stats);
                }
            } catch (RocksDBException e) {
                // Ignore — compaction stats are best-effort.
            }
        }

        if (job.getState() == CompactionJob.State.FAILED && job.getError() != null) {
            System.out.println();
            System.out.println("Error: " + job.getError());
        }
    }

    private String formatRow(CompactionJob job) {
        String started = STARTED_FMT.format(job.getStartedAt());
        String elapsed = formatElapsed(job);
        String pending = job.getState() == CompactionJob.State.RUNNING
            ? readPendingBytes(job.getCfName())
            : "-";
        return String.format("%-4d  %-30s  %-9s  %-19s  %-9s  %s",
            job.getId(),
            truncate(job.getCfName(), 30),
            job.getState(),
            started,
            elapsed,
            pending);
    }

    private String formatElapsed(CompactionJob job) {
        Instant end = job.getFinishedAt() != null ? job.getFinishedAt() : Instant.now();
        Duration d = Duration.between(job.getStartedAt(), end);
        long h = d.toHours();
        long m = d.toMinutesPart();
        long s = d.toSecondsPart();
        return String.format("%02d:%02d:%02d", h, m, s);
    }

    private String readPendingBytes(String cfName) {
        ColumnFamilyHandle handle = dbManager.getColumnFamilyByName(cfName);
        if (handle == null) return "-";
        try {
            String raw = dbManager.getDatabase()
                .getProperty(handle, "rocksdb.estimate-pending-compaction-bytes");
            if (raw == null || raw.isBlank()) return "-";
            return formatBytes(Long.parseLong(raw.trim()));
        } catch (Exception e) {
            return "-";
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        String[] units = {"KB", "MB", "GB", "TB"};
        double v = bytes / 1024.0;
        int idx = 0;
        while (v >= 1024 && idx < units.length - 1) {
            v /= 1024;
            idx++;
        }
        return String.format("%.1f %s", v, units[idx]);
    }

    private static String truncate(String s, int n) {
        return s.length() <= n ? s : s.substring(0, n - 1) + "…";
    }

    @Override
    public String getHelp() {
        return "Show compaction job status (all jobs, or detail for a specific job id)";
    }

    @Override
    public String getUsage() {
        return "db compact-status [<job-id>]";
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCompactStatusCommandTest"`
Expected: All 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbCompactStatusCommand.java \
        src/test/java/net/consensys/bric/commands/DbCompactStatusCommandTest.java
git commit -m "$(cat <<'EOF'
feat: add DbCompactStatusCommand listing and per-job detail

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: `DbCompactCancelCommand`

**Files:**
- Create: `src/main/java/net/consensys/bric/commands/DbCompactCancelCommand.java`
- Create: `src/test/java/net/consensys/bric/commands/DbCompactCancelCommandTest.java`

- [ ] **Step 1: Write the failing tests**

Create `src/test/java/net/consensys/bric/commands/DbCompactCancelCommandTest.java`:

```java
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCompactCancelCommandTest"`
Expected: Compilation failure — `DbCompactCancelCommand` does not exist.

- [ ] **Step 3: Implement `DbCompactCancelCommand`**

Create `src/main/java/net/consensys/bric/commands/DbCompactCancelCommand.java`:

```java
package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.CompactionJob;
import net.consensys.bric.db.CompactionJobManager;

import java.util.Optional;

/**
 * Cancel a running compaction job by id. Per-job (sibling jobs unaffected).
 */
public class DbCompactCancelCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbCompactCancelCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }
        if (!dbManager.isWritable()) {
            System.err.println(
                "Error: Database is open in read-only mode. "
                + "Reopen with 'db open <path> --write'.");
            return;
        }
        if (args.length == 0) {
            System.err.println("Error: Missing job id");
            System.err.println("Usage: " + getUsage());
            return;
        }

        int id;
        try {
            id = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.err.println("Error: Invalid job id: " + args[0]);
            return;
        }

        CompactionJobManager jobManager = dbManager.getCompactionJobManager();
        Optional<CompactionJob> opt = jobManager.get(id);
        if (opt.isEmpty()) {
            System.err.println("Error: No such job: " + id);
            return;
        }
        CompactionJob job = opt.get();
        if (!job.isRunning()) {
            System.err.println(
                "Error: Job " + id + " is already " + job.getState());
            return;
        }

        boolean transitioned = jobManager.cancel(id);
        if (transitioned) {
            System.out.println("Cancelled job " + id);
        } else {
            System.out.println(
                "Cancel signalled for job " + id
                + " (worker did not transition within timeout)");
        }
    }

    @Override
    public String getHelp() {
        return "Cancel a running compaction job by id";
    }

    @Override
    public String getUsage() {
        return "db compact-cancel <job-id>";
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCompactCancelCommandTest"`
Expected: All 8 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbCompactCancelCommand.java \
        src/test/java/net/consensys/bric/commands/DbCompactCancelCommandTest.java
git commit -m "$(cat <<'EOF'
feat: add DbCompactCancelCommand for per-job cancellation

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Wire the three commands into `DbCommand`

**Files:**
- Modify: `src/main/java/net/consensys/bric/commands/DbCommand.java`
- Modify: `src/test/java/net/consensys/bric/commands/DbCommandTest.java`

- [ ] **Step 1: Write the failing tests**

Append these tests to `DbCommandTest.java` (before the closing `}` of the class):

```java
    @Test
    void testExecuteCompactSubcommand() {
        // The test just verifies routing; the command itself will print an error
        // because the mocked db manager reports closed.
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{"compact", "ACCOUNT_INFO_STATE"});

        // DbCompactCommand prints to stderr when the db is closed.
        assertThat(errorStream.toString()).contains("No database is open");
    }

    @Test
    void testExecuteCompactStatusSubcommand() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{"compact-status"});
        assertThat(errorStream.toString()).contains("No database is open");
    }

    @Test
    void testExecuteCompactCancelSubcommand() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{"compact-cancel", "1"});
        assertThat(errorStream.toString()).contains("No database is open");
    }

    @Test
    void testUsageMentionsCompactSubcommands() {
        String usage = command.getUsage();
        assertThat(usage).contains("db compact ");
        assertThat(usage).contains("db compact-status");
        assertThat(usage).contains("db compact-cancel");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCommandTest"`
Expected: 4 new tests FAIL — current `DbCommand` doesn't route these subcommands.

- [ ] **Step 3: Wire the three new subcommands into `DbCommand`**

In `DbCommand.java`:

3a. Add three field declarations after `private final DbStatsCommand statsCommand;`:

```java
    private final DbCompactCommand compactCommand;
    private final DbCompactStatusCommand compactStatusCommand;
    private final DbCompactCancelCommand compactCancelCommand;
```

3b. Initialize them at the end of the constructor (after `this.statsCommand = new DbStatsCommand(dbManager);`):

```java
        this.compactCommand = new DbCompactCommand(dbManager);
        this.compactStatusCommand = new DbCompactStatusCommand(dbManager);
        this.compactCancelCommand = new DbCompactCancelCommand(dbManager);
```

3c. Add three switch cases inside the `switch (subcommand)` block (right before `default:`):

```java
            case "compact":
                compactCommand.execute(subArgs);
                break;
            case "compact-status":
                compactStatusCommand.execute(subArgs);
                break;
            case "compact-cancel":
                compactCancelCommand.execute(subArgs);
                break;
```

3d. Update `getUsage()` to include the three new subcommands. Replace the existing returned string:

```java
    @Override
    public String getUsage() {
        return "db <subcommand> [args]\n" +
               "                               Subcommands:\n" +
               "                                 db open <path> [--write]                   - Open a database (read-only by default)\n" +
               "                                 db close                                   - Close the currently open database\n" +
               "                                 db info                                    - Display database statistics\n" +
               "                                 db get <segment> <hex-key>                 - Retrieve a raw value by key\n" +
               "                                 db put <segment> <hex-key> <hex-value>     - Write a raw value by key (requires --write)\n" +
               "                                 db scan <segment> [--limit n] [--offset n] - Scan raw key-value entries\n" +
               "                                 db drop-cf <segment>                       - Drop a column family (requires --write)\n" +
               "                                 db stats [cf-name]                         - Print detailed RocksDB stats\n" +
               "                                 db compact <segment...|--all>              - Submit async manual compactions (requires --write)\n" +
               "                                 db compact-status [<job-id>]               - Show compaction job status\n" +
               "                                 db compact-cancel <job-id>                 - Cancel a running compaction job (requires --write)\n" +
               "\n" +
               "                               Column Family Formats (<segment>):\n" +
               "                                 Predefined segment names (ACCOUNT_INFO_STATE, CODE_STORAGE, etc.)\n" +
               "                                 UTF-8 names (any custom column family name)\n" +
               "                                 Hex IDs (0x06, {6}, or raw 1-byte/4-byte representations)";
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCommandTest"`
Expected: All tests PASS (existing + 4 new).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbCommand.java \
        src/test/java/net/consensys/bric/commands/DbCommandTest.java
git commit -m "$(cat <<'EOF'
feat: route db compact, compact-status, compact-cancel through DbCommand

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: REPL exit auto-cancels running jobs

**Files:**
- Modify: `src/main/java/net/consensys/bric/BricApplication.java`

This task has no unit test — `BricApplication` is integration-level and the existing project doesn't unit-test it. We rely on the manual verification in Task 12.

- [ ] **Step 1: Update the shutdown hook in `BricApplication`**

In `BricApplication.java`, the shutdown hook is at lines 70–79. Replace it with:

```java
        // Add shutdown hook to close database
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (processor.getDbManager().isOpen()) {
                    LOG.info("Closing database on shutdown...");
                    if (processor.getDbManager().getCompactionJobManager().hasRunning()) {
                        LOG.info("Cancelling running compaction jobs...");
                        processor.getDbManager().getCompactionJobManager().cancelAll();
                    }
                    processor.getDbManager().closeDatabase();
                }
            } catch (Exception e) {
                LOG.error("Error closing database on shutdown", e);
            }
        }));
```

- [ ] **Step 2: Update the REPL exit path**

The REPL exit happens in `runRepl` when the user types `exit`/`quit` or hits Ctrl-D. The current code just `break`s out of the loop and returns from `call()`, then the JVM-level shutdown hook runs and (with the change above) cancels jobs and closes.

Verify the existing behavior is intact: the shutdown hook handles both Ctrl-D (`EndOfFileException` → `break`) and the `exit`/`quit` typed commands (also `break`). No further code change is needed in `runRepl`.

- [ ] **Step 3: Build to verify no compilation errors**

Run: `./gradlew build -x test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Run the full test suite**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL with all tests passing.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/BricApplication.java
git commit -m "$(cat <<'EOF'
feat: auto-cancel running compactions on REPL exit

The shutdown hook now calls cancelAll() before closeDatabase(), so
process termination is clean even when long-running jobs are active.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 11: Autocomplete entries

**Files:**
- Modify: `src/main/java/net/consensys/bric/completion/BricCompleter.java`

- [ ] **Step 1: Add the three new subcommands to the autocomplete set**

In `BricCompleter.java`, the relevant constant is at line 25:

```java
    private static final Set<String> DB_SUBCOMMANDS = Set.of("open", "close", "info", "get", "put", "scan", "drop-cf", "stats");
```

Replace with:

```java
    private static final Set<String> DB_SUBCOMMANDS = Set.of(
        "open", "close", "info", "get", "put", "scan", "drop-cf", "stats",
        "compact", "compact-status", "compact-cancel");
```

- [ ] **Step 2: Add CF completion for the `compact` subcommand**

In the `complete` method, find the existing block (around line 79):

```java
                } else if ("drop-cf".equals(subcommand) || "stats".equals(subcommand)) {
                    if (wordIndex == 2) {
                        completeSegments(words.length > 2 ? words[2] : "", candidates);
                    }
                }
```

Replace with:

```java
                } else if ("drop-cf".equals(subcommand) || "stats".equals(subcommand)
                    || "compact".equals(subcommand)) {
                    // For compact, every positional arg is a CF (varargs); offer
                    // segment completion at any position >= 2.
                    if (wordIndex >= 2) {
                        completeSegments(words.length > wordIndex ? words[wordIndex] : "", candidates);
                    }
                }
```

- [ ] **Step 3: Build to verify no compilation errors**

Run: `./gradlew build -x test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Run the full test suite**

Run: `./gradlew test`
Expected: BUILD SUCCESSFUL with all tests passing.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/completion/BricCompleter.java
git commit -m "$(cat <<'EOF'
feat: autocomplete db compact, compact-status, compact-cancel

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: Manual verification on a real database

**Files:** None (verification only — no code changes; nothing to commit unless you discover bugs).

This is the final integration check. Use a real Besu database (or a small test database — even an empty one will work for the cancel/status flow).

- [ ] **Step 1: Build the fat JAR**

Run: `./gradlew fatJar`
Expected: BUILD SUCCESSFUL. JAR at `build/libs/bric-1.0.0-SNAPSHOT-all.jar`.

- [ ] **Step 2: Open a real Besu DB in write mode**

In a tmux/screen session (so the test can survive disconnects if you're SSH'd in):

```
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar
bric> db open /path/to/besu/database --write
```

Expected: `Successfully opened database` line.

- [ ] **Step 3: Submit a compaction and verify status**

```
bric> db compact ACCOUNT_INFO_STATE
```

Expected: `Submitted job 1: ACCOUNT_INFO_STATE` and prompt returns immediately.

```
bric> db compact-status
```

Expected: tabular output with job 1 in RUNNING state, an ELAPSED counter, and a PENDING_BYTES value (or `-` if compaction has very little to do).

```
bric> db compact-status 1
```

Expected: same row, plus a `--- Compaction Stats ---` block with multi-level RocksDB stats.

- [ ] **Step 4: Cancel and verify state transition**

Submit another compaction (e.g., on a CF you don't mind cancelling):

```
bric> db compact ACCOUNT_STORAGE_STORAGE
bric> db compact-cancel 2
```

Expected (within ~5 seconds): `Cancelled job 2`. Then:

```
bric> db compact-status
```

Expected: job 2 shows `CANCELLED` state with a finite ELAPSED.

- [ ] **Step 5: Verify cancellation isolation**

Run two compactions, cancel one, confirm the other continues. Submit a fresh job after cancellation to confirm the executor still works.

- [ ] **Step 6: Verify close-while-running refusal**

Submit a job, then try `db close`:

```
bric> db compact ACCOUNT_INFO_STATE
bric> db close
```

Expected: error message `Cannot close: compaction jobs still running: [3]. Cancel them first with 'db compact-cancel <job-id>'.`

Cancel the job, then close again:

```
bric> db compact-cancel 3
bric> db close
```

Expected: `Closed database: <path>` (no error).

- [ ] **Step 7: Verify autocomplete**

After reopening a DB, type `db comp<TAB>`. Expected: suggestions `compact`, `compact-status`, `compact-cancel`. After `db compact <TAB>` with the DB open: column-family name suggestions appear.

- [ ] **Step 8: Verify exit cleanup**

Submit a long-running compaction, then type `exit`. Expected: process terminates cleanly within ~5 seconds (no hang). Reopen the database — expected: opens cleanly (no lock-file orphans, no corruption).

If any step fails, capture the output and the relevant log lines, then debug. Otherwise the feature is complete.
