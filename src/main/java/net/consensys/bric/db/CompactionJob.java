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
