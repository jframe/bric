package net.consensys.bric.commands;

/**
 * Helper class for reporting progress at regular time intervals.
 * Useful for long-running operations like batch processing.
 */
public class ProgressReporter {

    private static final long INITIAL_DELAY_MILLIS = 3000L;

    private final long intervalMillis;
    private long lastReportTime;
    private long startTime;
    private boolean firstReportDone;

    /**
     * Create a progress reporter with the specified interval.
     * The first progress report fires after 3 seconds for early feedback,
     * then subsequent reports use the specified interval.
     *
     * @param intervalSeconds Time interval in seconds between progress updates
     */
    public ProgressReporter(int intervalSeconds) {
        this.intervalMillis = intervalSeconds * 1000L;
        this.startTime = System.currentTimeMillis();
        this.lastReportTime = startTime;
        this.firstReportDone = false;
    }

    /**
     * Check if it's time to report progress and update the last report time if so.
     * Uses a shorter initial delay for early feedback.
     *
     * @return true if progress should be reported, false otherwise
     */
    public boolean shouldReport() {
        long currentTime = System.currentTimeMillis();
        long threshold = firstReportDone ? intervalMillis : Math.min(INITIAL_DELAY_MILLIS, intervalMillis);
        if (currentTime - lastReportTime >= threshold) {
            lastReportTime = currentTime;
            firstReportDone = true;
            return true;
        }
        return false;
    }

    /**
     * Get elapsed time since start in seconds.
     *
     * @return elapsed time in seconds
     */
    public long getElapsedSeconds() {
        return (System.currentTimeMillis() - startTime) / 1000;
    }

    /**
     * Format and print a progress message with current count, total, and elapsed time.
     *
     * @param current Current progress count
     * @param total Total count
     * @param itemName Name of items being processed (e.g., "blocks")
     */
    public void reportProgress(long current, long total, String itemName) {
        if (shouldReport()) {
            long elapsed = getElapsedSeconds();
            double percentage = (current * 100.0) / total;
            System.out.printf("Progress: %d/%d %s (%.1f%%) - Elapsed: %ds%n",
                    current, total, itemName, percentage, elapsed);
        }
    }

    /**
     * Print final summary with total time.
     *
     * @param total Total count processed
     * @param itemName Name of items processed
     */
    public void reportComplete(long total, String itemName) {
        long elapsed = getElapsedSeconds();
        System.out.printf("Completed: %d %s in %ds%n", total, itemName, elapsed);
    }
}
