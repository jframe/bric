package net.consensys.bric.besu;

/**
 * Reports progress after each batch a paged trie walk processes (see
 * {@link FlatDbHealer#healAccountRange}/{@link FlatDbHealer#healAccountStorage}). Fires once per
 * batch, including the final one — callers apply their own rate-limiting (see
 * {@link HeartbeatThrottle}) before surfacing this to a user-facing listener.
 */
interface BatchProgressListener {
    /**
     * @param scannedInUnit running count of trie leaves scanned so far in this range/account
     * @param percentOfKeyspace how far into the full 256-bit key space the last-scanned leaf falls,
     *     0-100. Reflects the actual last leaf reached, not the nominal range/account boundary — the
     *     two can differ when no leaf exists all the way out to that boundary.
     */
    void onBatch(long scannedInUnit, double percentOfKeyspace);
}
