package net.consensys.bric.besu;

/** Callback for reporting progress during a {@link FlatDbHealer#heal} run. */
public interface FlatDbHealProgressListener {
    void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed);

    void onStorageAccountComplete(int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed);

    /**
     * Reports incremental progress partway through a single account range's walk, so a range
     * spanning many batches doesn't sit silent between {@link #onRangeComplete} events. Fired once
     * per completed batch except the last, whose totals {@code onRangeComplete} already reports.
     * {@code accountsScannedInRange} is the running count of accounts walked in this range so far.
     * Defaults to no-op so listeners that don't care about sub-range progress need not implement it.
     */
    default void onRangeProgress(int rangeIndex, int totalRanges, long accountsScannedInRange) {
    }

    /** Listener that discards every event — used by tests and dry-run callers that don't print progress. */
    FlatDbHealProgressListener NO_OP = new FlatDbHealProgressListener() {
        @Override
        public void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed) {
        }

        @Override
        public void onStorageAccountComplete(
                int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed) {
        }
    };
}
