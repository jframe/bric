package net.consensys.bric.besu;

/** Callback for reporting progress during a {@link FlatDbHealer#heal} run. */
public interface FlatDbHealProgressListener {
    void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed);

    void onStorageAccountComplete(int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed);

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
