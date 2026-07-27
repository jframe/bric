package net.consensys.bric.besu;

import org.hyperledger.besu.datatypes.Hash;

/** Callback for reporting progress during a {@link FlatDbHealer#heal} run. */
public interface FlatDbHealProgressListener {
    void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed);

    void onStorageAccountComplete(int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed);

    /**
     * Reports a periodic (at most once per minute — see {@link HeartbeatThrottle}) heartbeat while
     * walking a single account range, so a range large enough to span many batches doesn't sit
     * silent between {@link #onRangeComplete} calls. {@code percentComplete} (0-100) is the fraction
     * of the full 256-bit key space scanned so far. Defaults to no-op.
     */
    default void onRangeHeartbeat(int rangeIndex, int totalRanges, double percentComplete) {
    }

    /**
     * Storage-phase counterpart of {@link #onRangeHeartbeat}: a periodic heartbeat while walking a
     * single account's storage trie, reporting how far into that account's key space the walk has
     * reached. Defaults to no-op.
     */
    default void onStorageHeartbeat(int accountsHealed, int totalAccountsToHeal, double percentComplete) {
    }

    /**
     * Reports an account whose codeHash (read from the trie) has no matching entry in
     * {@code CODE_STORAGE} — invisible to the account-level add/update/remove diff, since the
     * account trie only stores the codeHash, never the bytecode itself. Fired immediately per
     * finding rather than buffered, since accumulating every affected account in memory could
     * itself grow unbounded on a badly-affected mainnet-size database. Detection only: unlike
     * accounts/storage, missing code can't be re-derived locally (no trie-derivable source of truth
     * for arbitrary bytecode bytes). Defaults to no-op.
     */
    default void onMissingCode(Hash accountHash, Hash codeHash) {
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
