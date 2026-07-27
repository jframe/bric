package net.consensys.bric.besu;

/** Aggregated outcome of a full {@link FlatDbHealer#heal(boolean, FlatDbHealProgressListener)} run. */
public class FlatDbHealResult {
    public final long accountsAdded;
    public final long accountsUpdated;
    public final long accountsRemoved;
    public final long slotsAdded;
    public final long slotsUpdated;
    public final long slotsRemoved;
    /**
     * Count of accounts whose codeHash has no matching entry in CODE_STORAGE. Not included in
     * {@link #totalAccountsFixed()} — unlike accounts/storage, missing code can't be re-derived
     * locally, so this is a detection-only count, never fixed by this run regardless of dryRun.
     */
    public final long accountsWithMissingCode;
    public final boolean dryRun;

    public FlatDbHealResult(
            long accountsAdded, long accountsUpdated, long accountsRemoved,
            long slotsAdded, long slotsUpdated, long slotsRemoved,
            long accountsWithMissingCode, boolean dryRun) {
        this.accountsAdded = accountsAdded;
        this.accountsUpdated = accountsUpdated;
        this.accountsRemoved = accountsRemoved;
        this.slotsAdded = slotsAdded;
        this.slotsUpdated = slotsUpdated;
        this.slotsRemoved = slotsRemoved;
        this.accountsWithMissingCode = accountsWithMissingCode;
        this.dryRun = dryRun;
    }

    public long totalAccountsFixed() {
        return accountsAdded + accountsUpdated + accountsRemoved;
    }

    public long totalSlotsFixed() {
        return slotsAdded + slotsUpdated + slotsRemoved;
    }
}
