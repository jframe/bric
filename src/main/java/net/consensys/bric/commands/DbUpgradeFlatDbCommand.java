package net.consensys.bric.commands;

import net.consensys.bric.besu.FlatDbHealCancelledException;
import net.consensys.bric.besu.FlatDbHealProgressListener;
import net.consensys.bric.besu.FlatDbHealResult;
import net.consensys.bric.besu.FlatDbHealer;
import net.consensys.bric.db.BesuDatabaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

/**
 * Reconciles a Bonsai database's flat account/storage tables against the account and
 * storage tries, then marks the flat DB mode FULL. See
 * docs/superpowers/specs/2026-07-24-flatdb-heal-design.md for the algorithm.
 */
public class DbUpgradeFlatDbCommand implements Command {

    private static final Logger LOG = LoggerFactory.getLogger(DbUpgradeFlatDbCommand.class);

    private final BesuDatabaseManager dbManager;

    public DbUpgradeFlatDbCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        BesuDatabaseManager.DatabaseFormat format = dbManager.getFormat();
        boolean isBonsai = format == BesuDatabaseManager.DatabaseFormat.BONSAI;
        boolean isBonsaiArchive = format == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;
        if (!isBonsai && !isBonsaiArchive) {
            System.err.println(
                "Error: Flat DB healing is only supported for Bonsai databases. Current format: " + format);
            return;
        }

        boolean dryRun = Arrays.asList(args).contains("--dry-run");
        if (!dryRun && !dbManager.isWritable()) {
            System.err.println(
                "Error: Database is open in read-only mode. Reopen with 'db open <path> --write'.");
            return;
        }

        if (dryRun) {
            System.out.println("Dry run - no changes will be written.");
        }

        FlatDbHealer healer;
        try {
            healer = new FlatDbHealer(dbManager);
        } catch (Exception e) {
            System.err.println("Error: Failed to initialize database healer: " + e.getMessage());
            return;
        }

        Instant start = Instant.now();
        FlatDbHealResult result;
        try {
            result = healer.heal(dryRun, new PrintingProgressListener());
        } catch (FlatDbHealCancelledException e) {
            System.out.println("Cancelled - no incomplete state was left behind"
                + (dryRun ? "." : "; re-run to resume from the last checkpoint."));
            return;
        } catch (RuntimeException e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }
        Duration elapsed = Duration.between(start, Instant.now());

        if (dryRun) {
            System.out.println("Would fix " + result.totalAccountsFixed() + " accounts and "
                + result.totalSlotsFixed() + " storage slots.");
        } else {
            System.out.println("Flat DB mode upgraded to " + (isBonsaiArchive ? "ARCHIVE" : "FULL") + ".");
            System.out.println(String.format(
                "Done in %02d:%02d:%02d. Accounts: %d fixed. Storage slots: %d fixed.",
                elapsed.toHoursPart(), elapsed.toMinutesPart(), elapsed.toSecondsPart(),
                result.totalAccountsFixed(), result.totalSlotsFixed()));
        }
    }

    private static final class PrintingProgressListener implements FlatDbHealProgressListener {
        @Override
        public void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed) {
            System.out.printf("Range %d/%d - %d accounts checked, %d fixed (%.1f%% of account phase complete)%n",
                rangeIndex, totalRanges, accountsChecked, accountsFixed, rangeIndex * 100.0 / totalRanges);
        }

        @Override
        public void onRangeHeartbeat(int rangeIndex, int totalRanges, double percentComplete) {
            LOG.info("Range {}/{} - still running ({}% of account phase complete)",
                rangeIndex, totalRanges, String.format("%.1f", percentComplete));
        }

        @Override
        public void onStorageAccountComplete(
                int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed) {
            System.out.printf(
                "Storage %d/%d accounts - %d slots checked, %d fixed (%.1f%% of storage phase complete)%n",
                accountsHealed, totalAccountsToHeal, slotsChecked, slotsFixed,
                accountsHealed * 100.0 / totalAccountsToHeal);
        }

        @Override
        public void onStorageHeartbeat(int accountsHealed, int totalAccountsToHeal, double percentComplete) {
            LOG.info("Storage {}/{} accounts - still running ({}% of this account's storage scanned)",
                accountsHealed, totalAccountsToHeal, String.format("%.1f", percentComplete));
        }
    }

    @Override
    public String getHelp() {
        return "Upgrade a Bonsai database's flat DB from PARTIAL to FULL by reconciling it against the tries";
    }

    @Override
    public String getUsage() {
        return "db upgrade-flatdb [--dry-run]\n"
             + "                               Reconciles ACCOUNT_INFO_STATE/ACCOUNT_STORAGE_STORAGE\n"
             + "                               against the account/storage tries. Requires --write mode\n"
             + "                               unless --dry-run (which only reports, never writes).";
    }
}
