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
