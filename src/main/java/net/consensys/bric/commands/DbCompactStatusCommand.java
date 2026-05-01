package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.CompactionJob;
import net.consensys.bric.db.CompactionJobManager;
import org.rocksdb.ColumnFamilyHandle;
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
