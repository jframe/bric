package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseManager.DatabaseStats;

import java.util.ArrayList;
import java.util.List;

/**
 * Command to display database information and statistics.
 */
public class DbInfoCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbInfoCommand() {
        this.dbManager = BesuDatabaseManager.getInstance();
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db-open <path>' first.");
            return;
        }

        System.out.println("\nDatabase Information:");
        System.out.println("  Path: " + dbManager.getCurrentPath());
        System.out.println("  Format: " + dbManager.getFormat());
        System.out.println();

        System.out.println("Column Family Statistics:");
        System.out.println("─".repeat(100));
        System.out.printf("%-35s %15s %15s %15s %15s%n",
            "Column Family", "Keys (est)", "SST Size", "Blob Size", "Total Size");
        System.out.println("─".repeat(100));

        List<DatabaseStats> allStats = new ArrayList<>();
        long totalKeys = 0;
        long totalSize = 0;

        for (String cfName : dbManager.getColumnFamilyNames()) {
            try {
                DatabaseStats stats = dbManager.getStats(cfName);
                allStats.add(stats);

                String keysStr = stats.estimatedKeys >= 0
                    ? formatNumber(stats.estimatedKeys)
                    : "N/A";
                String sstStr = stats.totalSstSize >= 0
                    ? formatBytes(stats.totalSstSize)
                    : "N/A";
                String blobStr = stats.totalBlobSize >= 0
                    ? formatBytes(stats.totalBlobSize)
                    : "N/A";
                String totalStr = formatBytes(stats.getTotalSize());

                System.out.printf("%-35s %15s %15s %15s %15s%n",
                    truncate(cfName, 35), keysStr, sstStr, blobStr, totalStr);

                if (stats.estimatedKeys >= 0) {
                    totalKeys += stats.estimatedKeys;
                }
                totalSize += stats.getTotalSize();

            } catch (Exception e) {
                System.out.printf("%-35s %15s%n", truncate(cfName, 35), "Error: " + e.getMessage());
            }
        }

        System.out.println("─".repeat(100));
        System.out.printf("%-35s %15s %15s%n",
            "TOTAL", formatNumber(totalKeys), formatBytes(totalSize));
        System.out.println();
    }

    private String formatBytes(long bytes) {
        if (bytes < 0) {
            return "N/A";
        }
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
        }
    }

    private String formatNumber(long number) {
        return String.format("%,d", number);
    }

    private String truncate(String str, int maxLength) {
        if (str.length() <= maxLength) {
            return str;
        }
        return str.substring(0, maxLength - 3) + "...";
    }

    @Override
    public String getHelp() {
        return "Display database statistics and column family information";
    }

    @Override
    public String getUsage() {
        return "db-info";
    }
}
