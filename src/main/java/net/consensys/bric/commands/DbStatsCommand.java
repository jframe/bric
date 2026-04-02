package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.ColumnFamilyResolver;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class DbStatsCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbStatsCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        RocksDB db = dbManager.getDatabase();

        // DB-level summary
        int liveFiles = db.getLiveFilesMetaData().size();
        int maxOpenFiles = dbManager.getMaxOpenFiles();
        String maxStr = maxOpenFiles == -1 ? "unlimited" : String.valueOf(maxOpenFiles);
        System.out.println("\nDB-Level Stats:");
        System.out.println("  Open SST files: " + liveFiles + " / " + maxStr + " (max_open_files)");
        System.out.println();

        if (args.length > 0) {
            // Single CF mode
            String cfInput = args[0];
            ColumnFamilyHandle handle;
            try {
                handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, cfInput);
            } catch (IllegalArgumentException e) {
                System.err.println("Error: " + e.getMessage());
                return;
            }
            if (handle == null) {
                System.err.println("Error: Column family not found: " + cfInput);
                System.err.println("Available column families: " + dbManager.getColumnFamilyNames());
                return;
            }
            printCfStats(db, handle, cfInput.toUpperCase());
        } else {
            // All CFs mode — skip empty ones
            for (String cfName : dbManager.getColumnFamilyNames()) {
                try {
                    BesuDatabaseManager.DatabaseStats summary = dbManager.getStats(cfName);
                    if (summary.estimatedKeys == 0 && summary.getTotalSize() == 0) {
                        continue;
                    }
                    ColumnFamilyHandle handle = dbManager.getColumnFamilyByName(cfName);
                    if (handle == null) {
                        continue;
                    }
                    printCfStats(db, handle, cfName);
                } catch (Exception e) {
                    System.err.println("Warning: Could not get stats for " + cfName
                        + ": " + e.getMessage());
                }
            }
        }
    }

    private void printCfStats(RocksDB db, ColumnFamilyHandle handle, String cfName) {
        System.out.println("═".repeat(80));
        System.out.println("Column Family: " + cfName);
        System.out.println("═".repeat(80));

        printProperty(db, handle, "rocksdb.stats", "RocksDB Stats");
        printProperty(db, handle, "rocksdb.levelstats", "Level Stats");
        printPropertyFiltered(db, handle, "rocksdb.sstables", "SST Files", "blob_file_number:");
    }

    private void printProperty(RocksDB db, ColumnFamilyHandle handle,
                               String property, String label) {
        try {
            String value = db.getProperty(handle, property);
            if (value != null && !value.isBlank()) {
                System.out.println("\n--- " + label + " ---");
                System.out.println(value);
            }
        } catch (RocksDBException e) {
            System.out.println("\n--- " + label + " (unavailable) ---");
        }
    }

    private void printPropertyFiltered(RocksDB db, ColumnFamilyHandle handle,
                                       String property, String label, String... excludePrefixes) {
        try {
            String value = db.getProperty(handle, property);
            if (value != null && !value.isBlank()) {
                StringBuilder filtered = new StringBuilder();
                for (String line : value.split("\n")) {
                    boolean exclude = false;
                    for (String prefix : excludePrefixes) {
                        if (line.trim().startsWith(prefix)) {
                            exclude = true;
                            break;
                        }
                    }
                    if (!exclude) {
                        filtered.append(line).append("\n");
                    }
                }
                String result = filtered.toString().stripTrailing();
                if (!result.isBlank()) {
                    System.out.println("\n--- " + label + " ---");
                    System.out.println(result);
                }
            }
        } catch (RocksDBException e) {
            System.out.println("\n--- " + label + " (unavailable) ---");
        }
    }

    @Override
    public String getHelp() {
        return "Print detailed RocksDB stats: compaction, levels, SST files, and open file counts";
    }

    @Override
    public String getUsage() {
        return "db stats [cf-name]";
    }
}
