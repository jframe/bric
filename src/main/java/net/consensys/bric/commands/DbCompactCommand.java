package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.ColumnFamilyResolver;
import net.consensys.bric.db.CompactionJobManager;
import org.rocksdb.ColumnFamilyHandle;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Submit asynchronous manual compactions for one or more column families.
 * Use {@code db compact-status} to monitor and {@code db compact-cancel} to abort.
 */
public class DbCompactCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbCompactCommand(BesuDatabaseManager dbManager) {
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
            System.err.println("Error: Missing column family argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        // Resolve targets atomically: build the full list before submitting any job.
        Map<String, ColumnFamilyHandle> targets = new LinkedHashMap<>();
        if (args.length == 1 && "--all".equals(args[0])) {
            if (!collectAllNonEmptyCfs(targets)) {
                return;
            }
            if (targets.isEmpty()) {
                System.out.println("No non-empty column families to compact.");
                return;
            }
        } else {
            for (String input : args) {
                if ("--all".equals(input)) {
                    System.err.println(
                        "Error: --all cannot be combined with other arguments");
                    return;
                }
                ColumnFamilyHandle handle;
                try {
                    handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, input);
                } catch (IllegalArgumentException e) {
                    System.err.println("Error: " + e.getMessage());
                    return;
                }
                if (handle == null) {
                    System.err.println("Error: Column family not found: " + input);
                    System.err.println("Available in this database: "
                        + dbManager.getColumnFamilyNames().stream()
                            .sorted().collect(Collectors.joining(", ")));
                    return;
                }
                String cfName = resolveCfName(handle);
                if (cfName == null) {
                    System.err.println("Error: Could not identify column family name");
                    return;
                }
                targets.put(cfName, handle);
            }
        }

        CompactionJobManager jobManager = dbManager.getCompactionJobManager();
        for (Map.Entry<String, ColumnFamilyHandle> entry : targets.entrySet()) {
            int id = jobManager.submit(entry.getKey(), entry.getValue());
            System.out.println("Submitted job " + id + ": " + entry.getKey());
        }
    }

    /** Populate {@code targets} with all CFs whose stats are non-empty. */
    private boolean collectAllNonEmptyCfs(Map<String, ColumnFamilyHandle> targets) {
        List<String> cfNames = new ArrayList<>(dbManager.getColumnFamilyNames());
        cfNames.sort(String::compareTo);
        for (String cfName : cfNames) {
            try {
                BesuDatabaseManager.DatabaseStats stats = dbManager.getStats(cfName);
                if (stats.estimatedKeys == 0 && stats.getTotalSize() == 0) {
                    continue;
                }
            } catch (Exception e) {
                System.err.println("Warning: skipping " + cfName
                    + " (could not read stats: " + e.getMessage() + ")");
                continue;
            }
            ColumnFamilyHandle handle = dbManager.getColumnFamilyByName(cfName);
            if (handle != null) {
                targets.put(cfName, handle);
            }
        }
        return true;
    }

    /** Reverse-look-up the stored CF name for a handle. */
    private String resolveCfName(ColumnFamilyHandle handle) {
        for (String name : dbManager.getColumnFamilyNames()) {
            if (dbManager.getColumnFamilyByName(name) == handle) {
                return name;
            }
        }
        return null;
    }

    @Override
    public String getHelp() {
        return "Submit async manual compactions for one or more column families";
    }

    @Override
    public String getUsage() {
        return "db compact <segment...|--all>\n"
             + "                               Submit async compactions; use 'db compact-status'\n"
             + "                               to monitor, 'db compact-cancel <id>' to abort.\n"
             + "                               Requires --write mode.";
    }
}
