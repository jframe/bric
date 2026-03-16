package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.ColumnFamilyResolver;
import org.rocksdb.ColumnFamilyHandle;

import java.util.stream.Collectors;

/**
 * Command to drop a column family from the open RocksDB database.
 * Requires the database to be open in write mode.
 */
public class DbDropCfCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbDropCfCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        // Guard: default CF check fires before any DB state check
        if (args.length > 0 && "default".equalsIgnoreCase(args[0])) {
            System.err.println("Error: Cannot drop the default column family");
            return;
        }

        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (!dbManager.isWritable()) {
            System.err.println("Error: Database is open in read-only mode. Reopen with 'db open <path> --write'.");
            return;
        }

        if (args.length < 1) {
            System.err.println("Error: Missing segment name");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String input = args[0];

        // Try to resolve CF using ColumnFamilyResolver
        ColumnFamilyHandle handle;
        try {
            handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, input);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }

        if (handle == null) {
            System.err.println("Error: Column family not found: " + input);
            System.err.println("Available in this database: " +
                dbManager.getColumnFamilyNames().stream().sorted().collect(Collectors.joining(", ")));
            return;
        }

        // Get the actual CF name to drop (reverse lookup from handle)
        String cfNameToDrop = null;
        for (String cfName : dbManager.getColumnFamilyNames()) {
            ColumnFamilyHandle testHandle = dbManager.getColumnFamilyByName(cfName);
            if (testHandle == handle) {
                cfNameToDrop = cfName;
                break;
            }
        }

        if (cfNameToDrop == null) {
            System.err.println("Error: Could not identify column family name");
            return;
        }

        try {
            dbManager.dropColumnFamily(cfNameToDrop);
            System.out.println("Dropped column family: " + cfNameToDrop);
        } catch (Exception e) {
            System.err.println("Error: Failed to drop column family: " + e.getMessage());
        }
    }

    @Override
    public String getHelp() {
        return "Drop a column family from the database (requires --write mode)";
    }

    @Override
    public String getUsage() {
        return "db drop-cf <segment|name|hex>\n" +
               "                               Drop a column family (requires --write mode)\n" +
               "                               Examples:\n" +
               "                                 db drop-cf TRIE_LOG_STORAGE (enum)\n" +
               "                                 db drop-cf \"CUSTOM_CF\" (UTF-8 name)\n" +
               "                                 db drop-cf 0x0a (hex ID)";
    }
}
