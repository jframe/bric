package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;

import java.util.stream.Collectors;
import java.util.stream.Stream;

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

        String segmentName = args[0].toUpperCase();
        KeyValueSegmentIdentifier segment = resolveSegment(segmentName);
        if (segment == null) {
            System.err.println("Error: Unknown segment '" + segmentName + "'");
            System.err.println("Available segments: " + getAvailableSegments());
            return;
        }

        String cfName = segment.getName();
        if (!dbManager.getColumnFamilyNames().contains(cfName)) {
            System.err.println("Error: Segment '" + cfName + "' is not present in this database");
            System.err.println("Available in this database: " +
                dbManager.getColumnFamilyNames().stream().sorted().collect(Collectors.joining(", ")));
            return;
        }

        try {
            dbManager.dropColumnFamily(cfName);
            System.out.println("Dropped column family: " + cfName);
        } catch (Exception e) {
            System.err.println("Error: Failed to drop column family: " + e.getMessage());
        }
    }

    private KeyValueSegmentIdentifier resolveSegment(String name) {
        try {
            return KeyValueSegmentIdentifier.valueOf(name);
        } catch (IllegalArgumentException e) {
            // fall through
        }
        for (KeyValueSegmentIdentifier seg : KeyValueSegmentIdentifier.values()) {
            if (seg.getName().equalsIgnoreCase(name)) {
                return seg;
            }
        }
        return null;
    }

    private String getAvailableSegments() {
        return Stream.of(KeyValueSegmentIdentifier.values())
            .map(KeyValueSegmentIdentifier::getName)
            .collect(Collectors.joining(", "));
    }

    @Override
    public String getHelp() {
        return "Drop a column family from the database (requires --write mode)";
    }

    @Override
    public String getUsage() {
        return "db drop-cf <segment>\n" +
               "                               Examples:\n" +
               "                                 db drop-cf TRIE_LOG_STORAGE\n" +
               "                                 db drop-cf ACCOUNT_INFO_STATE_ARCHIVE";
    }
}
