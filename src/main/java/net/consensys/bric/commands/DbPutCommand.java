package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.ColumnFamilyHandle;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command to write a raw value to a column family by key.
 */
public class DbPutCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbPutCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (!dbManager.isWritable()) {
            System.err.println("Error: Database is open in read-only mode. Reopen with 'db open <path> --write'.");
            return;
        }

        if (args.length < 3) {
            System.err.println("Error: Missing arguments");
            System.err.println("Usage: " + getUsage());
            System.err.println("\nAvailable segments: " + getAvailableSegments());
            return;
        }

        String segmentName = args[0].toUpperCase();
        String keyHex = args[1];
        String valueHex = args[2];

        KeyValueSegmentIdentifier segment = resolveSegment(segmentName);
        if (segment == null) {
            System.err.println("Error: Unknown segment '" + segmentName + "'");
            System.err.println("Available segments: " + getAvailableSegments());
            return;
        }

        if (!dbManager.getColumnFamilyNames().contains(segment.getName())) {
            System.err.println("Error: Segment '" + segment.getName() + "' is not present in this database");
            System.err.println("Available in this database: " +
                dbManager.getColumnFamilyNames().stream().sorted().collect(Collectors.joining(", ")));
            return;
        }

        byte[] key;
        try {
            key = InputParser.parseKeyBytes(keyHex);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: Invalid key: " + keyHex);
            return;
        }

        byte[] value;
        try {
            value = InputParser.parseKeyBytes(valueHex);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: Invalid value: " + valueHex);
            return;
        }

        try {
            ColumnFamilyHandle handle = dbManager.getColumnFamily(segment);
            dbManager.put(handle, key, value);
            System.out.println("OK");
            System.out.println("Key   (" + key.length + " bytes): " + Bytes.wrap(key).toHexString());
            System.out.println("Value (" + value.length + " bytes): " + Bytes.wrap(value).toHexString());
        } catch (Exception e) {
            System.err.println("Error writing to database: " + e.getMessage());
        }
    }

    private KeyValueSegmentIdentifier resolveSegment(String name) {
        try {
            return KeyValueSegmentIdentifier.valueOf(name);
        } catch (IllegalArgumentException e) {
            // Fall through to name comparison
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
        return "Write a raw value to a column family by hex key (requires --write mode)";
    }

    @Override
    public String getUsage() {
        return "db put <segment> <key> <value>\n" +
               "                               Key/value formats: 0xdeadbeef (hex) or \"string\" (UTF-8)\n" +
               "                               Examples:\n" +
               "                                 db put ACCOUNT_INFO_STATE 0x1234abcd... 0xdeadbeef...\n" +
               "                                 db put VARIABLES \"MY_KEY\" \"MY_VALUE\"";
    }
}
