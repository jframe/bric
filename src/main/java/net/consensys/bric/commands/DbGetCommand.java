package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import net.consensys.bric.db.SegmentReader;
import org.apache.tuweni.bytes.Bytes;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command to retrieve a raw value from a column family by key.
 */
public class DbGetCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final SegmentReader segmentReader;

    public DbGetCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.segmentReader = new SegmentReader(dbManager);
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (args.length < 2) {
            System.err.println("Error: Missing arguments");
            System.err.println("Usage: " + getUsage());
            System.err.println("\nAvailable segments: " + getAvailableSegments());
            return;
        }

        String segmentName = args[0].toUpperCase();
        String keyHex = args[1];

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

        Optional<byte[]> value = segmentReader.get(segment, key);
        if (value.isEmpty()) {
            System.out.println("Not found");
            return;
        }

        byte[] raw = value.get();
        System.out.println("Key   (" + key.length + " bytes): " + Bytes.wrap(key).toHexString());
        System.out.println("Value (" + raw.length + " bytes): " + Bytes.wrap(raw).toHexString());
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
        return "Retrieve a raw value from a column family by hex key";
    }

    @Override
    public String getUsage() {
        return "db get <segment> <key>\n" +
               "                               Key formats: 0xdeadbeef (hex) or \"string\" (UTF-8)\n" +
               "                               Examples:\n" +
               "                                 db get ACCOUNT_INFO_STATE 0x1234abcd...\n" +
               "                                 db get VARIABLES \"FLAT_DB_MODE\"";
    }
}
