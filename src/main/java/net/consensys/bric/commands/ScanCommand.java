package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import net.consensys.bric.db.SegmentReader;
import org.apache.tuweni.bytes.Bytes;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Command to scan and list raw key-value entries in a column family.
 * Useful for exploring unknown database state.
 */
public class ScanCommand implements Command {

    private static final int DEFAULT_LIMIT = 20;

    private final BesuDatabaseManager dbManager;
    private final SegmentReader segmentReader;

    public ScanCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.segmentReader = new SegmentReader(dbManager);
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (args.length < 1) {
            System.err.println("Error: Missing segment name");
            System.err.println("Usage: " + getUsage());
            System.err.println("\nAvailable segments: " + getAvailableSegments());
            return;
        }

        String segmentName = args[0].toUpperCase();
        int limit = DEFAULT_LIMIT;
        int offset = 0;

        // Parse optional flags
        for (int i = 1; i < args.length; i++) {
            if ("--limit".equals(args[i]) && i + 1 < args.length) {
                try {
                    limit = Integer.parseInt(args[i + 1]);
                    if (limit < 1) {
                        System.err.println("Error: Limit must be at least 1");
                        return;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Error: Invalid limit value: " + args[i + 1]);
                    return;
                }
                i++;
            } else if ("--offset".equals(args[i]) && i + 1 < args.length) {
                try {
                    offset = Integer.parseInt(args[i + 1]);
                    if (offset < 0) {
                        System.err.println("Error: Offset cannot be negative");
                        return;
                    }
                } catch (NumberFormatException e) {
                    System.err.println("Error: Invalid offset value: " + args[i + 1]);
                    return;
                }
                i++;
            }
        }

        // Resolve the segment
        KeyValueSegmentIdentifier segment = resolveSegment(segmentName);
        if (segment == null) {
            System.err.println("Error: Unknown segment '" + segmentName + "'");
            System.err.println("Available segments: " + getAvailableSegments());
            return;
        }

        // Check that the column family exists in the open database
        if (!dbManager.getColumnFamilyNames().contains(segment.getName())) {
            System.err.println("Error: Segment '" + segment.getName() + "' is not present in this database");
            System.err.println("Available in this database: " +
                dbManager.getColumnFamilyNames().stream().sorted().collect(Collectors.joining(", ")));
            return;
        }

        scanSegment(segment, offset, limit);
    }

    private void scanSegment(KeyValueSegmentIdentifier segment, int offset, int limit) {
        long estimatedKeys = segmentReader.countEstimated(segment);

        System.out.println("\nScan: " + segment.getName());
        System.out.println("Estimated keys: " + String.format("%,d", estimatedKeys));
        if (offset > 0) {
            System.out.println("Offset: " + String.format("%,d", offset));
        }
        System.out.println("Limit: " + limit);
        System.out.println();

        int[] displayed = {0};
        int[] skipped = {0};
        final int finalLimit = limit;
        final int finalOffset = offset;

        segmentReader.iterateKeyValue(segment, (key, value) -> {
            if (skipped[0] < finalOffset) {
                skipped[0]++;
                return;
            }
            if (displayed[0] >= finalLimit) {
                return;
            }
            displayed[0]++;

            String keyHex = Bytes.wrap(key).toHexString();

            System.out.printf("[%d] Key (%d bytes): %s%n",
                (long) finalOffset + displayed[0], key.length, truncateHex(keyHex, 80));
            System.out.printf("    Value (%d bytes): %s%n",
                value.length, truncateHex(Bytes.wrap(value).toHexString(), 80));
        });

        if (displayed[0] == 0) {
            System.out.println("(no entries found)");
        } else {
            System.out.println();
            System.out.println("Displayed " + displayed[0] + " entries" +
                (offset > 0 ? " (starting from offset " + offset + ")" : ""));
        }
    }

    /**
     * Truncate a hex string for display, showing start and end.
     */
    private String truncateHex(String hex, int maxLen) {
        if (hex.length() <= maxLen) {
            return hex;
        }
        int keep = (maxLen - 3) / 2;
        return hex.substring(0, keep) + "..." + hex.substring(hex.length() - keep);
    }

    /**
     * Resolve a segment name (case-insensitive, supports short aliases).
     */
    private KeyValueSegmentIdentifier resolveSegment(String name) {
        // Try exact enum match first
        try {
            return KeyValueSegmentIdentifier.valueOf(name);
        } catch (IllegalArgumentException e) {
            // Fall through to alias matching
        }

        // Support short aliases
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
        return "Scan raw key-value entries in a column family";
    }

    @Override
    public String getUsage() {
        return "scan <segment> [--limit <n>] [--offset <n>]\n" +
               "                               Defaults: limit=20, offset=0\n" +
               "                               Examples:\n" +
               "                                 scan ACCOUNT_INFO_STATE\n" +
               "                                 scan TRIE_LOG_STORAGE --limit 5\n" +
               "                                 scan CODE_STORAGE --offset 100 --limit 10";
    }
}
