package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.ColumnFamilyResolver;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import net.consensys.bric.db.SegmentReader;
import org.apache.tuweni.bytes.Bytes;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;

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
            System.err.println("Error: Missing segment");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String cfInput = args[0];
        int limit = Integer.MAX_VALUE;

        // Parse optional --limit flag
        for (int i = 1; i < args.length; i++) {
            if ("--limit".equals(args[i]) && i + 1 < args.length) {
                try {
                    int parsedLimit = Integer.parseInt(args[i + 1]);
                    if (parsedLimit < 1) {
                        System.err.println("Error: Invalid limit value: " + args[i + 1]);
                        return;
                    }
                    limit = parsedLimit;
                } catch (NumberFormatException e) {
                    System.err.println("Error: Invalid limit value: " + args[i + 1]);
                    return;
                }
                i++; // Skip next arg
            }
        }

        // Resolve CF using ColumnFamilyResolver
        ColumnFamilyHandle handle;
        try {
            handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, cfInput);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }

        if (handle == null) {
            System.err.println("Error: Column family not found: " + cfInput);
            return;
        }

        // Scan and display
        try (RocksIterator iterator = dbManager.getDatabase().newIterator(handle)) {
            iterator.seekToFirst();
            int count = 0;
            while (iterator.isValid() && count < limit) {
                byte[] key = iterator.key();
                byte[] value = iterator.value();
                System.out.println("Key: " + bytesToHex(key) + " -> Value: " + bytesToHex(value));
                iterator.next();
                count++;
            }
            if (count == 0) {
                System.out.println("(no entries found)");
            } else if (count >= limit && iterator.isValid()) {
                System.out.println("(Limited to " + limit + " entries)");
            } else if (count > 0) {
                System.out.println("Displayed " + count + " entries");
            }
        } catch (Exception e) {
            System.err.println("Error scanning column family: " + e.getMessage());
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder("0x");
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public String getHelp() {
        return "Scan raw key-value entries in a column family";
    }

    @Override
    public String getUsage() {
        return "db scan <segment|name|hex> [--limit <count>]\n" +
               "                               Scan entries in a column family\n" +
               "                               Examples:\n" +
               "                                 db scan TRIE_LOG_STORAGE\n" +
               "                                 db scan \"CUSTOM_CF\" --limit 10\n" +
               "                                 db scan 0x0a --limit 5";
    }
}
