package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.ColumnFamilyResolver;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * Command to retrieve a raw value from a column family by key.
 */
public class DbGetCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbGetCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (args.length < 2) {
            System.err.println("Error: Missing segment and/or key");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String cfInput = args[0];
        String keyHex = args[1];

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

        // Parse key as hex
        byte[] key;
        try {
            key = ColumnFamilyResolver.parseInput(keyHex);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: Invalid key format: " + e.getMessage());
            return;
        }

        // Read value from database
        try {
            byte[] value = dbManager.getDatabase().get(handle, key);
            if (value == null) {
                System.out.println("Key not found: " + keyHex);
            } else {
                System.out.println("Key: " + bytesToHex(key));
                System.out.println("Value: " + bytesToHex(value));
            }
        } catch (RocksDBException e) {
            System.err.println("Error reading from database: " + e.getMessage());
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
        return "Retrieve a raw value from a column family by hex key";
    }

    @Override
    public String getUsage() {
        return "db get <segment|name|hex> <key-hex>\n" +
               "                               Read a value from a column family\n" +
               "                               Examples:\n" +
               "                                 db get TRIE_LOG_STORAGE 0xabcdef\n" +
               "                                 db get \"CUSTOM_CF\" 0xabcdef\n" +
               "                                 db get 0x0a 0xabcdef";
    }
}
