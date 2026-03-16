package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.ColumnFamilyResolver;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

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
            System.err.println("Error: Missing segment, key, and/or value");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String cfInput = args[0];
        String keyHex = args[1];
        String valueHex = args[2];

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

        // Parse key and value as hex
        byte[] key, value;
        try {
            key = ColumnFamilyResolver.parseInput(keyHex);
            value = ColumnFamilyResolver.parseInput(valueHex);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: Invalid key or value format: " + e.getMessage());
            return;
        }

        // Write value to database
        try {
            dbManager.getDatabase().put(handle, key, value);
            System.out.println("Wrote to column family: " + cfInput);
            System.out.println("Key: " + bytesToHex(key));
            System.out.println("Value: " + bytesToHex(value));
        } catch (RocksDBException e) {
            System.err.println("Error writing to database: " + e.getMessage());
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
        return "Write a raw value to a column family by hex key (requires --write mode)";
    }

    @Override
    public String getUsage() {
        return "db put <segment|name|hex> <key-hex> <value-hex>\n" +
               "                               Write a value to a column family (requires --write mode)\n" +
               "                               Examples:\n" +
               "                                 db put TRIE_LOG_STORAGE 0xabcd 0xdeadbeef\n" +
               "                                 db put \"CUSTOM_CF\" 0xabcd 0xdeadbeef\n" +
               "                                 db put 0x0a 0xabcd 0xdeadbeef";
    }
}
