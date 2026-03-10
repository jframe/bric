package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;

/**
 * Command to open a Besu database.
 */
public class DbOpenCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbOpenCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (args.length < 1) {
            System.err.println("Error: Missing database path");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String path = InputParser.stripQuotes(args[0]);

        // Expand tilde to user home directory
        if (path.startsWith("~")) {
            path = System.getProperty("user.home") + path.substring(1);
        }

        boolean writable = false;
        for (int i = 1; i < args.length; i++) {
            if ("--write".equals(args[i])) {
                writable = true;
            }
        }

        try {
            dbManager.openDatabase(path, writable);
            System.out.println("Successfully opened database at: " + path);
            System.out.println("Database format: " + dbManager.getFormat());
            System.out.println("Column families: " + dbManager.getColumnFamilyNames().size());
            if (writable) {
                System.out.println("Warning: database opened in write mode");
            }
        } catch (Exception e) {
            System.err.println("Error opening database: " + e.getMessage());
        }
    }

    @Override
    public String getHelp() {
        return "Open a Besu database (read-only by default, use --write for write access)";
    }

    @Override
    public String getUsage() {
        return "db open <path> [--write]\n" +
               "                               Examples:\n" +
               "                                 db open /path/to/besu/database\n" +
               "                                 db open ~/besu-data/database --write";
    }
}
