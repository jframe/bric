package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;

/**
 * Command to open a Besu database.
 */
public class DbOpenCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbOpenCommand() {
        this.dbManager = BesuDatabaseManager.getInstance();
    }

    @Override
    public void execute(String[] args) {
        if (args.length < 1) {
            System.err.println("Error: Missing database path");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String path = args[0];

        try {
            dbManager.openDatabase(path);
            System.out.println("Successfully opened database at: " + path);
            System.out.println("Database format: " + dbManager.getFormat());
            System.out.println("Column families: " + dbManager.getColumnFamilyNames().size());
        } catch (Exception e) {
            System.err.println("Error opening database: " + e.getMessage());
        }
    }

    @Override
    public String getHelp() {
        return "Open a Besu database in read-only mode";
    }

    @Override
    public String getUsage() {
        return "db-open <path>";
    }
}
