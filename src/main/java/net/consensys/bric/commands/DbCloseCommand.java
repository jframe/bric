package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;

/**
 * Command to close the currently open database.
 */
public class DbCloseCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbCloseCommand() {
        this.dbManager = BesuDatabaseManager.getInstance();
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.out.println("No database is currently open");
            return;
        }

        String path = dbManager.getCurrentPath();
        dbManager.closeDatabase();
        System.out.println("Closed database: " + path);
    }

    @Override
    public String getHelp() {
        return "Close the currently open database";
    }

    @Override
    public String getUsage() {
        return "db-close";
    }
}
