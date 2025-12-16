package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;

import java.util.Arrays;

/**
 * Parent command for database operations with subcommands.
 * Supports: db open, db close, db info
 */
public class DbCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final DbOpenCommand openCommand;
    private final DbCloseCommand closeCommand;
    private final DbInfoCommand infoCommand;

    public DbCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.openCommand = new DbOpenCommand(dbManager);
        this.closeCommand = new DbCloseCommand(dbManager);
        this.infoCommand = new DbInfoCommand(dbManager);
    }

    @Override
    public void execute(String[] args) {
        if (args.length == 0) {
            System.err.println("Error: Missing subcommand");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String subcommand = args[0].toLowerCase();
        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        switch (subcommand) {
            case "open":
                openCommand.execute(subArgs);
                break;
            case "close":
                closeCommand.execute(subArgs);
                break;
            case "info":
                infoCommand.execute(subArgs);
                break;
            default:
                System.err.println("Error: Unknown subcommand '" + subcommand + "'");
                System.err.println("Usage: " + getUsage());
                break;
        }
    }

    @Override
    public String getHelp() {
        return "Database operations (open, close, info)";
    }

    @Override
    public String getUsage() {
        return "db <subcommand> [args]\n" +
               "                               Subcommands:\n" +
               "                                 db open <path>       - Open a database in read-only mode\n" +
               "                                 db close             - Close the currently open database\n" +
               "                                 db info              - Display database statistics";
    }
}
