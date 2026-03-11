package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;

import java.util.Arrays;

/**
 * Parent command for database operations with subcommands.
 * Supports: db open, db close, db info, db get, db scan
 */
public class DbCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final DbOpenCommand openCommand;
    private final DbCloseCommand closeCommand;
    private final DbInfoCommand infoCommand;
    private final DbGetCommand getCommand;
    private final DbPutCommand putCommand;
    private final ScanCommand scanCommand;
    private final DbDropCfCommand dropCfCommand;

    public DbCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.openCommand = new DbOpenCommand(dbManager);
        this.closeCommand = new DbCloseCommand(dbManager);
        this.infoCommand = new DbInfoCommand(dbManager);
        this.getCommand = new DbGetCommand(dbManager);
        this.putCommand = new DbPutCommand(dbManager);
        this.scanCommand = new ScanCommand(dbManager);
        this.dropCfCommand = new DbDropCfCommand(dbManager);
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
            case "get":
                getCommand.execute(subArgs);
                break;
            case "put":
                putCommand.execute(subArgs);
                break;
            case "scan":
                scanCommand.execute(subArgs);
                break;
            case "drop-cf":
                dropCfCommand.execute(subArgs);
                break;
            default:
                System.err.println("Error: Unknown subcommand '" + subcommand + "'");
                System.err.println("Usage: " + getUsage());
                break;
        }
    }

    @Override
    public String getHelp() {
        return "Database operations (open, close, info, get, put, scan, drop-cf)";
    }

    @Override
    public String getUsage() {
        return "db <subcommand> [args]\n" +
               "                               Subcommands:\n" +
               "                                 db open <path> [--write]                   - Open a database (read-only by default)\n" +
               "                                 db close                                   - Close the currently open database\n" +
               "                                 db info                                    - Display database statistics\n" +
               "                                 db get <segment> <hex-key>                 - Retrieve a raw value by key\n" +
               "                                 db put <segment> <hex-key> <hex-value>     - Write a raw value by key (requires --write)\n" +
               "                                 db scan <segment> [--limit n] [--offset n] - Scan raw key-value entries\n" +
               "                                 db drop-cf <segment>                               - Drop a column family (requires --write)";
    }
}
