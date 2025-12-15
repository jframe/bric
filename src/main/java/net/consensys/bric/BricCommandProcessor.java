package net.consensys.bric;

import net.consensys.bric.commands.*;
import net.consensys.bric.db.BesuDatabaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BricCommandProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BricCommandProcessor.class);
    private final boolean verbose;
    private final Map<String, Command> commands = new LinkedHashMap<>();
    private final BesuDatabaseManager dbManager;

    public BricCommandProcessor(boolean verbose) {
        this.verbose = verbose;
        this.dbManager = new BesuDatabaseManager();
        registerBuiltInCommands();
        registerDatabaseCommands();
    }

    public BesuDatabaseManager getDbManager() {
        return dbManager;
    }

    /**
     * Register a command.
     */
    public void registerCommand(String name, Command command) {
        commands.put(name.toLowerCase(), command);
    }

    /**
     * Register built-in commands.
     */
    private void registerBuiltInCommands() {
        // Register built-in commands as anonymous classes
        registerCommand("help", new Command() {
            @Override
            public void execute(String[] args) {
                printHelp();
            }

            @Override
            public String getHelp() {
                return "Display this help message";
            }

            @Override
            public String getUsage() {
                return "help";
            }
        });

        registerCommand("version", new Command() {
            @Override
            public void execute(String[] args) {
                printVersion();
            }

            @Override
            public String getHelp() {
                return "Display version information";
            }

            @Override
            public String getUsage() {
                return "version";
            }
        });

        registerCommand("status", new Command() {
            @Override
            public void execute(String[] args) {
                printStatus();
            }

            @Override
            public String getHelp() {
                return "Display REPL status";
            }

            @Override
            public String getUsage() {
                return "status";
            }
        });
    }

    /**
     * Register database commands.
     */
    private void registerDatabaseCommands() {
        registerCommand("db-open", new DbOpenCommand(dbManager));
        registerCommand("db-close", new DbCloseCommand(dbManager));
        registerCommand("db-info", new DbInfoCommand(dbManager));
    }

    public void processCommand(String commandLine) {
        String[] parts = commandLine.split("\\s+");
        String commandName = parts[0].toLowerCase();
        String[] args = Arrays.copyOfRange(parts, 1, parts.length);

        if (verbose) {
            LOG.info("Processing command: {} with args: {}", commandName, Arrays.toString(args));
        }

        Command command = commands.get(commandName);
        if (command != null) {
            try {
                command.execute(args);
            } catch (Exception e) {
                System.err.println("Error executing command: " + e.getMessage());
                if (verbose) {
                    e.printStackTrace();
                }
            }
        } else {
            System.out.println("Unknown command: " + commandName);
            System.out.println("Type 'help' for available commands");
        }
    }

    private void printHelp() {
        System.out.println("\nAvailable commands:");
        for (Map.Entry<String, Command> entry : commands.entrySet()) {
            String name = entry.getKey();
            Command cmd = entry.getValue();
            System.out.printf("  %-30s - %s%n", cmd.getUsage(), cmd.getHelp());
        }
        System.out.println("  exit/quit                      - Exit the REPL");
        System.out.println();
    }

    private void printVersion() {
        System.out.println("Bric CLI version 1.0.0");
    }

    private void printStatus() {
        System.out.println("Status:");
        System.out.println("  Verbose mode: " + (verbose ? "enabled" : "disabled"));
        System.out.println("  Java version: " + System.getProperty("java.version"));
        System.out.println("  Working directory: " + System.getProperty("user.dir"));
    }

    public boolean isVerbose() {
        return verbose;
    }
}
