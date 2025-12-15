package net.consensys.bric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class BricCommandProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(BricCommandProcessor.class);
    private final boolean verbose;

    public BricCommandProcessor(boolean verbose) {
        this.verbose = verbose;
    }

    public void processCommand(String commandLine) {
        String[] parts = commandLine.split("\\s+");
        String command = parts[0].toLowerCase();
        String[] args = Arrays.copyOfRange(parts, 1, parts.length);

        if (verbose) {
            LOG.info("Processing command: {} with args: {}", command, Arrays.toString(args));
        }

        switch (command) {
            case "help":
                printHelp();
                break;
            case "version":
                printVersion();
                break;
            case "echo":
                echo(args);
                break;
            case "status":
                printStatus();
                break;
            default:
                System.out.println("Unknown command: " + command);
                System.out.println("Type 'help' for available commands");
        }
    }

    private void printHelp() {
        System.out.println("\nAvailable commands:");
        System.out.println("  help        - Display this help message");
        System.out.println("  version     - Display version information");
        System.out.println("  echo <msg>  - Echo back the message");
        System.out.println("  status      - Display REPL status");
        System.out.println("  exit/quit   - Exit the REPL");
        System.out.println();
    }

    private void printVersion() {
        System.out.println("Bric CLI version 1.0.0");
    }

    private void echo(String[] args) {
        if (args.length == 0) {
            System.out.println();
        } else {
            System.out.println(String.join(" ", args));
        }
    }

    private void printStatus() {
        System.out.println("Status:");
        System.out.println("  Verbose mode: " + (verbose ? "enabled" : "disabled"));
        System.out.println("  Java version: " + System.getProperty("java.version"));
        System.out.println("  Working directory: " + System.getProperty("user.dir"));
    }
}
