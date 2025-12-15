package net.consensys.bric.commands;

/**
 * Interface for REPL commands.
 */
public interface Command {

    /**
     * Execute the command with given arguments.
     *
     * @param args Command arguments (not including the command name itself)
     */
    void execute(String[] args);

    /**
     * Get help text for the command.
     */
    String getHelp();

    /**
     * Get usage syntax for the command.
     */
    String getUsage();
}
