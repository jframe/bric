package net.consensys.bric.completion;

import net.consensys.bric.BricCommandProcessor;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.FileNameCompleter;

import java.util.List;
import java.util.Set;

/**
 * Custom completer for Bric REPL commands.
 * Provides completion for top-level commands, subcommands, and file paths.
 */
public class BricCompleter implements Completer {

    private final BricCommandProcessor processor;
    private final FileNameCompleter fileCompleter;
    private static final Set<String> DB_SUBCOMMANDS = Set.of("open", "close", "info");
    private static final Set<String> EXIT_COMMANDS = Set.of("exit", "quit");

    public BricCompleter(BricCommandProcessor processor) {
        this.processor = processor;
        this.fileCompleter = new FileNameCompleter();
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        String[] words = line.words().toArray(new String[0]);
        int wordIndex = line.wordIndex();

        // Handle empty input or first word completion
        if (wordIndex == 0) {
            completeTopLevelCommands(words, candidates);
            return;
        }

        // Get the first word (command name)
        String command = words[0].toLowerCase();

        // Handle "db" subcommand completion
        if ("db".equals(command)) {
            if (wordIndex == 1) {
                // Complete subcommands
                completeDbSubcommands(words, candidates);
            } else if (wordIndex == 2 && words.length > 1 && "open".equals(words[1].toLowerCase())) {
                // Complete file paths for "db open <path>"
                fileCompleter.complete(reader, line, candidates);
            }
            return;
        }

        // Handle "code" command with --save flag
        if ("code".equals(command)) {
            // Check if --save flag is present and we're completing after it
            for (int i = 1; i < words.length - 1; i++) {
                if ("--save".equals(words[i]) && wordIndex == i + 1) {
                    fileCompleter.complete(reader, line, candidates);
                    return;
                }
            }
        }
    }

    /**
     * Complete top-level commands.
     */
    private void completeTopLevelCommands(String[] words, List<Candidate> candidates) {
        String prefix = words.length > 0 ? words[0].toLowerCase() : "";

        // Add all registered commands
        for (String commandName : processor.getCommandNames()) {
            if (commandName.startsWith(prefix)) {
                candidates.add(new Candidate(commandName));
            }
        }

        // Add exit commands
        for (String exitCommand : EXIT_COMMANDS) {
            if (exitCommand.startsWith(prefix)) {
                candidates.add(new Candidate(exitCommand));
            }
        }
    }

    /**
     * Complete "db" subcommands.
     */
    private void completeDbSubcommands(String[] words, List<Candidate> candidates) {
        String prefix = words.length > 1 ? words[1].toLowerCase() : "";

        for (String subcommand : DB_SUBCOMMANDS) {
            if (subcommand.startsWith(prefix)) {
                candidates.add(new Candidate(subcommand));
            }
        }
    }
}
