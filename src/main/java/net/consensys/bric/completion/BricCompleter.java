package net.consensys.bric.completion;

import net.consensys.bric.BricCommandProcessor;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.completer.FileNameCompleter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Custom completer for Bric REPL commands.
 * Provides completion for top-level commands, subcommands, flags, and file paths.
 */
public class BricCompleter implements Completer {

    private final BricCommandProcessor processor;
    private final FileNameCompleter fileCompleter;
    private static final Set<String> DB_SUBCOMMANDS = Set.of("open", "close", "info", "get", "put", "scan", "drop-cf");
    private static final Set<String> EXIT_COMMANDS = Set.of("exit", "quit");

    private static final Set<String> SEGMENT_NAMES = Stream.of(KeyValueSegmentIdentifier.values())
        .map(KeyValueSegmentIdentifier::getName)
        .collect(Collectors.toUnmodifiableSet());

    /** Flags available per command. */
    private static final Map<String, Set<String>> COMMAND_FLAGS = Map.of(
        "account", Set.of("--block"),
        "storage", Set.of("--block", "--raw"),
        "code", Set.of("--save", "--hash"),
        "trielog", Set.of("--address"),
        "trielog-compare", Set.of("--verbose"),
        "db-scan", Set.of("--from", "--limit", "--offset")
    );

    /** Flags that take a file path as their next argument. */
    private static final Set<String> FILE_PATH_FLAGS = Set.of("--save");

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

        String command = words[0].toLowerCase();

        // Handle "db" subcommand completion
        if ("db".equals(command)) {
            if (wordIndex == 1) {
                completeFromSet(words.length > 1 ? words[1] : "", DB_SUBCOMMANDS, candidates);
                return;
            }
            if (words.length > 1) {
                String subcommand = words[1].toLowerCase();
                if ("open".equals(subcommand)) {
                    fileCompleter.complete(reader, line, candidates);
                } else if ("get".equals(subcommand) || "put".equals(subcommand) || "scan".equals(subcommand)) {
                    if (wordIndex == 2) {
                        completeSegments(words.length > 2 ? words[2] : "", candidates);
                    } else if ("scan".equals(subcommand)) {
                        completeFlags(words, wordIndex, "db-scan", reader, line, candidates);
                    }
                } else if ("drop-cf".equals(subcommand)) {
                    if (wordIndex == 2) {
                        completeSegments(words.length > 2 ? words[2] : "", candidates);
                    }
                }
            }
            return;
        }

        // Handle flag and file path completion for other commands
        completeFlags(words, wordIndex, command, reader, line, candidates);
    }

    /**
     * Complete flags for a command, including file path completion after file-path flags.
     */
    private void completeFlags(String[] words, int wordIndex, String command,
                               LineReader reader, ParsedLine line, List<Candidate> candidates) {
        // Check if previous word is a flag that takes a file path
        if (wordIndex >= 2) {
            String prevWord = words[wordIndex - 1];
            if (FILE_PATH_FLAGS.contains(prevWord)) {
                fileCompleter.complete(reader, line, candidates);
                return;
            }
        }

        // Offer flag completion if current word starts with "--" or is empty and flags are available
        String currentWord = wordIndex < words.length ? words[wordIndex] : "";
        Set<String> flags = COMMAND_FLAGS.get(command);
        if (flags == null) {
            return;
        }

        // Only suggest flags that haven't already been used
        for (String flag : flags) {
            if (flag.startsWith(currentWord) && !alreadyUsed(words, wordIndex, flag)) {
                candidates.add(new Candidate(flag));
            }
        }
    }

    /**
     * Check if a flag is already present in the words before the current position.
     */
    private boolean alreadyUsed(String[] words, int currentIndex, String flag) {
        for (int i = 1; i < currentIndex; i++) {
            if (flag.equals(words[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * Complete top-level commands.
     */
    private void completeTopLevelCommands(String[] words, List<Candidate> candidates) {
        String prefix = words.length > 0 ? words[0].toLowerCase() : "";

        for (String commandName : processor.getCommandNames()) {
            if (commandName.startsWith(prefix)) {
                candidates.add(new Candidate(commandName));
            }
        }

        for (String exitCommand : EXIT_COMMANDS) {
            if (exitCommand.startsWith(prefix)) {
                candidates.add(new Candidate(exitCommand));
            }
        }
    }

    /**
     * Complete segment names, using the open database's actual column families when available,
     * falling back to the known enum names.
     */
    private void completeSegments(String prefix, List<Candidate> candidates) {
        Set<String> segments = processor.getDbManager().isOpen()
            ? processor.getDbManager().getColumnFamilyNames()
            : SEGMENT_NAMES;
        String upperPrefix = prefix.toUpperCase();
        for (String segName : segments) {
            if (segName.toUpperCase().startsWith(upperPrefix)) {
                candidates.add(new Candidate(segName));
            }
        }
    }

    /**
     * Complete from a set of options matching a prefix.
     */
    private void completeFromSet(String prefix, Set<String> options, List<Candidate> candidates) {
        String lowerPrefix = prefix.toLowerCase();
        for (String option : options) {
            if (option.startsWith(lowerPrefix)) {
                candidates.add(new Candidate(option));
            }
        }
    }
}
