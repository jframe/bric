package net.consensys.bric;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.EndOfFileException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

@Command(
    name = "bric",
    description = "Bric CLI",
    mixinStandardHelpOptions = true,
    version = "1.0.0"
)
public class BricApplication implements Callable<Integer> {

    private static final Logger LOG = LoggerFactory.getLogger(BricApplication.class);
    private static final String PROMPT = "bric> ";

    @Option(names = {"-v", "--verbose"}, description = "Verbose mode")
    private boolean verbose;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new BricApplication()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        printWelcomeBanner();

        try (Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build()) {

            LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .build();

            runRepl(reader);

        } catch (IOException e) {
            LOG.error("Failed to initialize terminal", e);
            return 1;
        }

        return 0;
    }

    private void runRepl(LineReader reader) {
        BricCommandProcessor processor = new BricCommandProcessor(verbose);

        while (true) {
            String line;
            try {
                line = reader.readLine(PROMPT);
            } catch (UserInterruptException e) {
                // Ctrl+C pressed
                continue;
            } catch (EndOfFileException e) {
                // Ctrl+D pressed
                break;
            }

            if (line == null || line.trim().isEmpty()) {
                continue;
            }

            String trimmedLine = line.trim();

            // Check for exit commands
            if (trimmedLine.equalsIgnoreCase("exit") ||
                trimmedLine.equalsIgnoreCase("quit")) {
                System.out.println("Goodbye!");
                break;
            }

            // Process the command
            processor.processCommand(trimmedLine);
        }
    }

    private void printWelcomeBanner() {
        System.out.println("=================================");
        System.out.println("  Bric CLI");
        System.out.println("  Version 1.0.0");
        System.out.println("=================================");
        System.out.println("Type 'help' for available commands");
        System.out.println("Type 'exit' or 'quit' to exit");
        System.out.println();
    }
}
