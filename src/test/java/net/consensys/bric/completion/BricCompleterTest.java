package net.consensys.bric.completion;

import net.consensys.bric.BricCommandProcessor;
import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class BricCompleterTest {

    private BricCommandProcessor mockProcessor;
    private BricCompleter completer;
    private LineReader mockReader;
    private ParsedLine mockParsedLine;

    @BeforeEach
    void setUp() {
        mockProcessor = Mockito.mock(BricCommandProcessor.class);
        mockReader = Mockito.mock(LineReader.class);
        mockParsedLine = Mockito.mock(ParsedLine.class);

        // Setup mock processor with default commands
        when(mockProcessor.getCommandNames())
            .thenReturn(Set.of("help", "version", "status", "db", "account", "storage", "code", "trielog"));

        completer = new BricCompleter(mockProcessor);
    }

    @Test
    void testCompleteTopLevelCommandsWithEmptyInput() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList(""));
        when(mockParsedLine.wordIndex()).thenReturn(0);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).isNotEmpty();
        assertThat(candidates).anyMatch(c -> c.value().equals("help"));
        assertThat(candidates).anyMatch(c -> c.value().equals("db"));
        assertThat(candidates).anyMatch(c -> c.value().equals("account"));
        assertThat(candidates).anyMatch(c -> c.value().equals("exit"));
        assertThat(candidates).anyMatch(c -> c.value().equals("quit"));
    }

    @Test
    void testCompleteTopLevelCommandsWithPartialMatch() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("acc"));
        when(mockParsedLine.wordIndex()).thenReturn(0);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates.get(0).value()).isEqualTo("account");
    }

    @Test
    void testCompleteTopLevelCommandsWithNoMatch() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("xyz"));
        when(mockParsedLine.wordIndex()).thenReturn(0);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).isEmpty();
    }

    @Test
    void testCompleteDbSubcommands() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", ""));
        when(mockParsedLine.wordIndex()).thenReturn(1);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(3);
        assertThat(candidates).anyMatch(c -> c.value().equals("open"));
        assertThat(candidates).anyMatch(c -> c.value().equals("close"));
        assertThat(candidates).anyMatch(c -> c.value().equals("info"));
    }

    @Test
    void testCompleteDbSubcommandWithPartialMatch() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "op"));
        when(mockParsedLine.wordIndex()).thenReturn(1);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates.get(0).value()).isEqualTo("open");
    }

    @Test
    void testCompleteDbSubcommandCaseInsensitive() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("DB", "OP"));
        when(mockParsedLine.wordIndex()).thenReturn(1);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates.get(0).value()).isEqualTo("open");
    }

    @Test
    void testCompleteExitCommands() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("ex"));
        when(mockParsedLine.wordIndex()).thenReturn(0);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates.get(0).value()).isEqualTo("exit");
    }

    @Test
    void testCompleteQuitCommand() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("qu"));
        when(mockParsedLine.wordIndex()).thenReturn(0);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates.get(0).value()).isEqualTo("quit");
    }

    @Test
    void testNoCompletionForNonDbCommandSecondArgument() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("account", "0x123"));
        when(mockParsedLine.wordIndex()).thenReturn(1);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        // Should not add any command completions for second argument of non-db commands
        // (file completions might be added by FileNameCompleter for certain cases, but we don't test that here)
        assertThat(candidates).isEmpty();
    }

    @Test
    void testCompleteMultipleMatchingCommands() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("s"));
        when(mockParsedLine.wordIndex()).thenReturn(0);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        // Should match both "status" and "storage"
        assertThat(candidates).hasSizeGreaterThanOrEqualTo(2);
        assertThat(candidates).anyMatch(c -> c.value().equals("status"));
        assertThat(candidates).anyMatch(c -> c.value().equals("storage"));
    }
}
