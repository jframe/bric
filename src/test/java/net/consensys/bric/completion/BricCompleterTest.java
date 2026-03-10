package net.consensys.bric.completion;

import net.consensys.bric.BricCommandProcessor;
import net.consensys.bric.db.BesuDatabaseManager;
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
    private BesuDatabaseManager mockDbManager;
    private BricCompleter completer;
    private LineReader mockReader;
    private ParsedLine mockParsedLine;

    @BeforeEach
    void setUp() {
        mockProcessor = Mockito.mock(BricCommandProcessor.class);
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        mockReader = Mockito.mock(LineReader.class);
        mockParsedLine = Mockito.mock(ParsedLine.class);

        when(mockProcessor.getCommandNames())
            .thenReturn(Set.of("help", "version", "status", "db", "account", "storage", "code", "trielog",
                               "trielog-compare", "trielog-check"));
        when(mockProcessor.getDbManager()).thenReturn(mockDbManager);
        when(mockDbManager.isOpen()).thenReturn(false);

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

        assertThat(candidates).hasSize(6);
        assertThat(candidates).anyMatch(c -> c.value().equals("open"));
        assertThat(candidates).anyMatch(c -> c.value().equals("close"));
        assertThat(candidates).anyMatch(c -> c.value().equals("info"));
        assertThat(candidates).anyMatch(c -> c.value().equals("get"));
        assertThat(candidates).anyMatch(c -> c.value().equals("put"));
        assertThat(candidates).anyMatch(c -> c.value().equals("scan"));
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

    // --- Segment completion for db get / db put / db scan ---

    @Test
    void testCompleteDbGetSegment() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "get", ""));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).isNotEmpty();
        assertThat(candidates).anyMatch(c -> c.value().equals("ACCOUNT_INFO_STATE"));
        assertThat(candidates).anyMatch(c -> c.value().equals("TRIE_LOG_STORAGE"));
    }

    @Test
    void testCompleteDbPutSegment() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "put", ""));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).isNotEmpty();
        assertThat(candidates).anyMatch(c -> c.value().equals("ACCOUNT_INFO_STATE"));
    }

    @Test
    void testCompleteDbScanSegment() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "scan", ""));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).isNotEmpty();
        assertThat(candidates).anyMatch(c -> c.value().equals("ACCOUNT_INFO_STATE"));
    }

    @Test
    void testCompleteDbGetSegmentWithPrefix() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "get", "ACCOUNT"));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).isNotEmpty();
        assertThat(candidates).allMatch(c -> c.value().startsWith("ACCOUNT"));
    }

    @Test
    void testCompleteDbGetSegmentFromOpenDatabase() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames())
            .thenReturn(Set.of("ACCOUNT_INFO_STATE", "TRIE_LOG_STORAGE", "default"));
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "get", ""));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(3);
        assertThat(candidates).anyMatch(c -> c.value().equals("default"));
    }

    @Test
    void testCompleteDbScanFlags() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "scan", "ACCOUNT_INFO_STATE", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(3);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(3);
        assertThat(candidates).anyMatch(c -> c.value().equals("--from"));
        assertThat(candidates).anyMatch(c -> c.value().equals("--limit"));
        assertThat(candidates).anyMatch(c -> c.value().equals("--offset"));
    }

    @Test
    void testCompleteDbScanFlagNotSuggestedWhenAlreadyUsed() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("db", "scan", "ACCOUNT_INFO_STATE", "--limit", "10", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(5);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(2);
        assertThat(candidates).anyMatch(c -> c.value().equals("--from"));
        assertThat(candidates).anyMatch(c -> c.value().equals("--offset"));
        assertThat(candidates).noneMatch(c -> c.value().equals("--limit"));
    }

    // --- Flag completion ---

    @Test
    void testCompleteAccountFlags() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("account", "0x123", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates).anyMatch(c -> c.value().equals("--block"));
    }

    @Test
    void testCompleteStorageFlags() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("storage", "0xaddr", "0", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(3);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(2);
        assertThat(candidates).anyMatch(c -> c.value().equals("--block"));
        assertThat(candidates).anyMatch(c -> c.value().equals("--raw"));
    }

    @Test
    void testCompleteCodeFlags() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("code", "0xaddr", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(2);
        assertThat(candidates).anyMatch(c -> c.value().equals("--save"));
        assertThat(candidates).anyMatch(c -> c.value().equals("--hash"));
    }

    @Test
    void testCompleteTrielogFlags() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("trielog", "12345", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates).anyMatch(c -> c.value().equals("--address"));
    }

    @Test
    void testCompleteTrielogCompareFlags() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("trielog-compare", "100..200", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(2);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).hasSize(1);
        assertThat(candidates).anyMatch(c -> c.value().equals("--verbose"));
    }

    @Test
    void testFlagNotSuggestedWhenAlreadyUsed() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("storage", "0xaddr", "0", "--raw", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(4);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        // --raw already used, only --block should be offered
        assertThat(candidates).hasSize(1);
        assertThat(candidates.get(0).value()).isEqualTo("--block");
    }

    @Test
    void testNoFlagCompletionForUnknownCommand() {
        when(mockParsedLine.words()).thenReturn(Arrays.asList("help", "--"));
        when(mockParsedLine.wordIndex()).thenReturn(1);

        List<Candidate> candidates = new ArrayList<>();
        completer.complete(mockReader, mockParsedLine, candidates);

        assertThat(candidates).isEmpty();
    }
}
