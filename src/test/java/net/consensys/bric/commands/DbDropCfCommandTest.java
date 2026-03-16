package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DbDropCfCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbDropCfCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbDropCfCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testNoDatabaseOpen() {
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{"TRIE_LOG_STORAGE"});

        assertThat(errorStream.toString()).contains("Error: No database is open");
        assertThat(errorStream.toString()).contains("db open");
    }

    @Test
    void testReadOnlyDatabase() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(false);

        command.execute(new String[]{"TRIE_LOG_STORAGE"});

        assertThat(errorStream.toString()).contains("Error: Database is open in read-only mode");
        assertThat(errorStream.toString()).contains("--write");
    }

    @Test
    void testMissingArg() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);

        command.execute(new String[]{});

        assertThat(errorStream.toString()).contains("Error: Missing segment name");
        assertThat(errorStream.toString()).contains("Usage:");
    }

    @Test
    void testUnknownSegment() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyByName("NONEXISTENT_SEGMENT")).thenReturn(null);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("BLOCKCHAIN"));

        command.execute(new String[]{"NONEXISTENT_SEGMENT"});

        assertThat(errorStream.toString()).contains("Error: Column family not found: NONEXISTENT_SEGMENT");
    }

    @Test
    void testSegmentNotInDatabase() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyByName("TRIE_LOG_STORAGE")).thenReturn(null);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("BLOCKCHAIN"));

        command.execute(new String[]{"TRIE_LOG_STORAGE"});

        assertThat(errorStream.toString()).contains("Error: Column family not found: TRIE_LOG_STORAGE");
    }

    @Test
    void testCannotDropDefaultColumnFamily() {
        // No mock setup — the "default" guard fires before any dbManager call.
        // Do NOT stub isOpen()/isWritable() here: verifyNoInteractions would fail
        // because Mockito counts when(...) stub setup calls as interactions.
        command.execute(new String[]{"default"});

        assertThat(errorStream.toString()).contains("Error: Cannot drop the default column family");
        verifyNoInteractions(mockDbManager);
    }

    @Test
    void testSuccessfulDrop() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("TRIE_LOG_STORAGE"));
        ColumnFamilyHandle mockHandle = Mockito.mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("TRIE_LOG_STORAGE")).thenReturn(mockHandle);

        command.execute(new String[]{"TRIE_LOG_STORAGE"});

        verify(mockDbManager).dropColumnFamily("TRIE_LOG_STORAGE");
        assertThat(outputStream.toString()).contains("Dropped column family: TRIE_LOG_STORAGE");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testSegmentNameCaseInsensitive() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("TRIE_LOG_STORAGE"));
        ColumnFamilyHandle mockHandle = Mockito.mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("TRIE_LOG_STORAGE")).thenReturn(mockHandle);

        command.execute(new String[]{"trie_log_storage"});

        verify(mockDbManager).dropColumnFamily("TRIE_LOG_STORAGE");
        assertThat(outputStream.toString()).contains("Dropped column family: TRIE_LOG_STORAGE");
    }

    @Test
    void testRocksDBException() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("TRIE_LOG_STORAGE"));
        ColumnFamilyHandle mockHandle = Mockito.mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("TRIE_LOG_STORAGE")).thenReturn(mockHandle);
        doThrow(new RocksDBException("io error")).when(mockDbManager).dropColumnFamily("TRIE_LOG_STORAGE");

        command.execute(new String[]{"TRIE_LOG_STORAGE"});

        assertThat(errorStream.toString()).contains("Error: Failed to drop column family");
        assertThat(errorStream.toString()).contains("io error");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("drop");
        assertThat(command.getHelp()).containsIgnoringCase("write");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db drop-cf");
        assertThat(usage).contains("<segment|name|hex>");
    }

    @Test
    void testDropArbitraryUtf8CF() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("CUSTOM_CF"));
        ColumnFamilyHandle mockHandle = Mockito.mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);

        command.execute(new String[]{"CUSTOM_CF"});

        verify(mockDbManager).dropColumnFamily("CUSTOM_CF");
        assertThat(outputStream.toString()).contains("Dropped column family: CUSTOM_CF");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testDropArbitraryHexCF() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("TRIE_LOG_STORAGE"));
        ColumnFamilyHandle mockHandle = Mockito.mock(ColumnFamilyHandle.class);
        // Hex 0x0a resolves to TRIE_LOG_STORAGE enum via Strategy 2 (enum ID lookup)
        when(mockDbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE)).thenReturn(mockHandle);
        // Also need to mock getColumnFamilyByName for the reverse lookup
        when(mockDbManager.getColumnFamilyByName("TRIE_LOG_STORAGE")).thenReturn(mockHandle);

        command.execute(new String[]{"0x0a"});

        verify(mockDbManager).dropColumnFamily("TRIE_LOG_STORAGE");
        assertThat(outputStream.toString()).contains("Dropped column family: TRIE_LOG_STORAGE");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testDropQuotedArbitraryName() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("MY_CUSTOM_CF"));
        ColumnFamilyHandle mockHandle = Mockito.mock(ColumnFamilyHandle.class);
        when(mockDbManager.getColumnFamilyByName("MY_CUSTOM_CF")).thenReturn(mockHandle);

        command.execute(new String[]{"\"MY_CUSTOM_CF\""});

        verify(mockDbManager).dropColumnFamily("MY_CUSTOM_CF");
        assertThat(outputStream.toString()).contains("Dropped column family: MY_CUSTOM_CF");
        assertThat(errorStream.toString()).doesNotContain("Error");
    }

    @Test
    void testDropNonexistentCFError() throws RocksDBException {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.isWritable()).thenReturn(true);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("BLOCKCHAIN"));

        command.execute(new String[]{"NONEXISTENT"});

        verify(mockDbManager, never()).dropColumnFamily(anyString());
        assertThat(errorStream.toString()).contains("Error: Column family not found: NONEXISTENT");
        assertThat(errorStream.toString()).contains("Available in this database");
    }
}
