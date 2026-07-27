package net.consensys.bric.commands;

import net.consensys.bric.besu.RocksDBSegmentedStorage;
import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbUpgradeFlatDbCommandTest {

    @TempDir
    Path tempDir;

    private BesuDatabaseManager mockDbManager;
    private DbUpgradeFlatDbCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;
    private BesuDatabaseManager realDbManager;

    @BeforeEach
    void setUp() {
        // Needed unconditionally (not just by the real-database test below): depending on test
        // class execution order, this may otherwise be the first test in the suite to touch any
        // RocksDB native class (even indirectly, via a Mockito mock), which fails with an
        // UnsatisfiedLinkError if the native library hasn't been loaded yet in this JVM.
        RocksDB.loadLibrary();

        mockDbManager = mock(BesuDatabaseManager.class);
        command = new DbUpgradeFlatDbCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @AfterEach
    void tearDown() {
        if (realDbManager != null && realDbManager.isOpen()) {
            realDbManager.closeDatabase();
        }
    }

    @Test
    void refusesWhenDbClosed() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void refusesWhenNotBonsaiFormat() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.FOREST);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("only supported for Bonsai databases");
        assertThat(errorStream.toString()).contains("FOREST");
    }

    @Test
    void allowsBonsaiArchiveFormat() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{"--dry-run"});
        // Fails later (no real database behind the mock), but must get past the format guard —
        // BONSAI_ARCHIVE is no longer rejected as "not a Bonsai database".
        assertThat(errorStream.toString()).doesNotContain("only supported for Bonsai databases");
    }

    @Test
    void stillRefusesForestAndUnknownFormats() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.UNKNOWN);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("only supported for Bonsai databases");
        assertThat(errorStream.toString()).contains("UNKNOWN");
    }

    @Test
    void refusesWhenReadOnlyAndNotDryRun() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("read-only mode");
    }

    @Test
    void dryRunDoesNotRequireWritable() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{"--dry-run"});
        // Fails later (no real database behind the mock), but must get past the write-mode guard.
        // The failure must not be attributed to read-only mode: --dry-run works against a
        // read-only-opened database now that FlatDbHealer tolerates Besu's rejected metadata
        // write-back (see FlatDbHealer's constructor).
        assertThat(errorStream.toString()).doesNotContain("read-only mode");
    }

    @Test
    void includesExceptionMessageWhenFlatDbHealerConstructionFails() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(true);

        // Execute with a mock dbManager that doesn't have proper setup,
        // which will cause FlatDbHealer construction to fail
        command.execute(new String[]{});

        String errorOutput = errorStream.toString();
        // Verify that error message includes both the generic error and the exception message
        assertThat(errorOutput).contains("Failed to initialize database healer");
        // The actual exception message should be included (not just the generic message)
        assertThat(errorOutput).isNotEqualTo("Error: Failed to initialize database healer\n");
    }

    /**
     * Regression test: heal() can throw RuntimeExceptions other than IllegalStateException (e.g.
     * a MerkleTrieException if a trie node is missing/corrupt). The command must catch those
     * cleanly too, not just IllegalStateException, matching this command's existing "Error: ..."
     * style rather than letting a raw stack trace reach the user.
     */
    @Test
    void catchesNonIllegalStateRuntimeExceptionFromHealCleanly() throws Exception {
        // Build a real, minimal Bonsai-shaped database whose persisted world state root points
        // at a hash with no corresponding trie nodes at all, so FlatDbHealer's real heal() throws
        // a MerkleTrieException (a RuntimeException, but not an IllegalStateException) as soon as
        // it tries to walk the account trie.
        RocksDB.loadLibrary();
        List<ColumnFamilyDescriptor> descriptors = List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.CODE_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.VARIABLES.getId()));
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        try (RocksDB db = RocksDB.open(options, tempDir.toString(), descriptors, handles)) {
            for (ColumnFamilyHandle handle : handles) {
                handle.close();
            }
        }

        realDbManager = new BesuDatabaseManager();
        realDbManager.openDatabase(tempDir.toString(), true);

        RocksDBSegmentedStorage storage = new RocksDBSegmentedStorage(realDbManager);
        byte[] bogusRoot = new byte[32];
        bogusRoot[31] = 0x77;
        var transaction = storage.startTransaction();
        transaction.put(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY,
            bogusRoot);
        transaction.commit();
        transaction.close();

        DbUpgradeFlatDbCommand realCommand = new DbUpgradeFlatDbCommand(realDbManager);
        realCommand.execute(new String[]{"--dry-run"});

        String errorOutput = errorStream.toString();
        assertThat(errorOutput).contains("Error: ");
        assertThat(errorOutput).doesNotContain("at net.consensys.bric"); // no raw stack trace leaked
    }
}
