package net.consensys.bric.db;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BesuDatabaseManagerCompactionTest {

    private Path tempDbDir;
    private BesuDatabaseManager manager;

    @BeforeEach
    void setUp() throws Exception {
        tempDbDir = Files.createTempDirectory("bric-cfmgr-test-");
        // Bootstrap an empty RocksDB so BesuDatabaseManager (which uses
        // setCreateIfMissing(false)) can open it.
        RocksDB.loadLibrary();
        try (Options opts = new Options().setCreateIfMissing(true);
             RocksDB initDb = RocksDB.open(opts, tempDbDir.toString())) {
            // Empty DB created.
        }
        manager = new BesuDatabaseManager();
        manager.openDatabase(tempDbDir.toString(), true);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (manager.isOpen()) {
            // Replace any stub job manager with a real one so close succeeds.
            manager.setCompactionJobManagerForTesting(
                new CompactionJobManager(manager.getDatabase()));
            manager.closeDatabase();
        }
        deleteRecursively(tempDbDir);
    }

    @Test
    void getCompactionJobManagerIsNonNullWhileOpen() {
        assertThat(manager.getCompactionJobManager()).isNotNull();
    }

    @Test
    void getCompactionJobManagerThrowsWhenClosed() {
        manager.setCompactionJobManagerForTesting(
            new CompactionJobManager(manager.getDatabase()));
        manager.closeDatabase();
        assertThatThrownBy(() -> manager.getCompactionJobManager())
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeDatabaseRefusesWhenJobsRunning() {
        CompactionJobManager stub = new CompactionJobManager(manager.getDatabase()) {
            @Override
            public boolean hasRunning() { return true; }
            @Override
            public List<Integer> runningJobIds() { return List.of(3, 5); }
        };
        manager.setCompactionJobManagerForTesting(stub);

        assertThatThrownBy(() -> manager.closeDatabase())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("compaction jobs still running")
            .hasMessageContaining("[3, 5]");
    }

    private static void deleteRecursively(Path dir) throws Exception {
        if (!Files.exists(dir)) return;
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                  .forEach(p -> {
                      try { Files.delete(p); }
                      catch (Exception ignored) {}
                  });
        }
    }
}
