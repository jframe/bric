package net.consensys.bric.db;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Manager for Besu RocksDB database connections.
 * Provides read-only access to Besu databases.
 */
public class BesuDatabaseManager {

    private static final Logger LOG = LoggerFactory.getLogger(BesuDatabaseManager.class);

    private RocksDB db;
    private String currentPath;
    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private final Map<String, ColumnFamilyHandle> handlesByName = new HashMap<>();
    private DatabaseFormat format;
    private boolean isOpen = false;

    public enum DatabaseFormat {
        BONSAI,
        BONSAI_ARCHIVE,
        FOREST,
        UNKNOWN
    }

    public BesuDatabaseManager() {
    }

    /**
     * Open a Besu database in read-only mode.
     *
     * @param path Path to the database directory
     * @throws Exception if database cannot be opened
     */
    public synchronized void openDatabase(String path) throws Exception {
        if (isOpen) {
            throw new IllegalStateException(
                "Database is already open at: " + currentPath + ". Close it first with db-close.");
        }

        Path dbPath = Paths.get(path);
        if (!Files.exists(dbPath)) {
            throw new IllegalArgumentException("Path does not exist: " + path);
        }
        if (!Files.isDirectory(dbPath)) {
            throw new IllegalArgumentException("Path is not a directory: " + path);
        }

        LOG.info("Loading RocksDB library...");
        RocksDB.loadLibrary();

        LOG.info("Opening database at: {}", path);

        // List existing column families
        Options options = new Options();
        List<byte[]> cfNames;
        try {
            cfNames = RocksDB.listColumnFamilies(options, path);
        } catch (RocksDBException e) {
            throw new Exception("Failed to list column families. Is this a valid RocksDB database?", e);
        } finally {
            options.close();
        }

        if (cfNames.isEmpty()) {
            // If no column families found, add default
            cfNames.add(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        LOG.info("Found {} column families", cfNames.size());

        // Create column family descriptors
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        for (byte[] cfName : cfNames) {
            cfDescriptors.add(new ColumnFamilyDescriptor(cfName));
        }

        // Open database in read-only mode
        DBOptions dbOptions = new DBOptions()
            .setCreateIfMissing(false)
            .setCreateMissingColumnFamilies(false);

        try {
            db = RocksDB.openReadOnly(dbOptions, path, cfDescriptors, columnFamilyHandles);
        } catch (RocksDBException e) {
            dbOptions.close();
            throw new Exception("Failed to open database", e);
        }

        dbOptions.close();

        // Map column family handles by name
        for (int i = 0; i < cfNames.size(); i++) {
            String name = KeyValueSegmentIdentifier.idToString(cfNames.get(i));
            handlesByName.put(name, columnFamilyHandles.get(i));
            LOG.debug("Mapped column family: {}", name);
        }

        currentPath = path;
        format = detectDatabaseFormat();
        isOpen = true;

        LOG.info("Database opened successfully. Format: {}", format);
    }

    /**
     * Close the currently open database.
     */
    public synchronized void closeDatabase() {
        if (!isOpen) {
            LOG.warn("No database is currently open");
            return;
        }

        LOG.info("Closing database at: {}", currentPath);

        // Close column family handles
        for (ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
        }
        columnFamilyHandles.clear();
        handlesByName.clear();

        // Close database
        if (db != null) {
            db.close();
            db = null;
        }

        currentPath = null;
        format = null;
        isOpen = false;

        LOG.info("Database closed successfully");
    }

    /**
     * Get column family handle by segment identifier.
     */
    public ColumnFamilyHandle getColumnFamily(KeyValueSegmentIdentifier segment) {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        return handlesByName.get(segment.getName());
    }

    /**
     * Get column family handle by name.
     */
    public ColumnFamilyHandle getColumnFamilyByName(String name) {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        return handlesByName.get(name);
    }

    /**
     * Get all column family names.
     */
    public Set<String> getColumnFamilyNames() {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        return Collections.unmodifiableSet(handlesByName.keySet());
    }

    /**
     * Get the RocksDB instance.
     */
    public RocksDB getDatabase() {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        return db;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public String getCurrentPath() {
        return currentPath;
    }

    public DatabaseFormat getFormat() {
        return format;
    }

    /**
     * Detect database format based on available column families.
     */
    private DatabaseFormat detectDatabaseFormat() {
        Set<String> cfNames = handlesByName.keySet();

        // Check for Bonsai Archive
        if (cfNames.contains("ACCOUNT_INFO_STATE_ARCHIVE") ||
            cfNames.contains("ACCOUNT_STORAGE_ARCHIVE")) {
            return DatabaseFormat.BONSAI_ARCHIVE;
        }

        // Check for Bonsai
        if (cfNames.contains("ACCOUNT_INFO_STATE") &&
            cfNames.contains("TRIE_LOG_STORAGE")) {
            return DatabaseFormat.BONSAI;
        }

        // Check for Forest (has WORLD_STATE but not ACCOUNT_INFO_STATE)
        if (cfNames.contains("WORLD_STATE") &&
            !cfNames.contains("ACCOUNT_INFO_STATE")) {
            return DatabaseFormat.FOREST;
        }

        return DatabaseFormat.UNKNOWN;
    }

    /**
     * Get database statistics for a column family.
     */
    public DatabaseStats getStats(String cfName) throws RocksDBException {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }

        ColumnFamilyHandle handle = handlesByName.get(cfName);
        if (handle == null) {
            throw new IllegalArgumentException("Column family not found: " + cfName);
        }

        DatabaseStats stats = new DatabaseStats();
        stats.columnFamilyName = cfName;

        try {
            stats.estimatedKeys = Long.parseLong(
                db.getProperty(handle, "rocksdb.estimate-num-keys"));
        } catch (Exception e) {
            stats.estimatedKeys = -1;
        }

        try {
            stats.totalSstSize = Long.parseLong(
                db.getProperty(handle, "rocksdb.total-sst-files-size"));
        } catch (Exception e) {
            stats.totalSstSize = -1;
        }

        try {
            stats.totalBlobSize = Long.parseLong(
                db.getProperty(handle, "rocksdb.total-blob-file-size"));
        } catch (Exception e) {
            stats.totalBlobSize = -1;
        }

        try {
            stats.liveDataSize = Long.parseLong(
                db.getProperty(handle, "rocksdb.estimate-live-data-size"));
        } catch (Exception e) {
            stats.liveDataSize = -1;
        }

        return stats;
    }

    public static class DatabaseStats {
        public String columnFamilyName;
        public long estimatedKeys;
        public long totalSstSize;
        public long totalBlobSize;
        public long liveDataSize;

        public long getTotalSize() {
            long total = 0;
            if (totalSstSize > 0) total += totalSstSize;
            if (totalBlobSize > 0) total += totalBlobSize;
            return total;
        }
    }
}
