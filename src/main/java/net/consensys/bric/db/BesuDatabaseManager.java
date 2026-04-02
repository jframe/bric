package net.consensys.bric.db;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.Arrays;

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
    private boolean writable = false;

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
        openDatabase(path, false);
    }

    /**
     * Open a Besu database.
     *
     * @param path     Path to the database directory
     * @param writable Whether to open in read-write mode
     * @throws Exception if database cannot be opened
     */
    public synchronized void openDatabase(String path, boolean writable) throws Exception {
        if (isOpen) {
            throw new IllegalStateException(
                "Database is already open at: " + currentPath + ". Close it first with 'db close'.");
        }

        Path dbPath = Paths.get(path);
        if (!Files.exists(dbPath)) {
            throw new IllegalArgumentException("Path does not exist: " + path);
        }
        if (!Files.isDirectory(dbPath)) {
            throw new IllegalArgumentException("Path is not a directory: " + path);
        }

        LOG.info("Loading RocksDB library...");
        try {
            RocksDB.loadLibrary();
        } catch (UnsatisfiedLinkError e) {
            throw new Exception(
                "Failed to load RocksDB native library. " +
                "This may be a platform compatibility issue. " +
                "OS: " + System.getProperty("os.name") + " " + System.getProperty("os.arch") +
                ", Java: " + System.getProperty("java.version"), e);
        }

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

        // Build descriptors and open options, loading from OPTIONS file for write mode
        // to avoid column family option mismatches (Besu uses non-default CF options)
        List<ColumnFamilyDescriptor> cfDescriptors;
        DBOptions dbOptions;

        if (writable) {
            DBOptions loadedOptions = new DBOptions();
            List<ColumnFamilyDescriptor> loadedDescs = new ArrayList<>();
            boolean loaded = false;
            try (ConfigOptions configOptions = new ConfigOptions().setIgnoreUnknownOptions(true)) {
                OptionsUtil.loadLatestOptions(configOptions, path, loadedOptions, loadedDescs);
                loaded = true;
                LOG.debug("Loaded options from OPTIONS file ({} column families)", loadedDescs.size());
            } catch (RocksDBException e) {
                loadedOptions.close();
                LOG.warn("Could not load OPTIONS file, falling back to default options: {}", e.getMessage());
            }

            if (loaded && !loadedDescs.isEmpty()) {
                cfDescriptors = loadedDescs;
                dbOptions = loadedOptions;
            } else {
                dbOptions = new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
                cfDescriptors = new ArrayList<>();
                for (byte[] cfName : cfNames) {
                    cfDescriptors.add(new ColumnFamilyDescriptor(cfName));
                }
            }
        } else {
            dbOptions = new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
            cfDescriptors = new ArrayList<>();
            for (byte[] cfName : cfNames) {
                cfDescriptors.add(new ColumnFamilyDescriptor(cfName));
            }
        }

        try {
            if (writable) {
                db = RocksDB.open(dbOptions, path, cfDescriptors, columnFamilyHandles);
            } else {
                db = RocksDB.openReadOnly(dbOptions, path, cfDescriptors, columnFamilyHandles);
            }
        } catch (RocksDBException e) {
            dbOptions.close();
            throw new Exception("Failed to open database: " + e.getMessage(), e);
        }

        dbOptions.close();

        // Map column family handles by name
        for (int i = 0; i < cfDescriptors.size(); i++) {
            String name = KeyValueSegmentIdentifier.idToString(cfDescriptors.get(i).getName());
            handlesByName.put(name, columnFamilyHandles.get(i));
            LOG.debug("Mapped column family: {}", name);
        }

        currentPath = path;
        format = detectDatabaseFormat();
        isOpen = true;
        this.writable = writable;

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
        writable = false;

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
     * Get column family handle by raw name bytes.
     * Used when CF name is not a standard UTF-8 string or needs byte-exact lookup.
     *
     * @param cfNameBytes the column family name as bytes
     * @return ColumnFamilyHandle if found, null otherwise
     */
    public ColumnFamilyHandle getColumnFamilyByNameBytes(byte[] cfNameBytes) {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }

        // Try exact match on stored handles
        for (Map.Entry<String, ColumnFamilyHandle> entry : handlesByName.entrySet()) {
            if (Arrays.equals(entry.getKey().getBytes(StandardCharsets.UTF_8), cfNameBytes)) {
                return entry.getValue();
            }
        }

        return null;
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

    public boolean isWritable() {
        return writable;
    }

    /**
     * Write a key-value pair to a column family.
     *
     * @throws IllegalStateException if the database is not open or not writable
     */
    public void put(ColumnFamilyHandle handle, byte[] key, byte[] value) throws RocksDBException {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        if (!writable) {
            throw new IllegalStateException("Database is open in read-only mode");
        }
        db.put(handle, key, value);
    }

    /**
     * Drop a column family from the database.
     * The handle is removed from internal tracking and closed.
     *
     * @param cfName the stored column family name (as returned by getColumnFamilyNames())
     * @throws IllegalStateException    if no database is open
     * @throws IllegalArgumentException if the column family is not found
     * @throws RocksDBException         if RocksDB fails to drop the column family
     */
    public synchronized void dropColumnFamily(String cfName) throws RocksDBException {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        if (!writable) {
            throw new IllegalStateException("Database is open in read-only mode");
        }
        ColumnFamilyHandle handle = handlesByName.get(cfName);
        if (handle == null) {
            throw new IllegalArgumentException("Column family not found: " + cfName);
        }
        LOG.info("Dropping column family: {}", cfName);
        db.dropColumnFamily(handle);
        handlesByName.remove(cfName);
        columnFamilyHandles.remove(handle);
        handle.close();
    }

    public String getCurrentPath() {
        return currentPath;
    }

    public DatabaseFormat getFormat() {
        return format;
    }

    /**
     * Parse max_open_files from the RocksDB OPTIONS file in the current database directory.
     * Returns -1 if the value is unlimited, not found, or the file cannot be read.
     */
    public int getMaxOpenFiles() {
        if (!isOpen) {
            throw new IllegalStateException("No database is open");
        }
        try {
            long latestSeq = -1;
            Path latestOptions = null;
            try (DirectoryStream<Path> stream =
                    Files.newDirectoryStream(Paths.get(currentPath), "OPTIONS-*")) {
                for (Path p : stream) {
                    String name = p.getFileName().toString();
                    try {
                        long seq = Long.parseLong(name.substring("OPTIONS-".length()));
                        if (seq > latestSeq) {
                            latestSeq = seq;
                            latestOptions = p;
                        }
                    } catch (NumberFormatException e) {
                        // skip non-numeric suffixes
                    }
                }
            }
            if (latestOptions == null) {
                return -1;
            }
            for (String line : Files.readAllLines(latestOptions)) {
                String trimmed = line.trim();
                if (trimmed.startsWith("max_open_files=")) {
                    int value = Integer.parseInt(
                        trimmed.substring("max_open_files=".length()).trim());
                    return value;
                }
            }
        } catch (Exception e) {
            LOG.warn("Could not parse max_open_files from OPTIONS file: {}", e.getMessage());
        }
        return -1;
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
