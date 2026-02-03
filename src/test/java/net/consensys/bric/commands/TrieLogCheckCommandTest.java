package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Hash;
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

/**
 * Tests for TrieLogCheckCommand.
 */
class TrieLogCheckCommandTest {

    @TempDir
    Path tempDir;

    private TrieLogCheckCommand command;
    private BesuDatabaseManager dbManager;
    private RocksDB rocksDB;
    private List<ColumnFamilyHandle> columnFamilyHandles;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;
    private PrintStream originalOut;
    private PrintStream originalErr;

    // Block hash by number prefix for BLOCKCHAIN segment (0x05 = BLOCK_HASH_BY_NUMBER)
    private static final byte[] BLOCK_HASH_PREFIX = new byte[] {0x05};
    // Block header prefix for BLOCKCHAIN segment (0x02 = BLOCK_HEADER)
    private static final byte[] BLOCK_HEADER_PREFIX = new byte[] {0x02};

    @BeforeEach
    void setUp() throws Exception {
        RocksDB.loadLibrary();
        dbManager = new BesuDatabaseManager();
        command = new TrieLogCheckCommand(dbManager);
        columnFamilyHandles = new ArrayList<>();

        // Capture System.out and System.err for verification
        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        originalOut = System.out;
        originalErr = System.err;
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @AfterEach
    void tearDown() {
        // Restore System.out and System.err
        System.setOut(originalOut);
        System.setErr(originalErr);

        if (dbManager.isOpen()) {
            dbManager.closeDatabase();
        }
        if (rocksDB != null) {
            for (ColumnFamilyHandle handle : columnFamilyHandles) {
                handle.close();
            }
            rocksDB.close();
        }
    }

    @Test
    void testDatabaseNotOpen_PrintsError() {
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"0..10"});

        String error = errorStream.toString();
        assertThat(error).contains("Error: No database is open");
        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testNoArguments_PrintsError() throws Exception {
        setupTestDatabase();

        command.execute(new String[]{});

        String error = errorStream.toString();
        assertThat(error).contains("Error: Missing range argument");
    }

    @Test
    void testValidRange_ExecutesSuccessfully() throws Exception {
        setupTestDatabase();

        command.execute(new String[]{"0..5"});

        String output = outputStream.toString();
        assertThat(output).contains("Trielog Check Results");
        assertThat(output).contains("Range: 0 to 5");
    }

    @Test
    void testInvalidRangeFormat_PrintsError() throws Exception {
        setupTestDatabase();

        command.execute(new String[]{"invalid"});

        String error = errorStream.toString();
        assertThat(error).contains("Error: Invalid range format");
    }

    @Test
    void testInvalidRangeSeparator_PrintsError() throws Exception {
        setupTestDatabase();

        command.execute(new String[]{"0-5"});

        String error = errorStream.toString();
        assertThat(error).contains("Error: Invalid range format");
    }

    @Test
    void testStartGreaterThanEnd_PrintsError() throws Exception {
        setupTestDatabase();

        command.execute(new String[]{"10..5"});

        String error = errorStream.toString();
        assertThat(error).contains("Error: Start block must be <= end block");
    }

    @Test
    void testNegativeBlockNumber_PrintsError() throws Exception {
        setupTestDatabase();

        command.execute(new String[]{"-1..5"});

        String error = errorStream.toString();
        assertThat(error).contains("Error");
        assertThat(error).contains("negative");
    }

    @Test
    void testAllTrielogsPresent_ShowsSuccess() throws Exception {
        setupDatabaseWithTrielogs(0, 10);

        command.execute(new String[]{"0..10"});

        String output = outputStream.toString();
        assertThat(output).contains("Trielog Check Results");
        assertThat(output).contains("Total blocks checked: 11");
        assertThat(output).contains("Trielogs found: 11");
        assertThat(output).contains("Trielogs missing: 0");
        assertThat(output).contains("Success: All trielogs are present");
    }

    @Test
    void testSomeTrielogsMissing_ShowsMissingBlocks() throws Exception {
        // Create trielogs for blocks 0, 2, 4, 6, 8, 10 (skip odd numbers)
        setupDatabaseWithSelectedTrielogs(new long[]{0, 2, 4, 6, 8, 10});

        command.execute(new String[]{"0..10"});

        String output = outputStream.toString();
        assertThat(output).contains("Trielog Check Results");
        assertThat(output).contains("Total blocks checked: 11");
        assertThat(output).contains("Trielogs found: 6");
        assertThat(output).contains("Trielogs missing: 5");
        assertThat(output).contains("Missing trielog blocks:");
        assertThat(output).contains("Block 1");
        assertThat(output).contains("Block 3");
        assertThat(output).contains("Block 5");
    }

    @Test
    void testMissingBlocks_ShowsAllMissingBlocks() throws Exception {
        // Create trielogs for blocks 0, 1, 2, 10, 11, 12 (missing 3-9)
        setupDatabaseWithSelectedTrielogs(new long[]{0, 1, 2, 10, 11, 12});

        command.execute(new String[]{"0..12"});

        String output = outputStream.toString();
        assertThat(output).contains("Missing trielog blocks:");
        assertThat(output).contains("Block 3");
        assertThat(output).contains("Block 4");
        assertThat(output).contains("Block 9");
    }

    @Test
    void testGetHelp_ReturnsDescription() {
        String help = command.getHelp();
        assertThat(help).isNotEmpty();
        assertThat(help).contains("trielog");
    }

    @Test
    void testGetUsage_ReturnsUsageString() {
        String usage = command.getUsage();
        assertThat(usage).isNotEmpty();
        assertThat(usage).contains("trielog-check");
        assertThat(usage).contains("start..end");
    }

    // ========== Helper Methods ==========

    /**
     * Setup a minimal test database with basic structure.
     */
    private void setupTestDatabase() throws Exception {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.BLOCKCHAIN.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()));

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        rocksDB = RocksDB.open(dbOptions, tempDir.toString(), columnFamilyDescriptors, columnFamilyHandles);

        // Write a block header for block 10 (so chain head = 10)
        ColumnFamilyHandle blockchainCf = columnFamilyHandles.get(1);
        byte[] blockKey = createBlockHeaderKey(10L);
        rocksDB.put(blockchainCf, blockKey, new byte[]{0x01}); // Dummy block header

        // Close and reopen with BesuDatabaseManager
        closeRocksDB();
        dbManager.openDatabase(tempDir.toString());
    }

    /**
     * Setup database with trielogs for all blocks in range [start, end].
     * Only stores minimal data - just enough to check existence.
     */
    private void setupDatabaseWithTrielogs(long start, long end) throws Exception {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.BLOCKCHAIN.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()));

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        rocksDB = RocksDB.open(dbOptions, tempDir.toString(), columnFamilyDescriptors, columnFamilyHandles);

        ColumnFamilyHandle blockchainCf = columnFamilyHandles.get(1);
        ColumnFamilyHandle trielogCf = columnFamilyHandles.get(2);

        // Create trielogs for each block in range
        for (long blockNum = start; blockNum <= end; blockNum++) {
            // Create a deterministic block hash based on block number
            Hash blockHash = Hash.hash(Bytes.ofUnsignedLong(blockNum));

            // Store block hash in BLOCKCHAIN segment with block hash lookup key
            byte[] blockHashKey = createBlockHashKey(blockNum);
            rocksDB.put(blockchainCf, blockHashKey, blockHash.toArrayUnsafe());

            // Also store block header for chain head detection
            byte[] blockHeaderKey = createBlockHeaderKey(blockNum);
            rocksDB.put(blockchainCf, blockHeaderKey, new byte[]{0x01}); // Dummy header

            // Store minimal trielog data (just a non-empty byte array)
            // We don't need valid RLP since we only check existence
            rocksDB.put(trielogCf, blockHash.toArrayUnsafe(), new byte[]{0x01});
        }

        closeRocksDB();
        dbManager.openDatabase(tempDir.toString());
    }

    /**
     * Setup database with trielogs only for selected blocks.
     */
    private void setupDatabaseWithSelectedTrielogs(long[] blockNumbers) throws Exception {
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.BLOCKCHAIN.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()));

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        rocksDB = RocksDB.open(dbOptions, tempDir.toString(), columnFamilyDescriptors, columnFamilyHandles);

        ColumnFamilyHandle blockchainCf = columnFamilyHandles.get(1);
        ColumnFamilyHandle trielogCf = columnFamilyHandles.get(2);

        // Find max block for chain head
        long maxBlock = blockNumbers[0];
        for (long blockNum : blockNumbers) {
            if (blockNum > maxBlock) {
                maxBlock = blockNum;
            }
        }

        // Create block headers for all blocks up to max (but only trielogs for selected blocks)
        for (long blockNum = 0; blockNum <= maxBlock; blockNum++) {
            // Create a deterministic block hash based on block number
            Hash blockHash = Hash.hash(Bytes.ofUnsignedLong(blockNum));

            // Store block hash in BLOCKCHAIN segment with block hash lookup key
            byte[] blockHashKey = createBlockHashKey(blockNum);
            rocksDB.put(blockchainCf, blockHashKey, blockHash.toArrayUnsafe());

            // Also store block header for chain head detection
            byte[] blockHeaderKey = createBlockHeaderKey(blockNum);
            rocksDB.put(blockchainCf, blockHeaderKey, new byte[]{0x01}); // Dummy header

            // Only create trielog if this block is in the selected list
            boolean shouldCreateTrielog = false;
            for (long selectedBlock : blockNumbers) {
                if (blockNum == selectedBlock) {
                    shouldCreateTrielog = true;
                    break;
                }
            }

            if (shouldCreateTrielog) {
                // Store minimal trielog data (just a non-empty byte array)
                rocksDB.put(trielogCf, blockHash.toArrayUnsafe(), new byte[]{0x01});
            }
        }

        closeRocksDB();
        dbManager.openDatabase(tempDir.toString());
    }

    /**
     * Create a block hash lookup key: [0x05][blockNumber as UInt256]
     */
    private byte[] createBlockHashKey(long blockNumber) {
        UInt256 blockNumberUInt = UInt256.valueOf(blockNumber);
        return Bytes.concatenate(
                Bytes.wrap(BLOCK_HASH_PREFIX),
                blockNumberUInt
        ).toArrayUnsafe();
    }

    /**
     * Create a block header key: [0x02][blockNumber(8 bytes)]
     */
    private byte[] createBlockHeaderKey(long blockNumber) {
        return Bytes.concatenate(
                Bytes.wrap(BLOCK_HEADER_PREFIX),
                Bytes.ofUnsignedLong(blockNumber)
        ).toArrayUnsafe();
    }

    /**
     * Close RocksDB instance and handles.
     */
    private void closeRocksDB() {
        if (rocksDB != null) {
            for (ColumnFamilyHandle handle : columnFamilyHandles) {
                handle.close();
            }
            columnFamilyHandles.clear();
            rocksDB.close();
            rocksDB = null;
        }
    }
}
