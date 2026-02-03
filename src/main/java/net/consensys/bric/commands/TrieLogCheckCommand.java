package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Hash;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Command to check that all trielogs are present for a specified range.
 * If no range is specified, checks from block 0 to chain head.
 * Only checks for key existence without decoding for efficiency.
 */
public class TrieLogCheckCommand implements Command {

    private static final Logger LOG = LoggerFactory.getLogger(TrieLogCheckCommand.class);

    private final BesuDatabaseManager dbManager;
    private final BesuDatabaseReader dbReader;

    public TrieLogCheckCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.dbReader = new BesuDatabaseReader(dbManager);
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        try {
            long startBlock;
            long endBlock;

            if (args.length == 0) {
                // No range specified, use 0 to chain head
                startBlock = 0;
                Optional<Long> chainHead = findChainHead();
                if (chainHead.isEmpty()) {
                    System.err.println("Error: Could not determine chain head");
                    return;
                }
                endBlock = chainHead.get();
                System.out.println("Checking trielogs from block 0 to chain head (block " + endBlock + ")...");
            } else if (args[0].contains("..")) {
                // Range format: "start..end"
                String[] parts = args[0].split("\\.\\.");
                if (parts.length != 2) {
                    System.err.println("Error: Invalid range format. Expected: start..end");
                    System.err.println("Usage: " + getUsage());
                    return;
                }
                startBlock = parseBlockNumber(parts[0]);
                endBlock = parseBlockNumber(parts[1]);
                System.out.println("Checking trielogs from block " + startBlock + " to " + endBlock + "...");
            } else {
                System.err.println("Error: Invalid argument. Expected range (start..end) or no argument for full check");
                System.err.println("Usage: " + getUsage());
                return;
            }

            if (startBlock > endBlock) {
                System.err.println("Error: Start block must be <= end block");
                return;
            }

            // Perform the check
            checkTrieLogRange(startBlock, endBlock);

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error checking trielogs: " + e.getMessage());
            LOG.error("Error checking trielogs", e);
        }
    }

    /**
     * Check that all trielogs exist in the specified range.
     * Only checks for key existence without decoding the trielog for efficiency.
     */
    private void checkTrieLogRange(long startBlock, long endBlock) {
        List<Long> missingBlocks = new ArrayList<>();
        long totalBlocks = endBlock - startBlock + 1;
        long checkedBlocks = 0;
        long foundTrielogs = 0;

        // Check each block in range
        for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
            checkedBlocks++;

            // Show progress for large ranges (every 1000 blocks)
            if (checkedBlocks % 1000 == 0) {
                System.out.println("Progress: " + checkedBlocks + "/" + totalBlocks + " blocks checked...");
            }

            // Use efficient existence check without decoding
            if (dbReader.trieLogExists(blockNum)) {
                foundTrielogs++;
            } else {
                missingBlocks.add(blockNum);
            }
        }

        // Display results
        System.out.println();
        System.out.println("=== Trielog Check Results ===");
        System.out.println("Range: " + startBlock + " to " + endBlock);
        System.out.println("Total blocks checked: " + totalBlocks);
        System.out.println("Trielogs found: " + foundTrielogs);
        System.out.println("Trielogs missing: " + missingBlocks.size());

        if (!missingBlocks.isEmpty()) {
            System.out.println();
            System.out.println("Missing trielog blocks:");

            // Show first 20 missing blocks
            int displayLimit = Math.min(20, missingBlocks.size());
            for (int i = 0; i < displayLimit; i++) {
                System.out.println("  Block " + missingBlocks.get(i));
            }

            if (missingBlocks.size() > displayLimit) {
                System.out.println("  ... and " + (missingBlocks.size() - displayLimit) + " more");
            }

            // Show summary of missing ranges
            System.out.println();
            System.out.println("Missing ranges:");
            printMissingRanges(missingBlocks);
        } else {
            System.out.println();
            System.out.println("Success: All trielogs are present in the specified range!");
        }
    }

    /**
     * Print missing blocks as consolidated ranges for easier reading.
     */
    private void printMissingRanges(List<Long> missingBlocks) {
        if (missingBlocks.isEmpty()) {
            return;
        }

        long rangeStart = missingBlocks.get(0);
        long rangeEnd = missingBlocks.get(0);

        for (int i = 1; i < missingBlocks.size(); i++) {
            long current = missingBlocks.get(i);
            if (current == rangeEnd + 1) {
                // Extend current range
                rangeEnd = current;
            } else {
                // Print previous range and start new one
                printRange(rangeStart, rangeEnd);
                rangeStart = current;
                rangeEnd = current;
            }
        }

        // Print final range
        printRange(rangeStart, rangeEnd);
    }

    private void printRange(long start, long end) {
        if (start == end) {
            System.out.println("  " + start);
        } else {
            System.out.println("  " + start + ".." + end);
        }
    }

    /**
     * Find the chain head by scanning the BLOCKCHAIN segment for the highest block number.
     * Uses reverse iteration to find the last block efficiently.
     */
    private Optional<Long> findChainHead() {
        try {
            ColumnFamilyHandle cfHandle = dbManager.getColumnFamily(KeyValueSegmentIdentifier.BLOCKCHAIN);
            if (cfHandle == null) {
                LOG.warn("BLOCKCHAIN column family not found");
                return Optional.empty();
            }

            try (RocksIterator iterator = dbManager.getDatabase().newIterator(cfHandle)) {
                // Seek to the last key
                iterator.seekToLast();

                // Iterate backwards to find the highest block number
                // Block keys are prefixed with 0x02 (BLOCK_HEADER_PREFIX) followed by block number
                long maxBlockNumber = -1;

                while (iterator.isValid()) {
                    byte[] key = iterator.key();

                    // Block hash keys are prefixed with 0x02 and followed by 8-byte block number
                    // Format: [0x02][blockNumber(8 bytes)]
                    if (key.length >= 9 && key[0] == 0x02) {
                        // Extract block number from bytes 1-8 (big-endian long)
                        long blockNumber = Bytes.wrap(key, 1, 8).toLong();
                        if (blockNumber > maxBlockNumber) {
                            maxBlockNumber = blockNumber;
                        }
                    }

                    iterator.prev();

                    // Stop after checking a reasonable number of entries (prevent infinite loop)
                    if (maxBlockNumber >= 0) {
                        break;  // Found at least one block, that's our max since we're iterating backwards
                    }
                }

                if (maxBlockNumber >= 0) {
                    return Optional.of(maxBlockNumber);
                }
            }

            LOG.warn("No blocks found in BLOCKCHAIN segment");
            return Optional.empty();

        } catch (Exception e) {
            LOG.error("Error finding chain head: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Parse and validate block number.
     */
    private long parseBlockNumber(String blockNumberStr) {
        try {
            long blockNumber = Long.parseLong(blockNumberStr);
            if (blockNumber < 0) {
                throw new IllegalArgumentException(
                    "Block number cannot be negative: " + blockNumberStr
                );
            }
            return blockNumber;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid block number format. Expected: numeric value. Got: " + blockNumberStr
            );
        }
    }

    @Override
    public String getHelp() {
        return "Check that all trielogs are present for a block range";
    }

    @Override
    public String getUsage() {
        return "trielog-check [start..end]\n" +
               "                               Check trielog presence for blocks in range.\n" +
               "                               If no range specified, checks from 0 to chain head.\n" +
               "                               Examples:\n" +
               "                                 trielog-check              (check all blocks)\n" +
               "                                 trielog-check 0..1000      (check blocks 0-1000)\n" +
               "                                 trielog-check 12345..12350 (check blocks 12345-12350)";
    }
}
