package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Command to check that all trielogs are present for a specified range.
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

        if (args.length == 0) {
            System.err.println("Error: Missing range argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        try {
            // Parse range format: "start..end"
            String rangeStr = args[0];
            if (!rangeStr.contains("..")) {
                System.err.println("Error: Invalid range format. Expected: start..end");
                System.err.println("Usage: " + getUsage());
                return;
            }

            String[] parts = rangeStr.split("\\.\\.");
            if (parts.length != 2) {
                System.err.println("Error: Invalid range format. Expected: start..end");
                System.err.println("Usage: " + getUsage());
                return;
            }

            long startBlock = parseBlockNumber(parts[0]);
            long endBlock = parseBlockNumber(parts[1]);

            if (startBlock > endBlock) {
                System.err.println("Error: Start block must be <= end block");
                return;
            }

            System.out.println("Checking trielogs from block " + startBlock + " to " + endBlock + "...");

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

        // Progress reporter updates every 30 seconds
        ProgressReporter progress = new ProgressReporter(30);

        // Check each block in range
        for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
            checkedBlocks++;

            // Report progress every 30 seconds
            progress.reportProgress(checkedBlocks, totalBlocks, "blocks checked");

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

            // Show all missing blocks
            for (Long blockNum : missingBlocks) {
                System.out.println("  Block " + blockNum);
            }
        } else {
            System.out.println();
            System.out.println("Success: All trielogs are present in the specified range!");
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
        return "trielog-check <start..end>\n" +
               "                               Check trielog presence for blocks in range.\n" +
               "                               Examples:\n" +
               "                                 trielog-check 0..1000      (check blocks 0-1000)\n" +
               "                                 trielog-check 12345..12350 (check blocks 12345-12350)";
    }
}
