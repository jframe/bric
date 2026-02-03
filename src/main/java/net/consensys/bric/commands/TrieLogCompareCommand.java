package net.consensys.bric.commands;

import net.consensys.bric.db.*;
import net.consensys.bric.formatters.TrieLogCompareFormatter;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Command to compare trielog data against Bonsai Archive flatdb storage.
 * Validates that trielog "updated" values match what's stored in the archive.
 */
public class TrieLogCompareCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final BesuDatabaseReader dbReader;
    private final TrieLogCompareFormatter formatter;
    private boolean verbose = false;

    public TrieLogCompareCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.dbReader = new BesuDatabaseReader(dbManager);
        this.formatter = new TrieLogCompareFormatter();
    }

    @Override
    public void execute(String[] args) {
        // 1. Validate database format
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            System.err.println("Error: trielog-compare only works with Bonsai Archive databases");
            System.err.println("Current database format: " + dbManager.getFormat());
            return;
        }

        // 2. Parse arguments
        if (args.length < 1) {
            System.err.println("Error: Missing block argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        // Check for --verbose flag
        verbose = hasFlag(args, "--verbose");

        String blockArg = args[0];

        // 3. Determine if single block or range
        if (blockArg.contains("..")) {
            // Range format: "start..end"
            executeRange(blockArg);
        } else {
            // Single block
            executeSingleBlock(blockArg);
        }
    }

    /**
     * Execute comparison for a single block.
     */
    private void executeSingleBlock(String blockIdentifier) {
        try {
            // Get trielog data
            Optional<TrieLogData> trieLogOpt;
            long blockNumber;

            if (isBlockNumber(blockIdentifier)) {
                blockNumber = parseBlockNumber(blockIdentifier);
                trieLogOpt = dbReader.readTrieLogByNumber(blockNumber);
            } else {
                Hash blockHash = parseHash(blockIdentifier);
                trieLogOpt = dbReader.readTrieLog(blockHash);

                if (trieLogOpt.isEmpty()) {
                    System.err.println("Error: Trielog not found for block: " + blockIdentifier);
                    return;
                }

                blockNumber = trieLogOpt.get().blockNumber;
            }

            if (trieLogOpt.isEmpty()) {
                System.err.println("Error: Trielog not found for block: " + blockIdentifier);
                return;
            }

            // Perform comparison
            TrieLogComparisonResult result = compareTrieLog(trieLogOpt.get(), blockNumber);

            // Format and display
            String formatted = formatter.format(result, verbose);
            System.out.println(formatted);

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error comparing trielog: " + e.getMessage());
        }
    }

    /**
     * Execute comparison for a range of blocks.
     */
    private void executeRange(String rangeStr) {
        try {
            String[] parts = rangeStr.split("\\.\\.");
            if (parts.length != 2) {
                System.err.println("Error: Invalid range format. Expected: start..end");
                return;
            }

            long startBlock = parseBlockNumber(parts[0]);
            long endBlock = parseBlockNumber(parts[1]);

            if (startBlock > endBlock) {
                System.err.println("Error: Start block must be <= end block");
                return;
            }

            long totalBlocks = endBlock - startBlock + 1;
            System.out.println("Comparing trielogs from block " + startBlock + " to " + endBlock + "...");

            // Compare each block in range
            List<TrieLogComparisonResult> results = new ArrayList<>();
            long checkedBlocks = 0;

            // Progress reporter updates every 30 seconds
            ProgressReporter progress = new ProgressReporter(30);

            for (long blockNum = startBlock; blockNum <= endBlock; blockNum++) {
                checkedBlocks++;

                // Report progress every 30 seconds
                progress.reportProgress(checkedBlocks, totalBlocks, "blocks compared");

                Optional<TrieLogData> trieLogOpt = dbReader.readTrieLogByNumber(blockNum);

                if (trieLogOpt.isEmpty()) {
                    System.err.println("Warning: Trielog not found for block " + blockNum + ", skipping");
                    continue;
                }

                TrieLogComparisonResult result = compareTrieLog(trieLogOpt.get(), blockNum);
                results.add(result);
            }

            if (results.isEmpty()) {
                System.err.println("Error: No trielogs found in range " + rangeStr);
                return;
            }

            // Format and display range results
            String formatted = formatter.formatRange(results, verbose);
            System.out.println(formatted);

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error comparing trielog range: " + e.getMessage());
        }
    }

    /**
     * Core comparison logic: compare trielog against archive storage.
     */
    private TrieLogComparisonResult compareTrieLog(TrieLogData trieLog, long blockNumber) {
        TrieLogComparisonResult result = new TrieLogComparisonResult();
        result.blockNumber = blockNumber;
        result.blockHash = trieLog.blockHash;

        // Compare account changes
        compareAccountChanges(trieLog, blockNumber, result);

        // Compare storage changes
        compareStorageChanges(trieLog, blockNumber, result);

        // Compare code changes
        compareCodeChanges(trieLog, blockNumber, result);

        return result;
    }

    /**
     * Compare account changes between trielog and archive.
     */
    private void compareAccountChanges(TrieLogData trieLog, long blockNumber, TrieLogComparisonResult result) {
        Map<Address, PathBasedValue<AccountValue>> accountChanges = trieLog.trieLogLayer.getAccountChanges();

        for (Map.Entry<Address, PathBasedValue<AccountValue>> entry : accountChanges.entrySet()) {
            Address address = entry.getKey();
            AccountValue updated = entry.getValue().getUpdated();

            // Skip deleted accounts (updated == null)
            if (updated == null) {
                result.totalAccountComparisons++;
                result.accountMatches++;
                continue;
            }

            // Query archive at this block
            Optional<AccountData> archiveData =
                dbReader.readAccountAtBlock(address, blockNumber);

            if (archiveData.isEmpty()) {
                // Mismatch: trielog says account exists, but not in archive
                TrieLogComparisonResult.AccountMismatch mismatch = new TrieLogComparisonResult.AccountMismatch();
                mismatch.address = address;
                mismatch.field = "account";
                mismatch.expectedValue = "EXISTS";
                mismatch.actualValue = "NOT_FOUND";
                result.accountMismatchList.add(mismatch);
                result.totalAccountComparisons++;
                result.accountMismatches++;
                continue;
            }

            // Compare each field
            result.totalAccountComparisons++;
            boolean hasFieldMismatch = false;

            AccountData archive = archiveData.get();

            // Compare nonce
            if (updated.getNonce() != archive.nonce) {
                TrieLogComparisonResult.AccountMismatch mismatch = new TrieLogComparisonResult.AccountMismatch();
                mismatch.address = address;
                mismatch.field = "nonce";
                mismatch.expectedValue = String.valueOf(updated.getNonce());
                mismatch.actualValue = String.valueOf(archive.nonce);
                mismatch.archiveBlockNumber = archive.blockNumber;
                result.accountMismatchList.add(mismatch);
                hasFieldMismatch = true;
            }

            // Compare balance
            if (!updated.getBalance().equals(archive.balance)) {
                TrieLogComparisonResult.AccountMismatch mismatch = new TrieLogComparisonResult.AccountMismatch();
                mismatch.address = address;
                mismatch.field = "balance";
                mismatch.expectedValue = updated.getBalance().toHexString();
                mismatch.actualValue = archive.balance.toHexString();
                mismatch.archiveBlockNumber = archive.blockNumber;
                result.accountMismatchList.add(mismatch);
                hasFieldMismatch = true;
            }

            // Compare storageRoot
            if (!updated.getStorageRoot().equals(archive.storageRoot)) {
                TrieLogComparisonResult.AccountMismatch mismatch = new TrieLogComparisonResult.AccountMismatch();
                mismatch.address = address;
                mismatch.field = "storageRoot";
                mismatch.expectedValue = updated.getStorageRoot().toHexString();
                mismatch.actualValue = archive.storageRoot.toHexString();
                mismatch.archiveBlockNumber = archive.blockNumber;
                result.accountMismatchList.add(mismatch);
                hasFieldMismatch = true;
            }

            // Compare codeHash
            if (!updated.getCodeHash().equals(archive.codeHash)) {
                TrieLogComparisonResult.AccountMismatch mismatch = new TrieLogComparisonResult.AccountMismatch();
                mismatch.address = address;
                mismatch.field = "codeHash";
                mismatch.expectedValue = updated.getCodeHash().toHexString();
                mismatch.actualValue = archive.codeHash.toHexString();
                mismatch.archiveBlockNumber = archive.blockNumber;
                result.accountMismatchList.add(mismatch);
                hasFieldMismatch = true;
            }

            if (hasFieldMismatch) {
                result.accountMismatches++;
            } else {
                result.accountMatches++;
                if (verbose) {
                    TrieLogComparisonResult.AccountMatch match = new TrieLogComparisonResult.AccountMatch();
                    match.address = address;
                    match.archiveBlockNumber = archive.blockNumber;
                    result.accountMatchList.add(match);
                }
            }
        }
    }

    /**
     * Compare storage changes between trielog and archive.
     */
    private void compareStorageChanges(TrieLogData trieLog, long blockNumber, TrieLogComparisonResult result) {
        Map<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> storageChanges =
            trieLog.trieLogLayer.getStorageChanges();

        for (Map.Entry<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> accountEntry : storageChanges.entrySet()) {
            Address address = accountEntry.getKey();

            for (Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> slotEntry : accountEntry.getValue().entrySet()) {
                Hash slotHash = slotEntry.getKey().getSlotHash();
                UInt256 updatedValue = slotEntry.getValue().getUpdated();

                result.totalStorageComparisons++;

                // Skip if updated is null (deleted)
                if (updatedValue == null) {
                    result.storageMatches++;
                    continue;
                }

                // Query storage archive
                Optional<StorageData> archiveStorage =
                    dbReader.readStorageByHashAtBlock(address.addressHash(), slotHash, blockNumber);

                if (archiveStorage.isEmpty()) {
                    // Check if expected value is zero (might not be stored)
                    if (updatedValue.isZero()) {
                        result.storageMatches++;
                        continue;
                    }

                    // Mismatch: trielog says non-zero, but not in archive
                    TrieLogComparisonResult.StorageMismatch mismatch = new TrieLogComparisonResult.StorageMismatch();
                    mismatch.address = address;
                    mismatch.slotHash = slotHash;
                    mismatch.expectedValue = updatedValue;
                    mismatch.actualValue = UInt256.ZERO;
                    result.storageMismatchList.add(mismatch);
                    result.storageMismatches++;
                    continue;
                }

                UInt256 archiveValue = archiveStorage.get().value;

                if (!updatedValue.equals(archiveValue)) {
                    TrieLogComparisonResult.StorageMismatch mismatch = new TrieLogComparisonResult.StorageMismatch();
                    mismatch.address = address;
                    mismatch.slotHash = slotHash;
                    mismatch.expectedValue = updatedValue;
                    mismatch.actualValue = archiveValue;
                    mismatch.archiveBlockNumber = archiveStorage.get().blockNumber;
                    result.storageMismatchList.add(mismatch);
                    result.storageMismatches++;
                } else {
                    result.storageMatches++;
                    if (verbose) {
                        TrieLogComparisonResult.StorageMatch match = new TrieLogComparisonResult.StorageMatch();
                        match.address = address;
                        match.slotHash = slotHash;
                        match.value = archiveValue;
                        match.archiveBlockNumber = archiveStorage.get().blockNumber;
                        result.storageMatchList.add(match);
                    }
                }
            }
        }
    }

    /**
     * Compare code changes between trielog and archive.
     */
    private void compareCodeChanges(TrieLogData trieLog, long blockNumber, TrieLogComparisonResult result) {
        Map<Address, PathBasedValue<Bytes>> codeChanges = trieLog.trieLogLayer.getCodeChanges();

        for (Map.Entry<Address, PathBasedValue<Bytes>> entry : codeChanges.entrySet()) {
            Address address = entry.getKey();
            Bytes updatedCode = entry.getValue().getUpdated();

            result.totalCodeComparisons++;

            if (updatedCode == null) {
                // Code deleted/cleared
                result.codeMatches++;
                continue;
            }

            Hash expectedCodeHash = Hash.hash(updatedCode);

            // Query account to get actual code hash
            Optional<AccountData> accountData =
                dbReader.readAccountAtBlock(address, blockNumber);

            if (accountData.isEmpty()) {
                TrieLogComparisonResult.CodeMismatch mismatch = new TrieLogComparisonResult.CodeMismatch();
                mismatch.address = address;
                mismatch.expectedCodeHash = expectedCodeHash;
                mismatch.actualCodeHash = Hash.EMPTY;
                mismatch.expectedSize = updatedCode.size();
                mismatch.actualSize = 0;
                result.codeMismatchList.add(mismatch);
                result.codeMismatches++;
                continue;
            }

            Hash actualCodeHash = accountData.get().codeHash;

            if (!expectedCodeHash.equals(actualCodeHash)) {
                TrieLogComparisonResult.CodeMismatch mismatch = new TrieLogComparisonResult.CodeMismatch();
                mismatch.address = address;
                mismatch.expectedCodeHash = expectedCodeHash;
                mismatch.actualCodeHash = actualCodeHash;
                mismatch.expectedSize = updatedCode.size();
                // Would need to query code storage to get actual size
                result.codeMismatchList.add(mismatch);
                result.codeMismatches++;
            } else {
                result.codeMatches++;
                if (verbose) {
                    TrieLogComparisonResult.CodeMatch match = new TrieLogComparisonResult.CodeMatch();
                    match.address = address;
                    match.codeHash = actualCodeHash;
                    match.size = updatedCode.size();
                    result.codeMatchList.add(match);
                }
            }
        }
    }

    /**
     * Check if the input string is a block number (numeric) rather than a hash.
     */
    private boolean isBlockNumber(String input) {
        if (input.startsWith("0x") || input.startsWith("0X")) {
            return false;
        }
        try {
            Long.parseLong(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
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

    /**
     * Parse and validate 32-byte block hash.
     */
    private Hash parseHash(String hashStr) {
        if (!hashStr.startsWith("0x")) {
            throw new IllegalArgumentException(
                "Invalid block hash format. Expected: 0x-prefixed hex (64 chars). Got: " + hashStr
            );
        }

        if (hashStr.length() != 66) {
            throw new IllegalArgumentException(
                "Invalid block hash length. Expected: 66 chars (0x + 64 hex). Got: " + hashStr.length()
            );
        }

        try {
            return Hash.fromHexString(hashStr);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid block hash format: " + e.getMessage()
            );
        }
    }

    /**
     * Check if a flag is present in the arguments.
     */
    private boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (flag.equals(arg)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getHelp() {
        return "Compare trielog data against Bonsai Archive flatdb storage";
    }

    @Override
    public String getUsage() {
        return "trielog-compare <block-number|block-hash|start..end> [--verbose]\n" +
               "                               Examples:\n" +
               "                                 trielog-compare 12345\n" +
               "                                 trielog-compare 12345..12350\n" +
               "                                 trielog-compare 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef --verbose";
    }
}
