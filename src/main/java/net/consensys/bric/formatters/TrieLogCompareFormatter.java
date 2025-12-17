package net.consensys.bric.formatters;

import net.consensys.bric.db.TrieLogComparisonResult;
import net.consensys.bric.db.TrieLogComparisonResult.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

/**
 * Formats trielog comparison results for human-readable display.
 */
public class TrieLogCompareFormatter {

    private static final BigInteger WEI_PER_ETH = new BigInteger("1000000000000000000");

    /**
     * Format single block comparison result.
     */
    public String format(TrieLogComparisonResult result, boolean verbose) {
        StringBuilder sb = new StringBuilder();

        sb.append("\nTrieLog Comparison Result:\n");
        sb.append("  Block Number: ").append(String.format("%,d", result.blockNumber)).append("\n");
        sb.append("  Block Hash: ").append(result.blockHash.toHexString()).append("\n\n");

        // Summary statistics
        sb.append("═══ Summary ═══\n\n");

        int totalChecks = result.totalAccountComparisons +
                         result.totalStorageComparisons +
                         result.totalCodeComparisons;
        int totalMatches = result.accountMatches +
                          result.storageMatches +
                          result.codeMatches;
        int totalMismatches = result.accountMismatches +
                             result.storageMismatches +
                             result.codeMismatches;

        sb.append("Total Checks:    ").append(totalChecks).append("\n");
        sb.append("Matches:         ").append(totalMatches).append("\n");
        sb.append("Mismatches:      ").append(totalMismatches).append("\n\n");

        sb.append("Account Checks:  ").append(result.totalAccountComparisons)
          .append(" (").append(result.accountMatches).append(" matches, ")
          .append(result.accountMismatches).append(" mismatches)\n");

        sb.append("Storage Checks:  ").append(result.totalStorageComparisons)
          .append(" (").append(result.storageMatches).append(" matches, ")
          .append(result.storageMismatches).append(" mismatches)\n");

        sb.append("Code Checks:     ").append(result.totalCodeComparisons)
          .append(" (").append(result.codeMatches).append(" matches, ")
          .append(result.codeMismatches).append(" mismatches)\n\n");

        // Show mismatches if any
        if (totalMismatches > 0) {
            sb.append("═══ Mismatches ═══\n\n");

            if (!result.accountMismatchList.isEmpty()) {
                sb.append("--- Account Mismatches ---\n\n");
                for (AccountMismatch mismatch : result.accountMismatchList) {
                    formatAccountMismatch(sb, mismatch);
                }
            }

            if (!result.storageMismatchList.isEmpty()) {
                sb.append("--- Storage Mismatches ---\n\n");
                for (StorageMismatch mismatch : result.storageMismatchList) {
                    formatStorageMismatch(sb, mismatch);
                }
            }

            if (!result.codeMismatchList.isEmpty()) {
                sb.append("--- Code Mismatches ---\n\n");
                for (CodeMismatch mismatch : result.codeMismatchList) {
                    formatCodeMismatch(sb, mismatch);
                }
            }
        } else {
            sb.append("✓ All checks passed - trielog matches archive storage\n");
        }

        // Show matches if verbose
        if (verbose && totalMatches > 0) {
            sb.append("\n═══ Matches (Verbose) ═══\n\n");

            if (!result.accountMatchList.isEmpty()) {
                sb.append("--- Account Matches ---\n");
                for (AccountMatch match : result.accountMatchList) {
                    sb.append("  ✓ ").append(match.address.toHexString());
                    if (match.archiveBlockNumber != null) {
                        sb.append(" (block ").append(match.archiveBlockNumber).append(")");
                    }
                    sb.append("\n");
                }
                sb.append("\n");
            }

            if (!result.storageMatchList.isEmpty()) {
                sb.append("--- Storage Matches ---\n");
                for (StorageMatch match : result.storageMatchList) {
                    sb.append("  ✓ ").append(match.address.toHexString())
                      .append(" slot ").append(match.slotHash.toHexString());
                    if (match.archiveBlockNumber != null) {
                        sb.append(" (block ").append(match.archiveBlockNumber).append(")");
                    }
                    sb.append("\n");
                }
                sb.append("\n");
            }

            if (!result.codeMatchList.isEmpty()) {
                sb.append("--- Code Matches ---\n");
                for (CodeMatch match : result.codeMatchList) {
                    sb.append("  ✓ ").append(match.address.toHexString())
                      .append(" (").append(match.size).append(" bytes)\n");
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    /**
     * Format range comparison results (summary of multiple blocks).
     */
    public String formatRange(List<TrieLogComparisonResult> results, boolean verbose) {
        if (results.isEmpty()) {
            return "\nNo comparison results available.\n";
        }

        StringBuilder sb = new StringBuilder();

        sb.append("\nTrieLog Range Comparison:\n");
        sb.append("  Blocks: ").append(results.get(0).blockNumber)
          .append(" to ").append(results.get(results.size() - 1).blockNumber)
          .append(" (").append(results.size()).append(" blocks)\n\n");

        // Aggregate statistics
        int totalBlocks = results.size();
        int blocksWithMismatches = 0;
        int totalMismatches = 0;
        int totalChecks = 0;
        int totalMatches = 0;

        for (TrieLogComparisonResult result : results) {
            int blockMismatches = result.accountMismatches +
                                 result.storageMismatches +
                                 result.codeMismatches;
            if (blockMismatches > 0) {
                blocksWithMismatches++;
                totalMismatches += blockMismatches;
            }

            totalChecks += result.totalAccountComparisons +
                          result.totalStorageComparisons +
                          result.totalCodeComparisons;
            totalMatches += result.accountMatches +
                           result.storageMatches +
                           result.codeMatches;
        }

        sb.append("═══ Range Summary ═══\n\n");
        sb.append("Blocks Checked:      ").append(totalBlocks).append("\n");
        sb.append("Blocks with Issues:  ").append(blocksWithMismatches).append("\n");
        sb.append("Total Checks:        ").append(totalChecks).append("\n");
        sb.append("Total Matches:       ").append(totalMatches).append("\n");
        sb.append("Total Mismatches:    ").append(totalMismatches).append("\n\n");

        if (blocksWithMismatches > 0) {
            sb.append("═══ Blocks with Mismatches ═══\n\n");

            for (TrieLogComparisonResult result : results) {
                int blockMismatches = result.accountMismatches +
                                     result.storageMismatches +
                                     result.codeMismatches;
                if (blockMismatches > 0) {
                    sb.append("Block ").append(String.format("%,d", result.blockNumber))
                      .append(": ").append(blockMismatches).append(" mismatch");
                    if (blockMismatches != 1) {
                        sb.append("es");
                    }
                    sb.append(" (")
                      .append(result.accountMismatches).append(" account, ")
                      .append(result.storageMismatches).append(" storage, ")
                      .append(result.codeMismatches).append(" code)\n");
                }
            }

            // Show detailed results for each problematic block if verbose
            if (verbose) {
                sb.append("\n═══ Detailed Results ═══\n");
                for (TrieLogComparisonResult result : results) {
                    int blockMismatches = result.accountMismatches +
                                         result.storageMismatches +
                                         result.codeMismatches;
                    if (blockMismatches > 0) {
                        sb.append("\n--- Block ").append(result.blockNumber).append(" ---\n\n");

                        if (!result.accountMismatchList.isEmpty()) {
                            sb.append("Account Mismatches:\n");
                            for (AccountMismatch mismatch : result.accountMismatchList) {
                                formatAccountMismatch(sb, mismatch);
                            }
                        }

                        if (!result.storageMismatchList.isEmpty()) {
                            sb.append("Storage Mismatches:\n");
                            for (StorageMismatch mismatch : result.storageMismatchList) {
                                formatStorageMismatch(sb, mismatch);
                            }
                        }

                        if (!result.codeMismatchList.isEmpty()) {
                            sb.append("Code Mismatches:\n");
                            for (CodeMismatch mismatch : result.codeMismatchList) {
                                formatCodeMismatch(sb, mismatch);
                            }
                        }
                    }
                }
            }
        } else {
            sb.append("✓ All blocks passed - trielog matches archive storage\n");
        }

        return sb.toString();
    }

    private void formatAccountMismatch(StringBuilder sb, AccountMismatch mismatch) {
        sb.append("Address: ").append(mismatch.address.toHexString()).append("\n");
        sb.append("  Field:    ").append(mismatch.field).append("\n");

        // Special formatting for balance fields (add ETH conversion)
        if ("balance".equals(mismatch.field)) {
            sb.append("  Expected: ").append(formatBalanceWithEth(mismatch.expectedValue)).append("\n");
            sb.append("  Actual:   ").append(formatBalanceWithEth(mismatch.actualValue)).append("\n");
        } else {
            sb.append("  Expected: ").append(mismatch.expectedValue).append("\n");
            sb.append("  Actual:   ").append(mismatch.actualValue).append("\n");
        }

        if (mismatch.archiveBlockNumber != null) {
            sb.append("  Archive Block: ").append(mismatch.archiveBlockNumber).append("\n");
        }
        sb.append("\n");
    }

    private void formatStorageMismatch(StringBuilder sb, StorageMismatch mismatch) {
        sb.append("Address: ").append(mismatch.address.toHexString()).append("\n");
        sb.append("  Slot:     ").append(mismatch.slotHash.toHexString()).append("\n");
        sb.append("  Expected: ").append(mismatch.expectedValue.toHexString()).append("\n");
        sb.append("  Actual:   ").append(mismatch.actualValue.toHexString()).append("\n");
        if (mismatch.archiveBlockNumber != null) {
            sb.append("  Archive Block: ").append(mismatch.archiveBlockNumber).append("\n");
        }
        sb.append("\n");
    }

    private void formatCodeMismatch(StringBuilder sb, CodeMismatch mismatch) {
        sb.append("Address: ").append(mismatch.address.toHexString()).append("\n");
        sb.append("  Expected Hash: ").append(mismatch.expectedCodeHash.toHexString())
          .append(" (").append(mismatch.expectedSize).append(" bytes)\n");
        sb.append("  Actual Hash:   ").append(mismatch.actualCodeHash.toHexString());
        if (mismatch.actualSize > 0) {
            sb.append(" (").append(mismatch.actualSize).append(" bytes)");
        }
        sb.append("\n\n");
    }

    /**
     * Format a balance hex string with ETH conversion.
     * Input is expected to be a hex string (e.g., "0x1234..." or just the balance value).
     */
    private String formatBalanceWithEth(String hexValue) {
        if (hexValue == null || hexValue.isEmpty()) {
            return "0x0 (0 ETH)";
        }

        try {
            // Parse the hex string as BigInteger
            BigInteger weiValue;
            if (hexValue.startsWith("0x") || hexValue.startsWith("0X")) {
                weiValue = new BigInteger(hexValue.substring(2), 16);
            } else {
                // Try parsing as hex without prefix
                weiValue = new BigInteger(hexValue, 16);
            }

            if (weiValue.equals(BigInteger.ZERO)) {
                return "0x0 (0 ETH)";
            }

            // Convert to ETH for human-readable reference
            BigDecimal ethValue = new BigDecimal(weiValue)
                .divide(new BigDecimal(WEI_PER_ETH), 18, RoundingMode.DOWN)
                .stripTrailingZeros();

            String ethStr = ethValue.toPlainString();
            String hexOutput = "0x" + weiValue.toString(16);

            if (ethStr.equals("0")) {
                return String.format("%s (< 0.000000000000000001 ETH)", hexOutput);
            }

            return String.format("%s (%s ETH)", hexOutput, ethStr);

        } catch (Exception e) {
            // If parsing fails, return the original value
            return hexValue;
        }
    }
}
