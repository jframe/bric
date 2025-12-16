package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.TrieLogData;
import net.consensys.bric.formatters.TrieLogFormatter;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

/**
 * Command to query trie logs (state diffs) by block hash or block number.
 */
public class TrieLogCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final BesuDatabaseReader dbReader;
    private final TrieLogFormatter formatter;

    public TrieLogCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.dbReader = new BesuDatabaseReader(dbManager);
        this.formatter = new TrieLogFormatter();
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db-open <path>' first.");
            return;
        }

        if (args.length < 1) {
            System.err.println("Error: Missing block identifier argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String blockIdentifier = args[0];

        try {
            Optional<TrieLogData> trieLog;

            // Detect if input is a block number (numeric) or block hash (0x-prefixed hex)
            if (isBlockNumber(blockIdentifier)) {
                long blockNumber = parseBlockNumber(blockIdentifier);
                trieLog = dbReader.readTrieLogByNumber(blockNumber);

                if (trieLog.isEmpty()) {
                    System.out.println("Trie log not found for block number: " + blockNumber);
                    System.out.println("Note: Trie logs are only available for Bonsai Archive databases.");
                    System.out.println("      Block may not exist or database may not have trie logs enabled.");
                    return;
                }
            } else {
                Hash blockHash = parseHash(blockIdentifier);
                trieLog = dbReader.readTrieLog(blockHash);

                if (trieLog.isEmpty()) {
                    System.out.println("Trie log not found for block hash: " + blockIdentifier);
                    System.out.println("Note: Trie logs are only available for Bonsai Archive databases.");
                    System.out.println("      Block may not exist or database may not have trie logs enabled.");
                    return;
                }
            }

            String formatted = formatter.format(trieLog.get());
            System.out.println(formatted);

            if (formatter.isEmpty(trieLog.get())) {
                System.out.println("Note: This block has no state changes (empty block or only transfers).");
            }

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error querying trie log: " + e.getMessage());
        }
    }

    /**
     * Check if the input string is a block number (numeric) rather than a hash.
     */
    private boolean isBlockNumber(String input) {
        // If it starts with 0x, it's a hash
        if (input.startsWith("0x") || input.startsWith("0X")) {
            return false;
        }
        // Otherwise, check if it's numeric
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

    @Override
    public String getHelp() {
        return "Query trie log (state diff) by block hash or block number";
    }

    @Override
    public String getUsage() {
        return "trielog <block-hash|block-number>\n" +
               "                               Examples:\n" +
               "                                 trielog 12345\n" +
               "                                 trielog 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    }
}
