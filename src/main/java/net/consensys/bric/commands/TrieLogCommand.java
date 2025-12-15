package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.TrieLogData;
import net.consensys.bric.formatters.TrieLogFormatter;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

/**
 * Command to query trie logs (state diffs) by block hash.
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
            System.err.println("Error: Missing block hash argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String blockHashStr = args[0];

        try {
            Hash blockHash = parseHash(blockHashStr);

            Optional<TrieLogData> trieLog = dbReader.readTrieLog(blockHash);

            if (trieLog.isEmpty()) {
                System.out.println("Trie log not found for block: " + blockHashStr);
                System.out.println("Note: Trie logs are only available for Bonsai Archive databases.");
                System.out.println("      Block may not exist or database may not have trie logs enabled.");
                return;
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
        return "Query trie log (state diff) by block hash";
    }

    @Override
    public String getUsage() {
        return "trielog <block-hash>\n" +
               "                               Example:\n" +
               "                                 trielog 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    }
}
