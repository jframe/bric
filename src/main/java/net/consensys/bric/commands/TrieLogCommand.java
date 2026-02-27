package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.TrieLogData;
import net.consensys.bric.formatters.TrieLogFormatter;
import org.hyperledger.besu.datatypes.Address;
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
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (args.length < 1) {
            System.err.println("Error: Missing block identifier argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String blockIdentifier = args[0];

        // Parse optional --address filter
        Optional<Address> addressFilter = Optional.empty();
        for (int i = 1; i < args.length; i++) {
            if ("--address".equals(args[i]) && i + 1 < args.length) {
                addressFilter = Optional.of(InputParser.parseAddress(args[i + 1]));
                break;
            }
        }

        try {
            Optional<TrieLogData> trieLog;

            // Detect if input is a block number (numeric) or block hash (0x-prefixed hex)
            if (InputParser.isBlockNumber(blockIdentifier)) {
                long blockNumber = InputParser.parseBlockNumber(blockIdentifier);
                trieLog = dbReader.readTrieLogByNumber(blockNumber);

                if (trieLog.isEmpty()) {
                    System.out.println("Trie log not found for block number: " + blockNumber);
                    System.out.println("Note: Trie logs are only available for Bonsai Archive databases.");
                    System.out.println("      Block may not exist or database may not have trie logs enabled.");
                    return;
                }
            } else {
                Hash blockHash = InputParser.parseHash(blockIdentifier, "block hash");
                trieLog = dbReader.readTrieLog(blockHash);

                if (trieLog.isEmpty()) {
                    System.out.println("Trie log not found for block hash: " + blockIdentifier);
                    System.out.println("Note: Trie logs are only available for Bonsai Archive databases.");
                    System.out.println("      Block may not exist or database may not have trie logs enabled.");
                    return;
                }
            }

            String formatted = addressFilter.isPresent()
                ? formatter.format(trieLog.get(), addressFilter.get())
                : formatter.format(trieLog.get());
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

    @Override
    public String getHelp() {
        return "Query trie log (state diff) by block hash or block number";
    }

    @Override
    public String getUsage() {
        return "trielog <block-hash|block-number> [--address <address>]\n" +
               "                               Examples:\n" +
               "                                 trielog 12345\n" +
               "                                 trielog 12345 --address 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb\n" +
               "                                 trielog 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
    }
}
