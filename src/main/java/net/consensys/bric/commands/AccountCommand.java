package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.BesuDatabaseReader.AccountData;
import net.consensys.bric.formatters.AccountFormatter;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

/**
 * Command to query account information by Ethereum address.
 */
public class AccountCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final BesuDatabaseReader dbReader;
    private final AccountFormatter formatter;

    public AccountCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.dbReader = new BesuDatabaseReader(dbManager);
        this.formatter = new AccountFormatter();
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db-open <path>' first.");
            return;
        }

        if (args.length < 1) {
            System.err.println("Error: Missing address argument");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String addressOrHash = args[0];
        Optional<Long> blockNumber = Optional.empty();
        Optional<Hash> blockHash = Optional.empty();

        // Parse optional --block parameter
        for (int i = 1; i < args.length; i++) {
            if ("--block".equals(args[i]) && i + 1 < args.length) {
                String blockValue = args[i + 1];

                // Check if it's a block hash (0x + 64 hex chars) or block number
                if (blockValue.startsWith("0x") && blockValue.length() == 66) {
                    blockHash = Optional.of(parseBlockHash(blockValue));
                } else {
                    try {
                        blockNumber = Optional.of(Long.parseLong(blockValue));
                    } catch (NumberFormatException e) {
                        System.err.println("Error: Invalid block number format: " + blockValue);
                        return;
                    }
                }
                break;
            }
        }

        // Validate block parameter only works with Bonsai Archive
        if ((blockNumber.isPresent() || blockHash.isPresent()) &&
            dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            System.err.println("Error: Block parameter only supported for Bonsai Archive databases");
            System.err.println("Current database format: " + dbManager.getFormat());
            return;
        }

        // Auto-detect based on size: 42 chars (0x + 40 hex) = address, 66 chars (0x + 64 hex) = hash
        boolean isHashQuery = addressOrHash.startsWith("0x") && addressOrHash.length() == 66;

        try {
            Optional<AccountData> accountData;

            // If block hash specified, convert to block number first
            if (blockHash.isPresent()) {
                Optional<Long> resolvedBlockNumber = dbReader.getBlockNumberFromHash(blockHash.get());
                if (resolvedBlockNumber.isEmpty()) {
                    System.err.println("Error: Block not found for hash: " + blockHash.get().toHexString());
                    return;
                }
                blockNumber = resolvedBlockNumber;
            }

            if (isHashQuery) {
                Hash accountHash = parseHash(addressOrHash);
                if (blockNumber.isPresent()) {
                    accountData = dbReader.readAccountByHashAtBlock(accountHash, blockNumber.get());
                } else {
                    accountData = dbReader.readAccountByHash(accountHash);
                }
            } else {
                Address address = parseAddress(addressOrHash);
                if (blockNumber.isPresent()) {
                    accountData = dbReader.readAccountAtBlock(address, blockNumber.get());
                } else {
                    accountData = dbReader.readAccount(address);
                }
            }

            if (accountData.isEmpty()) {
                String identifier = isHashQuery ? "hash" : "address";
                String blockInfo = blockNumber.map(bn -> " at block " + bn).orElse("");
                System.out.println("Account not found: " + addressOrHash + blockInfo);
                System.out.println("Note: Account may not exist or database may not be fully synced.");
                System.out.println("Queried by " + identifier + ": " + addressOrHash);
                return;
            }

            String formatted = formatter.format(accountData.get());
            System.out.println(formatted);

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error querying account: " + e.getMessage());
        }
    }

    /**
     * Parse and validate Ethereum address.
     */
    private Address parseAddress(String addressStr) {
        if (!addressStr.startsWith("0x")) {
            throw new IllegalArgumentException(
                "Invalid address format. Expected: 0x-prefixed hex (40 chars). Got: " + addressStr
            );
        }

        if (addressStr.length() != 42) {
            throw new IllegalArgumentException(
                "Invalid address length. Expected: 42 chars (0x + 40 hex). Got: " + addressStr.length()
            );
        }

        try {
            return Address.fromHexString(addressStr);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid address format: " + e.getMessage()
            );
        }
    }

    /**
     * Parse and validate 32-byte hash.
     */
    private Hash parseHash(String hashStr) {
        if (!hashStr.startsWith("0x")) {
            throw new IllegalArgumentException(
                "Invalid hash format. Expected: 0x-prefixed hex (64 chars). Got: " + hashStr
            );
        }

        if (hashStr.length() != 66) {
            throw new IllegalArgumentException(
                "Invalid hash length. Expected: 66 chars (0x + 64 hex). Got: " + hashStr.length()
            );
        }

        try {
            return Hash.fromHexString(hashStr);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid hash format: " + e.getMessage()
            );
        }
    }

    /**
     * Parse and validate block hash.
     */
    private Hash parseBlockHash(String hashStr) {
        return parseHash(hashStr);
    }

    @Override
    public String getHelp() {
        return "Query account information by Ethereum address";
    }

    @Override
    public String getUsage() {
        return "account <address|hash> [--block <number|hash>]\n" +
               "                               Examples:\n" +
               "                                 account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb\n" +
               "                                 account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb --block 12345\n" +
               "                                 account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb --block 0xabcd...\n" +
               "                               Note: --block parameter only works with Bonsai Archive databases";
    }
}
