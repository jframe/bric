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

        // Check if this is a raw hash query (--hash flag or 64-char hex)
        boolean isHashQuery = false;
        if (args.length >= 2 && args[1].equals("--hash")) {
            isHashQuery = true;
        } else if (addressOrHash.startsWith("0x") && addressOrHash.length() == 66) {
            isHashQuery = true;
        }

        try {
            Optional<AccountData> accountData;

            if (isHashQuery) {
                Hash accountHash = parseHash(addressOrHash);
                accountData = dbReader.readAccountByHash(accountHash);
            } else {
                Address address = parseAddress(addressOrHash);
                accountData = dbReader.readAccount(address);
            }

            if (accountData.isEmpty()) {
                String identifier = isHashQuery ? "hash" : "address";
                System.out.println("Account not found: " + addressOrHash);
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

    @Override
    public String getHelp() {
        return "Query account information by Ethereum address";
    }

    @Override
    public String getUsage() {
        return "account <address> [--hash]\n" +
               "                               Examples:\n" +
               "                                 account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb\n" +
               "                                 account 0x1234...abcd --hash";
    }
}
