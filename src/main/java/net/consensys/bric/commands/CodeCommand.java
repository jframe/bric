package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.CodeData;
import net.consensys.bric.formatters.CodeFormatter;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Command to query contract bytecode by address or code hash.
 */
public class CodeCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final BesuDatabaseReader dbReader;
    private final CodeFormatter formatter;

    public CodeCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.dbReader = new BesuDatabaseReader(dbManager);
        this.formatter = new CodeFormatter();
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db-open <path>' first.");
            return;
        }

        if (args.length < 1) {
            System.err.println("Error: Missing arguments");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String addressOrHash = args[0];

        // Check for --save flag
        String saveFilePath = null;
        if (args.length >= 3 && args[1].equals("--save")) {
            saveFilePath = args[2];
        }

        // Check for --hash flag (query by code hash directly)
        boolean isHashQuery = false;
        if (args.length >= 2 && args[1].equals("--hash")) {
            isHashQuery = true;
        } else if (addressOrHash.startsWith("0x") && addressOrHash.length() == 66) {
            // Auto-detect hash format
            isHashQuery = true;
        }

        try {
            Optional<CodeData> codeData;

            if (isHashQuery) {
                Hash codeHash = parseHash(addressOrHash);
                codeData = dbReader.readCodeByHash(codeHash);
            } else {
                Address address = parseAddress(addressOrHash);
                codeData = dbReader.readCode(address);
            }

            if (codeData.isEmpty()) {
                String identifier = isHashQuery ? "code hash" : "address";
                System.out.println("Contract code not found for " + identifier + ": " + addressOrHash);
                System.out.println("Note: Address may be an EOA (no code) or database may not be fully synced.");
                return;
            }

            // Save to file if --save flag provided
            if (saveFilePath != null) {
                saveCodeToFile(codeData.get(), saveFilePath);
                System.out.println("Bytecode saved to: " + saveFilePath);
                System.out.println("Size: " + formatter.getSize(codeData.get()) + " bytes");
            } else {
                // Display formatted code
                String formatted = formatter.format(codeData.get());
                System.out.println(formatted);
            }

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error querying code: " + e.getMessage());
        }
    }

    /**
     * Save bytecode to file in hex format.
     */
    private void saveCodeToFile(CodeData code, String filePath) throws IOException {
        Path path = Paths.get(filePath);

        // Create parent directories if they don't exist
        if (path.getParent() != null) {
            Files.createDirectories(path.getParent());
        }

        String bytecodeHex = formatter.toBytecodeHex(code);
        Files.writeString(path, bytecodeHex);
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
        return "Query contract bytecode by address or code hash";
    }

    @Override
    public String getUsage() {
        return "code <address> [--save <file>] [--hash]\n" +
               "                               Examples:\n" +
               "                                 code 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb\n" +
               "                                 code 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb --save contract.hex\n" +
               "                                 code 0x1234...abcd --hash";
    }
}
