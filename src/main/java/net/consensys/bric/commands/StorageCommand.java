package net.consensys.bric.commands;

import org.apache.tuweni.units.bigints.UInt256;
import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.StorageData;
import net.consensys.bric.formatters.StorageFormatter;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

/**
 * Command to query storage slot values by contract address and slot.
 */
public class StorageCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final BesuDatabaseReader dbReader;
    private final StorageFormatter formatter;

    public StorageCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.dbReader = new BesuDatabaseReader(dbManager);
        this.formatter = new StorageFormatter();
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        if (args.length < 2) {
            System.err.println("Error: Missing arguments");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String firstArg = args[0];
        String secondArg = args[1];

        // Check if this is a raw hash query
        boolean isRawQuery = hasFlag(args, "--raw");

        // Parse optional --block parameter
        Optional<Long> blockNumber = Optional.empty();
        Optional<Hash> blockHash = Optional.empty();
        for (int i = 0; i < args.length; i++) {
            if ("--block".equals(args[i]) && i + 1 < args.length) {
                String blockValue = args[i + 1];
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

        try {
            // If block hash specified, convert to block number first
            if (blockHash.isPresent()) {
                Optional<Long> resolvedBlockNumber = dbReader.getBlockNumberFromHash(blockHash.get());
                if (resolvedBlockNumber.isEmpty()) {
                    System.err.println("Error: Block not found for hash: " + blockHash.get().toHexString());
                    return;
                }
                blockNumber = resolvedBlockNumber;
            }

            Optional<StorageData> storageData;

            if (isRawQuery) {
                Hash accountHash = parseHash(firstArg, "account hash");
                Hash slotHash = parseHash(secondArg, "slot hash");
                if (blockNumber.isPresent()) {
                    storageData = dbReader.readStorageByHashAtBlock(accountHash, slotHash, blockNumber.get());
                } else {
                    storageData = dbReader.readStorageByHash(accountHash, slotHash);
                }
            } else {
                Address address = parseAddress(firstArg);
                UInt256 slot = parseSlot(secondArg);
                if (blockNumber.isPresent()) {
                    storageData = dbReader.readStorageAtBlock(address, slot, blockNumber.get());
                } else {
                    storageData = dbReader.readStorage(address, slot);
                }
            }

            if (storageData.isEmpty()) {
                System.out.println("Storage slot not found");
                System.out.println("Note: Slot may be empty or database may not be fully synced.");
                System.out.println("Queried: address=" + firstArg + ", slot=" + secondArg);
                return;
            }

            String formatted = formatter.format(storageData.get());
            System.out.println(formatted);

            if (formatter.isEmpty(storageData.get())) {
                System.out.println("Note: Storage slot contains zero value (empty slot)");
            }

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error querying storage: " + e.getMessage());
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
     * Parse storage slot - accepts decimal or hex format.
     */
    private UInt256 parseSlot(String slotStr) {
        try {
            if (slotStr.startsWith("0x") || slotStr.startsWith("0X")) {
                // Hex format
                return UInt256.fromHexString(slotStr);
            } else {
                // Decimal format
                return UInt256.valueOf(Long.parseLong(slotStr));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid slot format. Expected: decimal number or 0x-prefixed hex. Got: " + slotStr
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid slot value: " + e.getMessage()
            );
        }
    }

    /**
     * Parse and validate 32-byte hash.
     */
    private Hash parseHash(String hashStr, String fieldName) {
        if (!hashStr.startsWith("0x")) {
            throw new IllegalArgumentException(
                "Invalid " + fieldName + " format. Expected: 0x-prefixed hex (64 chars). Got: " + hashStr
            );
        }

        if (hashStr.length() != 66) {
            throw new IllegalArgumentException(
                "Invalid " + fieldName + " length. Expected: 66 chars (0x + 64 hex). Got: " + hashStr.length()
            );
        }

        try {
            return Hash.fromHexString(hashStr);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid " + fieldName + " format: " + e.getMessage()
            );
        }
    }

    /**
     * Parse and validate block hash.
     */
    private Hash parseBlockHash(String hashStr) {
        return parseHash(hashStr, "block hash");
    }

    /**
     * Check if a flag is present anywhere in the args array.
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
        return "Query storage slot value by contract address and slot";
    }

    @Override
    public String getUsage() {
        return "storage <address> <slot> [--block <number|hash>] [--raw]\n" +
               "                               Examples:\n" +
               "                                 storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb 0\n" +
               "                                 storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb 0x1234\n" +
               "                                 storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb 0 --block 12345\n" +
               "                                 storage 0x1234...abcd 0x5678...ef01 --raw\n" +
               "                               Note: --block parameter only works with Bonsai Archive databases";
    }
}
