package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseReader;
import net.consensys.bric.db.AccountData;
import net.consensys.bric.db.StorageData;
import net.consensys.bric.db.SegmentReader;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.Optional;

/**
 * Debug command to display raw database values for verification.
 * Shows both raw hex bytes and decoded values for accounts and storage.
 */
public class DebugCommand implements Command {

    private final BesuDatabaseManager dbManager;
    private final BesuDatabaseReader dbReader;
    private final SegmentReader segmentReader;

    public DebugCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.dbReader = new BesuDatabaseReader(dbManager);
        this.segmentReader = new SegmentReader(dbManager);
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db-open <path>' first.");
            return;
        }

        if (args.length < 1) {
            System.err.println("Error: Missing subcommand");
            System.err.println("Usage: " + getUsage());
            return;
        }

        String subcommand = args[0].toLowerCase();

        switch (subcommand) {
            case "account":
                if (args.length < 2) {
                    System.err.println("Error: Missing address argument");
                    System.err.println("Usage: debug account <address> [--block <number>]");
                    return;
                }
                Optional<Long> accountBlock = parseBlockNumber(args, 2);
                debugAccount(args[1], accountBlock);
                break;

            case "storage":
                if (args.length < 3) {
                    System.err.println("Error: Missing arguments");
                    System.err.println("Usage: debug storage <address> <slot> [--block <number>]");
                    return;
                }
                Optional<Long> storageBlock = parseBlockNumber(args, 3);
                debugStorage(args[1], args[2], storageBlock);
                break;

            default:
                System.err.println("Error: Unknown subcommand: " + subcommand);
                System.err.println("Usage: " + getUsage());
        }
    }

    private Optional<Long> parseBlockNumber(String[] args, int startIndex) {
        for (int i = startIndex; i < args.length; i++) {
            if ("--block".equals(args[i]) && i + 1 < args.length) {
                try {
                    return Optional.of(Long.parseLong(args[i + 1]));
                } catch (NumberFormatException e) {
                    System.err.println("Error: Invalid block number: " + args[i + 1]);
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    private void debugAccount(String addressStr, Optional<Long> blockNumber) {
        try {
            Address address = parseAddress(addressStr);
            Hash accountHash = Hash.hash(address);
            boolean isArchive = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;

            System.out.println("=== DEBUG: Account Query ===");
            System.out.println("Address:      " + address.toHexString());
            System.out.println("Account Hash: " + accountHash.toHexString());
            if (blockNumber.isPresent()) {
                System.out.println("Block Number: " + blockNumber.get());
            }
            System.out.println();

            // Validate block parameter
            if (blockNumber.isPresent() && !isArchive) {
                System.err.println("Error: --block parameter only supported for Bonsai Archive databases");
                System.err.println("Current database format: " + dbManager.getFormat());
                return;
            }

            // Determine segment
            KeyValueSegmentIdentifier segment = isArchive
                    ? KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE
                    : KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;

            // Construct key with block number suffix for archive databases
            byte[] key;
            if (isArchive && blockNumber.isPresent()) {
                // Key = accountHash (32 bytes) + blockNumber (8 bytes unsigned long)
                key = Bytes.concatenate(
                        Bytes.wrap(accountHash.toArrayUnsafe()),
                        Bytes.ofUnsignedLong(blockNumber.get())
                ).toArrayUnsafe();
            } else if (isArchive) {
                // For archive without block number, use Long.MAX_VALUE to get latest
                key = Bytes.concatenate(
                        Bytes.wrap(accountHash.toArrayUnsafe()),
                        Bytes.ofUnsignedLong(Long.MAX_VALUE)
                ).toArrayUnsafe();
            } else {
                // Regular Bonsai: just the account hash
                key = accountHash.toArrayUnsafe();
            }

            System.out.println("Segment: " + segment.getName());
            System.out.println("Query Key (hex): " + Bytes.wrap(key).toHexString());
            System.out.println("Query Key (length): " + key.length + " bytes");
            System.out.println();

            // Read raw value from database
            Optional<byte[]> rawValue;
            byte[] actualKey;

            if (isArchive) {
                // Use getNearestBefore for archive databases
                Optional<SegmentReader.KeyValuePair> result = segmentReader.getNearestBefore(segment, key, 32);
                if (result.isEmpty()) {
                    System.out.println("Account not found in database");
                    return;
                }
                rawValue = Optional.of(result.get().value);
                actualKey = result.get().key;
            } else {
                rawValue = segmentReader.get(segment, key);
                actualKey = key;
            }

            if (rawValue.isEmpty()) {
                System.out.println("Account not found in database");
                return;
            }

            System.out.println("=== RAW DATABASE KEY-VALUE ===");
            System.out.println("Actual Key (hex): " + Bytes.wrap(actualKey).toHexString());
            System.out.println("Actual Key (length): " + actualKey.length + " bytes");

            // Extract block number from key if archive
            if (isArchive && actualKey.length >= 40) {
                byte[] blockBytes = new byte[8];
                System.arraycopy(actualKey, 32, blockBytes, 0, 8);
                long extractedBlock = Bytes.wrap(blockBytes).toLong();
                System.out.println("Block Number (from key): " + extractedBlock);
            }

            System.out.println("Raw Value (hex): " + Bytes.wrap(rawValue.get()).toHexString());
            System.out.println("Raw Value (length): " + rawValue.get().length + " bytes");
            System.out.println();

            // Read decoded value using Besu's logic
            Optional<AccountData> accountData;
            if (blockNumber.isPresent()) {
                accountData = dbReader.readAccountAtBlock(address, blockNumber.get());
            } else {
                accountData = dbReader.readAccount(address);
            }

            if (accountData.isEmpty()) {
                System.err.println("Error: Failed to decode account data");
                return;
            }

            System.out.println("=== DECODED VALUE (using Besu's BonsaiAccount.fromRLP) ===");
            AccountData data = accountData.get();
            System.out.println("Nonce:        " + data.nonce);
            System.out.println("Balance:      " + data.balance + " Wei");
            System.out.println("Storage Root: " + data.storageRoot.toHexString());
            System.out.println("Code Hash:    " + data.codeHash.toHexString());

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void debugStorage(String addressStr, String slotStr, Optional<Long> blockNumber) {
        try {
            Address address = parseAddress(addressStr);
            UInt256 slot = parseSlot(slotStr);
            Hash accountHash = Hash.hash(address);
            Hash slotHash = Hash.hash(slot);
            boolean isArchive = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;

            System.out.println("=== DEBUG: Storage Query ===");
            System.out.println("Address:      " + address.toHexString());
            System.out.println("Slot:         " + slot.toHexString());
            System.out.println("Account Hash: " + accountHash.toHexString());
            System.out.println("Slot Hash:    " + slotHash.toHexString());
            if (blockNumber.isPresent()) {
                System.out.println("Block Number: " + blockNumber.get());
            }
            System.out.println();

            // Validate block parameter
            if (blockNumber.isPresent() && !isArchive) {
                System.err.println("Error: --block parameter only supported for Bonsai Archive databases");
                System.err.println("Current database format: " + dbManager.getFormat());
                return;
            }

            // Determine segment
            KeyValueSegmentIdentifier segment = isArchive
                    ? KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE
                    : KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

            // Construct key with block number suffix for archive databases
            byte[] key;
            if (isArchive && blockNumber.isPresent()) {
                // Key = accountHash (32 bytes) + slotHash (32 bytes) + blockNumber (8 bytes unsigned long)
                key = Bytes.concatenate(
                        Bytes.wrap(accountHash.toArrayUnsafe()),
                        Bytes.wrap(slotHash.toArrayUnsafe()),
                        Bytes.ofUnsignedLong(blockNumber.get())
                ).toArrayUnsafe();
            } else if (isArchive) {
                // For archive without block number, use Long.MAX_VALUE to get latest
                key = Bytes.concatenate(
                        Bytes.wrap(accountHash.toArrayUnsafe()),
                        Bytes.wrap(slotHash.toArrayUnsafe()),
                        Bytes.ofUnsignedLong(Long.MAX_VALUE)
                ).toArrayUnsafe();
            } else {
                // Regular Bonsai: accountHash + slotHash
                key = segmentReader.computeStorageKey(accountHash, slotHash);
            }

            System.out.println("Segment: " + segment.getName());
            System.out.println("Query Key (hex): " + Bytes.wrap(key).toHexString());
            System.out.println("Query Key (length): " + key.length + " bytes");
            System.out.println();

            // Read raw value from database
            Optional<byte[]> rawValue;
            byte[] actualKey;

            if (isArchive) {
                // Use getNearestBefore for archive databases (prefix length = 64 for accountHash + slotHash)
                Optional<SegmentReader.KeyValuePair> result = segmentReader.getNearestBefore(segment, key, 64);
                if (result.isEmpty()) {
                    System.out.println("Storage slot not found in database");
                    return;
                }
                rawValue = Optional.of(result.get().value);
                actualKey = result.get().key;
            } else {
                rawValue = segmentReader.get(segment, key);
                actualKey = key;
            }

            if (rawValue.isEmpty()) {
                System.out.println("Storage slot not found in database");
                return;
            }

            System.out.println("=== RAW DATABASE KEY-VALUE ===");
            System.out.println("Actual Key (hex): " + Bytes.wrap(actualKey).toHexString());
            System.out.println("Actual Key (length): " + actualKey.length + " bytes");

            // Extract block number from key if archive
            if (isArchive && actualKey.length >= 72) {
                byte[] blockBytes = new byte[8];
                System.arraycopy(actualKey, 64, blockBytes, 0, 8);
                long extractedBlock = Bytes.wrap(blockBytes).toLong();
                System.out.println("Block Number (from key): " + extractedBlock);
            }

            System.out.println("Raw Value (hex): " + Bytes.wrap(rawValue.get()).toHexString());
            System.out.println("Raw Value (length): " + rawValue.get().length + " bytes");
            System.out.println();

            // Read decoded value using Besu's logic
            Optional<StorageData> storageData;
            if (blockNumber.isPresent()) {
                storageData = dbReader.readStorageAtBlock(address, slot, blockNumber.get());
            } else {
                storageData = dbReader.readStorage(address, slot);
            }

            if (storageData.isEmpty()) {
                System.err.println("Error: Failed to decode storage data");
                return;
            }

            System.out.println("=== DECODED VALUE (using RLP.decodeValue) ===");
            StorageData data = storageData.get();
            System.out.println("Value (decimal): " + data.value);
            System.out.println("Value (hex):     " + data.value.toHexString());

        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

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

    private UInt256 parseSlot(String slotStr) {
        try {
            if (slotStr.startsWith("0x") || slotStr.startsWith("0X")) {
                return UInt256.fromHexString(slotStr);
            } else {
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

    @Override
    public String getHelp() {
        return "Debug command to display raw database values for verification";
    }

    @Override
    public String getUsage() {
        return "debug <subcommand> <args>\n" +
               "                               Subcommands:\n" +
               "                                 debug account <address> [--block <number>]       - Show raw and decoded account data\n" +
               "                                 debug storage <address> <slot> [--block <number>] - Show raw and decoded storage data\n" +
               "                               Examples:\n" +
               "                                 debug account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb\n" +
               "                                 debug account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb --block 12345\n" +
               "                                 debug storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb 0\n" +
               "                                 debug storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb 0 --block 12345";
    }
}
