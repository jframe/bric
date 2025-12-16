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
                    System.err.println("Usage: debug account <address>");
                    return;
                }
                debugAccount(args[1]);
                break;

            case "storage":
                if (args.length < 3) {
                    System.err.println("Error: Missing arguments");
                    System.err.println("Usage: debug storage <address> <slot>");
                    return;
                }
                debugStorage(args[1], args[2]);
                break;

            default:
                System.err.println("Error: Unknown subcommand: " + subcommand);
                System.err.println("Usage: " + getUsage());
        }
    }

    private void debugAccount(String addressStr) {
        try {
            Address address = parseAddress(addressStr);
            Hash accountHash = Hash.hash(address);

            System.out.println("=== DEBUG: Account Query ===");
            System.out.println("Address:      " + address.toHexString());
            System.out.println("Account Hash: " + accountHash.toHexString());
            System.out.println();

            // Read raw value from database
            KeyValueSegmentIdentifier segment = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE
                    ? KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE
                    : KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE;

            System.out.println("Segment: " + segment.getName());
            System.out.println("Key (hex): " + accountHash.toHexString());
            System.out.println();

            Optional<byte[]> rawValue = segmentReader.get(segment, accountHash.toArrayUnsafe());

            if (rawValue.isEmpty()) {
                System.out.println("Account not found in database");
                return;
            }

            System.out.println("=== RAW DATABASE VALUE ===");
            System.out.println("Raw Value (hex): " + Bytes.wrap(rawValue.get()).toHexString());
            System.out.println("Raw Value (length): " + rawValue.get().length + " bytes");
            System.out.println();

            // Read decoded value using Besu's logic
            Optional<AccountData> accountData = dbReader.readAccount(address);

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

    private void debugStorage(String addressStr, String slotStr) {
        try {
            Address address = parseAddress(addressStr);
            UInt256 slot = parseSlot(slotStr);
            Hash accountHash = Hash.hash(address);
            Hash slotHash = Hash.hash(slot);

            System.out.println("=== DEBUG: Storage Query ===");
            System.out.println("Address:      " + address.toHexString());
            System.out.println("Slot:         " + slot.toHexString());
            System.out.println("Account Hash: " + accountHash.toHexString());
            System.out.println("Slot Hash:    " + slotHash.toHexString());
            System.out.println();

            // Compute storage key (accountHash + slotHash)
            byte[] storageKey = segmentReader.computeStorageKey(accountHash, slotHash);

            KeyValueSegmentIdentifier segment = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE
                    ? KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE
                    : KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;

            System.out.println("Segment: " + segment.getName());
            System.out.println("Key (hex): " + Bytes.wrap(storageKey).toHexString());
            System.out.println("Key (length): " + storageKey.length + " bytes");
            System.out.println();

            Optional<byte[]> rawValue = segmentReader.get(segment, storageKey);

            if (rawValue.isEmpty()) {
                System.out.println("Storage slot not found in database");
                return;
            }

            System.out.println("=== RAW DATABASE VALUE ===");
            System.out.println("Raw Value (hex): " + Bytes.wrap(rawValue.get()).toHexString());
            System.out.println("Raw Value (length): " + rawValue.get().length + " bytes");
            System.out.println();

            // Read decoded value using Besu's logic
            Optional<StorageData> storageData = dbReader.readStorage(address, slot);

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
               "                                 debug account <address>       - Show raw and decoded account data\n" +
               "                                 debug storage <address> <slot> - Show raw and decoded storage data\n" +
               "                               Examples:\n" +
               "                                 debug account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb\n" +
               "                                 debug storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb 0";
    }
}
