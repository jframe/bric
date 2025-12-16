package net.consensys.bric.formatters;

import org.apache.tuweni.units.bigints.UInt256;
import net.consensys.bric.db.StorageData;

import java.math.BigInteger;

/**
 * Formats storage data for human-readable display.
 */
public class StorageFormatter {

    /**
     * Format storage data as a multi-line string.
     */
    public String format(StorageData storage) {
        StringBuilder sb = new StringBuilder();

        sb.append("\nStorage Information:\n");

        if (storage.address != null) {
            sb.append("  Address:      ").append(storage.address.toHexString()).append("\n");
        }
        sb.append("  Account Hash: ").append(storage.accountHash.toHexString()).append("\n");

        if (storage.slot != null) {
            sb.append("  Slot:         ").append(formatSlot(storage.slot)).append("\n");
        }
        sb.append("  Slot Hash:    ").append(storage.slotHash.toHexString()).append("\n");

        sb.append("  Value (raw):  ").append(storage.value.toHexString()).append("\n");
        sb.append("  Value (dec):  ").append(formatDecimal(storage.value)).append("\n");

        return sb.toString();
    }

    /**
     * Format slot with both decimal and hex representation.
     */
    private String formatSlot(UInt256 slot) {
        if (slot.isZero()) {
            return "0";
        }

        String hexStr = slot.toHexString();
        BigInteger decValue = slot.toBigInteger();

        // If the decimal value is small enough to be readable, show both
        if (decValue.compareTo(BigInteger.valueOf(1_000_000)) < 0) {
            return String.format("%,d (%s)", decValue, hexStr);
        }

        return hexStr;
    }

    /**
     * Format value with comma separators for readability.
     */
    private String formatDecimal(UInt256 value) {
        if (value.isZero()) {
            return "0";
        }

        return String.format("%,d", value.toBigInteger());
    }

    /**
     * Format a compact single-line summary.
     */
    public String formatCompact(StorageData storage) {
        String addressStr = storage.address != null
            ? storage.address.toHexString()
            : storage.accountHash.toHexString();

        String slotStr = storage.slot != null
            ? storage.slot.toHexString()
            : storage.slotHash.toHexString();

        return String.format("%s | slot %s = %s",
            addressStr, slotStr, storage.value.toHexString());
    }

    /**
     * Check if storage value is zero (empty slot).
     */
    public boolean isEmpty(StorageData storage) {
        return storage.value.isZero();
    }
}
