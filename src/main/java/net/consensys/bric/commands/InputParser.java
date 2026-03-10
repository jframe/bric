package net.consensys.bric.commands;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

/**
 * Shared input parsing and validation for REPL commands.
 */
public final class InputParser {

    private InputParser() {}

    /**
     * Parse and validate an Ethereum address (0x + 40 hex chars).
     */
    public static Address parseAddress(String addressStr) {
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
     * Parse and validate a 32-byte hash (0x + 64 hex chars).
     */
    public static Hash parseHash(String hashStr) {
        return parseHash(hashStr, "hash");
    }

    /**
     * Parse and validate a 32-byte hash with a descriptive field name for error messages.
     */
    public static Hash parseHash(String hashStr, String fieldName) {
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
     * Parse a storage slot — accepts decimal or 0x-prefixed hex format.
     * Supports the full 256-bit range for decimal input.
     */
    public static UInt256 parseSlot(String slotStr) {
        try {
            if (slotStr.startsWith("0x") || slotStr.startsWith("0X")) {
                return UInt256.fromHexString(slotStr);
            } else {
                BigInteger value = new BigInteger(slotStr);
                if (value.signum() < 0) {
                    throw new IllegalArgumentException(
                        "Slot number cannot be negative: " + slotStr
                    );
                }
                if (value.bitLength() > 256) {
                    throw new IllegalArgumentException(
                        "Slot number exceeds 256 bits: " + slotStr
                    );
                }
                return UInt256.valueOf(value);
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid slot format. Expected: decimal number or 0x-prefixed hex. Got: " + slotStr
            );
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid slot value: " + e.getMessage()
            );
        }
    }

    /**
     * Parse and validate a non-negative block number.
     */
    public static long parseBlockNumber(String blockNumberStr) {
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
     * Check if a string looks like a block number (numeric) rather than a hash (0x-prefixed hex).
     */
    public static boolean isBlockNumber(String input) {
        if (input.startsWith("0x") || input.startsWith("0X")) {
            return false;
        }
        try {
            Long.parseLong(input);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * Parse a raw key argument as bytes.
     * If the argument is surrounded by single or double quotes, the inner string is
     * converted to bytes using UTF-8 encoding. Otherwise it is parsed as a hex string.
     *
     * @throws IllegalArgumentException if the value is neither a valid quoted string nor valid hex
     */
    public static byte[] parseKeyBytes(String arg) {
        if (arg.length() >= 2) {
            char first = arg.charAt(0);
            char last = arg.charAt(arg.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return arg.substring(1, arg.length() - 1).getBytes(StandardCharsets.UTF_8);
            }
        }
        return Bytes.fromHexString(arg).toArrayUnsafe();
    }

    /**
     * Strip outer single or double quotes from a string if present.
     */
    public static String stripQuotes(String arg) {
        if (arg.length() >= 2) {
            char first = arg.charAt(0);
            char last = arg.charAt(arg.length() - 1);
            if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
                return arg.substring(1, arg.length() - 1);
            }
        }
        return arg;
    }

    /**
     * Check if a flag is present anywhere in an args array.
     */
    public static boolean hasFlag(String[] args, String flag) {
        for (String arg : args) {
            if (flag.equals(arg)) {
                return true;
            }
        }
        return false;
    }
}
