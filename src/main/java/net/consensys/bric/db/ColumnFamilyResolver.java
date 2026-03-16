package net.consensys.bric.db;

import org.rocksdb.ColumnFamilyHandle;

import java.nio.charset.StandardCharsets;

/**
 * Resolver for parsing column family identifiers from various input formats.
 * Supports hex format (0x0a, 0xabcdef1234), UTF-8 strings (TRIE_LOG_STORAGE),
 * and quoted strings ("MY_CUSTOM_CF").
 */
public class ColumnFamilyResolver {

    /**
     * Parse column family identifier from string input.
     *
     * @param input The input string in hex (0x...), UTF-8, or quoted format
     * @return The parsed byte array representation
     * @throws IllegalArgumentException if the input format is invalid
     */
    public static byte[] parseInput(String input) {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("Input cannot be null or empty");
        }

        // Remove surrounding quotes if present
        String processed = input;
        if (processed.startsWith("\"") && processed.endsWith("\"")) {
            processed = processed.substring(1, processed.length() - 1);
            if (processed.isEmpty()) {
                throw new IllegalArgumentException("Input cannot be empty after quote removal");
            }
        }

        // Check for hex format (0x prefix, case-insensitive)
        if (processed.toLowerCase().startsWith("0x")) {
            return parseHex(processed);
        }

        // Otherwise, treat as UTF-8 string and convert to uppercase
        return processed.toUpperCase().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Parse hexadecimal string with 0x prefix.
     *
     * @param hex The hex string (must start with 0x or 0X)
     * @return The parsed byte array
     * @throws IllegalArgumentException if the format is invalid
     */
    private static byte[] parseHex(String hex) {
        String hexDigits = hex.substring(2);

        if (hexDigits.isEmpty()) {
            throw new IllegalArgumentException("Hex string cannot be empty (format: 0x...)");
        }

        if (hexDigits.length() % 2 != 0) {
            throw new IllegalArgumentException(
                    "invalid hex format: odd number of hex digits");
        }

        byte[] bytes = new byte[hexDigits.length() / 2];
        for (int i = 0; i < hexDigits.length(); i += 2) {
            String hexByte = hexDigits.substring(i, i + 2);
            try {
                bytes[i / 2] = (byte) Integer.parseInt(hexByte, 16);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "invalid hex characters in input: " + hexByte);
            }
        }

        return bytes;
    }

    /**
     * Resolve column family handle from various input formats.
     * Tries resolution in this order:
     * 1. By enum name (KeyValueSegmentIdentifier.valueOf)
     * 2. By enum ID (KeyValueSegmentIdentifier.fromId)
     * 3. By arbitrary CF name (as UTF-8 string)
     *
     * @param dbManager The database manager instance
     * @param input The input string (enum name, hex ID, or arbitrary CF name)
     * @return The column family handle, or null if not found
     * @throws IllegalStateException if database is not open
     * @throws IllegalArgumentException if input format is invalid (e.g., bad hex)
     */
    public static ColumnFamilyHandle resolveColumnFamily(
            BesuDatabaseManager dbManager, String input) {

        // Check if database is open
        if (!dbManager.isOpen()) {
            throw new IllegalStateException("No database is open");
        }

        // Parse input (will throw IllegalArgumentException for invalid hex)
        byte[] parsedBytes = parseInput(input);
        String inputUpper = input;

        // Remove quotes if present for enum lookup
        if (inputUpper.startsWith("\"") && inputUpper.endsWith("\"")) {
            inputUpper = inputUpper.substring(1, inputUpper.length() - 1);
        }
        inputUpper = inputUpper.toUpperCase();

        // Strategy 1: Try enum by name
        try {
            KeyValueSegmentIdentifier segment = KeyValueSegmentIdentifier.valueOf(inputUpper);
            ColumnFamilyHandle handle = dbManager.getColumnFamily(segment);
            if (handle != null) {
                return handle;
            }
        } catch (IllegalArgumentException e) {
            // Not a valid enum name, continue to next strategy
        }

        // Strategy 2: Try enum by ID (hex lookup)
        KeyValueSegmentIdentifier segment = KeyValueSegmentIdentifier.fromId(parsedBytes);
        if (segment != null) {
            ColumnFamilyHandle handle = dbManager.getColumnFamily(segment);
            if (handle != null) {
                return handle;
            }
        }

        // Strategy 3: Try arbitrary CF name (convert parsed bytes to UTF-8 string)
        try {
            String cfName = new String(parsedBytes, StandardCharsets.UTF_8);
            ColumnFamilyHandle handle = dbManager.getColumnFamilyByName(cfName);
            if (handle != null) {
                return handle;
            }
        } catch (Exception e) {
            // Not valid UTF-8, continue
        }

        // Not found
        return null;
    }
}
