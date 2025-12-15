package net.consensys.bric.db;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Segment identifiers for Besu's RocksDB column families.
 * Based on org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier
 */
public enum KeyValueSegmentIdentifier {
    // Common segments
    BLOCKCHAIN(new byte[] {1}, "BLOCKCHAIN"),
    VARIABLES(new byte[] {11}, "VARIABLES"),

    // Bonsai-specific segments
    ACCOUNT_INFO_STATE(new byte[] {6}, "ACCOUNT_INFO_STATE"),
    CODE_STORAGE(new byte[] {7}, "CODE_STORAGE"),
    ACCOUNT_STORAGE_STORAGE(new byte[] {8}, "ACCOUNT_STORAGE_STORAGE"),
    TRIE_BRANCH_STORAGE(new byte[] {9}, "TRIE_BRANCH_STORAGE"),
    TRIE_LOG_STORAGE(new byte[] {10}, "TRIE_LOG_STORAGE"),

    // Bonsai Archive segments
    ACCOUNT_INFO_STATE_ARCHIVE("ACCOUNT_INFO_STATE_ARCHIVE".getBytes(StandardCharsets.UTF_8), "ACCOUNT_INFO_STATE_ARCHIVE"),
    ACCOUNT_STORAGE_ARCHIVE("ACCOUNT_STORAGE_ARCHIVE".getBytes(StandardCharsets.UTF_8), "ACCOUNT_STORAGE_ARCHIVE");

    private final byte[] id;
    private final String name;

    KeyValueSegmentIdentifier(byte[] id, String name) {
        this.id = id;
        this.name = name;
    }

    public byte[] getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    /**
     * Find segment by column family name bytes
     */
    public static KeyValueSegmentIdentifier fromId(byte[] id) {
        for (KeyValueSegmentIdentifier segment : values()) {
            if (Arrays.equals(segment.getId(), id)) {
                return segment;
            }
        }
        return null;
    }

    /**
     * Convert byte array to readable string
     */
    public static String idToString(byte[] id) {
        KeyValueSegmentIdentifier segment = fromId(id);
        if (segment != null) {
            return segment.getName();
        }
        // Try as UTF-8 string
        try {
            return new String(id, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // Return as hex if not UTF-8
            StringBuilder sb = new StringBuilder();
            for (byte b : id) {
                sb.append(String.format("%02x", b));
            }
            return "0x" + sb.toString();
        }
    }
}
