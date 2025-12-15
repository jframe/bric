package net.consensys.bric.formatters;

import net.consensys.bric.db.BesuDatabaseReader.CodeData;
import org.apache.tuweni.bytes.Bytes;

/**
 * Formats contract bytecode for human-readable display.
 */
public class CodeFormatter {

    private static final int DEFAULT_TRUNCATE_LENGTH = 1000; // bytes
    private static final int BYTES_PER_LINE = 32; // 32 bytes per line for readability

    /**
     * Format code data with default truncation (1000 bytes).
     */
    public String format(CodeData code) {
        return format(code, DEFAULT_TRUNCATE_LENGTH);
    }

    /**
     * Format code data with specified truncation length.
     * Set truncateAt to 0 or negative to show full bytecode.
     */
    public String format(CodeData code, int truncateAt) {
        StringBuilder sb = new StringBuilder();

        sb.append("\nContract Code Information:\n");

        if (code.address != null) {
            sb.append("  Address:   ").append(code.address.toHexString()).append("\n");
        }
        sb.append("  Code Hash: ").append(code.codeHash.toHexString()).append("\n");
        sb.append("  Size:      ").append(formatSize(code.bytecode.length)).append("\n");

        sb.append("\nBytecode:\n");

        boolean truncated = truncateAt > 0 && code.bytecode.length > truncateAt;
        int displayLength = truncated ? truncateAt : code.bytecode.length;

        String hex = Bytes.wrap(code.bytecode, 0, displayLength).toHexString();

        // Format as lines of 64 hex chars (32 bytes) for readability
        int hexCharsPerLine = BYTES_PER_LINE * 2; // 2 hex chars per byte
        for (int i = 0; i < hex.length(); i += hexCharsPerLine) {
            int lineEnd = Math.min(i + hexCharsPerLine, hex.length());
            sb.append("  ").append(hex.substring(i, lineEnd)).append("\n");
        }

        if (truncated) {
            int remaining = code.bytecode.length - truncateAt;
            sb.append("  ... (").append(remaining).append(" more bytes truncated)\n");
        }

        return sb.toString();
    }

    /**
     * Format bytecode size with appropriate units.
     */
    private String formatSize(int bytes) {
        if (bytes < 1024) {
            return bytes + " bytes";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB (%d bytes)", bytes / 1024.0, bytes);
        } else {
            return String.format("%.2f MB (%d bytes)", bytes / (1024.0 * 1024), bytes);
        }
    }

    /**
     * Format a compact single-line summary.
     */
    public String formatCompact(CodeData code) {
        String addressStr = code.address != null
            ? code.address.toHexString()
            : code.codeHash.toHexString();

        return String.format("%s | %s | %s",
            addressStr, code.codeHash.toHexString().substring(0, 10) + "...", formatSize(code.bytecode.length));
    }

    /**
     * Get the full bytecode as hex string (for saving to file).
     */
    public String toBytecodeHex(CodeData code) {
        return Bytes.wrap(code.bytecode).toHexString();
    }

    /**
     * Get the bytecode size.
     */
    public int getSize(CodeData code) {
        return code.bytecode.length;
    }
}
