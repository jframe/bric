package net.consensys.bric.db;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for ColumnFamilyResolver hex/UTF-8 parsing functionality.
 */
class ColumnFamilyResolverTest {

    // ============================================================================
    // Hex Format Tests
    // ============================================================================

    @Test
    void testParseHexLowercase() {
        byte[] result = ColumnFamilyResolver.parseInput("0x0a");
        assertThat(result).isEqualTo(new byte[] {10});
    }

    @Test
    void testParseHexUppercase() {
        byte[] result = ColumnFamilyResolver.parseInput("0x0A");
        assertThat(result).isEqualTo(new byte[] {10});
    }

    @Test
    void testParseHexMixedCase() {
        byte[] result = ColumnFamilyResolver.parseInput("0xAbCdEf");
        assertThat(result).isEqualTo(new byte[] {(byte) 0xab, (byte) 0xcd, (byte) 0xef});
    }

    @Test
    void testParseHexMultipleBytesLowercase() {
        byte[] result = ColumnFamilyResolver.parseInput("0xabcdef1234");
        assertThat(result).isEqualTo(new byte[] {(byte) 0xab, (byte) 0xcd, (byte) 0xef, (byte) 0x12, (byte) 0x34});
    }

    @Test
    void testParseHexSingleByte() {
        byte[] result = ColumnFamilyResolver.parseInput("0xff");
        assertThat(result).isEqualTo(new byte[] {(byte) 0xff});
    }

    @Test
    void testParseHexZeros() {
        byte[] result = ColumnFamilyResolver.parseInput("0x0000");
        assertThat(result).isEqualTo(new byte[] {0, 0});
    }

    // ============================================================================
    // UTF-8 Format Tests
    // ============================================================================

    @Test
    void testParseUtf8Standard() {
        byte[] result = ColumnFamilyResolver.parseInput("TRIE_LOG_STORAGE");
        assertThat(result).isEqualTo("TRIE_LOG_STORAGE".getBytes());
    }

    @Test
    void testParseUtf8AccountInfoStateArchive() {
        byte[] result = ColumnFamilyResolver.parseInput("ACCOUNT_INFO_STATE_ARCHIVE");
        assertThat(result).isEqualTo("ACCOUNT_INFO_STATE_ARCHIVE".getBytes());
    }

    @Test
    void testParseUtf8AccountStorageArchive() {
        byte[] result = ColumnFamilyResolver.parseInput("ACCOUNT_STORAGE_ARCHIVE");
        assertThat(result).isEqualTo("ACCOUNT_STORAGE_ARCHIVE".getBytes());
    }

    @Test
    void testParseUtf8LowercaseConverted() {
        byte[] result = ColumnFamilyResolver.parseInput("trie_log_storage");
        assertThat(result).isEqualTo("TRIE_LOG_STORAGE".getBytes());
    }

    @Test
    void testParseUtf8MixedCase() {
        byte[] result = ColumnFamilyResolver.parseInput("Trie_Log_Storage");
        assertThat(result).isEqualTo("TRIE_LOG_STORAGE".getBytes());
    }

    @Test
    void testParseUtf8AccountInfoState() {
        byte[] result = ColumnFamilyResolver.parseInput("ACCOUNT_INFO_STATE");
        assertThat(result).isEqualTo("ACCOUNT_INFO_STATE".getBytes());
    }

    // ============================================================================
    // Quoted String Format Tests
    // ============================================================================

    @Test
    void testParseQuotedUtf8() {
        byte[] result = ColumnFamilyResolver.parseInput("\"MY_CUSTOM_CF\"");
        assertThat(result).isEqualTo("MY_CUSTOM_CF".getBytes());
    }

    @Test
    void testParseQuotedUtf8UpperCase() {
        byte[] result = ColumnFamilyResolver.parseInput("\"TRIE_LOG_STORAGE\"");
        assertThat(result).isEqualTo("TRIE_LOG_STORAGE".getBytes());
    }

    @Test
    void testParseQuotedUtf8LowerCase() {
        byte[] result = ColumnFamilyResolver.parseInput("\"my_custom_cf\"");
        assertThat(result).isEqualTo("MY_CUSTOM_CF".getBytes());
    }

    @Test
    void testParseQuotedHex() {
        byte[] result = ColumnFamilyResolver.parseInput("\"0x0a\"");
        assertThat(result).isEqualTo(new byte[] {10});
    }

    // ============================================================================
    // Error Cases: Invalid Hex
    // ============================================================================

    @Test
    void testParseHexOddLength() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0x0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("odd")
                .hasMessageContaining("hex");
    }

    @Test
    void testParseHexOddLengthThreeChars() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0xabc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("odd")
                .hasMessageContaining("hex");
    }

    @Test
    void testParseHexInvalidCharacters() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0xgg"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid hex");
    }

    @Test
    void testParseHexInvalidCharactersInMiddle() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0xabXYcd"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("invalid hex");
    }

    @Test
    void testParseHexNoPrefix() {
        // Without 0x prefix, should be treated as UTF-8
        byte[] result = ColumnFamilyResolver.parseInput("abcd");
        assertThat(result).isEqualTo("ABCD".getBytes());
    }

    // ============================================================================
    // Edge Cases
    // ============================================================================

    @Test
    void testParseEmptyStringAfterQuoteRemoval() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("\"\""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testParseHexOnlyPrefix() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0x"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testParseWhitespaceInHex() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0x ab cd"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testParseUtf8WithSpaces() {
        byte[] result = ColumnFamilyResolver.parseInput("MY CUSTOM CF");
        assertThat(result).isEqualTo("MY CUSTOM CF".getBytes());
    }

    @Test
    void testParseUtf8WithSpecialChars() {
        byte[] result = ColumnFamilyResolver.parseInput("MY-CUSTOM-CF_123");
        assertThat(result).isEqualTo("MY-CUSTOM-CF_123".getBytes());
    }

    @Test
    void testParseHexWithLeadingZeros() {
        byte[] result = ColumnFamilyResolver.parseInput("0x000f");
        assertThat(result).isEqualTo(new byte[] {0, 15});
    }

    @Test
    void testParseHexAllZeros() {
        byte[] result = ColumnFamilyResolver.parseInput("0x00000000");
        assertThat(result).isEqualTo(new byte[] {0, 0, 0, 0});
    }

    @Test
    void testParseHexAllFs() {
        byte[] result = ColumnFamilyResolver.parseInput("0xffffffff");
        assertThat(result).isEqualTo(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff});
    }
}
