package net.consensys.bric.commands;

import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for InputParser shared parsing utility.
 */
class InputParserTest {

    // --- parseAddress ---

    @Test
    void parseAddress_validAddress() {
        Address address = InputParser.parseAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
        assertThat(address).isNotNull();
    }

    @Test
    void parseAddress_missingPrefix() {
        assertThatThrownBy(() -> InputParser.parseAddress("742d35Cc6634C0532925a3b844Bc9e7595f0bEb0"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("0x-prefixed");
    }

    @Test
    void parseAddress_wrongLength() {
        assertThatThrownBy(() -> InputParser.parseAddress("0x1234"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("42 chars");
    }

    // --- parseHash ---

    @Test
    void parseHash_validHash() {
        String hex = "0x" + "ab".repeat(32);
        Hash hash = InputParser.parseHash(hex);
        assertThat(hash).isNotNull();
    }

    @Test
    void parseHash_withFieldName() {
        assertThatThrownBy(() -> InputParser.parseHash("0x1234", "block hash"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("block hash");
    }

    @Test
    void parseHash_missingPrefix() {
        String hex = "ab".repeat(32);
        assertThatThrownBy(() -> InputParser.parseHash(hex))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("0x-prefixed");
    }

    @Test
    void parseHash_wrongLength() {
        assertThatThrownBy(() -> InputParser.parseHash("0xabcdef"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("66 chars");
    }

    // --- parseSlot ---

    @Test
    void parseSlot_decimal() {
        UInt256 slot = InputParser.parseSlot("42");
        assertThat(slot).isEqualTo(UInt256.valueOf(42));
    }

    @Test
    void parseSlot_decimalZero() {
        UInt256 slot = InputParser.parseSlot("0");
        assertThat(slot).isEqualTo(UInt256.ZERO);
    }

    @Test
    void parseSlot_hex() {
        UInt256 slot = InputParser.parseSlot("0x2a");
        assertThat(slot).isEqualTo(UInt256.valueOf(42));
    }

    @Test
    void parseSlot_largeBigInteger() {
        // Larger than Long.MAX_VALUE
        String large = "99999999999999999999999999999999";
        UInt256 slot = InputParser.parseSlot(large);
        assertThat(slot).isNotNull();
    }

    @Test
    void parseSlot_negative() {
        assertThatThrownBy(() -> InputParser.parseSlot("-1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("negative");
    }

    @Test
    void parseSlot_exceeds256Bits() {
        // 2^257 is definitely > 256 bits
        String tooBig = "2".repeat(80); // very large number
        assertThatThrownBy(() -> InputParser.parseSlot(tooBig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("256 bits");
    }

    @Test
    void parseSlot_invalidFormat() {
        assertThatThrownBy(() -> InputParser.parseSlot("not_a_number"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid slot format");
    }

    // --- parseBlockNumber ---

    @Test
    void parseBlockNumber_valid() {
        long block = InputParser.parseBlockNumber("12345");
        assertThat(block).isEqualTo(12345);
    }

    @Test
    void parseBlockNumber_zero() {
        long block = InputParser.parseBlockNumber("0");
        assertThat(block).isEqualTo(0);
    }

    @Test
    void parseBlockNumber_negative() {
        assertThatThrownBy(() -> InputParser.parseBlockNumber("-1"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("negative");
    }

    @Test
    void parseBlockNumber_invalidFormat() {
        assertThatThrownBy(() -> InputParser.parseBlockNumber("abc"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid block number format");
    }

    // --- isBlockNumber ---

    @Test
    void isBlockNumber_numeric() {
        assertThat(InputParser.isBlockNumber("12345")).isTrue();
    }

    @Test
    void isBlockNumber_zero() {
        assertThat(InputParser.isBlockNumber("0")).isTrue();
    }

    @Test
    void isBlockNumber_hexPrefix() {
        assertThat(InputParser.isBlockNumber("0xabcdef")).isFalse();
    }

    @Test
    void isBlockNumber_nonNumeric() {
        assertThat(InputParser.isBlockNumber("hello")).isFalse();
    }

    // --- hasFlag ---

    @Test
    void hasFlag_present() {
        String[] args = {"arg1", "--verbose", "arg2"};
        assertThat(InputParser.hasFlag(args, "--verbose")).isTrue();
    }

    @Test
    void hasFlag_absent() {
        String[] args = {"arg1", "arg2"};
        assertThat(InputParser.hasFlag(args, "--verbose")).isFalse();
    }

    @Test
    void hasFlag_emptyArgs() {
        String[] args = {};
        assertThat(InputParser.hasFlag(args, "--verbose")).isFalse();
    }
}
