package net.consensys.bric.formatters;

import net.consensys.bric.db.BesuDatabaseReader.CodeData;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CodeFormatterTest {

    private CodeFormatter formatter;

    @BeforeEach
    void setUp() {
        formatter = new CodeFormatter();
    }

    @Test
    void testFormatSmallCode() {
        CodeData code = new CodeData();
        code.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        code.codeHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        code.bytecode = new byte[]{0x60, (byte) 0x80, 0x60, 0x40}; // Simple bytecode

        String formatted = formatter.format(code);

        assertThat(formatted).isNotNull();
        assertThat(formatted).isNotEmpty();
        assertThat(formatted).contains("Contract Code Information:");
        assertThat(formatted).contains("Address:");
        assertThat(formatted).contains("Code Hash:");
        assertThat(formatted).contains("Size:");
        assertThat(formatted).contains("Bytecode:");
    }

    @Test
    void testFormatWithoutAddress() {
        CodeData code = new CodeData();
        code.codeHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        code.bytecode = new byte[]{0x60, (byte) 0x80};

        String formatted = formatter.format(code);

        assertThat(formatted).contains("Contract Code Information:");
        assertThat(formatted).doesNotContain("Address:");
        assertThat(formatted).contains("Code Hash:");
    }

    @Test
    void testFormatLargeCodeTruncated() {
        CodeData code = new CodeData();
        code.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        code.codeHash = Hash.ZERO;
        code.bytecode = new byte[2000]; // Large bytecode (> 1000 bytes)

        String formatted = formatter.format(code);

        assertThat(formatted).contains("truncated");
        assertThat(formatted).contains("more bytes");
    }

    @Test
    void testFormatLargeCodeNoTruncation() {
        CodeData code = new CodeData();
        code.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        code.codeHash = Hash.ZERO;
        code.bytecode = new byte[2000]; // Large bytecode

        String formatted = formatter.format(code, -1); // No truncation

        assertThat(formatted).doesNotContain("truncated");
    }

    @Test
    void testFormatCompact() {
        CodeData code = new CodeData();
        code.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        code.codeHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        code.bytecode = new byte[100];

        String compact = formatter.formatCompact(code);

        assertThat(compact).isNotNull();
        assertThat(compact).isNotEmpty();
        assertThat(compact).contains("100 bytes");
    }

    @Test
    void testToBytecodeHex() {
        CodeData code = new CodeData();
        code.codeHash = Hash.ZERO;
        code.bytecode = new byte[]{0x60, (byte) 0x80, 0x60, 0x40};

        String hex = formatter.toBytecodeHex(code);

        assertThat(hex).isEqualTo("0x60806040");
    }

    @Test
    void testGetSize() {
        CodeData code = new CodeData();
        code.codeHash = Hash.ZERO;
        code.bytecode = new byte[42];

        int size = formatter.getSize(code);

        assertThat(size).isEqualTo(42);
    }

    @Test
    void testFormatSizeBytes() {
        CodeData code = new CodeData();
        code.codeHash = Hash.ZERO;
        code.bytecode = new byte[500];

        String formatted = formatter.format(code);

        assertThat(formatted).contains("500 bytes");
    }

    @Test
    void testFormatSizeKilobytes() {
        CodeData code = new CodeData();
        code.codeHash = Hash.ZERO;
        code.bytecode = new byte[2048];

        String formatted = formatter.format(code);

        assertThat(formatted).containsIgnoringCase("KB");
    }
}
