package net.consensys.bric.formatters;

import net.consensys.bric.db.AccountData;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;

class AccountFormatterTest {

    private AccountFormatter formatter;
    private static final Hash EMPTY_CODE_HASH = Hash.fromHexString(
        "0xc5d2460186f7233c927e7db2dcc703c0e6b061b9d7daad5c4b9b1cd0b6d9c5c6"
    );

    @BeforeEach
    void setUp() {
        formatter = new AccountFormatter();
    }

    @Test
    void testFormatAccountWithAddress() {
        AccountData account = new AccountData();
        account.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        account.accountHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        account.nonce = 5;
        account.balance = Wei.of(new BigInteger("1500000000000000000")); // 1.5 ETH
        account.storageRoot = Hash.fromHexString(
            "0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        account.codeHash = Hash.fromHexString(
            "0x1111111111111111111111111111111111111111111111111111111111111111");

        String formatted = formatter.format(account);

        assertThat(formatted).isNotNull();
        assertThat(formatted).isNotEmpty();
        assertThat(formatted).contains("Account Information:");
        assertThat(formatted).contains("Address:");
        assertThat(formatted).contains("Nonce:");
        assertThat(formatted).contains("Balance:");
        assertThat(formatted).contains("0x14d1120d7b160000");
        assertThat(formatted).contains("1.5 ETH");
    }

    @Test
    void testFormatAccountWithoutAddress() {
        AccountData account = new AccountData();
        account.accountHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        account.nonce = 0;
        account.balance = Wei.ZERO;
        account.storageRoot = Hash.ZERO;
        account.codeHash = EMPTY_CODE_HASH;

        String formatted = formatter.format(account);

        assertThat(formatted).contains("Account Information:");
        assertThat(formatted).doesNotContain("Address:");
        assertThat(formatted).contains("Account Hash:");
        assertThat(formatted).contains("0x0 (0 ETH)");
    }

    @Test
    void testFormatZeroBalance() {
        AccountData account = new AccountData();
        account.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        account.accountHash = Hash.ZERO;
        account.nonce = 0;
        account.balance = Wei.ZERO;
        account.storageRoot = Hash.ZERO;
        account.codeHash = EMPTY_CODE_HASH;

        String formatted = formatter.format(account);

        assertThat(formatted).contains("0x0 (0 ETH)");
    }

    @Test
    void testFormatLargeBalance() {
        AccountData account = new AccountData();
        account.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        account.accountHash = Hash.ZERO;
        account.nonce = 100;
        account.balance = Wei.of(new BigInteger("123456789012345678901234567890")); // Large value
        account.storageRoot = Hash.ZERO;
        account.codeHash = EMPTY_CODE_HASH;

        String formatted = formatter.format(account);

        assertThat(formatted).contains("Balance:");
        assertThat(formatted).contains("0x");
        assertThat(formatted).contains("ETH");
    }

    @Test
    void testFormatEmptyCodeHash() {
        AccountData account = new AccountData();
        account.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        account.accountHash = Hash.ZERO;
        account.nonce = 0;
        account.balance = Wei.ZERO;
        account.storageRoot = Hash.ZERO;
        account.codeHash = EMPTY_CODE_HASH;

        String formatted = formatter.format(account);

        assertThat(formatted).contains("no code - EOA");
    }

    @Test
    void testFormatContractCodeHash() {
        AccountData account = new AccountData();
        account.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        account.accountHash = Hash.ZERO;
        account.nonce = 1;
        account.balance = Wei.ZERO;
        account.storageRoot = Hash.ZERO;
        account.codeHash = Hash.fromHexString(
            "0x1111111111111111111111111111111111111111111111111111111111111111");

        String formatted = formatter.format(account);

        assertThat(formatted).contains("contract");
    }

    @Test
    void testFormatCompact() {
        AccountData account = new AccountData();
        account.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        account.accountHash = Hash.ZERO;
        account.nonce = 5;
        account.balance = Wei.of(new BigInteger("1500000000000000000")); // 1.5 ETH
        account.storageRoot = Hash.ZERO;
        account.codeHash = EMPTY_CODE_HASH;

        String compact = formatter.formatCompact(account);

        assertThat(compact).isNotNull();
        assertThat(compact).isNotEmpty();
        assertThat(compact).containsIgnoringCase("nonce");
        assertThat(compact).contains("EOA");
    }

    @Test
    void testFormatCompactContract() {
        AccountData account = new AccountData();
        account.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        account.accountHash = Hash.ZERO;
        account.nonce = 1;
        account.balance = Wei.of(new BigInteger("1000000000000000000")); // 1 ETH
        account.storageRoot = Hash.ZERO;
        account.codeHash = Hash.fromHexString(
            "0x1111111111111111111111111111111111111111111111111111111111111111");

        String compact = formatter.formatCompact(account);

        assertThat(compact).contains("Contract");
    }
}
