package net.consensys.bric.formatters;

import org.apache.tuweni.units.bigints.UInt256;
import net.consensys.bric.db.StorageData;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StorageFormatterTest {

    private StorageFormatter formatter;

    @BeforeEach
    void setUp() {
        formatter = new StorageFormatter();
    }

    @Test
    void testFormatStorageWithAddress() {
        StorageData storage = new StorageData();
        storage.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        storage.accountHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        storage.slot = UInt256.valueOf(0);
        storage.slotHash = Hash.fromHexString(
            "0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        storage.value = UInt256.valueOf(42);

        String formatted = formatter.format(storage);

        assertThat(formatted).isNotNull();
        assertThat(formatted).isNotEmpty();
        assertThat(formatted).contains("Storage Information:");
        assertThat(formatted).contains("Address:");
        assertThat(formatted).contains("Slot:");
        assertThat(formatted).contains("Value");
    }

    @Test
    void testFormatStorageWithoutAddress() {
        StorageData storage = new StorageData();
        storage.accountHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        storage.slotHash = Hash.fromHexString(
            "0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd");
        storage.value = UInt256.valueOf(100);

        String formatted = formatter.format(storage);

        assertThat(formatted).contains("Storage Information:");
        assertThat(formatted).doesNotContain("Address:");
        assertThat(formatted).contains("Account Hash:");
        assertThat(formatted).contains("Slot Hash:");
        assertThat(formatted).contains("100");
    }

    @Test
    void testFormatZeroValue() {
        StorageData storage = new StorageData();
        storage.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        storage.accountHash = Hash.ZERO;
        storage.slot = UInt256.ZERO;
        storage.slotHash = Hash.ZERO;
        storage.value = UInt256.ZERO;

        String formatted = formatter.format(storage);

        assertThat(formatted).contains("0x0");
        assertThat(formatted).contains("0");
    }

    @Test
    void testFormatLargeValue() {
        StorageData storage = new StorageData();
        storage.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        storage.accountHash = Hash.ZERO;
        storage.slot = UInt256.valueOf(5);
        storage.slotHash = Hash.ZERO;
        storage.value = UInt256.valueOf(999999);

        String formatted = formatter.format(storage);

        assertThat(formatted).contains("999,999");
    }

    @Test
    void testFormatCompact() {
        StorageData storage = new StorageData();
        storage.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        storage.accountHash = Hash.ZERO;
        storage.slot = UInt256.valueOf(0);
        storage.slotHash = Hash.ZERO;
        storage.value = UInt256.valueOf(42);

        String compact = formatter.formatCompact(storage);

        assertThat(compact).isNotNull();
        assertThat(compact).isNotEmpty();
        assertThat(compact).containsIgnoringCase("slot");
    }

    @Test
    void testIsEmpty() {
        StorageData emptyStorage = new StorageData();
        emptyStorage.value = UInt256.ZERO;

        StorageData nonEmptyStorage = new StorageData();
        nonEmptyStorage.value = UInt256.valueOf(1);

        assertThat(formatter.isEmpty(emptyStorage)).isTrue();
        assertThat(formatter.isEmpty(nonEmptyStorage)).isFalse();
    }

    @Test
    void testFormatHexSlot() {
        StorageData storage = new StorageData();
        storage.address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
        storage.accountHash = Hash.ZERO;
        storage.slot = UInt256.fromHexString("0x1234");
        storage.slotHash = Hash.ZERO;
        storage.value = UInt256.valueOf(100);

        String formatted = formatter.format(storage);

        assertThat(formatted).isNotNull();
        assertThat(formatted).isNotEmpty();
    }
}
