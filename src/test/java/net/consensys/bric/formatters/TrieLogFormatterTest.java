package net.consensys.bric.formatters;

import net.consensys.bric.db.TrieLogData;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TrieLogFormatterTest {

    private TrieLogFormatter formatter;

    @BeforeEach
    void setUp() {
        formatter = new TrieLogFormatter();
    }

    @Test
    void testFormatEmptyTrieLog() {
        TrieLogData trieLog = new TrieLogData();
        trieLog.blockHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        trieLog.trieLogLayer = new TrieLogLayer();

        String formatted = formatter.format(trieLog);

        assertThat(formatted).isNotNull();
        assertThat(formatted).isNotEmpty();
        assertThat(formatted).contains("Trie Log");
        assertThat(formatted).contains("Block Hash");
        assertThat(formatted).contains("Empty block");
    }

    @Test
    void testFormatWithBlockNumber() {
        TrieLogData trieLog = new TrieLogData();
        trieLog.blockHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        trieLog.blockNumber = 12345L;
        trieLog.trieLogLayer = new TrieLogLayer();

        String formatted = formatter.format(trieLog);

        assertThat(formatted).contains("Block Number:");
        assertThat(formatted).contains("12,345");
    }

    @Test
    void testFormatCompact() {
        TrieLogData trieLog = new TrieLogData();
        trieLog.blockHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        trieLog.trieLogLayer = new TrieLogLayer();

        String compact = formatter.formatCompact(trieLog);

        assertThat(compact).isNotNull();
        assertThat(compact).isNotEmpty();
        assertThat(compact).contains("Block");
        assertThat(compact).contains("0 account");
    }

    @Test
    void testIsEmpty() {
        TrieLogData emptyTrieLog = new TrieLogData();
        emptyTrieLog.blockHash = Hash.ZERO;
        emptyTrieLog.trieLogLayer = new TrieLogLayer();

        assertThat(formatter.isEmpty(emptyTrieLog)).isTrue();
    }

    @Test
    void testFormatBasicStructure() {
        TrieLogData trieLog = new TrieLogData();
        trieLog.blockHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        trieLog.blockNumber = 100L;
        trieLog.trieLogLayer = new TrieLogLayer();

        String formatted = formatter.format(trieLog);

        // Verify basic structure
        assertThat(formatted).contains("Trie Log (State Diff):");
        assertThat(formatted).contains("Block Hash:");
        assertThat(formatted).contains("Block Number:");
    }

    @Test
    void testFormatWithAccountChange() {
        TrieLogData trieLog = new TrieLogData();
        trieLog.blockHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
        trieLog.blockNumber = 100L;

        TrieLogLayer layer = new TrieLogLayer();
        Address address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");

        PmtStateTrieAccountValue oldAccount = new PmtStateTrieAccountValue(
            1L,
            Wei.of(1000000000000000000L),
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY
        );

        PmtStateTrieAccountValue newAccount = new PmtStateTrieAccountValue(
            2L,
            Wei.of(2000000000000000000L),
            Hash.EMPTY_TRIE_HASH,
            Hash.EMPTY
        );

        layer.addAccountChange(address, oldAccount, newAccount);
        trieLog.trieLogLayer = layer;

        String formatted = formatter.format(trieLog);

        assertThat(formatted).contains("Account Changes");
        assertThat(formatted).contains(address.toHexString());
        assertThat(formatted).contains("UPDATED");
    }

    @Test
    void testFormatWithStorageChange() {
        TrieLogData trieLog = new TrieLogData();
        trieLog.blockHash = Hash.fromHexString(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");

        TrieLogLayer layer = new TrieLogLayer();
        Address address = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0");
        StorageSlotKey slotKey = new StorageSlotKey(Hash.ZERO, Optional.empty());

        layer.addStorageChange(address, slotKey, UInt256.ZERO, UInt256.ONE);
        trieLog.trieLogLayer = layer;

        String formatted = formatter.format(trieLog);

        assertThat(formatted).contains("Storage Changes");
        assertThat(formatted).contains(address.toHexString());
        assertThat(formatted).contains("0x0 →");
    }
}
