package net.consensys.bric.db;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * High-level read operations for Besu database.
 * Handles RLP decoding and data transformation.
 */
public class BesuDatabaseReader {

    private static final Logger LOG = LoggerFactory.getLogger(BesuDatabaseReader.class);
    private final SegmentReader segmentReader;

    public BesuDatabaseReader(BesuDatabaseManager dbManager) {
        this.segmentReader = new SegmentReader(dbManager);
    }

    /**
     * Read account information by Ethereum address.
     *
     * @param address The Ethereum address
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccount(Address address) {
        Hash accountHash = segmentReader.computeAccountHash(address);

        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE,
            accountHash.toArrayUnsafe()
        );

        if (rawData.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(parseAccountData(rawData.get(), address, accountHash));
        } catch (Exception e) {
            LOG.error("Failed to parse account data for address {}", address, e);
            return Optional.empty();
        }
    }

    /**
     * Read account information by account hash (for debugging).
     *
     * @param accountHash The 32-byte account hash
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountByHash(Hash accountHash) {
        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE,
            accountHash.toArrayUnsafe()
        );

        if (rawData.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(parseAccountData(rawData.get(), null, accountHash));
        } catch (Exception e) {
            LOG.error("Failed to parse account data for hash {}", accountHash, e);
            return Optional.empty();
        }
    }

    /**
     * Read storage slot value by address and slot.
     *
     * @param address The contract address
     * @param slot The storage slot (as UInt256)
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorage(Address address, UInt256 slot) {
        Hash accountHash = segmentReader.computeAccountHash(address);
        Hash slotHash = Hash.hash(slot);

        byte[] storageKey = segmentReader.computeStorageKey(accountHash, slotHash);

        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
            storageKey
        );

        if (rawData.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(parseStorageData(rawData.get(), address, slot, accountHash, slotHash));
        } catch (Exception e) {
            LOG.error("Failed to parse storage data for address {} slot {}", address, slot, e);
            return Optional.empty();
        }
    }

    /**
     * Read storage slot value by raw hashes (for debugging).
     *
     * @param accountHash The account hash
     * @param slotHash The slot hash
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorageByHash(Hash accountHash, Hash slotHash) {
        byte[] storageKey = segmentReader.computeStorageKey(accountHash, slotHash);

        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
            storageKey
        );

        if (rawData.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(parseStorageData(rawData.get(), null, null, accountHash, slotHash));
        } catch (Exception e) {
            LOG.error("Failed to parse storage data for hashes {} {}", accountHash, slotHash, e);
            return Optional.empty();
        }
    }

    /**
     * Parse RLP-encoded account data.
     * Format: RLP[nonce, balance, storageRoot, codeHash]
     */
    private AccountData parseAccountData(byte[] rawData, Address address, Hash accountHash) {
        RLPInput rlpInput = new BytesValueRLPInput(Bytes.wrap(rawData), false);

        rlpInput.enterList();

        long nonce = rlpInput.readLongScalar();
        Wei balance = Wei.of(rlpInput.readUInt256Scalar());

        Bytes32 storageRoot;
        if (rlpInput.nextIsNull()) {
            storageRoot = Hash.EMPTY_TRIE_HASH;
            rlpInput.skipNext();
        } else {
            storageRoot = rlpInput.readBytes32();
        }

        Bytes32 codeHash;
        if (rlpInput.nextIsNull()) {
            codeHash = Hash.EMPTY;
            rlpInput.skipNext();
        } else {
            codeHash = rlpInput.readBytes32();
        }

        rlpInput.leaveList();

        AccountData data = new AccountData();
        data.address = address;
        data.accountHash = accountHash;
        data.nonce = nonce;
        data.balance = balance;
        data.storageRoot = Hash.wrap(storageRoot);
        data.codeHash = Hash.wrap(codeHash);

        return data;
    }

    /**
     * Parse RLP-encoded storage value.
     * Format: RLP-encoded UInt256
     */
    private StorageData parseStorageData(byte[] rawData, Address address, UInt256 slot,
                                         Hash accountHash, Hash slotHash) {
        RLPInput rlpInput = new BytesValueRLPInput(Bytes.wrap(rawData), false);
        UInt256 value = rlpInput.readUInt256Scalar();

        StorageData data = new StorageData();
        data.address = address;
        data.slot = slot;
        data.accountHash = accountHash;
        data.slotHash = slotHash;
        data.value = value;

        return data;
    }

    /**
     * Account data container.
     */
    public static class AccountData {
        public Address address;
        public Hash accountHash;
        public long nonce;
        public Wei balance;
        public Hash storageRoot;
        public Hash codeHash;
    }

    /**
     * Storage data container.
     */
    public static class StorageData {
        public Address address;
        public UInt256 slot;
        public Hash accountHash;
        public Hash slotHash;
        public UInt256 value;
    }
}
