package net.consensys.bric.db;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.RLPInput;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.trielog.TrieLogFactoryImpl;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;
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
    private final BesuDatabaseManager dbManager;

    // Block number suffix for archive queries (matching Besu's BonsaiArchiveFlatDbStrategy)
    // Uses Long.MAX_VALUE to find the most recent version of an account/storage
    private static final byte[] MAX_BLOCK_SUFFIX = Bytes.ofUnsignedLong(Long.MAX_VALUE).toArrayUnsafe();
    private static final int ACCOUNT_HASH_SIZE = 32;

    // Block header prefix for BLOCKCHAIN segment (matching Besu's KeyValueStoragePrefixedKeyBlockchainStorage)
    private static final byte[] BLOCK_HEADER_PREFIX = new byte[] {0x02};

    public BesuDatabaseReader(BesuDatabaseManager dbManager) {
        this.segmentReader = new SegmentReader(dbManager);
        this.dbManager = dbManager;
    }

    /**
     * Read account information by Ethereum address.
     * For Bonsai Archive databases, falls back to archive query if not found in main column family.
     *
     * @param address The Ethereum address
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccount(Address address) {
        Hash accountHash = segmentReader.computeAccountHash(address);

        // Try regular lookup first
        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE,
            accountHash.toArrayUnsafe()
        );

        if (rawData.isPresent()) {
            try {
                return Optional.of(parseAccountData(rawData.get(), address, accountHash, null));
            } catch (Exception e) {
                LOG.error("Failed to parse account data for address {}", address, e);
                return Optional.empty();
            }
        }

        // For Bonsai Archive databases, try archive lookup
        if (dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            return readAccountArchive(address, accountHash, Optional.empty());
        }

        return Optional.empty();
    }

    /**
     * Read account information by Ethereum address at a specific block number.
     * Only works with Bonsai Archive databases.
     *
     * @param address The Ethereum address
     * @param blockNumber The block number to query
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountAtBlock(Address address, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readAccountAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        Hash accountHash = segmentReader.computeAccountHash(address);
        return readAccountArchive(address, accountHash, Optional.of(blockNumber));
    }

    /**
     * Read account information by account hash (for debugging).
     * For Bonsai Archive databases, falls back to archive query if not found in main column family.
     *
     * @param accountHash The 32-byte account hash
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountByHash(Hash accountHash) {
        // Try regular lookup first
        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE,
            accountHash.toArrayUnsafe()
        );

        if (rawData.isPresent()) {
            try {
                return Optional.of(parseAccountData(rawData.get(), null, accountHash, null));
            } catch (Exception e) {
                LOG.error("Failed to parse account data for hash {}", accountHash, e);
                return Optional.empty();
            }
        }

        // For Bonsai Archive databases, try archive lookup
        if (dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            return readAccountArchive(null, accountHash, Optional.empty());
        }

        return Optional.empty();
    }

    /**
     * Read account information by account hash at a specific block number.
     * Only works with Bonsai Archive databases.
     *
     * @param accountHash The 32-byte account hash
     * @param blockNumber The block number to query
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountByHashAtBlock(Hash accountHash, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readAccountByHashAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        return readAccountArchive(null, accountHash, Optional.of(blockNumber));
    }

    /**
     * Read account from archive column families using block number suffixes.
     * Searches for the account at or before the specified block (or latest if no block specified).
     *
     * @param address The Ethereum address (can be null if only hash is known)
     * @param accountHash The account hash
     * @param blockNumber Optional block number to query (empty = latest)
     * @return Optional containing account data if found
     */
    private Optional<AccountData> readAccountArchive(Address address, Hash accountHash, Optional<Long> blockNumber) {
        // Create search key: accountHash + blockSuffix
        byte[] blockSuffix = blockNumber
            .map(bn -> Bytes.ofUnsignedLong(bn).toArrayUnsafe())
            .orElse(MAX_BLOCK_SUFFIX);

        byte[] searchKey = Bytes.concatenate(
            Bytes.wrap(accountHash.toArrayUnsafe()),
            Bytes.wrap(blockSuffix)
        ).toArrayUnsafe();

        // Try ACCOUNT_INFO_STATE first (may have archive entries with suffixes)
        Optional<SegmentReader.KeyValuePair> result = segmentReader.getNearestBefore(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE,
            searchKey,
            ACCOUNT_HASH_SIZE
        );

        // If not found in ACCOUNT_INFO_STATE, try ACCOUNT_INFO_STATE_ARCHIVE
        if (result.isEmpty()) {
            result = segmentReader.getNearestBefore(
                KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
                searchKey,
                ACCOUNT_HASH_SIZE
            );
        }

        if (result.isEmpty()) {
            return Optional.empty();
        }

        // Extract block number from key suffix (last 8 bytes)
        Long foundBlockNumber = null;
        if (result.get().key.length >= ACCOUNT_HASH_SIZE + 8) {
            byte[] suffix = new byte[8];
            System.arraycopy(result.get().key, ACCOUNT_HASH_SIZE, suffix, 0, 8);
            foundBlockNumber = Bytes.wrap(suffix).toLong();
        }

        try {
            return Optional.of(parseAccountData(result.get().value, address, accountHash, foundBlockNumber));
        } catch (Exception e) {
            LOG.error("Failed to parse archive account data for address {} hash {}", address, accountHash, e);
            return Optional.empty();
        }
    }

    /**
     * Get block number from block hash by retrieving and parsing the block header.
     * Parses RLP directly to extract block number (field #8 in header).
     *
     * Block header RLP structure:
     * [0] parentHash, [1] ommersHash, [2] coinbase, [3] stateRoot,
     * [4] transactionsRoot, [5] receiptsRoot, [6] logsBloom, [7] difficulty,
     * [8] number ← BLOCK NUMBER, [9] gasLimit, [10] gasUsed, [11] timestamp, ...
     *
     * @param blockHash The block hash to look up
     * @return Optional containing block number if found
     */
    public Optional<Long> getBlockNumberFromHash(Hash blockHash) {
        try {
            // Create key: BLOCK_HEADER_PREFIX (0x02) + blockHash (32 bytes)
            byte[] key = Bytes.concatenate(
                Bytes.wrap(BLOCK_HEADER_PREFIX),
                Bytes.wrap(blockHash.toArrayUnsafe())
            ).toArrayUnsafe();

            // Read block header from BLOCKCHAIN segment
            Optional<byte[]> rawData = segmentReader.get(
                KeyValueSegmentIdentifier.BLOCKCHAIN,
                key
            );

            if (rawData.isEmpty()) {
                LOG.debug("Block header not found for hash: {}", blockHash.toHexString());
                return Optional.empty();
            }

            // Parse block header RLP
            RLPInput headerRlp = new BytesValueRLPInput(Bytes.wrap(rawData.get()), false).readAsRlp();
            if (headerRlp.enterList() == 0) {
                LOG.error("Empty block header RLP for hash: {}", blockHash.toHexString());
                return Optional.empty();
            }

            // Skip fields 0-7 to get to block number (field #8)
            headerRlp.skipNext();  // 0: parentHash
            headerRlp.skipNext();  // 1: ommersHash
            headerRlp.skipNext();  // 2: coinbase
            headerRlp.skipNext();  // 3: stateRoot
            headerRlp.skipNext();  // 4: transactionsRoot
            headerRlp.skipNext();  // 5: receiptsRoot
            headerRlp.skipNext();  // 6: logsBloom
            headerRlp.skipNext();  // 7: difficulty

            // Read field #8: block number
            long blockNumber = headerRlp.readLongScalar();

            return Optional.of(blockNumber);

        } catch (Exception e) {
            LOG.error("Error retrieving block number from hash {}: {}", blockHash.toHexString(), e.getMessage());
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
    private AccountData parseAccountData(byte[] rawData, Address address, Hash accountHash, Long blockNumber) {
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
        data.blockNumber = blockNumber;

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
     * Read contract bytecode by address.
     *
     * @param address The contract address
     * @return Optional containing code data if found
     */
    public Optional<CodeData> readCode(Address address) {
        // First read the account to get the code hash
        Optional<AccountData> accountData = readAccount(address);

        if (accountData.isEmpty()) {
            return Optional.empty();
        }

        Hash codeHash = accountData.get().codeHash;

        // Check for empty code hash (EOA)
        if (codeHash.equals(Hash.EMPTY)) {
            return Optional.empty();
        }

        return readCodeByHash(codeHash, address);
    }

    /**
     * Read contract bytecode by code hash.
     *
     * @param codeHash The 32-byte code hash
     * @return Optional containing code data if found
     */
    public Optional<CodeData> readCodeByHash(Hash codeHash) {
        return readCodeByHash(codeHash, null);
    }

    /**
     * Internal method to read code by hash with optional address.
     */
    private Optional<CodeData> readCodeByHash(Hash codeHash, Address address) {
        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.CODE_STORAGE,
            codeHash.toArrayUnsafe()
        );

        if (rawData.isEmpty()) {
            return Optional.empty();
        }

        CodeData data = new CodeData();
        data.address = address;
        data.codeHash = codeHash;
        data.bytecode = rawData.get();

        return Optional.of(data);
    }

    /**
     * Read block hash by block number.
     * Uses the BLOCKCHAIN segment with prefix 0x05 + UInt256(blockNumber).
     *
     * @param blockNumber The block number
     * @return Optional containing block hash if found
     */
    public Optional<Hash> readBlockHashByNumber(long blockNumber) {
        // Key format: BLOCK_HASH_PREFIX (0x05) + UInt256(blockNumber)
        byte[] prefix = new byte[]{0x05};
        UInt256 blockNumberUInt = UInt256.valueOf(blockNumber);
        byte[] key = Bytes.concatenate(Bytes.of(prefix), blockNumberUInt).toArrayUnsafe();

        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.BLOCKCHAIN,
            key
        );

        if (rawData.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(Hash.wrap(Bytes32.wrap(rawData.get())));
        } catch (Exception e) {
            LOG.error("Failed to parse block hash for block number {}", blockNumber, e);
            return Optional.empty();
        }
    }

    /**
     * Read trie log (state diff) by block hash.
     * Parses the RLP data into a TrieLogLayer using Besu's internal classes.
     *
     * @param blockHash The block hash
     * @return Optional containing trie log data if found
     */
    public Optional<TrieLogData> readTrieLog(Hash blockHash) {
        Optional<byte[]> rawData = segmentReader.get(
            KeyValueSegmentIdentifier.TRIE_LOG_STORAGE,
            blockHash.toArrayUnsafe()
        );

        if (rawData.isEmpty()) {
            return Optional.empty();
        }

        try {
            // Parse RLP data using Besu's TrieLogFactoryImpl
            TrieLogFactoryImpl factory = new TrieLogFactoryImpl();
            TrieLogLayer layer = factory.deserialize(rawData.get());

            TrieLogData trieLog = new TrieLogData();
            trieLog.blockHash = blockHash;
            trieLog.trieLogLayer = layer;

            // Extract block number if available
            if (layer.getBlockNumber().isPresent()) {
                trieLog.blockNumber = layer.getBlockNumber().get();
            }

            return Optional.of(trieLog);
        } catch (Exception e) {
            LOG.error("Failed to parse trie log for block {}", blockHash, e);
            return Optional.empty();
        }
    }

    /**
     * Read trie log (state diff) by block number.
     * First looks up the block hash, then retrieves the trie log.
     *
     * @param blockNumber The block number
     * @return Optional containing trie log data if found
     */
    public Optional<TrieLogData> readTrieLogByNumber(long blockNumber) {
        Optional<Hash> blockHash = readBlockHashByNumber(blockNumber);

        if (blockHash.isEmpty()) {
            return Optional.empty();
        }

        return readTrieLog(blockHash.get());
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
        public Long blockNumber;  // Block number when account was retrieved (archive databases only)
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

    /**
     * Code data container.
     */
    public static class CodeData {
        public Address address;
        public Hash codeHash;
        public byte[] bytecode;
    }
}
