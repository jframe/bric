package net.consensys.bric.db;

import net.consensys.bric.besu.BesuFlatDbReader;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
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
    private final BesuFlatDbReader besuReader;

    // Block header prefix for BLOCKCHAIN segment (matching Besu's KeyValueStoragePrefixedKeyBlockchainStorage)
    private static final byte[] BLOCK_HEADER_PREFIX = new byte[] {0x02};

    public BesuDatabaseReader(BesuDatabaseManager dbManager) {
        this.segmentReader = new SegmentReader(dbManager);
        this.dbManager = dbManager;
        this.besuReader = new BesuFlatDbReader(dbManager);
    }

    /**
     * Read account information by Ethereum address.
     *
     * @param address The Ethereum address
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccount(Address address) {
        return besuReader.readAccount(address);
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
        return besuReader.readAccountAtBlock(address, blockNumber);
    }

    /**
     * Read account information by account hash (for debugging).
     *
     * @param accountHash The 32-byte account hash
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountByHash(Hash accountHash) {
        return besuReader.readAccountByHash(accountHash);
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

        return besuReader.readAccountByHashAtBlock(accountHash, blockNumber);
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
            byte[] key = Bytes.concatenate(
                Bytes.wrap(BLOCK_HEADER_PREFIX),
                Bytes.wrap(blockHash.toArrayUnsafe())
            ).toArrayUnsafe();

            Optional<byte[]> rawData = segmentReader.get(
                KeyValueSegmentIdentifier.BLOCKCHAIN,
                key
            );

            if (rawData.isEmpty()) {
                LOG.debug("Block header not found for hash: {}", blockHash.toHexString());
                return Optional.empty();
            }

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
        return besuReader.readStorage(address, slot);
    }

    /**
     * Read storage slot value by raw hashes (for debugging).
     *
     * @param accountHash The account hash
     * @param slotHash The slot hash
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorageByHash(Hash accountHash, Hash slotHash) {
        return besuReader.readStorageByHash(accountHash, slotHash);
    }

    /**
     * Read storage slot value at a specific block number.
     * Only works with Bonsai Archive databases.
     *
     * @param address The contract address
     * @param slot The storage slot (as UInt256)
     * @param blockNumber The block number to query
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorageAtBlock(Address address, UInt256 slot, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readStorageAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        return besuReader.readStorageAtBlock(address, slot, blockNumber);
    }

    /**
     * Read storage slot value by raw hashes at a specific block number.
     * Only works with Bonsai Archive databases.
     *
     * @param accountHash The account hash
     * @param slotHash The slot hash
     * @param blockNumber The block number to query
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorageByHashAtBlock(Hash accountHash, Hash slotHash, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readStorageByHashAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        return besuReader.readStorageByHashAtBlock(accountHash, slotHash, blockNumber);
    }


    /**
     * Read contract bytecode by address.
     *
     * @param address The contract address
     * @return Optional containing code data if found
     */
    public Optional<CodeData> readCode(Address address) {
        Optional<AccountData> accountData = readAccount(address);

        if (accountData.isEmpty()) {
            return Optional.empty();
        }

        Hash codeHash = accountData.get().codeHash;

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
     *
     * @param blockNumber The block number
     * @return Optional containing block hash if found
     */
    public Optional<Hash> readBlockHashByNumber(long blockNumber) {
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
            TrieLogFactoryImpl factory = new TrieLogFactoryImpl();
            TrieLogLayer layer = factory.deserialize(rawData.get());

            TrieLogData trieLog = new TrieLogData();
            trieLog.blockHash = blockHash;
            trieLog.trieLogLayer = layer;

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
}
