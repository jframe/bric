package net.consensys.bric.besu;

import net.consensys.bric.db.AccountData;
import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import net.consensys.bric.db.StorageData;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.apache.tuweni.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFullFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Wrapper that uses Besu's native FlatDbStrategy classes to read account and storage data.
 * Uses BonsaiFullFlatDbStrategy for regular Bonsai databases and BonsaiArchiveFlatDbStrategy
 * for archive databases. This ensures we use the exact same encoding/decoding logic as Besu.
 */
public class BesuFlatDbReader {

    private static final Logger LOG = LoggerFactory.getLogger(BesuFlatDbReader.class);

    private final BesuDatabaseManager dbManager;
    private final SegmentedKeyValueStorage storage;
    private final BonsaiFullFlatDbStrategy fullStrategy;
    private final BonsaiArchiveFlatDbStrategy archiveStrategy;

    public BesuFlatDbReader(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
        this.storage = new RocksDBSegmentedStorage(dbManager);

        // Initialize Besu's strategies with NoOpMetricsSystem from besu-metrics-core
        NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
        CodeHashCodeStorageStrategy codeStrategy = new CodeHashCodeStorageStrategy();

        this.fullStrategy = new BonsaiFullFlatDbStrategy(metricsSystem, codeStrategy);
        this.archiveStrategy = new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStrategy);
    }

    /**
     * Read current account data using Besu's FlatDbStrategy.
     * Uses BonsaiFullFlatDbStrategy for regular Bonsai, BonsaiArchiveFlatDbStrategy for archive.
     *
     * @param address The account address
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccount(Address address) {
        if (!isBonsaiDatabase()) {
            LOG.warn("readAccount only supported for Bonsai databases");
            return Optional.empty();
        }

        Hash accountHash = Hash.hash(address);
        boolean isArchive = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;

        try {
            Optional<Bytes> accountBytes = isArchive ?
                    archiveStrategy.getFlatAccount(
                            Optional::empty,  // worldStateRootHashSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            storage
                    ) :
                    fullStrategy.getFlatAccount(
                            Optional::empty,  // worldStateRootHashSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            storage
                    );

            if (accountBytes.isEmpty()) {
                return Optional.empty();
            }

            // Parse account value using Besu's BonsaiAccount.fromRLP()
            BonsaiAccount account = BonsaiAccount.fromRLP(
                    null,  // worldView - not needed for read-only
                    address,
                    accountBytes.get(),
                    false,  // mutable = false (read-only)
                    null   // codeCache - not needed for read-only
            );

            AccountData data = new AccountData();
            data.address = address;
            data.accountHash = accountHash;
            data.nonce = account.getNonce();
            data.balance = account.getBalance();
            data.storageRoot = account.getStorageRoot();
            data.codeHash = account.getCodeHash();
            data.blockNumber = null;  // Current state, no specific block

            return Optional.of(data);

        } catch (Exception e) {
            LOG.error("Failed to read account: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Read account by hash using Besu's FlatDbStrategy.
     *
     * @param accountHash The account hash
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountByHash(Hash accountHash) {
        if (!isBonsaiDatabase()) {
            LOG.warn("readAccountByHash only supported for Bonsai databases");
            return Optional.empty();
        }

        boolean isArchive = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;

        try {
            Optional<Bytes> accountBytes = isArchive ?
                    archiveStrategy.getFlatAccount(
                            Optional::empty,  // worldStateRootHashSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            storage
                    ) :
                    fullStrategy.getFlatAccount(
                            Optional::empty,  // worldStateRootHashSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            storage
                    );

            if (accountBytes.isEmpty()) {
                return Optional.empty();
            }

            // Parse account value using Besu's BonsaiAccount.fromRLP()
            // Note: We don't have the address, but BonsaiAccount.fromRLP requires it
            // For hash-only queries, we'll need to pass a placeholder or handle this differently
            BonsaiAccount account = BonsaiAccount.fromRLP(
                    null,  // worldView - not needed for read-only
                    Address.ZERO,  // placeholder since we don't have the actual address
                    accountBytes.get(),
                    false,  // mutable = false (read-only)
                    null   // codeCache - not needed for read-only
            );

            AccountData data = new AccountData();
            data.address = null;  // Not available from hash-only query
            data.accountHash = accountHash;
            data.nonce = account.getNonce();
            data.balance = account.getBalance();
            data.storageRoot = account.getStorageRoot();
            data.codeHash = account.getCodeHash();
            data.blockNumber = null;

            return Optional.of(data);

        } catch (Exception e) {
            LOG.error("Failed to read account by hash: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Read account data at a specific block number using Besu's archive strategy.
     *
     * @param address The account address
     * @param blockNumber The block number
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountAtBlock(Address address, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readAccountAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        Hash accountHash = Hash.hash(address);

        try {
            // Construct key with block number suffix: accountHash (32 bytes) + blockNumber (8 bytes)
            Bytes searchKey = Bytes.concatenate(
                    accountHash,
                    Bytes.ofUnsignedLong(blockNumber)
            );

            // Use getNearestBefore to find the account state at or before the specified block
            Optional<SegmentedKeyValueStorage.NearestKeyValue> result = storage.getNearestBefore(
                    KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
                    searchKey
            );

            if (result.isEmpty() || result.get().value().isEmpty()) {
                return Optional.empty();
            }

            Bytes returnedKey = result.get().key();
            byte[] accountBytes = result.get().value().get();

            // Parse account value using Besu's BonsaiAccount.fromRLP()
            BonsaiAccount account = BonsaiAccount.fromRLP(
                    null,  // worldView - not needed for read-only
                    address,
                    Bytes.wrap(accountBytes),
                    false,  // mutable = false (read-only)
                    null   // codeCache - not needed for read-only
            );

            // Extract actual block number from the returned key (last 8 bytes)
            Long actualBlockNumber = null;
            if (returnedKey.size() >= 40) {
                actualBlockNumber = returnedKey.slice(32, 8).toLong();
            }

            AccountData data = new AccountData();
            data.address = address;
            data.accountHash = accountHash;
            data.nonce = account.getNonce();
            data.balance = account.getBalance();
            data.storageRoot = account.getStorageRoot();
            data.codeHash = account.getCodeHash();
            data.blockNumber = actualBlockNumber;

            return Optional.of(data);

        } catch (Exception e) {
            LOG.error("Failed to read account from archive: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Read account data by hash at a specific block number using Besu's archive strategy.
     *
     * @param accountHash The account hash
     * @param blockNumber The block number
     * @return Optional containing account data if found
     */
    public Optional<AccountData> readAccountByHashAtBlock(Hash accountHash, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readAccountByHashAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        try {
            // Construct key with block number suffix: accountHash (32 bytes) + blockNumber (8 bytes)
            Bytes searchKey = Bytes.concatenate(
                    accountHash,
                    Bytes.ofUnsignedLong(blockNumber)
            );

            // Use getNearestBefore to find the account state at or before the specified block
            Optional<SegmentedKeyValueStorage.NearestKeyValue> result = storage.getNearestBefore(
                    KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
                    searchKey
            );

            if (result.isEmpty() || result.get().value().isEmpty()) {
                return Optional.empty();
            }

            Bytes returnedKey = result.get().key();
            byte[] accountBytes = result.get().value().get();

            // Parse account value using Besu's BonsaiAccount.fromRLP()
            // Note: We don't have the address, but BonsaiAccount.fromRLP requires it
            // For hash-only queries, we'll need to pass a placeholder or handle this differently
            BonsaiAccount account = BonsaiAccount.fromRLP(
                    null,  // worldView - not needed for read-only
                    Address.ZERO,  // placeholder since we don't have the actual address
                    Bytes.wrap(accountBytes),
                    false,  // mutable = false (read-only)
                    null   // codeCache - not needed for read-only
            );

            // Extract actual block number from the returned key (last 8 bytes)
            Long actualBlockNumber = null;
            if (returnedKey.size() >= 40) {
                actualBlockNumber = returnedKey.slice(32, 8).toLong();
            }

            AccountData data = new AccountData();
            data.address = null;  // Not available from hash-only query
            data.accountHash = accountHash;
            data.nonce = account.getNonce();
            data.balance = account.getBalance();
            data.storageRoot = account.getStorageRoot();
            data.codeHash = account.getCodeHash();
            data.blockNumber = actualBlockNumber;

            return Optional.of(data);

        } catch (Exception e) {
            LOG.error("Failed to read account by hash from archive: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Read current storage slot value using Besu's FlatDbStrategy.
     *
     * @param address The contract address
     * @param slot The storage slot
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorage(Address address, UInt256 slot) {
        if (!isBonsaiDatabase()) {
            LOG.warn("readStorage only supported for Bonsai databases");
            return Optional.empty();
        }

        Hash accountHash = Hash.hash(address);
        Hash slotHash = Hash.hash(slot);
        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.of(slot));
        boolean isArchive = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;

        try {
            Optional<Bytes> storageBytes = isArchive ?
                    archiveStrategy.getFlatStorageValueByStorageSlotKey(
                            Optional::empty,  // worldStateRootHashSupplier
                            Optional::empty,  // storageRootSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            slotKey,
                            storage
                    ) :
                    fullStrategy.getFlatStorageValueByStorageSlotKey(
                            Optional::empty,  // worldStateRootHashSupplier
                            Optional::empty,  // storageRootSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            slotKey,
                            storage
                    );

            if (storageBytes.isEmpty()) {
                return Optional.empty();
            }

            // Storage values are RLP-encoded and need to be decoded
            UInt256 value = UInt256.fromBytes(Bytes32.leftPad(RLP.decodeValue(storageBytes.get())));

            StorageData data = new StorageData();
            data.address = address;
            data.slot = slot;
            data.accountHash = accountHash;
            data.slotHash = slotHash;
            data.value = value;
            data.blockNumber = null;

            return Optional.of(data);

        } catch (Exception e) {
            LOG.debug("Failed to read storage: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Read current storage slot by hashes using Besu's FlatDbStrategy.
     *
     * @param accountHash The account hash
     * @param slotHash The slot hash
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorageByHash(Hash accountHash, Hash slotHash) {
        if (!isBonsaiDatabase()) {
            LOG.warn("readStorageByHash only supported for Bonsai databases");
            return Optional.empty();
        }

        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.empty());
        boolean isArchive = dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;

        try {
            Optional<Bytes> storageBytes = isArchive ?
                    archiveStrategy.getFlatStorageValueByStorageSlotKey(
                            Optional::empty,  // worldStateRootHashSupplier
                            Optional::empty,  // storageRootSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            slotKey,
                            storage
                    ) :
                    fullStrategy.getFlatStorageValueByStorageSlotKey(
                            Optional::empty,  // worldStateRootHashSupplier
                            Optional::empty,  // storageRootSupplier
                            null,  // nodeLoader (not needed for flat reads)
                            accountHash,
                            slotKey,
                            storage
                    );

            if (storageBytes.isEmpty()) {
                return Optional.empty();
            }

            // Storage values are RLP-encoded and need to be decoded
            UInt256 value = UInt256.fromBytes(Bytes32.leftPad(RLP.decodeValue(storageBytes.get())));

            StorageData data = new StorageData();
            data.address = null;  // Not available from hash-only query
            data.slot = null;  // Not available from hash-only query
            data.accountHash = accountHash;
            data.slotHash = slotHash;
            data.value = value;
            data.blockNumber = null;

            return Optional.of(data);

        } catch (Exception e) {
            LOG.debug("Failed to read storage by hash: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Read storage slot value at a specific block number using Besu's archive strategy.
     *
     * @param address The contract address
     * @param slot The storage slot
     * @param blockNumber The block number
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorageAtBlock(Address address, UInt256 slot, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readStorageAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        Hash accountHash = Hash.hash(address);
        Hash slotHash = Hash.hash(slot);
        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.of(slot));

        try {
            // Use Besu's archive strategy to read storage
            Optional<Bytes> storageBytes = archiveStrategy.getFlatStorageValueByStorageSlotKey(
                    Optional::empty,  // worldStateRootHashSupplier
                    Optional::empty,  // storageRootSupplier
                    null,  // nodeLoader (not needed for flat reads)
                    accountHash,
                    slotKey,
                    storage
            );

            if (storageBytes.isEmpty()) {
                return Optional.empty();
            }

            // Storage values are RLP-encoded and need to be decoded
            UInt256 value = UInt256.fromBytes(Bytes32.leftPad(RLP.decodeValue(storageBytes.get())));

            StorageData data = new StorageData();
            data.address = address;
            data.slot = slot;
            data.accountHash = accountHash;
            data.slotHash = slotHash;
            data.value = value;
            data.blockNumber = blockNumber;

            return Optional.of(data);

        } catch (Exception e) {
            LOG.debug("Failed to read storage from archive: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Read storage slot by raw hashes at a specific block number.
     *
     * @param accountHash The account hash
     * @param slotHash The slot hash
     * @param blockNumber The block number
     * @return Optional containing storage data if found
     */
    public Optional<StorageData> readStorageByHashAtBlock(Hash accountHash, Hash slotHash, long blockNumber) {
        if (dbManager.getFormat() != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
            LOG.warn("readStorageByHashAtBlock only supported for Bonsai Archive databases");
            return Optional.empty();
        }

        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.empty());

        try {
            // Use Besu's archive strategy to read storage
            Optional<Bytes> storageBytes = archiveStrategy.getFlatStorageValueByStorageSlotKey(
                    Optional::empty,  // worldStateRootHashSupplier
                    Optional::empty,  // storageRootSupplier
                    null,  // nodeLoader (not needed for flat reads)
                    accountHash,
                    slotKey,
                    storage
            );

            if (storageBytes.isEmpty()) {
                return Optional.empty();
            }

            // Storage values are RLP-encoded and need to be decoded
            UInt256 value = UInt256.fromBytes(Bytes32.leftPad(RLP.decodeValue(storageBytes.get())));

            StorageData data = new StorageData();
            data.address = null;  // Not available from hash-only query
            data.slot = null;  // Not available from hash-only query
            data.accountHash = accountHash;
            data.slotHash = slotHash;
            data.value = value;
            data.blockNumber = blockNumber;

            return Optional.of(data);

        } catch (Exception e) {
            LOG.debug("Failed to read storage by hash from archive: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Helper method to check if the database is a Bonsai database (regular or archive).
     */
    private boolean isBonsaiDatabase() {
        BesuDatabaseManager.DatabaseFormat format = dbManager.getFormat();
        return format == BesuDatabaseManager.DatabaseFormat.BONSAI ||
               format == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;
    }
}
