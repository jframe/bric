package net.consensys.bric.besu;

import net.consensys.bric.db.AccountData;
import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import net.consensys.bric.db.StorageData;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.bouncycastle.util.Arrays;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFullFlatDbStrategy;
import org.hyperledger.besu.ethereum.trie.pathbased.common.BonsaiContext;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.flat.CodeHashCodeStorageStrategy;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.DELETED_STORAGE_VALUE;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.calculateArchiveKeyWithMaxSuffix;
import static org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiArchiveFlatDbStrategy.calculateNaturalSlotKey;

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

        final NoOpMetricsSystem metricsSystem = new NoOpMetricsSystem();
        final CodeHashCodeStorageStrategy codeStrategy = new CodeHashCodeStorageStrategy();
        this.fullStrategy = new BonsaiFullFlatDbStrategy(metricsSystem, codeStrategy);
        this.archiveStrategy = new BonsaiArchiveFlatDbStrategy(metricsSystem, codeStrategy);
    }

    public Optional<AccountData> readAccount(Address address) {
        return readAccountInternal(Hash.hash(address), address);
    }

    public Optional<AccountData> readAccountByHash(Hash accountHash) {
        return readAccountInternal(accountHash, null);
    }

    public Optional<AccountData> readAccountAtBlock(Address address, long blockNumber) {
        return readAccountAtBlockInternal(Hash.hash(address), address, blockNumber);
    }

    public Optional<AccountData> readAccountByHashAtBlock(Hash accountHash, long blockNumber) {
        return readAccountAtBlockInternal(accountHash, null, blockNumber);
    }

    public Optional<StorageData> readStorage(Address address, UInt256 slot) {
        Hash accountHash = Hash.hash(address);
        Hash slotHash = Hash.hash(slot);
        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.of(slot));
        return readStorageInternal(accountHash, slotKey, address, slot);
    }

    public Optional<StorageData> readStorageByHash(Hash accountHash, Hash slotHash) {
        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.empty());
        return readStorageInternal(accountHash, slotKey, null, null);
    }

    public Optional<StorageData> readStorageAtBlock(Address address, UInt256 slot, long blockNumber) {
        if (!isArchiveDatabase()) {
            return Optional.empty();
        }

        Hash accountHash = Hash.hash(address);
        Hash slotHash = Hash.hash(slot);
        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.of(slot));

        try {
            Optional<Bytes> storageBytes = archiveStrategy.getFlatStorageValueByStorageSlotKey(
                    Optional::empty, Optional::empty, null, accountHash, slotKey, storage);

            if (storageBytes.isEmpty()) {
                return Optional.empty();
            }

            UInt256 value = UInt256.fromBytes(storageBytes.get());
            return Optional.of(createStorageData(accountHash, slotHash, value, address, slot, blockNumber));

        } catch (Exception e) {
            LOG.debug("Failed to read storage from archive: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public Optional<StorageData> readStorageByHashAtBlock(Hash accountHash, Hash slotHash, long blockNumber) {
        if (!isArchiveDatabase()) {
            return Optional.empty();
        }

        StorageSlotKey slotKey = new StorageSlotKey(slotHash, Optional.empty());

        try {
            byte[] naturalKey = calculateNaturalSlotKey(accountHash, slotKey.getSlotHash());
            Bytes keyNearest = calculateArchiveKeyWithMaxSuffix(
                    Optional.of(new BonsaiContext(blockNumber)), naturalKey);

            // Find the nearest storage with fallback
            Optional<SegmentedKeyValueStorage.NearestKeyValue> nearestKeyValue =
                    storage.getNearestBefore(ACCOUNT_STORAGE_STORAGE, keyNearest)
                            .filter(found -> Bytes.of(naturalKey).commonPrefixLength(found.key()) >= naturalKey.length);

            if (nearestKeyValue.isEmpty()) {
                nearestKeyValue = storage.getNearestBefore(ACCOUNT_STORAGE_ARCHIVE, keyNearest)
                        .filter(found -> Bytes.of(naturalKey).commonPrefixLength(found.key()) >= naturalKey.length);
            }

            if (nearestKeyValue.isEmpty()) {
                return Optional.empty();
            }

            Optional<Bytes> storageBytes = nearestKeyValue
                    .filter(found -> !Arrays.areEqual(DELETED_STORAGE_VALUE,
                            found.value().orElse(DELETED_STORAGE_VALUE)))
                    .flatMap(SegmentedKeyValueStorage.NearestKeyValue::wrapBytes);

            if (storageBytes.isEmpty()) {
                return Optional.empty();
            }

            UInt256 value = UInt256.fromBytes(storageBytes.get());
            return Optional.of(createStorageData(accountHash, slotHash, value, null, null, blockNumber));

        } catch (Exception e) {
            LOG.debug("Failed to read storage by hash from archive: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<AccountData> readAccountInternal(Hash accountHash, Address address) {
        if (!isBonsaiDatabase()) {
            LOG.warn("Account reading only supported for Bonsai databases");
            return Optional.empty();
        }

        try {
            Optional<Bytes> accountBytes = isArchive() ?
                    archiveStrategy.getFlatAccount(Optional::empty, null, accountHash, storage) :
                    fullStrategy.getFlatAccount(Optional::empty, null, accountHash, storage);

            if (accountBytes.isEmpty()) {
                return Optional.empty();
            }

            BonsaiAccount account = parseBonsaiAccount(accountBytes.get(), address);
            return Optional.of(createAccountData(account, accountHash, address, null));

        } catch (Exception e) {
            LOG.error("Failed to read account: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    private Optional<AccountData> readAccountAtBlockInternal(
            Hash accountHash, Address address, long blockNumber) {

        if (!isArchiveDatabase()) {
            return Optional.empty();
        }

        try {
            Bytes searchKey = Bytes.concatenate(accountHash, Bytes.ofUnsignedLong(blockNumber));

            // Try ACCOUNT_INFO_STATE first, then fallback to ACCOUNT_INFO_STATE_ARCHIVE
            Optional<SegmentedKeyValueStorage.NearestKeyValue> result =
                    findNearestAccountKey(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, searchKey, accountHash);

            if (result.isEmpty()) {
                result = findNearestAccountKey(
                        KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE, searchKey, accountHash);
            }

            if (result.isEmpty() || result.get().value().isEmpty()) {
                return Optional.empty();
            }

            Bytes returnedKey = result.get().key();
            byte[] accountBytes = result.get().value().get();

            BonsaiAccount account = parseBonsaiAccount(Bytes.wrap(accountBytes), address);
            Long actualBlockNumber = extractBlockNumber(returnedKey);

            return Optional.of(createAccountData(account, accountHash, address, actualBlockNumber));

        } catch (Exception e) {
            LOG.error("Failed to read account from archive: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }

    private Optional<StorageData> readStorageInternal(
            Hash accountHash, StorageSlotKey slotKey, Address address, UInt256 slot) {

        if (!isBonsaiDatabase()) {
            LOG.warn("Storage reading only supported for Bonsai databases");
            return Optional.empty();
        }

        try {
            Optional<Bytes> storageBytes = isArchive() ?
                    archiveStrategy.getFlatStorageValueByStorageSlotKey(
                            Optional::empty, Optional::empty, null, accountHash, slotKey, storage) :
                    fullStrategy.getFlatStorageValueByStorageSlotKey(
                            Optional::empty, Optional::empty, null, accountHash, slotKey, storage);

            if (storageBytes.isEmpty()) {
                return Optional.empty();
            }

            UInt256 value = UInt256.fromBytes(storageBytes.get());
            return Optional.of(createStorageData(
                    accountHash, slotKey.getSlotHash(), value, address, slot, null));

        } catch (Exception e) {
            LOG.debug("Failed to read storage: {}", e.getMessage());
            return Optional.empty();
        }
    }

    private Optional<SegmentedKeyValueStorage.NearestKeyValue> findNearestAccountKey(
            KeyValueSegmentIdentifier segment, Bytes searchKey, Hash accountHash) {

        return storage.getNearestBefore(segment, searchKey)
                .filter(found -> accountHash.commonPrefixLength(found.key()) >= accountHash.size());
    }

    private BonsaiAccount parseBonsaiAccount(Bytes accountBytes, Address address) {
        Address addressToUse = address != null ? address : Address.ZERO;
        return BonsaiAccount.fromRLP(null, addressToUse, accountBytes, false, null);
    }

    private AccountData createAccountData(
            BonsaiAccount account, Hash accountHash, Address address, Long blockNumber) {
        AccountData data = new AccountData();
        data.address = address;
        data.accountHash = accountHash;
        data.nonce = account.getNonce();
        data.balance = account.getBalance();
        data.storageRoot = account.getStorageRoot();
        data.codeHash = account.getCodeHash();
        data.blockNumber = blockNumber;
        return data;
    }

    private StorageData createStorageData(
            Hash accountHash, Hash slotHash, UInt256 value,
            Address address, UInt256 slot, Long blockNumber) {
        StorageData data = new StorageData();
        data.address = address;
        data.slot = slot;
        data.accountHash = accountHash;
        data.slotHash = slotHash;
        data.value = value;
        data.blockNumber = blockNumber;
        return data;
    }

    private Long extractBlockNumber(Bytes key) {
        if (key.size() >= 40) {
            return key.slice(32, 8).toLong();
        }
        return null;
    }

    private boolean isArchive() {
        return dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;
    }

    private boolean isBonsaiDatabase() {
        BesuDatabaseManager.DatabaseFormat format = dbManager.getFormat();
        return format == BesuDatabaseManager.DatabaseFormat.BONSAI ||
               format == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;
    }

    private boolean isArchiveDatabase() {
        if (!isArchive()) {
            LOG.warn("Operation only supported for Bonsai Archive databases. Current format: {}",
                    dbManager.getFormat());
            return false;
        }
        return true;
    }
}
