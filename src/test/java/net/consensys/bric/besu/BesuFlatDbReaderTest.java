package net.consensys.bric.besu;

import net.consensys.bric.db.AccountData;
import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import net.consensys.bric.db.StorageData;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for BesuFlatDbReader using an in-memory RocksDB database.
 * These tests verify that the reader correctly reads account and storage data
 * using Besu's native FlatDbStrategy classes.
 */
class BesuFlatDbReaderTest {

    @TempDir
    Path tempDir;

    private BesuDatabaseManager dbManager;
    private BesuFlatDbReader reader;
    private RocksDB rocksDB;
    private List<ColumnFamilyHandle> columnFamilyHandles;

    // Test data
    private static final Address TEST_ADDRESS = Address.fromHexString("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb");
    private static final Hash TEST_ACCOUNT_HASH = Hash.hash(TEST_ADDRESS);
    private static final UInt256 TEST_SLOT = UInt256.valueOf(0);
    private static final Hash TEST_SLOT_HASH = Hash.hash(TEST_SLOT);
    private static final long TEST_BLOCK_NUMBER = 12345L;
    private static final long TEST_NONCE = 1L;
    private static final Wei TEST_BALANCE = Wei.of(1000000000000000000L); // 1 ETH
    private static final UInt256 TEST_STORAGE_VALUE = UInt256.valueOf(42);

    // Generate RLP-encoded test data using BonsaiAccount's actual serialization
    private static byte[] generateAccountRLP() {
        // Create a BonsaiAccount and use its serializeAccount() method
        // This ensures we're testing the exact encoding/decoding that Besu uses
        BonsaiAccount account = new BonsaiAccount(
                null,  // worldView - not needed for serialization
                TEST_ADDRESS,
                TEST_ACCOUNT_HASH,
                TEST_NONCE,
                TEST_BALANCE,
                Hash.EMPTY,  // storageRoot
                Hash.EMPTY,  // codeHash
                false,       // mutable = false
                null         // codeCache - not needed
        );
        return account.serializeAccount().toArrayUnsafe();
    }

    private static byte[] generateStorageRLP() {
        return TEST_STORAGE_VALUE.toArrayUnsafe();
    }

    @BeforeEach
    void setUp() throws Exception {
        RocksDB.loadLibrary();
        dbManager = new BesuDatabaseManager();
        columnFamilyHandles = new ArrayList<>();
    }

    @AfterEach
    void tearDown() {
        if (dbManager.isOpen()) {
            dbManager.closeDatabase();
        }
        if (rocksDB != null) {
            for (ColumnFamilyHandle handle : columnFamilyHandles) {
                handle.close();
            }
            rocksDB.close();
        }
    }

    private void setupBonsaiDatabase() throws Exception {
        // Create column families for Bonsai database
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.CODE_STORAGE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()));

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        rocksDB = RocksDB.open(dbOptions, tempDir.toString(), columnFamilyDescriptors, columnFamilyHandles);

        // Write test account data using Besu-encoded RLP
        ColumnFamilyHandle accountCf = columnFamilyHandles.get(1); // ACCOUNT_INFO_STATE
        rocksDB.put(accountCf, TEST_ACCOUNT_HASH.toArrayUnsafe(), generateAccountRLP());

        // Write test storage data using Besu-encoded RLP
        ColumnFamilyHandle storageCf = columnFamilyHandles.get(2); // ACCOUNT_STORAGE_STORAGE
        byte[] storageKey = Bytes.concatenate(
                Bytes.wrap(TEST_ACCOUNT_HASH.toArrayUnsafe()),
                Bytes.wrap(TEST_SLOT_HASH.toArrayUnsafe())
        ).toArrayUnsafe();
        rocksDB.put(storageCf, storageKey, generateStorageRLP());

        // Close the manually opened database before opening with BesuDatabaseManager
        for (ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
        }
        columnFamilyHandles.clear();
        rocksDB.close();
        rocksDB = null;

        // Open database with BesuDatabaseManager
        dbManager.openDatabase(tempDir.toString());
    }

    private void setupBonsaiArchiveDatabase() throws Exception {
        // Create column families for Bonsai Archive database
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.ACCOUNT_STORAGE_ARCHIVE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.CODE_STORAGE.getId()));
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(
                KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()));

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        rocksDB = RocksDB.open(dbOptions, tempDir.toString(), columnFamilyDescriptors, columnFamilyHandles);

        // Write test account data with block number suffix using Besu-encoded RLP
        // Updated indices: 0=default, 1=ACCOUNT_INFO_STATE, 2=ACCOUNT_INFO_STATE_ARCHIVE, etc.
        ColumnFamilyHandle accountCf = columnFamilyHandles.get(1); // ACCOUNT_INFO_STATE
        byte[] accountKey = Bytes.concatenate(
                Bytes.wrap(TEST_ACCOUNT_HASH.toArrayUnsafe()),
                Bytes.ofUnsignedLong(TEST_BLOCK_NUMBER)
        ).toArrayUnsafe();
        rocksDB.put(accountCf, accountKey, generateAccountRLP());

        // Write test storage data with block number suffix using Besu-encoded RLP
        ColumnFamilyHandle storageCf = columnFamilyHandles.get(3); // ACCOUNT_STORAGE_STORAGE
        byte[] storageKey = Bytes.concatenate(
                Bytes.wrap(TEST_ACCOUNT_HASH.toArrayUnsafe()),
                Bytes.wrap(TEST_SLOT_HASH.toArrayUnsafe()),
                Bytes.ofUnsignedLong(TEST_BLOCK_NUMBER)
        ).toArrayUnsafe();
        rocksDB.put(storageCf, storageKey, generateStorageRLP());

        // Close the manually opened database before opening with BesuDatabaseManager
        for (ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
        }
        columnFamilyHandles.clear();
        rocksDB.close();
        rocksDB = null;

        // Open database with BesuDatabaseManager
        dbManager.openDatabase(tempDir.toString());
    }

    // ========== Bonsai Database Tests ==========

    @Test
    void testReadAccount_Bonsai_Success() throws Exception {
        setupBonsaiDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute: Read account that was written using BonsaiAccount.serializeAccount()
        // This tests the full round-trip: BonsaiAccount encoding -> DB -> BesuFlatDbReader decoding
        Optional<AccountData> result = reader.readAccount(TEST_ADDRESS);

        // Verify: All fields match the original account values
        assertThat(result).isPresent();
        AccountData account = result.get();
        assertThat(account.address).isEqualTo(TEST_ADDRESS);
        assertThat(account.accountHash).isEqualTo(TEST_ACCOUNT_HASH);
        assertThat(account.nonce).isEqualTo(TEST_NONCE);
        assertThat(account.balance).isEqualTo(TEST_BALANCE);
        assertThat(account.storageRoot).isEqualTo(Hash.EMPTY);
        assertThat(account.codeHash).isEqualTo(Hash.EMPTY);
        assertThat(account.blockNumber).isNull();
    }

    @Test
    void testReadAccountByHash_Bonsai_Success() throws Exception {
        setupBonsaiDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<AccountData> result = reader.readAccountByHash(TEST_ACCOUNT_HASH);

        // Verify
        assertThat(result).isPresent();
        AccountData account = result.get();
        assertThat(account.address).isNull(); // Not available from hash-only query
        assertThat(account.accountHash).isEqualTo(TEST_ACCOUNT_HASH);
        assertThat(account.nonce).isEqualTo(TEST_NONCE);
        assertThat(account.balance).isEqualTo(TEST_BALANCE);
    }

    @Test
    void testReadAccount_Bonsai_NotFound() throws Exception {
        setupBonsaiDatabase();
        reader = new BesuFlatDbReader(dbManager);

        Address nonExistentAddress = Address.fromHexString("0x0000000000000000000000000000000000000001");

        // Execute
        Optional<AccountData> result = reader.readAccount(nonExistentAddress);

        // Verify
        assertThat(result).isEmpty();
    }

    @Test
    void testReadStorage_Bonsai_Success() throws Exception {
        setupBonsaiDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<StorageData> result = reader.readStorage(TEST_ADDRESS, TEST_SLOT);

        // Verify
        assertThat(result).isPresent();
        StorageData storage = result.get();
        assertThat(storage.address).isEqualTo(TEST_ADDRESS);
        assertThat(storage.slot).isEqualTo(TEST_SLOT);
        assertThat(storage.accountHash).isEqualTo(TEST_ACCOUNT_HASH);
        assertThat(storage.slotHash).isEqualTo(TEST_SLOT_HASH);
        assertThat(storage.value).isEqualTo(TEST_STORAGE_VALUE);
        assertThat(storage.blockNumber).isNull();
    }

    @Test
    void testReadStorageByHash_Bonsai_Success() throws Exception {
        setupBonsaiDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<StorageData> result = reader.readStorageByHash(TEST_ACCOUNT_HASH, TEST_SLOT_HASH);

        // Verify
        assertThat(result).isPresent();
        StorageData storage = result.get();
        assertThat(storage.address).isNull(); // Not available from hash-only query
        assertThat(storage.slot).isNull(); // Not available from hash-only query
        assertThat(storage.accountHash).isEqualTo(TEST_ACCOUNT_HASH);
        assertThat(storage.slotHash).isEqualTo(TEST_SLOT_HASH);
        assertThat(storage.value).isEqualTo(TEST_STORAGE_VALUE);
    }

    @Test
    void testReadStorage_Bonsai_NotFound() throws Exception {
        setupBonsaiDatabase();
        reader = new BesuFlatDbReader(dbManager);

        UInt256 nonExistentSlot = UInt256.valueOf(999);

        // Execute
        Optional<StorageData> result = reader.readStorage(TEST_ADDRESS, nonExistentSlot);

        // Verify
        assertThat(result).isEmpty();
    }

    // ========== Bonsai Archive Database Tests ==========

    @Test
    void testReadAccount_BonsaiArchive_Success() throws Exception {
        setupBonsaiArchiveDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<AccountData> result = reader.readAccount(TEST_ADDRESS);

        // Verify
        assertThat(result).isPresent();
        AccountData account = result.get();
        assertThat(account.address).isEqualTo(TEST_ADDRESS);
        assertThat(account.nonce).isEqualTo(TEST_NONCE);
        assertThat(account.balance).isEqualTo(TEST_BALANCE);
    }

    @Test
    void testReadAccountAtBlock_BonsaiArchive_Success() throws Exception {
        setupBonsaiArchiveDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<AccountData> result = reader.readAccountAtBlock(TEST_ADDRESS, TEST_BLOCK_NUMBER);

        // Verify
        assertThat(result).isPresent();
        AccountData account = result.get();
        assertThat(account.address).isEqualTo(TEST_ADDRESS);
        assertThat(account.nonce).isEqualTo(1L);
        assertThat(account.blockNumber).isEqualTo(TEST_BLOCK_NUMBER);
    }

    @Test
    void testReadAccountByHashAtBlock_BonsaiArchive_Success() throws Exception {
        setupBonsaiArchiveDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<AccountData> result = reader.readAccountByHashAtBlock(TEST_ACCOUNT_HASH, TEST_BLOCK_NUMBER);

        // Verify
        assertThat(result).isPresent();
        AccountData account = result.get();
        assertThat(account.address).isNull(); // Not available from hash-only query
        assertThat(account.accountHash).isEqualTo(TEST_ACCOUNT_HASH);
        assertThat(account.blockNumber).isEqualTo(TEST_BLOCK_NUMBER);
    }

    @Test
    void testReadStorage_BonsaiArchive_Success() throws Exception {
        setupBonsaiArchiveDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<StorageData> result = reader.readStorage(TEST_ADDRESS, TEST_SLOT);

        // Verify
        assertThat(result).isPresent();
        assertThat(result.get().value).isEqualTo(TEST_STORAGE_VALUE);
    }

    @Test
    void testReadStorageAtBlock_BonsaiArchive_Success() throws Exception {
        setupBonsaiArchiveDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<StorageData> result = reader.readStorageAtBlock(TEST_ADDRESS, TEST_SLOT, TEST_BLOCK_NUMBER);

        // Verify
        assertThat(result).isPresent();
        StorageData storage = result.get();
        assertThat(storage.address).isEqualTo(TEST_ADDRESS);
        assertThat(storage.value).isEqualTo(TEST_STORAGE_VALUE);
        assertThat(storage.blockNumber).isEqualTo(TEST_BLOCK_NUMBER);
    }

    @Test
    void testReadStorageByHashAtBlock_BonsaiArchive_Success() throws Exception {
        setupBonsaiArchiveDatabase();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<StorageData> result = reader.readStorageByHashAtBlock(
                TEST_ACCOUNT_HASH, TEST_SLOT_HASH, TEST_BLOCK_NUMBER);

        // Verify
        assertThat(result).isPresent();
        StorageData storage = result.get();
        assertThat(storage.accountHash).isEqualTo(TEST_ACCOUNT_HASH);
        assertThat(storage.slotHash).isEqualTo(TEST_SLOT_HASH);
        assertThat(storage.value).isEqualTo(TEST_STORAGE_VALUE);
        assertThat(storage.blockNumber).isEqualTo(TEST_BLOCK_NUMBER);
    }

    // ========== Error Handling Tests ==========

    @Test
    void testReadAccount_NoDatabaseOpen_ReturnsEmpty() {
        assertThat(dbManager.isOpen()).isFalse();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<AccountData> result = reader.readAccount(TEST_ADDRESS);

        // Verify
        assertThat(result).isEmpty();
    }

    @Test
    void testReadStorage_NoDatabaseOpen_ReturnsEmpty() {
        assertThat(dbManager.isOpen()).isFalse();
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<StorageData> result = reader.readStorage(TEST_ADDRESS, TEST_SLOT);

        // Verify
        assertThat(result).isEmpty();
    }

    @Test
    void testReadAccountAtBlock_NonArchiveDatabase_ReturnsEmpty() throws Exception {
        setupBonsaiDatabase(); // Regular Bonsai, not archive
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<AccountData> result = reader.readAccountAtBlock(TEST_ADDRESS, TEST_BLOCK_NUMBER);

        // Verify: Should return empty for non-archive databases
        assertThat(result).isEmpty();
    }

    @Test
    void testReadStorageAtBlock_NonArchiveDatabase_ReturnsEmpty() throws Exception {
        setupBonsaiDatabase(); // Regular Bonsai, not archive
        reader = new BesuFlatDbReader(dbManager);

        // Execute
        Optional<StorageData> result = reader.readStorageAtBlock(TEST_ADDRESS, TEST_SLOT, TEST_BLOCK_NUMBER);

        // Verify: Should return empty for non-archive databases
        assertThat(result).isEmpty();
    }

    @Test
    void testReaderInstantiation_DoesNotThrow() {
        // Execute & Verify: Should not throw exception during construction
        reader = new BesuFlatDbReader(dbManager);
        assertThat(reader).isNotNull();
    }
}
