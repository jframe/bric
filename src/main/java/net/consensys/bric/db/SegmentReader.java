package net.consensys.bric.db;

import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Low-level RocksDB segment reader.
 * Provides methods for reading from specific column families.
 */
public class SegmentReader {

    private static final Logger LOG = LoggerFactory.getLogger(SegmentReader.class);
    private final BesuDatabaseManager dbManager;

    public SegmentReader(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    /**
     * Read a value from a segment by key.
     */
    public Optional<byte[]> get(KeyValueSegmentIdentifier segment, byte[] key) {
        try {
            ColumnFamilyHandle cfHandle = dbManager.getColumnFamily(segment);
            if (cfHandle == null) {
                LOG.warn("Column family not found: {}", segment.getName());
                return Optional.empty();
            }

            byte[] value = dbManager.getDatabase().get(cfHandle, key);
            return Optional.ofNullable(value);

        } catch (RocksDBException e) {
            LOG.error("Error reading from segment {}: {}", segment.getName(), e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Read a value using Hash as key.
     */
    public Optional<byte[]> get(KeyValueSegmentIdentifier segment, Hash key) {
        return get(segment, key.toArrayUnsafe());
    }

    /**
     * Compute account hash from address (Keccak256).
     */
    public Hash computeAccountHash(Address address) {
        return address.addressHash();
    }

    /**
     * Compute storage key by concatenating account hash and slot hash.
     */
    public byte[] computeStorageKey(Hash accountHash, Hash slotHash) {
        return Bytes.concatenate(
            Bytes.wrap(accountHash.toArrayUnsafe()),
            Bytes.wrap(slotHash.toArrayUnsafe())
        ).toArrayUnsafe();
    }

    /**
     * Iterate over all keys in a segment.
     */
    public void iterate(KeyValueSegmentIdentifier segment, Consumer<byte[]> keyConsumer) {
        ColumnFamilyHandle cfHandle = dbManager.getColumnFamily(segment);
        if (cfHandle == null) {
            LOG.warn("Column family not found: {}", segment.getName());
            return;
        }

        try (RocksIterator iterator = dbManager.getDatabase().newIterator(cfHandle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                keyConsumer.accept(iterator.key());
                iterator.next();
            }
        }
    }

    /**
     * Iterate over keys and values in a segment.
     */
    public void iterateKeyValue(
        KeyValueSegmentIdentifier segment,
        KeyValueConsumer consumer) {

        ColumnFamilyHandle cfHandle = dbManager.getColumnFamily(segment);
        if (cfHandle == null) {
            LOG.warn("Column family not found: {}", segment.getName());
            return;
        }

        try (RocksIterator iterator = dbManager.getDatabase().newIterator(cfHandle)) {
            iterator.seekToFirst();
            while (iterator.isValid()) {
                consumer.accept(iterator.key(), iterator.value());
                iterator.next();
            }
        }
    }

    /**
     * Iterate with pagination support.
     */
    public void iterateWithLimit(
        KeyValueSegmentIdentifier segment,
        int offset,
        int limit,
        Consumer<byte[]> keyConsumer) {

        ColumnFamilyHandle cfHandle = dbManager.getColumnFamily(segment);
        if (cfHandle == null) {
            LOG.warn("Column family not found: {}", segment.getName());
            return;
        }

        try (RocksIterator iterator = dbManager.getDatabase().newIterator(cfHandle)) {
            iterator.seekToFirst();

            // Skip offset
            for (int i = 0; i < offset && iterator.isValid(); i++) {
                iterator.next();
            }

            // Read limit
            int count = 0;
            while (iterator.isValid() && count < limit) {
                keyConsumer.accept(iterator.key());
                iterator.next();
                count++;
            }
        }
    }

    /**
     * Count keys in a segment (estimated).
     */
    public long countEstimated(KeyValueSegmentIdentifier segment) {
        try {
            ColumnFamilyHandle cfHandle = dbManager.getColumnFamily(segment);
            if (cfHandle == null) {
                return 0;
            }

            String prop = dbManager.getDatabase().getProperty(
                cfHandle, "rocksdb.estimate-num-keys");
            return Long.parseLong(prop);

        } catch (Exception e) {
            LOG.error("Error counting keys in segment {}: {}", segment.getName(), e.getMessage());
            return 0;
        }
    }

    @FunctionalInterface
    public interface KeyValueConsumer {
        void accept(byte[] key, byte[] value);
    }
}
