package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Adapter that wraps our RocksDB database to implement Besu's SegmentedKeyValueStorage interface.
 * This allows Besu's native strategy classes to work with our database.
 */
public class RocksDBSegmentedStorage implements SegmentedKeyValueStorage {

    private final BesuDatabaseManager dbManager;

    public RocksDBSegmentedStorage(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public Optional<byte[]> get(SegmentIdentifier segment, byte[] key) {
        try {
            ColumnFamilyHandle cfHandle = getColumnFamilyHandle(segment);
            if (cfHandle == null) {
                return Optional.empty();
            }

            byte[] value = dbManager.getDatabase().get(cfHandle, key);
            return Optional.ofNullable(value);
        } catch (RocksDBException e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<NearestKeyValue> getNearestAfter(SegmentIdentifier segment, Bytes key) {
        try {
            ColumnFamilyHandle cfHandle = getColumnFamilyHandle(segment);
            if (cfHandle == null) {
                return Optional.empty();
            }

            try (RocksIterator iterator = dbManager.getDatabase().newIterator(cfHandle)) {
                iterator.seek(key.toArrayUnsafe());

                if (!iterator.isValid()) {
                    return Optional.empty();
                }

                byte[] foundKey = iterator.key();
                byte[] foundValue = iterator.value();

                return Optional.of(new NearestKeyValue(Bytes.wrap(foundKey), Optional.of(foundValue)));
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public Optional<NearestKeyValue> getNearestBefore(SegmentIdentifier segment, Bytes key) {
        try {
            ColumnFamilyHandle cfHandle = getColumnFamilyHandle(segment);
            if (cfHandle == null) {
                return Optional.empty();
            }

            try (RocksIterator iterator = dbManager.getDatabase().newIterator(cfHandle)) {
                iterator.seekForPrev(key.toArrayUnsafe());

                if (!iterator.isValid()) {
                    return Optional.empty();
                }

                byte[] foundKey = iterator.key();
                byte[] foundValue = iterator.value();

                // Skip deleted entries (empty values)
                if (foundValue.length == 0) {
                    return Optional.empty();
                }

                return Optional.of(new NearestKeyValue(Bytes.wrap(foundKey), Optional.of(foundValue)));
            }
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public Stream<Pair<byte[], byte[]>> stream(SegmentIdentifier segment) {
        throw new UnsupportedOperationException("Stream not implemented for read-only access");
    }

    @Override
    public Stream<Pair<byte[], byte[]>> streamFromKey(SegmentIdentifier segment, byte[] startKey) {
        throw new UnsupportedOperationException("Stream not implemented for read-only access");
    }

    @Override
    public Stream<Pair<byte[], byte[]>> streamFromKey(
            SegmentIdentifier segment, byte[] startKey, byte[] endKey) {
        throw new UnsupportedOperationException("Stream not implemented for read-only access");
    }

    @Override
    public Stream<byte[]> streamKeys(SegmentIdentifier segment) {
        throw new UnsupportedOperationException("Stream not implemented for read-only access");
    }

    @Override
    public boolean containsKey(SegmentIdentifier segment, byte[] key) {
        return get(segment, key).isPresent();
    }

    @Override
    public Set<byte[]> getAllKeysThat(SegmentIdentifier segment, Predicate<byte[]> returnCondition) {
        throw new UnsupportedOperationException("getAllKeysThat not implemented for read-only access");
    }

    @Override
    public Set<byte[]> getAllValuesFromKeysThat(SegmentIdentifier segment, Predicate<byte[]> returnCondition) {
        throw new UnsupportedOperationException("getAllValuesFromKeysThat not implemented for read-only access");
    }

    @Override
    public SegmentedKeyValueStorageTransaction startTransaction() {
        throw new UnsupportedOperationException("Transactions not supported in read-only mode");
    }

    @Override
    public boolean tryDelete(SegmentIdentifier segment, byte[] key) {
        throw new UnsupportedOperationException("Delete not supported in read-only mode");
    }

    @Override
    public boolean isClosed() {
        return !dbManager.isOpen();
    }

    @Override
    public void clear(SegmentIdentifier segment) {
        throw new UnsupportedOperationException("Clear not supported in read-only mode");
    }

    @Override
    public void close() {
        // Database is managed by BesuDatabaseManager, don't close here
    }

    /**
     * Map Besu's SegmentIdentifier to our ColumnFamilyHandle.
     */
    private ColumnFamilyHandle getColumnFamilyHandle(SegmentIdentifier segment) {
        String segmentName = segment.getName();

        for (KeyValueSegmentIdentifier kvSegment : KeyValueSegmentIdentifier.values()) {
            if (kvSegment.getName().equals(segmentName)) {
                return dbManager.getColumnFamily(kvSegment);
            }
        }

        return null;
    }
}
