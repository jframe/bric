package net.consensys.bric.besu;

import org.apache.commons.lang3.tuple.Pair;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Stand-in for Besu's trie-log KeyValueStorage. Flat DB healing never reads or writes
 * trie logs, so every method either no-ops or reports empty/absent.
 */
public class NoOpKeyValueStorage implements KeyValueStorage {

    @Override
    public void clear() {
    }

    @Override
    public boolean containsKey(byte[] key) {
        return false;
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return Optional.empty();
    }

    @Override
    public Stream<Pair<byte[], byte[]>> stream() {
        return Stream.empty();
    }

    @Override
    public Stream<Pair<byte[], byte[]>> streamFromKey(byte[] startKey) {
        return Stream.empty();
    }

    @Override
    public Stream<Pair<byte[], byte[]>> streamFromKey(byte[] startKey, byte[] endKey) {
        return Stream.empty();
    }

    @Override
    public Stream<byte[]> streamKeys() {
        return Stream.empty();
    }

    @Override
    public boolean tryDelete(byte[] key) {
        return true;
    }

    @Override
    public Set<byte[]> getAllKeysThat(Predicate<byte[]> returnCondition) {
        return Set.of();
    }

    @Override
    public Set<byte[]> getAllValuesFromKeysThat(Predicate<byte[]> returnCondition) {
        return Set.of();
    }

    @Override
    public KeyValueStorageTransaction startTransaction() {
        return new NoOpTransaction();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }

    private static final class NoOpTransaction implements KeyValueStorageTransaction {
        @Override
        public void put(byte[] key, byte[] value) {
        }

        @Override
        public void remove(byte[] key) {
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public void close() {
        }
    }
}
