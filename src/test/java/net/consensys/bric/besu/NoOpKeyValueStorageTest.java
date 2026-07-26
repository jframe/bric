package net.consensys.bric.besu;

import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NoOpKeyValueStorageTest {

    @Test
    void allReadsReturnEmptyOrFalse() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();

        assertThat(storage.containsKey("key".getBytes())).isFalse();
        assertThat(storage.get("key".getBytes())).isEmpty();
        assertThat(storage.stream()).isEmpty();
        assertThat(storage.streamFromKey("key".getBytes())).isEmpty();
        assertThat(storage.streamFromKey("a".getBytes(), "z".getBytes())).isEmpty();
        assertThat(storage.streamKeys()).isEmpty();
        assertThat(storage.getAllKeysThat(k -> true)).isEmpty();
        assertThat(storage.getAllValuesFromKeysThat(k -> true)).isEmpty();
        assertThat(storage.isClosed()).isFalse();
    }

    @Test
    void clearAndCloseDoNotThrow() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();
        storage.clear();
        storage.close();
    }

    @Test
    void tryDeleteReturnsTrue() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();
        assertThat(storage.tryDelete("key".getBytes())).isTrue();
    }

    @Test
    void transactionOperationsAreAllNoOps() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();
        KeyValueStorageTransaction transaction = storage.startTransaction();

        transaction.put("key".getBytes(), "value".getBytes());
        transaction.remove("key".getBytes());
        transaction.commit();
        transaction.rollback();
        transaction.close();
        // No exception means the no-op contract held.
    }
}
