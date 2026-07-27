package net.consensys.bric.db;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the operation/cancellation coordination the JVM shutdown hook uses to avoid
 * freeing RocksDB native handles out from under an in-progress operation (which segfaults the JVM).
 */
class BesuDatabaseManagerOperationTest {

    @Test
    void requestCancellationAndAwait_returnsTrueWhenOperationEndsBeforeTimeout() throws Exception {
        BesuDatabaseManager manager = new BesuDatabaseManager();
        manager.beginOperation();

        // Another thread finishes the operation shortly after cancellation is requested.
        Thread worker = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            manager.endOperation();
        });
        worker.start();

        boolean quiesced = manager.requestCancellationAndAwait(2000);

        worker.join();
        assertThat(quiesced).isTrue();
        assertThat(manager.isCancellationRequested()).isTrue();
        assertThat(manager.isOperationInProgress()).isFalse();
    }

    @Test
    void requestCancellationAndAwait_returnsFalseWhenOperationStaysInProgress() {
        BesuDatabaseManager manager = new BesuDatabaseManager();
        manager.beginOperation();

        boolean quiesced = manager.requestCancellationAndAwait(100);

        // Never ended, so the wait times out and the caller learns it must NOT close the DB.
        assertThat(quiesced).isFalse();
        assertThat(manager.isOperationInProgress()).isTrue();
    }

    @Test
    void beginOperation_clearsAnyPriorCancellationRequest() {
        BesuDatabaseManager manager = new BesuDatabaseManager();
        manager.requestCancellation();

        manager.beginOperation();

        assertThat(manager.isCancellationRequested()).isFalse();
        assertThat(manager.isOperationInProgress()).isTrue();
    }
}
