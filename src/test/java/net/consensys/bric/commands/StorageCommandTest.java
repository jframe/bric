package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StorageCommandTest {

    private StorageCommand command;
    private BesuDatabaseManager dbManager;

    @BeforeEach
    void setUp() {
        dbManager = new BesuDatabaseManager();
        command = new StorageCommand(dbManager);
    }

    @AfterEach
    void tearDown() {
        if (dbManager.isOpen()) {
            dbManager.closeDatabase();
        }
    }

    @Test
    void testDatabaseStillClosedAfterCommandWithNoDb() {
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb", "0"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testDatabaseStillClosedAfterMissingArguments() {
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testDatabaseStillClosedAfterMissingSlot() {
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testValidDecimalSlot() {
        // Valid decimal slot should not throw when parsed
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb", "0"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testValidHexSlot() {
        // Valid hex slot should not throw when parsed
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb", "0x1234"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testRawHashQuery() {
        // Raw hash query should not throw when parsed
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "0xabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcdabcd",
            "--raw"
        });

        assertThat(dbManager.isOpen()).isFalse();
    }
}
