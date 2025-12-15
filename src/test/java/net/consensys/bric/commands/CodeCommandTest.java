package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CodeCommandTest {

    private CodeCommand command;
    private BesuDatabaseManager dbManager;

    @BeforeEach
    void setUp() {
        dbManager = new BesuDatabaseManager();
        command = new CodeCommand(dbManager);
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

        command.execute(new String[]{"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testDatabaseStillClosedAfterMissingArgument() {
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testValidAddressFormat() {
        // Valid address should not throw when parsed
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testHashQueryFormat() {
        // Hash query should not throw when parsed
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            "--hash"
        });

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testSaveFlag() {
        // Test that --save flag is handled without crashing
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{
            "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb",
            "--save",
            "/tmp/test-code.hex"
        });

        assertThat(dbManager.isOpen()).isFalse();
    }
}
