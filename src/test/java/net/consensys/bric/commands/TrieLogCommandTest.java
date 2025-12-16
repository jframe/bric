package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TrieLogCommandTest {

    private TrieLogCommand command;
    private BesuDatabaseManager dbManager;

    @BeforeEach
    void setUp() {
        dbManager = new BesuDatabaseManager();
        command = new TrieLogCommand(dbManager);
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

        command.execute(new String[]{
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        });

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testDatabaseStillClosedAfterMissingArgument() {
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testValidBlockHashFormat() {
        // Valid block hash should not throw when parsed
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        });

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testValidBlockNumberFormat() {
        // Valid block number should not throw when parsed
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"12345"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testBlockNumberZero() {
        // Block number 0 should be valid
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"0"});

        assertThat(dbManager.isOpen()).isFalse();
    }

    @Test
    void testLargeBlockNumber() {
        // Large block numbers should be valid
        assertThat(dbManager.isOpen()).isFalse();

        command.execute(new String[]{"999999999"});

        assertThat(dbManager.isOpen()).isFalse();
    }
}
