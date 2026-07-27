package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbUpgradeFlatDbCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbUpgradeFlatDbCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = mock(BesuDatabaseManager.class);
        command = new DbUpgradeFlatDbCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void refusesWhenDbClosed() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void refusesWhenNotBonsaiFormat() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.FOREST);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("only supported for Bonsai databases");
        assertThat(errorStream.toString()).contains("FOREST");
    }

    @Test
    void refusesWhenReadOnlyAndNotDryRun() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("read-only mode");
    }

    @Test
    void dryRunDoesNotRequireWritable() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{"--dry-run"});
        // Fails later (no real database behind the mock), but must get past the write-mode guard.
        // The specific "Reopen with 'db open <path> --write'" message should not appear
        assertThat(errorStream.toString()).doesNotContain("Reopen with");
    }

    @Test
    void includesExceptionMessageWhenFlatDbHealerConstructionFails() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(true);

        // Execute with a mock dbManager that doesn't have proper setup,
        // which will cause FlatDbHealer construction to fail
        command.execute(new String[]{});

        String errorOutput = errorStream.toString();
        // Verify that error message includes both the generic error and the exception message
        assertThat(errorOutput).contains("Failed to initialize database healer");
        // The actual exception message should be included (not just the generic message)
        assertThat(errorOutput).isNotEqualTo("Error: Failed to initialize database healer\n");
    }
}
