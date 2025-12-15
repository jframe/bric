package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DbCloseCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbCloseCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbCloseCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testExecuteWhenNoDatabaseOpen() {
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{});

        String output = outputStream.toString();
        assertThat(output).contains("No database is currently open");
    }

    @Test
    void testExecuteSuccess() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getCurrentPath()).thenReturn("/path/to/db");

        command.execute(new String[]{});

        verify(mockDbManager).closeDatabase();
        String output = outputStream.toString();
        assertThat(output).contains("Closed database");
        assertThat(output).contains("/path/to/db");
    }

    @Test
    void testExecuteFailure() {
        when(mockDbManager.isOpen()).thenReturn(true);
        doThrow(new RuntimeException("Close failed"))
            .when(mockDbManager).closeDatabase();

        command.execute(new String[]{});

        String error = errorStream.toString();
        assertThat(error).contains("Error closing database");
        assertThat(error).contains("Close failed");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("close");
        assertThat(command.getHelp()).containsIgnoringCase("database");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db-close");
    }
}
