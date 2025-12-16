package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseManager.DatabaseFormat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DbOpenCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbOpenCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbOpenCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testExecuteWithMissingPath() {
        command.execute(new String[]{});

        String error = errorStream.toString();
        assertThat(error).contains("Error: Missing database path");
        assertThat(error).contains("Usage:");
    }

    @Test
    void testExecuteSuccess() throws Exception {
        when(mockDbManager.getFormat()).thenReturn(DatabaseFormat.BONSAI);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("cf1", "cf2"));

        command.execute(new String[]{"/path/to/db"});

        verify(mockDbManager).openDatabase("/path/to/db");
        String output = outputStream.toString();
        assertThat(output).contains("Successfully opened database");
        assertThat(output).contains("/path/to/db");
        assertThat(output).contains("BONSAI");
        assertThat(output).contains("Column families: 2");
    }

    @Test
    void testExecuteFailure() throws Exception {
        doThrow(new RuntimeException("Database not found"))
            .when(mockDbManager).openDatabase(anyString());

        command.execute(new String[]{"/invalid/path"});

        String error = errorStream.toString();
        assertThat(error).contains("Error opening database");
        assertThat(error).contains("Database not found");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("open");
        assertThat(command.getHelp()).containsIgnoringCase("database");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db open");
        assertThat(usage).contains("<path>");
        assertThat(usage).contains("Examples:");
    }
}
