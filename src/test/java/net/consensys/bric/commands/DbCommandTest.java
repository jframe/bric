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

class DbCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testExecuteWithNoArgs() {
        command.execute(new String[]{});

        String error = errorStream.toString();
        assertThat(error).contains("Error: Missing subcommand");
        assertThat(error).contains("Usage:");
    }

    @Test
    void testExecuteWithInvalidSubcommand() {
        command.execute(new String[]{"invalid"});

        String error = errorStream.toString();
        assertThat(error).contains("Error: Unknown subcommand 'invalid'");
        assertThat(error).contains("Usage:");
    }

    @Test
    void testExecuteOpenSubcommand() throws Exception {
        when(mockDbManager.getFormat()).thenReturn(DatabaseFormat.BONSAI);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("cf1", "cf2"));

        command.execute(new String[]{"open", "/path/to/db"});

        verify(mockDbManager).openDatabase("/path/to/db");
        String output = outputStream.toString();
        assertThat(output).contains("Successfully opened database");
    }

    @Test
    void testExecuteCloseSubcommand() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getCurrentPath()).thenReturn("/path/to/db");

        command.execute(new String[]{"close"});

        verify(mockDbManager).closeDatabase();
        String output = outputStream.toString();
        assertThat(output).contains("Closed database");
    }

    @Test
    void testExecuteInfoSubcommand() throws Exception {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getCurrentPath()).thenReturn("/path/to/db");
        when(mockDbManager.getFormat()).thenReturn(DatabaseFormat.BONSAI);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("cf1"));

        BesuDatabaseManager.DatabaseStats stats = new BesuDatabaseManager.DatabaseStats();
        stats.estimatedKeys = 1000;
        stats.totalSstSize = 1024;
        stats.totalBlobSize = 0;

        when(mockDbManager.getStats("cf1")).thenReturn(stats);

        command.execute(new String[]{"info"});

        String output = outputStream.toString();
        assertThat(output).contains("Database Information");
    }

    @Test
    void testExecuteOpenSubcommandCaseInsensitive() throws Exception {
        when(mockDbManager.getFormat()).thenReturn(DatabaseFormat.BONSAI);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("cf1"));

        command.execute(new String[]{"OPEN", "/path/to/db"});

        verify(mockDbManager).openDatabase("/path/to/db");
    }

    @Test
    void testGetHelp() {
        String help = command.getHelp();
        assertThat(help).isNotEmpty();
        assertThat(help).containsIgnoringCase("database");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db <subcommand>");
        assertThat(usage).contains("db open");
        assertThat(usage).contains("db close");
        assertThat(usage).contains("db info");
    }
}
