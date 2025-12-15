package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.BesuDatabaseManager.DatabaseFormat;
import net.consensys.bric.db.BesuDatabaseManager.DatabaseStats;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class DbInfoCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbInfoCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = Mockito.mock(BesuDatabaseManager.class);
        command = new DbInfoCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void testExecuteWhenNoDatabaseOpen() {
        when(mockDbManager.isOpen()).thenReturn(false);

        command.execute(new String[]{});

        String error = errorStream.toString();
        assertThat(error).contains("Error: No database is open");
    }

    @Test
    void testExecuteSuccess() throws Exception {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getCurrentPath()).thenReturn("/path/to/db");
        when(mockDbManager.getFormat()).thenReturn(DatabaseFormat.BONSAI);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("cf1", "cf2"));

        DatabaseStats stats1 = new DatabaseStats();
        stats1.estimatedKeys = 1000;
        stats1.totalSstSize = 1024 * 1024; // 1 MB
        stats1.totalBlobSize = 0;

        DatabaseStats stats2 = new DatabaseStats();
        stats2.estimatedKeys = 2000;
        stats2.totalSstSize = 2 * 1024 * 1024; // 2 MB
        stats2.totalBlobSize = 0;

        when(mockDbManager.getStats("cf1")).thenReturn(stats1);
        when(mockDbManager.getStats("cf2")).thenReturn(stats2);

        command.execute(new String[]{});

        String output = outputStream.toString();
        assertThat(output).contains("Database Information");
        assertThat(output).contains("/path/to/db");
        assertThat(output).contains("BONSAI");
        assertThat(output).contains("Column Family Statistics");
        assertThat(output).contains("cf1");
        assertThat(output).contains("cf2");
        assertThat(output).contains("TOTAL");
    }

    @Test
    void testExecuteWithStatsError() throws Exception {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getCurrentPath()).thenReturn("/path/to/db");
        when(mockDbManager.getFormat()).thenReturn(DatabaseFormat.BONSAI);
        when(mockDbManager.getColumnFamilyNames()).thenReturn(Set.of("cf1"));

        when(mockDbManager.getStats("cf1")).thenThrow(new RuntimeException("Stats error"));

        command.execute(new String[]{});

        String output = outputStream.toString();
        assertThat(output).contains("Database Information");
        assertThat(output).contains("cf1");
        assertThat(output).contains("Error");
    }

    @Test
    void testGetHelp() {
        assertThat(command.getHelp()).isNotEmpty();
        assertThat(command.getHelp()).containsIgnoringCase("database");
        assertThat(command.getHelp()).containsIgnoringCase("statistics");
    }

    @Test
    void testGetUsage() {
        String usage = command.getUsage();
        assertThat(usage).contains("db-info");
    }
}
