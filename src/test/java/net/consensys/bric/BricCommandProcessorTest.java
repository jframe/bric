package net.consensys.bric;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

class BricCommandProcessorTest {

    private BricCommandProcessor processor;
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @BeforeEach
    void setUp() {
        processor = new BricCommandProcessor(false);
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
    }

    @Test
    void testVersionCommand() {
        processor.processCommand("version");
        assertThat(outContent.toString()).contains("Bric CLI version");
    }

    @Test
    void testHelpCommand() {
        processor.processCommand("help");
        String output = outContent.toString();
        assertThat(output).contains("Available commands:");
        assertThat(output).contains("help");
        assertThat(output).contains("version");
        assertThat(output).contains("status");
        assertThat(output).contains("db");
    }

    @Test
    void testStatusCommand() {
        processor.processCommand("status");
        String output = outContent.toString();
        assertThat(output).contains("Status:");
        assertThat(output).contains("Verbose mode:");
        assertThat(output).contains("Java version:");
    }

    @Test
    void testUnknownCommand() {
        processor.processCommand("invalidcommand");
        String output = outContent.toString();
        assertThat(output).contains("Unknown command: invalidcommand");
        assertThat(output).contains("Type 'help' for available commands");
    }
}
