package net.consensys.bric.commands;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for ProgressReporter.
 */
class ProgressReporterTest {

    private ByteArrayOutputStream outputStream;
    private PrintStream originalOut;

    @BeforeEach
    void setUp() {
        outputStream = new ByteArrayOutputStream();
        originalOut = System.out;
        System.setOut(new PrintStream(outputStream));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
    }

    @Test
    void testInitialReport_DoesNotPrint() {
        ProgressReporter reporter = new ProgressReporter(1);

        // Should not print immediately
        reporter.reportProgress(1, 100, "items");

        String output = outputStream.toString();
        assertThat(output).isEmpty();
    }

    @Test
    void testReportAfterInterval_Prints() throws InterruptedException {
        ProgressReporter reporter = new ProgressReporter(1); // 1 second interval

        // First call - no output
        reporter.reportProgress(10, 100, "items");
        assertThat(outputStream.toString()).isEmpty();

        // Wait for interval to pass
        Thread.sleep(1100);

        // Second call - should print
        reporter.reportProgress(20, 100, "items");
        String output = outputStream.toString();
        assertThat(output).contains("Progress:");
        assertThat(output).contains("20/100 items");
        assertThat(output).contains("20.0%");
    }

    @Test
    void testMultipleReports_PrintsOnlyAfterInterval() throws InterruptedException {
        ProgressReporter reporter = new ProgressReporter(1);

        // Multiple rapid calls - should only print once per interval
        outputStream.reset();
        reporter.reportProgress(10, 100, "items");
        reporter.reportProgress(20, 100, "items");
        reporter.reportProgress(30, 100, "items");

        assertThat(outputStream.toString()).isEmpty();

        Thread.sleep(1100);
        reporter.reportProgress(40, 100, "items");

        String output = outputStream.toString();
        // Should only see one progress report
        assertThat(output).contains("40/100 items");
        assertThat(output.split("Progress:")).hasSize(2); // Empty string + one match
    }

    @Test
    void testGetElapsedSeconds_ReturnsElapsedTime() throws InterruptedException {
        ProgressReporter reporter = new ProgressReporter(30);

        long elapsed1 = reporter.getElapsedSeconds();
        assertThat(elapsed1).isEqualTo(0);

        Thread.sleep(1100);

        long elapsed2 = reporter.getElapsedSeconds();
        assertThat(elapsed2).isGreaterThanOrEqualTo(1);
    }

    @Test
    void testReportComplete_PrintsSummary() {
        ProgressReporter reporter = new ProgressReporter(30);

        reporter.reportComplete(1000, "blocks");

        String output = outputStream.toString();
        assertThat(output).contains("Completed:");
        assertThat(output).contains("1000 blocks");
        assertThat(output).contains("in");
        assertThat(output).endsWith("s\n");
    }

    @Test
    void testShouldReport_ReturnsFalseInitially() {
        ProgressReporter reporter = new ProgressReporter(1);

        // First call should return false (no time has passed)
        boolean shouldReport = reporter.shouldReport();
        assertThat(shouldReport).isFalse();
    }

    @Test
    void testShouldReport_ReturnsTrueAfterInterval() throws InterruptedException {
        ProgressReporter reporter = new ProgressReporter(1);

        // First call
        reporter.shouldReport();

        // Wait for interval
        Thread.sleep(1100);

        // Should return true now
        boolean shouldReport = reporter.shouldReport();
        assertThat(shouldReport).isTrue();
    }
}
