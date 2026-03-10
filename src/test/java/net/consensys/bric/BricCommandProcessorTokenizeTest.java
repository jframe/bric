package net.consensys.bric;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for BricCommandProcessor.tokenize() quote-preservation behaviour.
 */
class BricCommandProcessorTokenizeTest {

    @Test
    void tokenize_simpleTokens() {
        String[] tokens = BricCommandProcessor.tokenize("db get ACCOUNT_INFO_STATE 0xdeadbeef");
        assertThat(tokens).containsExactly("db", "get", "ACCOUNT_INFO_STATE", "0xdeadbeef");
    }

    @Test
    void tokenize_doubleQuotedTokenPreservesQuotes() {
        String[] tokens = BricCommandProcessor.tokenize("db get VARIABLES \"FLAT_DB_MODE\"");
        assertThat(tokens).containsExactly("db", "get", "VARIABLES", "\"FLAT_DB_MODE\"");
    }

    @Test
    void tokenize_singleQuotedTokenPreservesQuotes() {
        String[] tokens = BricCommandProcessor.tokenize("db get VARIABLES 'FLAT_DB_MODE'");
        assertThat(tokens).containsExactly("db", "get", "VARIABLES", "'FLAT_DB_MODE'");
    }

    @Test
    void tokenize_quotedStringWithSpacesIsSingleToken() {
        String[] tokens = BricCommandProcessor.tokenize("db get VARIABLES \"hello world\"");
        assertThat(tokens).containsExactly("db", "get", "VARIABLES", "\"hello world\"");
    }

    @Test
    void tokenize_quotedPath() {
        String[] tokens = BricCommandProcessor.tokenize("db open \"/path/to my/db\"");
        assertThat(tokens).containsExactly("db", "open", "\"/path/to my/db\"");
    }

    @Test
    void tokenize_emptyInput() {
        String[] tokens = BricCommandProcessor.tokenize("");
        assertThat(tokens).isEmpty();
    }

    @Test
    void tokenize_extraWhitespace() {
        String[] tokens = BricCommandProcessor.tokenize("  db   get  ");
        assertThat(tokens).containsExactly("db", "get");
    }
}
