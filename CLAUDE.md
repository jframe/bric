# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bric is a command-line REPL tool for exploring Hyperledger Besu databases. It provides read-only access to Besu's RocksDB database, allowing users to query account data, storage slots, contract bytecode, and trie logs (state diffs).

## Build and Test Commands

```bash
# Build the project
./gradlew build

# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "*TrieLogFormatterTest"

# Run with verbose output
./gradlew test --info

# Create a fat JAR with all dependencies
./gradlew fatJar

# Check for dependency updates
./gradlew dependencyUpdates

# Test coverage report
./gradlew test jacocoTestReport
# View at: build/reports/jacoco/test/html/index.html

# Clean build artifacts
./gradlew clean
```

## Running the Application

```bash
# Run via Gradle
./gradlew run

# Run with verbose mode
./gradlew run --args="--verbose"

# Run with database pre-loaded
./gradlew run --args="--database /path/to/besu/database"

# Run JAR directly
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar --database /path/to/besu/database --verbose
```

## Architecture

### Core Components

**BricApplication** (`src/main/java/net/consensys/bric/BricApplication.java`)
- Main entry point using Picocli for CLI argument parsing
- Sets up JLine terminal and LineReader for REPL interaction
- Handles shutdown hooks to properly close database connections
- Supports optional `--database` flag to auto-open a database on startup

**BricCommandProcessor** (`src/main/java/net/consensys/bric/BricCommandProcessor.java`)
- Central command dispatcher using a command registry pattern
- Manages command registration and execution
- All commands implement the `Command` interface with `execute()`, `getHelp()`, and `getUsage()` methods
- Commands are registered by name in lowercase (e.g., "db-open", "account", "storage")

**BesuDatabaseManager** (`src/main/java/net/consensys/bric/db/BesuDatabaseManager.java`)
- Manages RocksDB database lifecycle (open/close operations)
- Opens databases in **read-only mode** for safety
- Handles column family discovery and mapping
- Auto-detects database format: BONSAI, BONSAI_ARCHIVE, FOREST, or UNKNOWN
- Detection based on presence of specific column families:
  - Bonsai Archive: has `ACCOUNT_INFO_STATE_ARCHIVE` or `ACCOUNT_STORAGE_ARCHIVE`
  - Bonsai: has `ACCOUNT_INFO_STATE` and `TRIE_LOG_STORAGE`
  - Forest: has `WORLD_STATE` but not `ACCOUNT_INFO_STATE`
- Provides database statistics (key counts, SST sizes, blob sizes)

**BesuDatabaseReader** (`src/main/java/net/consensys/bric/db/BesuDatabaseReader.java`)
- High-level read operations with RLP decoding
- Uses Besu's internal RLP classes for parsing
- Account data: `RLP[nonce, balance, storageRoot, codeHash]`
- Storage data: `RLP[UInt256]`
- Trie logs: Complex nested RLP parsed via `TrieLogFactoryImpl` from Besu

**SegmentReader** (`src/main/java/net/consensys/bric/db/SegmentReader.java`)
- Low-level RocksDB access by column family (segment)
- Handles key encoding: account keys (32 bytes), storage keys (64 bytes: accountHash + slotHash)
- Computes Keccak256 hashes for addresses and slots

### Key Encoding Scheme

Besu uses specific key encodings that are critical to understand:
- **Account Keys** (Bonsai): `Keccak256(address)` (32 bytes)
- **Account Keys** (Bonsai Archive): `Keccak256(address) + BlockNumber` (40 bytes: 32 bytes hash + 8 bytes unsigned long)
- **Storage Keys**: `Concat(AccountHash, SlotHash)` where SlotHash = `Keccak256(slot)` (64 bytes total)
- **Code Keys**: Code hash (32 bytes)
- **Trie Log Keys**: Block hash (32 bytes)

#### Bonsai Archive Key Structure

In Bonsai Archive flatdb, historical account and storage data includes an 8-byte block number suffix:
- **Format**: `[Natural Key (32 bytes)] + [Block Number (8 bytes)]`
- **Block Number Encoding**: Unsigned 64-bit long (`Bytes.ofUnsignedLong(blockNumber)`)
- **Query Strategy**: Use `getNearestBefore()` with search key = `accountHash + blockNumber` to find the account state at or before that block
  - For latest state: use `Long.MAX_VALUE` as block number suffix
  - For historical state: use specific block number as suffix
- **Deleted Entries**: Marked with empty byte array value (length = 0)
- **Block Number Extraction**: The block number is extracted from the last 8 bytes of the returned key and displayed in query results

### Column Families (Segments)

Column families are identified by `KeyValueSegmentIdentifier` enum:
- `ACCOUNT_INFO_STATE` (id: `{6}`) - Current account data (nonce, balance, roots, hashes)
- `CODE_STORAGE` (id: `{7}`) - Contract bytecode indexed by code hash
- `ACCOUNT_STORAGE_STORAGE` (id: `{8}`) - Contract storage slots
- `TRIE_LOG_STORAGE` (id: `{10}`) - State diffs per block (Bonsai/Archive only)
- `ACCOUNT_INFO_STATE_ARCHIVE` - Historical account states (Archive only)
- `ACCOUNT_STORAGE_ARCHIVE` - Historical storage states (Archive only)

### Command Pattern

All REPL commands implement the `Command` interface:
```java
public interface Command {
    void execute(String[] args);
    String getHelp();
    String getUsage();
}
```

Commands are located in `src/main/java/net/consensys/bric/commands/`:
- `DbCommand` - Parent command for database operations with subcommands (open, close, info)
  - `DbOpenCommand` - Opens database in read-only mode (via `db open`)
  - `DbCloseCommand` - Closes current database (via `db close`)
  - `DbInfoCommand` - Shows database statistics (via `db info`)
- `AccountCommand` - Queries account by address or hash
  - Supports `--block <number|hash>` parameter for historical queries (Bonsai Archive only)
  - Automatically extracts block number from archive keys
- `StorageCommand` - Queries storage slot by address and slot number
- `CodeCommand` - Retrieves bytecode by address or code hash (supports `--save` flag)
- `TrieLogCommand` - Retrieves state diffs by block hash

The `DbCommand` uses a subcommand routing pattern where the first argument determines which subcommand to execute.

### Formatter Pattern

Each data type has a corresponding formatter for console output:
- `AccountFormatter` - Formats account data (nonce, balance in Wei and ETH, hashes)
- `StorageFormatter` - Formats storage slot data
- `CodeFormatter` - Formats bytecode (truncated if >1000 bytes)
- `TrieLogFormatter` - Formats state diffs with detailed change summaries

Formatters are tested in `src/test/java/net/consensys/bric/formatters/`.

## Dependencies

Key external dependencies:
- **JLine 3** - Terminal and readline functionality for REPL
- **Picocli** - CLI argument parsing
- **RocksDB** - Database access (rocksdbjni)
- **Apache Tuweni** - Bytes operations and RLP encoding/decoding
- **Hyperledger Besu** - Core Ethereum types (Address, Hash, Wei) and trie log parsing
- **BouncyCastle** - Keccak256 hashing

Maven repositories:
- Maven Central
- `https://hyperledger.jfrog.io/artifactory/besu-maven`
- `https://artifacts.consensys.net/public/maven/maven/`

## Testing Approach

Tests use JUnit 5, AssertJ, and Mockito. Test structure mirrors main source:
- Command tests verify command parsing, validation, and execution
- Formatter tests verify output format and edge cases
- Tests mock `BesuDatabaseManager` to avoid requiring actual databases

## Important Implementation Notes

### Database Safety
- All database operations are **read-only**
- Database manager prevents opening multiple databases simultaneously
- Shutdown hooks ensure databases are properly closed on exit

### RLP Parsing
- Account and storage data is RLP-encoded in the database
- Uses Besu's `BytesValueRLPInput` for decoding
- Handles null values for empty storage roots and code hashes

### Trie Logs (State Diffs)
- Only available in Bonsai Archive databases
- Parsed using Besu's internal `TrieLogFactoryImpl`
- Contains account changes, code changes, and storage changes per block
- Block number is optional in the trie log data structure

### Error Handling
- Commands print user-friendly error messages to stderr
- Verbose mode logs detailed stack traces
- Missing data returns empty Optional rather than throwing exceptions

### Hash Computation
- Uses BouncyCastle's Keccak256 for Ethereum-compatible hashing
- Address and slot hashing is centralized in `SegmentReader`
