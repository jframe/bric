# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bric (**B**esu **R**ocksDB **I**nteractive **C**ommand-line) is a command-line REPL tool for exploring and (optionally) repairing Hyperledger Besu databases. It provides read-only-by-default access to Besu's RocksDB database, allowing users to query account data, storage slots, contract bytecode, and trie logs (state diffs), plus lower-level RocksDB maintenance operations (raw get/put, compaction, flat DB healing).

## Build and Test Commands

```bash
# Build the project
./gradlew build

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "*TrieLogFormatterTest"

# Run a single test method
./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest.methodName"

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

## Toolchain Notes

- Java toolchain is pinned to **JavaLanguageVersion 25** (`build.gradle`), tracking the Besu BOM (`org.hyperledger.besu:bom:25.12.0`).
- The Besu BOM's log4j pin is deliberately overridden with a newer `log4j-bom` (needed for `BridgeAware`), and its Mockito pin (5.7.0) is overridden to 5.18.0 because 5.7.0 cannot instrument Java 25 bytecode. Keep these overrides when bumping the Besu BOM version.
- Besu's internal modules (`besu-ethereum-core`, `besu-ethereum-trie`, `besu-ethereum-rlp`, `besu-metrics-core`, all under `org.hyperledger.besu.internal`) are not on Maven Central — hence the extra Hyperledger/ConsenSys Artifactory repositories in `build.gradle`.

## Architecture

### Core Components

**BricApplication** (`src/main/java/net/consensys/bric/BricApplication.java`)
- Main entry point using Picocli for CLI argument parsing
- Sets up JLine terminal and LineReader for REPL interaction
- Registers a JVM shutdown hook that cancels running compaction jobs and safely closes the database — see "Safe cancellation" below for why this is more than a simple `close()` call
- Supports optional `--database` flag to auto-open a database on startup

**BricCommandProcessor** (`src/main/java/net/consensys/bric/BricCommandProcessor.java`)
- Central command dispatcher using a command registry pattern (`Map<String, Command>`)
- All commands implement the `Command` interface with `execute()`, `getHelp()`, and `getUsage()` methods
- Top-level commands are registered by name in lowercase (`db`, `account`, `storage`, `code`, `trielog`, `trielog-compare`, `trielog-check`, plus built-ins `help`/`version`/`status`)
- `db` itself is a nested dispatcher (see `DbCommand` below) rather than one command per subcommand

**BesuDatabaseManager** (`src/main/java/net/consensys/bric/db/BesuDatabaseManager.java`)
- Manages RocksDB database lifecycle (open/close operations)
- Opens databases **read-only by default**; `db open <path> --write` enables write mode for `put`/`drop-cf`/`compact`/`upgrade-flatdb`
- Handles column family discovery and mapping
- Auto-detects database format: BONSAI, BONSAI_ARCHIVE, FOREST, or UNKNOWN
  - Bonsai Archive: has `ACCOUNT_INFO_STATE_ARCHIVE` or `ACCOUNT_STORAGE_ARCHIVE`
  - Bonsai: has `ACCOUNT_INFO_STATE` and `TRIE_LOG_STORAGE`
  - Forest: has `WORLD_STATE` but not `ACCOUNT_INFO_STATE`
- Tracks in-flight long-running operations (`operationInProgress` / `cancellationRequested`) so the shutdown hook and flat DB heal can coordinate safe cancellation (see below)
- Provides database statistics (key counts, SST sizes, blob sizes) and owns the DB's `CompactionJobManager`

**BesuDatabaseReader** (`src/main/java/net/consensys/bric/db/BesuDatabaseReader.java`)
- High-level read operations with RLP decoding
- Account data: `RLP[nonce, balance, storageRoot, codeHash]`
- Storage data: `RLP[UInt256]`
- Trie logs: Complex nested RLP parsed via `TrieLogFactoryImpl` from Besu
- Also exposes cheap existence checks (e.g. `trieLogExists`) used by `TrieLogCheckCommand` without a full decode

**SegmentReader** (`src/main/java/net/consensys/bric/db/SegmentReader.java`)
- Low-level RocksDB access by column family (segment)
- Handles key encoding: account keys (32 bytes), storage keys (64 bytes: accountHash + slotHash)
- Computes Keccak256 hashes for addresses and slots

### Key Encoding Scheme

- **Account Keys** (Bonsai): `Keccak256(address)` (32 bytes)
- **Account Keys** (Bonsai Archive): `Keccak256(address) + BlockNumber` (40 bytes: 32 bytes hash + 8 bytes unsigned long)
- **Storage Keys**: `Concat(AccountHash, SlotHash)` where SlotHash = `Keccak256(slot)` (64 bytes total)
- **Code Keys**: Code hash (32 bytes)
- **Trie Log Keys**: Block hash (32 bytes)

#### Bonsai Archive Key Structure

Historical account and storage data includes an 8-byte block number suffix:
- **Format**: `[Natural Key (32 bytes)] + [Block Number (8 bytes)]`
- **Block Number Encoding**: Unsigned 64-bit long (`Bytes.ofUnsignedLong(blockNumber)`)
- **Query Strategy**: Use `getNearestBefore()` with search key = `accountHash + blockNumber` to find the account state at or before that block
  - For latest state: use `Long.MAX_VALUE` as block number suffix
  - For historical state: use specific block number as suffix
- **Deleted Entries**: Marked with empty byte array value (length = 0)
- **Block Number Extraction**: Extracted from the last 8 bytes of the returned key

### Column Families (Segments)

Identified by bric's own `KeyValueSegmentIdentifier` enum (`src/main/java/net/consensys/bric/db/KeyValueSegmentIdentifier.java`) — a local catalogue mirroring (but distinct from) Besu's own class of the same simple name:
- `BLOCKCHAIN` (id: `{1}`), `VARIABLES` (id: `{11}`, also used to persist bric's own flat-DB-heal checkpoint state)
- `ACCOUNT_INFO_STATE` (id: `{6}`) - Current account data (nonce, balance, roots, hashes)
- `CODE_STORAGE` (id: `{7}`) - Contract bytecode indexed by code hash
- `ACCOUNT_STORAGE_STORAGE` (id: `{8}`) - Contract storage slots
- `TRIE_BRANCH_STORAGE` (id: `{9}`)
- `TRIE_LOG_STORAGE` (id: `{10}`) - State diffs per block (Bonsai/Archive only)
- `ACCOUNT_INFO_STATE_ARCHIVE` / `ACCOUNT_STORAGE_ARCHIVE` - Historical states (Archive only)

`ColumnFamilyResolver` (`src/main/java/net/consensys/bric/db/ColumnFamilyResolver.java`) turns CLI string input into a `ColumnFamilyHandle`, trying (in order): known enum name, hex ID (`0x06`), then an arbitrary UTF-8 CF name looked up on the currently-open DB. Every CF-taking command (`get`/`put`/`scan`/`drop-cf`/`stats`/`compact`) goes through this resolver.

### Command Pattern

All REPL commands implement the `Command` interface:
```java
public interface Command {
    void execute(String[] args);
    String getHelp();
    String getUsage();
}
```

Commands live in `src/main/java/net/consensys/bric/commands/`. `DbCommand` is a parent dispatcher that manually `switch`es on `args[0]` (no Picocli here) and routes to one class per subcommand:
- `db open <path> [--write]` / `db close` / `db info` — lifecycle and stats
- `db get <segment> <hex-key>` / `db put <segment> <hex-key> <hex-value>` (`--write` required) / `db scan <segment> [--limit n]` — raw KV access
- `db drop-cf <segment>` (`--write` required, blocks dropping `default`)
- `db stats [cf-name]` — per-CF or whole-DB RocksDB internal stats (`rocksdb.stats`, `rocksdb.levelstats`, `rocksdb.sstables`)
- `db compact <segment...|--all>` / `db compact-status [<job-id>]` / `db compact-cancel <job-id>` — async manual compaction (see below)
- `db upgrade-flatdb [--dry-run]` — flat DB heal (see below)

Other top-level commands:
- `AccountCommand` / `StorageCommand` / `CodeCommand` — as before, with `--block`, `--raw`, `--save` support
- `TrieLogCommand` — state diff for a block, supports `--address` to filter output to one account
- `TrieLogCompareCommand` (`trielog-compare <block|start..end> [--verbose]`, **Bonsai Archive only**) — validates that a block's recorded trie log matches what's actually stored in the archive flat DB at that block, catching archive corruption/write bugs. Results modeled by `TrieLogComparisonResult`, printed via `TrieLogCompareFormatter`
- `TrieLogCheckCommand` (`trielog-check <block|start..end>`) — cheap existence-only check for gaps in trie-log retention (any format)

Shared command utilities: `InputParser` (address/hash/slot/block-number parsing), `ProgressReporter` (throttled progress printing for long block ranges), `BricCompleter` (JLine tab-completion for command names, `db` subcommands, column-family names, and per-command flags).

### Formatter Pattern

Each data type has a corresponding formatter for console output, tested in `src/test/java/net/consensys/bric/formatters/`:
- `AccountFormatter`, `StorageFormatter`, `CodeFormatter`, `TrieLogFormatter`, `TrieLogCompareFormatter`

### Manual Compaction (`db compact`)

`CompactionJobManager` / `CompactionJob` (`src/main/java/net/consensys/bric/db/`) run RocksDB's blocking `compactRange` calls on a dedicated cached thread pool (`bric-compaction` daemon threads), one job per column family, fully async:
- `db compact` returns a job id immediately; job state (`RUNNING`/`DONE`/`FAILED`/`CANCELLED`) is tracked in a `ConcurrentHashMap` and inspected with `db compact-status`
- Each job gets its own `CompactRangeOptions` so `db compact-cancel <job-id>` only cancels that job (via `setCanceled(true)`, waiting up to 5s for the worker to reach a terminal state)
- `BesuDatabaseManager` refuses to close the database while any compaction job is still `RUNNING`; the shutdown hook cancels all running jobs before attempting a close

### Flat DB Heal (`db upgrade-flatdb`)

Besu's Bonsai "flat DB" is a denormalized account/storage table layered over the canonical trie for fast reads; a DB left in `PARTIAL` flat-DB mode (e.g. after an interrupted snap-sync) can have flat entries that drift from the trie. `FlatDbHealer` (`src/main/java/net/consensys/bric/besu/FlatDbHealer.java`) reconciles this locally — walking the account trie and each divergent account's storage trie directly (reusing Besu's own `StoredMerklePatriciaTrie`/`BonsaiWorldStateKeyValueStorage`, not Besu's peer-network snap-sync healer, which isn't reusable outside a running node) and repairing the flat table to match. See `docs/superpowers/specs/2026-07-24-flatdb-heal-design.md` for the original design rationale.

Key behaviors worth knowing before touching this code:
- **Batched, paged walks** (`DEFAULT_BATCH_LIMIT = 50_000` leaves) bound memory per batch instead of materializing a whole range or a whole contract's storage.
- **Trie nodes are explicitly unloaded between batches** (`collectTrieBatch`, constructing `TrieIterator` with `unload=true`) — without this, Besu's `StoredNode.load()` caches every resolved node for the life of the reused root object, growing heap unboundedly across a long-running heal.
- **Checkpointing** persists `(stateRoot, phase, nextRangeIndex)` plus the list of accounts still pending storage-heal to the `VARIABLES` CF after *every* range, so an interrupted heal resumes correctly; if the chain has since moved past the checkpointed state root, the checkpoint is discarded and healing restarts from scratch. `--dry-run` never reads or writes a checkpoint.
- **Missing bytecode is detected but not fixed**: each account leaf's `codeHash` is checked against `CODE_STORAGE`; a miss can't be locally repaired (the trie only stores the hash, never the bytecode) and is reported separately as `accountsWithMissingCode`, explicitly excluded from the "total fixed" counts.
- **Progress** is reported at two granularities: coarse (`onRangeComplete`/`onStorageAccountComplete`, printed to stdout) and fine intra-batch heartbeats (`onRangeHeartbeat`/`onStorageHeartbeat`, percent of the 256-bit keyspace scanned) rate-limited to once/minute by `HeartbeatThrottle` and logged via SLF4J rather than stdout.
- **Safe cancellation**: batch loops poll `checkNotCancelled()` only between batches/ranges/accounts — never mid native-RocksDB-call — throwing `FlatDbHealCancelledException` on a clean stop. This exists because the JVM shutdown hook (Ctrl-C) must not free RocksDB column-family handles while a heal thread has a native call in flight (that segfaults the JVM); the hook instead requests cancellation and waits up to 2s for the operation to quiesce before closing, and skips the native close entirely if it times out (the process is exiting anyway).

`RocksDBSegmentedStorage` (`src/main/java/net/consensys/bric/besu/`) is the adapter that lets Besu's native flat-DB strategy classes operate against bric's RocksDB handle; `BesuFlatDbReader` is a separate, simpler reader (used by `account`/`storage`/`trielog-compare`, not the heal path) that decodes flat records the same way Besu would, including archive point-in-time lookups. `NoOpKeyValueStorage` is a stub satisfying `BonsaiWorldStateKeyValueStorage`'s trie-log-storage constructor argument, since healing never touches trie logs.

## Dependencies

Key external dependencies:
- **JLine 3** - Terminal and readline functionality for REPL
- **Picocli** - CLI argument parsing (used for top-level app args; `db` subcommands use manual dispatch, not Picocli)
- **RocksDB** - Database access (rocksdbjni)
- **Apache Tuweni** - Bytes operations and RLP encoding/decoding
- **Hyperledger Besu** - Core Ethereum types (Address, Hash, Wei) and trie/trie-log parsing
- **BouncyCastle** - Keccak256 hashing

Maven repositories:
- Maven Central
- `https://hyperledger.jfrog.io/artifactory/besu-maven`
- `https://artifacts.consensys.net/public/maven/maven/`

## Testing Approach

Tests use JUnit 5, AssertJ, Mockito, and Awaitility (for async compaction-job assertions). Test structure mirrors main source:
- Command tests verify command parsing, validation, and execution
- Formatter tests verify output format and edge cases
- `besu/` package tests (`FlatDbHealerTest`, `FlatDbHealCheckpointTest`, `HeartbeatThrottleTest`, `RocksDBSegmentedStorageTest`, `BesuFlatDbReaderTest`) exercise the flat DB heal path in isolation
- Tests mock `BesuDatabaseManager` to avoid requiring actual databases

## Important Implementation Notes

### Database Safety
- Databases are **read-only by default**; write access (`put`, `drop-cf`, `compact`, `upgrade-flatdb`) requires `db open <path> --write`
- Database manager prevents opening multiple databases simultaneously, and prevents closing while compaction jobs are still running
- Shutdown hook cancels running compaction jobs and coordinates with any in-flight flat DB heal before closing, to avoid a native-call/close race that can SIGSEGV the JVM

### RLP Parsing
- Account and storage data is RLP-encoded in the database
- Uses Besu's `BytesValueRLPInput` for decoding
- Handles null values for empty storage roots and code hashes

### Trie Logs (State Diffs)
- Only available in Bonsai Archive databases
- Parsed using Besu's internal `TrieLogFactoryImpl`
- Contains account changes, code changes, and storage changes per block
- Block number is optional in the trie log data structure
- `trielog-compare` cross-checks a trie log against the archive flat DB; `trielog-check` only checks for the trie log's existence

### Error Handling
- Commands print user-friendly error messages to stderr
- Verbose mode logs detailed stack traces
- Missing data returns empty Optional rather than throwing exceptions

### Hash Computation
- Uses BouncyCastle's Keccak256 for Ethereum-compatible hashing
- Address and slot hashing is centralized in `SegmentReader`
