# Bric - Besu RocksDB Interactive Command-line

**B**esu **R**ocksDB **I**nteractive **C**ommand-line

A command-line REPL tool for exploring Hyperledger Besu databases. Query account data, storage slots, contract bytecode, and trie logs (state diffs) directly from Besu's RocksDB database.

## Features

- 🔍 **Account Queries** - View account balance, nonce, storage root, and code hash
- 💾 **Storage Exploration** - Query contract storage slots by address and slot number
- 📜 **Bytecode Access** - Retrieve and save contract bytecode
- 📊 **Trie Log Analysis** - Examine state changes (diffs) per block with detailed formatting
- ✅ **Trie Log Validation** - Compare trie logs against archived flat DB state, or check for gaps in trie-log retention
- 🗄️ **Database Management** - Read-only (by default) access to Besu Bonsai and Bonsai Archive databases
- 📈 **Database Statistics** - View column family sizes and key counts
- 🛠️ **Low-Level Access** - Raw get/put/scan by column family, drop a column family, and run manual compactions (all require opting into write mode)
- 🩹 **Flat DB Heal** - Reconcile a Bonsai flat DB against the canonical trie, with resumable checkpoints and safe cancellation

## Requirements

- Java 25 or higher
- Gradle 8.x or higher (or use the Gradle wrapper)
- Access to a Besu database (Bonsai or Bonsai Archive format)

## Building the Project

```bash
# Build the project
./gradlew build

# Create a fat JAR with all dependencies
./gradlew fatJar

# Check for dependency updates
./gradlew dependencyUpdates
```

## Running the REPL

### Using Gradle

```bash
./gradlew run
```

### Using the JAR

```bash
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar
```

### With verbose mode

```bash
./gradlew run --args="--verbose"
# or
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar --verbose
```

### With database pre-loaded

```bash
./gradlew run --args="--database /path/to/besu/database"
# or
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar --database /path/to/besu/database

# Combined with verbose mode
java -jar build/libs/bric-1.0.0-SNAPSHOT-all.jar --database /path/to/besu/database --verbose
```

## Quick Start

```bash
# Build the project
./gradlew build

# Option 1: Run the REPL with database pre-loaded
./gradlew run --args="--database /path/to/besu/database"

# Option 2: Run the REPL and open database manually
./gradlew run
# In the REPL, open a Besu database
bric> db open /path/to/besu/database

# Query an account
bric> account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0

# View trie log (state diff) for a block
bric> trielog 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
```

## Available Commands

Once the REPL is running, you can use the following commands:

### General Commands
- `help` - Display available commands and their descriptions
- `version` - Display version information
- `status` - Display REPL status and open database info
- `exit` or `quit` - Exit the REPL

### Database Commands

#### `db open <path> [--write]`
Open a Besu database. Read-only by default; automatically detects database format (Bonsai, Bonsai Archive, or Forest). Pass `--write` to allow the write-mode-only commands below (`put`, `drop-cf`, `compact`, `upgrade-flatdb`).

**Examples:**
```
db open /path/to/besu/database
db open ~/besu-data/database --write
```

#### `db close`
Close the currently open database. Refuses to close while a manual compaction job is still running.

#### `db info`
Display detailed database statistics including column family sizes and estimated key counts.

#### `db get <segment> <hex-key>`
Read a single raw value by column family and key.

**Example:**
```
db get ACCOUNT_INFO_STATE 0x1234...abcd
```

#### `db put <segment> <hex-key> <hex-value>` (requires `--write`)
Write a raw value by column family and key. Use with care — this bypasses all higher-level validation.

#### `db scan <segment> [--limit n]`
Iterate raw key/value entries in a column family, printing each as `Key -> Value`. Defaults to unlimited; use `--limit` to cap output.

**Example:**
```
db scan CODE_STORAGE --limit 20
```

#### `db drop-cf <segment>` (requires `--write`)
Drop a column family entirely. Refuses to drop `default`.

#### `db stats [cf-name]`
Show detailed RocksDB internal stats (`rocksdb.stats`, `rocksdb.levelstats`, SST table info) for one column family, or all non-empty column families if none is given.

#### `db compact <segment...|--all>` (requires `--write`)
Submit an asynchronous manual compaction job for one or more column families (or every non-empty one with `--all`). Returns immediately with a job id per column family.

#### `db compact-status [<job-id>]`
List all compaction jobs (id, column family, state, elapsed time, pending compaction bytes), or show detail for a single job.

#### `db compact-cancel <job-id>` (requires `--write`)
Request cancellation of a running compaction job.

#### `db upgrade-flatdb [--dry-run]` (requires `--write` unless `--dry-run`)
Heal a Bonsai/Bonsai Archive flat DB left in `PARTIAL` mode by reconciling it against the canonical trie: walks the account trie and each divergent account's storage trie, fixes drifted flat entries, and reports (without attempting to fix) any accounts referencing a `codeHash` missing from `CODE_STORAGE`. Progress is reported per range/account, with throttled percent-complete heartbeats for long-running ranges. Safe to interrupt with Ctrl-C — a resumable checkpoint is saved after every range, and shutdown coordinates with the in-progress heal to avoid crashing the JVM. `--dry-run` reports what would change without writing anything or touching the checkpoint.

**Example:**
```
db upgrade-flatdb --dry-run
db upgrade-flatdb
```

### Account Commands

#### `account <address|hash>`
Query account information by Ethereum address or account hash. Automatically detects the input type based on length:
- 20-byte address (42 hex characters with 0x prefix)
- 32-byte hash (66 hex characters with 0x prefix)

Displays nonce, balance (in Wei and ETH), storage root, and code hash.

**Examples:**
```
account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0
account 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
account 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
```

### Storage Commands

#### `storage <address> <slot>`
Query storage slot value by contract address and slot number. Slot can be decimal or hex format.

**Examples:**
```
storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0 0
storage 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0 0x1234
storage 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045 42
```

#### `storage <account-hash> <slot-hash> --raw`
Query storage by raw hashes (for debugging).

**Example:**
```
storage 0x1234...abcd 0x5678...ef01 --raw
```

### Code Commands

#### `code <address>`
Retrieve contract bytecode by address. Shows code hash, size, and bytecode (truncated if >1000 bytes).

**Examples:**
```
code 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0
code 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

#### `code <address> --save <file>`
Save contract bytecode to a file.

**Example:**
```
code 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0 --save contract.bin
```

#### `code-hash <hash>`
Retrieve bytecode by code hash (for debugging).

**Example:**
```
code-hash 0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
```

### Trie Log Commands

#### `trielog <block-hash> [--address <address>]`
Query trie log (state diff) for a specific block. Shows detailed state changes including:
- **Account Changes**: Created, updated, or deleted accounts with balance/nonce/storage root changes
- **Code Changes**: Deployed or cleared contract code
- **Storage Changes**: Storage slot modifications

Pass `--address` to filter the output down to a single account.

**Example:**
```
trielog 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
trielog 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef --address 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0
```

#### `trielog-compare <block-number|block-hash|start..end> [--verbose]`
**Bonsai Archive only.** Validates that a block's recorded trie log matches what's actually stored in the archive flat DB at that block — useful for catching archive corruption or write bugs. Compares account (nonce/balance/storageRoot/codeHash), storage slot, and code hash changes; reports mismatches by default, or every comparison (matches included) with `--verbose`. Accepts a single block or a `start..end` range, printing progress periodically for ranges.

**Examples:**
```
trielog-compare 12345
trielog-compare 12000..12500
trielog-compare 0x1234...abcdef --verbose
```

#### `trielog-check <block|start..end>`
Cheap existence check (no decoding) for whether a trie log is present for each block in the given block or range. Works on any database format; use it to find gaps in trie-log retention.

**Example:**
```
trielog-check 12000..12500
```

**Output Example:**
```
Trie Log (State Diff):
  Block Hash: 0x1234...abcdef
  Block Number: 12,345

Summary:
  Account Changes: 2
  Code Changes:    1
  Storage Changes: 5

═══ Account Changes ═══

Address: 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0
  Status: UPDATED
  Nonce:       1 → 2
  Balance:     1,000,000,000,000,000,000 Wei (1.000000000000000000 ETH) →
               2,000,000,000,000,000,000 Wei (2.000000000000000000 ETH)

═══ Storage Changes ═══

Address: 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0
  Storage Slots (2 changes):
    Slot: 0x0000000000000000000000000000000000000000000000000000000000000000
      Value: 0x0 → 0x1
```

**Note:** Trie logs are only available in Besu Bonsai Archive databases (`trielog`, `trielog-compare`). `trielog-check` only tests for existence and works against any database format.

## Development

### Running Tests

```bash
# Run all tests
./gradlew test

# Run specific test class
./gradlew test --tests "*TrieLogFormatterTest"

# Run with verbose output
./gradlew test --info
```

### Cleaning Build Artifacts

```bash
./gradlew clean
```

### Code Coverage

To view test coverage:

```bash
./gradlew test jacocoTestReport
# View report at: build/reports/jacoco/test/html/index.html
```

## Usage Tips

### Finding Block Hashes

To query trie logs, you need block hashes. These can be obtained from:
1. Besu's JSON-RPC API: `eth_getBlockByNumber`
2. Block explorers (Etherscan, etc.)
3. Database exploration tools

## Technical Details

### Database Format Support

Bric supports Besu's **Bonsai** and **Bonsai Archive** storage formats:
- **Bonsai**: Current state only, optimized for space
- **Bonsai Archive**: Current state + historical state diffs (trie logs)

### Key Encoding

Besu uses specific key encoding schemes:
- **Account Keys**: `Keccak256(address)` (32 bytes)
- **Storage Keys**: `Concat(AccountHash, SlotHash)` (64 bytes)
- **Code Keys**: Code hash (32 bytes)
- **Trie Log Keys**: Block hash (32 bytes)

### RLP Decoding

Account and storage data is RLP-encoded in the database:
- **Account**: `RLP[nonce, balance, storageRoot, codeHash]`
- **Storage**: `RLP[UInt256]`
- **Trie Logs**: Complex nested RLP structure parsed using `TrieLogFactoryImpl`

### Write Mode and Safety

Bric opens databases read-only unless you pass `--write` to `db open`. Write mode is required for `db put`, `db drop-cf`, `db compact`, and `db upgrade-flatdb`. Manual compaction jobs run asynchronously in the background (tracked via `db compact-status`), and the database won't close while one is still running. A flat DB heal (`db upgrade-flatdb`) can be safely interrupted with Ctrl-C — it checkpoints its progress and coordinates with shutdown to avoid corrupting the database or crashing the JVM.

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
