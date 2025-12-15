# Bric - Besu Database Explorer REPL

A command-line REPL tool for exploring Hyperledger Besu databases. Query account data, storage slots, contract bytecode, and trie logs (state diffs) directly from Besu's RocksDB database.

## Features

- 🔍 **Account Queries** - View account balance, nonce, storage root, and code hash
- 💾 **Storage Exploration** - Query contract storage slots by address and slot number
- 📜 **Bytecode Access** - Retrieve and save contract bytecode
- 📊 **Trie Log Analysis** - Examine state changes (diffs) per block with detailed formatting
- 🗄️ **Database Management** - Read-only access to Besu Bonsai and Bonsai Archive databases
- 📈 **Database Statistics** - View column family sizes and key counts

## Requirements

- Java 21 or higher
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

## Quick Start

```bash
# Build the project
./gradlew build

# Run the REPL
./gradlew run

# In the REPL, open a Besu database
bric> db-open /path/to/besu/database

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

#### `db-open <path>`
Open a Besu database in read-only mode. Automatically detects database format (Bonsai or Bonsai Archive).

**Examples:**
```
db-open /path/to/besu/database
db-open ~/besu-data/database
```

#### `db-close`
Close the currently open database.

#### `db-info`
Display detailed database statistics including column family sizes and estimated key counts.

### Account Commands

#### `account <address>`
Query account information by Ethereum address. Displays nonce, balance (in Wei and ETH), storage root, and code hash.

**Examples:**
```
account 0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0
account 0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045
```

#### `account <hash> --hash`
Query account information by raw account hash (for debugging).

**Example:**
```
account 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef --hash
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

#### `trielog <block-hash>`
Query trie log (state diff) for a specific block. Shows detailed state changes including:
- **Account Changes**: Created, updated, or deleted accounts with balance/nonce/storage root changes
- **Code Changes**: Deployed or cleared contract code
- **Storage Changes**: Storage slot modifications

**Example:**
```
trielog 0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
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

**Note:** Trie logs are only available in Besu Bonsai Archive databases.

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

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.
