# Design: Support Arbitrary Column Families in DB Subcommands

**Date**: 2026-03-16
**Status**: Design Phase

## Overview

Extend `db drop-cf`, `db get`, `db put`, and `db scan` commands to accept arbitrary column family names (string UTF-8 or hexadecimal) in addition to predefined `KeyValueSegmentIdentifier` enum values. Auto-detect format: if input starts with `0x`, parse as hex bytes; otherwise treat as UTF-8 string.

## Motivation

Currently, bric only supports operations on predefined column families defined in the `KeyValueSegmentIdentifier` enum. Users cannot interact with custom or unknown column families in a database, limiting flexibility when working with non-standard RocksDB instances or exploring unfamiliar databases.

This feature enables users to:
- Drop arbitrary column families without prior knowledge of their names
- Read and write raw data to arbitrary column families
- Scan arbitrary column families for exploration
- Work with custom RocksDB schemas

## Architecture

### 1. New Component: ColumnFamilyResolver

**Purpose**: Parse and resolve column family identifiers from user input with auto-detection.

**Class**: `net.consensys.bric.db.ColumnFamilyResolver`

**Public Methods**:

```java
/**
 * Parse and resolve a column family identifier from user input.
 *
 * Auto-detection rules:
 * - If input starts with "0x": parse as hex bytes
 * - Otherwise: treat as UTF-8 string
 *
 * Resolution order:
 * 1. Try to resolve as KeyValueSegmentIdentifier enum
 * 2. Try to resolve as column family name in database
 * 3. Return null if not found
 *
 * @param dbManager the database manager
 * @param input the user input (e.g., "TRIE_LOG_STORAGE", "0x0a", "MY_CUSTOM_CF")
 * @return ColumnFamilyHandle if found, null otherwise
 * @throws IllegalArgumentException if input format is invalid
 */
static ColumnFamilyHandle resolveColumnFamily(BesuDatabaseManager dbManager, String input)
    throws IllegalArgumentException
```

**Implementation Details**:
- Detect format: check if input starts with `0x` (case-insensitive)
- Parse hex: validate format (even number of chars, all valid hex), convert to bytes
- Parse UTF-8: use string as-is
- Try enum lookup first: check `KeyValueSegmentIdentifier` by name or id
- Try arbitrary lookup: check `handlesByName` with resolved string/hex
- Return null if not found; throw exception only for invalid input

### 2. BesuDatabaseManager Enhancements

**New Methods**:

```java
/**
 * Get column family handle by raw name bytes.
 * Used for arbitrary column families that aren't in the enum.
 *
 * @param cfNameBytes the column family name as bytes
 * @return ColumnFamilyHandle if found, null otherwise
 */
public ColumnFamilyHandle getColumnFamilyByNameBytes(byte[] cfNameBytes)
```

**Rationale**: The internal `handlesByName` map uses string keys (display names), but arbitrary CFs may need byte-level lookup. This method provides fallback resolution.

### 3. DB Command Updates

#### DbDropCfCommand
- Accept arbitrary CF identifiers via `ColumnFamilyResolver`
- Guard remains: cannot drop default CF
- Updated usage: `db drop-cf <segment|name|hex>`
- Examples:
  - `db drop-cf TRIE_LOG_STORAGE` (enum)
  - `db drop-cf "CUSTOM_LOGS"` (UTF-8)
  - `db drop-cf 0x0a` (hex)

#### DbGetCommand
- Accept arbitrary CF identifiers
- Display raw bytes (hex with optional ASCII fallback)
- Updated usage: `db get <segment|name|hex> <key-hex>`
- Example: `db get 0x0a 0xabcdef1234` returns hex-formatted value

#### DbPutCommand
- Accept arbitrary CF identifiers
- Create CF if needed (writable mode only)
- Updated usage: `db put <segment|name|hex> <key-hex> <value-hex>`
- Example: `db put "CUSTOM_CF" 0xabcd 0xdeadbeef`

#### DbScanCommand
- Accept arbitrary CF identifiers
- Iterate raw key-value pairs, display as hex
- Updated usage: `db scan <segment|name|hex> [options]`
- Example: `db scan 0x0a --limit 10`

### 4. Output Handling

**Raw Display Format** (for arbitrary CFs):
```
Key: 0xabcdef1234
Value: 0xdeadbeef
```

For read commands (`db get`, `db scan`), display is always hex. No RLP decoding or special formatting applied to arbitrary CFs.

### 5. Error Handling

| Scenario | Error Message | Handling |
|----------|---------------|----------|
| Invalid hex format | `Invalid hex format: 0xZZ (expected even-length hex)` | Throw `IllegalArgumentException` |
| CF not found (read) | `Column family not found: CUSTOM_CF` | Return/show error |
| CF not found (write) | Create new CF | For `put` operations, create if needed |
| Invalid UTF-8 in display | Fall back to hex representation | Transparent to user |
| Multiple matching CFs | Use first match (enum, then database) | Document order in help |

### 6. Implementation Phases

**Phase 1: Foundation**
- Create `ColumnFamilyResolver` with full parsing and resolution logic
- Enhance `BesuDatabaseManager` with byte-level CF lookup
- Write unit tests for resolver

**Phase 2: Drop Command**
- Update `DbDropCfCommand` to use resolver
- Update help text and usage
- Add tests

**Phase 3: Read Commands**
- Update `DbGetCommand` to use resolver
- Update `DbScanCommand` to use resolver
- Add tests

**Phase 4: Write Commands**
- Update `DbPutCommand` to use resolver
- Add CF creation logic for writable mode
- Add tests

**Phase 5: Integration**
- Update tab completion in `BricCompleter` (for enum-based CFs only)
- Update main help text
- Add integration tests

## Testing Strategy

**Unit Tests**:
- `ColumnFamilyResolverTest`: Parsing, resolution, error cases
- `DbDropCfCommandTest`: Arbitrary CF drops
- `DbGetCommandTest`: Arbitrary CF reads
- `DbPutCommandTest`: Arbitrary CF writes with creation
- `DbScanCommandTest`: Arbitrary CF scans

**Integration Tests**:
- Test with real RocksDB instances containing custom CFs
- Verify round-trip: put → get for arbitrary CFs
- Verify scan output format for arbitrary CFs

## Backward Compatibility

- All existing enum-based commands continue to work unchanged
- Resolver tries enum first, so `db drop-cf TRIE_LOG_STORAGE` still works
- No changes to command API for existing use cases
- Help text updated but usage remains intuitive

## Future Extensions

- Add `--format` flag to control output (hex, ASCII, raw bytes)
- Add CF information to `db info` (list all CFs with sizes)
- Support creating new empty CFs via `db create-cf`
- Support renaming or copying CFs

---

## Files to Create/Modify

### Create
- `src/main/java/net/consensys/bric/db/ColumnFamilyResolver.java`
- `src/test/java/net/consensys/bric/db/ColumnFamilyResolverTest.java`

### Modify
- `src/main/java/net/consensys/bric/db/BesuDatabaseManager.java`
- `src/main/java/net/consensys/bric/commands/DbDropCfCommand.java`
- `src/main/java/net/consensys/bric/commands/DbGetCommand.java`
- `src/main/java/net/consensys/bric/commands/DbPutCommand.java`
- `src/main/java/net/consensys/bric/commands/DbScanCommand.java`
- `src/test/java/net/consensys/bric/commands/DbDropCfCommandTest.java`
- `src/test/java/net/consensys/bric/commands/DbGetCommandTest.java`
- `src/test/java/net/consensys/bric/commands/DbPutCommandTest.java`
- `src/test/java/net/consensys/bric/commands/DbScanCommandTest.java`
