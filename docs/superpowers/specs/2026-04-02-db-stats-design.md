# db stats subcommand — Design Spec

**Date:** 2026-04-02  
**Status:** Approved

---

## Overview

Add a `db stats` subcommand to the existing `db` command that prints detailed RocksDB statistics for one or all column families. This is distinct from `db info`, which shows a high-level summary table (estimated key counts, SST/blob sizes). `db stats` targets deeper diagnostics: compaction history, level breakdown, SST file listing, and open file counts.

---

## Command Syntax

```
db stats [cf-name]
```

- **No argument**: prints stats for all non-empty column families
- **With `cf-name`**: prints stats for a single CF; supports the same segment/UTF-8/hex formats as `db get`

---

## Output Sections

### 1. DB-Level Summary (shown once at the top)

```
DB-Level Stats:
  Open SST files:  142 / 5000 (max_open_files)
```

- **Open SST file count**: obtained via `db.getLiveFilesMetaData().size()`
- **max_open_files**: parsed from the `OPTIONS` file in the DB directory (key: `max_open_files`); shown as `unlimited` if `-1`

### 2. Per-Column-Family Sections (repeated for each CF)

Each CF section has three sub-sections separated by a header:

#### 2a. Full RocksDB Stats

Raw output of `db.getProperty(handle, "rocksdb.stats")`. Includes:
- Compaction stats per level (files, size, score, read/write GB, amplification)
- Stall counts
- File read latency histogram

#### 2b. Level Stats

Raw output of `db.getProperty(handle, "rocksdb.levelstats")`. Compact table showing file count and size per level.

#### 2c. SST Files

Raw output of `db.getProperty(handle, "rocksdb.sstables")`. Lists each SST file per level with size.

---

## Implementation

### New class: `DbStatsCommand`

- Location: `src/main/java/net/consensys/bric/commands/DbStatsCommand.java`
- Constructor: `DbStatsCommand(BesuDatabaseManager dbManager)`
- Follows the same pattern as `DbInfoCommand` (implements `Command`, checks `dbManager.isOpen()`)
- Accepts optional CF name argument (`args.length > 0`)
- If CF name provided, resolves via `ColumnFamilyResolver` (same as `DbGetCommand`) to support segment/UTF-8/hex formats
- Skips CFs with 0 estimated keys and 0 total size unless a specific CF was requested

### Stats retrieval in `DbStatsCommand`

Following the same pattern as `DbGetCommand`, stats properties are fetched directly via `dbManager.getDatabase().getProperty(handle, ...)`:
- `rocksdb.stats` — full compaction/level/stall stats
- `rocksdb.levelstats` — compact per-level file count and size table
- `rocksdb.sstables` — SST file listing per level

Live file count is obtained via `dbManager.getDatabase().getLiveFilesMetaData().size()`.

No new methods are added to `BesuDatabaseManager` for the property strings.

### New method: `BesuDatabaseManager.getMaxOpenFiles()`

Parses `max_open_files` from the `OPTIONS` file in `currentPath`. Returns `-1` (displayed as `unlimited`) if not found or if the value is `-1`. The OPTIONS file is a plain-text file RocksDB writes to the DB directory on open.

### `DbCommand` changes

- Add `statsCommand` field (constructed same as other subcommands)
- Add `case "stats":` to the switch in `execute()`
- Add `db stats [cf-name]` entry to `getUsage()`

---

## Error Handling

- If no database is open: print error and return (same as other commands)
- If a named CF is not found: print error listing available CFs
- If a property returns null (e.g. `rocksdb.levelstats` not available): skip that section silently with a note
- If OPTIONS file can't be parsed: show `max_open_files: unknown`

---

## Testing

- `DbStatsCommandTest` mocking `BesuDatabaseManager`
- Verify: no-arg path iterates CFs, single-CF path resolves and prints, db-not-open prints error
- No test for actual RocksDB property values (property strings come from native RocksDB)
