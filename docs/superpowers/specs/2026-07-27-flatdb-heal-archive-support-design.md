# Flat DB Heal: Support Bonsai Archive Databases

**Date:** 2026-07-27
**Status:** Approved

## Goal

Extend the existing `db upgrade-flatdb` command (see `docs/superpowers/specs/2026-07-24-flatdb-heal-design.md`) to also accept `BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE` databases, in addition to plain `BONSAI`. `FOREST` and `UNKNOWN` remain rejected.

Only the current-state flat tables (`ACCOUNT_INFO_STATE`, `ACCOUNT_STORAGE_STORAGE`) get healed — the archive-specific historical tables (`ACCOUNT_INFO_STATE_ARCHIVE`, `ACCOUNT_STORAGE_ARCHIVE`) are never read or written by this feature, on either format. This was confirmed as the intended scope: archive history is append-only, trie-log-derived data that this feature has no reason to touch.

## Why this is a small, low-risk change

Confirmed directly against the Besu 25.12.0 jar bric depends on (`besu-ethereum-core-25.12.0.jar`), not assumed from a newer snapshot (this codebase has hit real cross-version drift before — see `docs/superpowers/plans/2026-07-24-flatdb-heal.md`'s Task 4/8 fix history):

- `BonsaiArchiveFlatDbStrategy.putFlatAccount(...)` and `.putFlatAccountStorageValueByStorageSlotHash(...)` — the exact methods `FlatDbHealer` already calls via `BonsaiWorldStateKeyValueStorage.Updater` — write **only** to `ACCOUNT_INFO_STATE`/`ACCOUNT_STORAGE_STORAGE` (verified by disassembling their bytecode: the only `KeyValueSegmentIdentifier` field referenced in either method is `ACCOUNT_INFO_STATE` / `ACCOUNT_STORAGE_STORAGE`, never `*_ARCHIVE`). Besu's archive-specific logic lives entirely in the **read** path (`getFlatAccount`/`getFlatStorageValueByStorageSlotKey`, for historical-block lookups), which `FlatDbHealer` never calls.
- There is a single `BonsaiFlatDbStrategyProvider` class (no separate archive provider in 25.12.0); it picks `BonsaiFullFlatDbStrategy` vs `BonsaiArchiveFlatDbStrategy` based on the `DataStorageConfiguration` passed to its constructor.
- `BonsaiFlatDbStrategyProvider.upgradeToFullFlatDbMode(SegmentedKeyValueStorage)` — the method `FlatDbHealer.heal()` already calls at the end of a real run — branches internally on `dataStorageConfiguration.getDataStorageFormat()`: writes `FlatDbMode.FULL` for `DataStorageFormat.BONSAI`, or `FlatDbMode.ARCHIVE` for `DataStorageFormat.X_BONSAI_ARCHIVE` (confirmed by bytecode: both branches present, each logging "setting FlatDbStrategy to FULL"/"...to ARCHIVE" respectively and writing the corresponding `FlatDbMode` byte). So the same call site is already correct for both formats, as long as construction used the matching config.
- `DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG` exists as a public static constant in 25.12.0 (confirmed via `javap`), alongside the already-used `DEFAULT_BONSAI_CONFIG`.

The trie walk itself (`StoredMerklePatriciaTrie` over `TRIE_BRANCH_STORAGE`, `RangeManager`/`RangeStorageEntriesCollector`) and the checkpoint/resumability mechanics are entirely format-agnostic — they don't reference `DataStorageConfiguration` or `KeyValueSegmentIdentifier` archive variants anywhere.

## The change

### `FlatDbHealer` constructor becomes format-aware

`src/main/java/net/consensys/bric/besu/FlatDbHealer.java` currently hardcodes:
```java
BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
    new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
```

Change to select the config based on the actual database's format:
```java
DataStorageConfiguration dataStorageConfiguration =
    dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE
        ? DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG
        : DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;
BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
    new NoOpMetricsSystem(), dataStorageConfiguration);
```

No other line in the constructor, `heal()`, `healAccountRange()`, or `healAccountStorage()` changes — they're all already format-agnostic, per the verification above.

### `DbUpgradeFlatDbCommand`'s format guard accepts both

`src/main/java/net/consensys/bric/commands/DbUpgradeFlatDbCommand.java` currently rejects anything that isn't exactly `BONSAI`:
```java
if (format != BesuDatabaseManager.DatabaseFormat.BONSAI) {
```
becomes:
```java
if (format != BesuDatabaseManager.DatabaseFormat.BONSAI
        && format != BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE) {
```
The error message (`"...only supported for Bonsai databases. Current format: " + format`) stays as-is — it's still accurate for the remaining rejected cases (`FOREST`, `UNKNOWN`).

## Testing

- `FlatDbHealerTest`: a new test constructing `FlatDbHealer` against a fixture database whose column families make it detect as `BONSAI_ARCHIVE` (add `ACCOUNT_INFO_STATE_ARCHIVE` to the fixture's column family list, per `BesuDatabaseManager.detectDatabaseFormat()`'s existing rule), run a heal, and assert: (a) the account/storage fix is applied correctly to `ACCOUNT_INFO_STATE`/`ACCOUNT_STORAGE_STORAGE` exactly as the existing plain-Bonsai tests already verify, and (b) the persisted `flatDbStatus` byte after a real (non-dry-run) heal is `FlatDbMode.ARCHIVE`'s byte (`0x02`, per the existing `readFlatDbMode()` decoding already used elsewhere in bric), not `FULL`'s (`0x01`) — this is the one behavior that's actually format-conditional and worth a direct assertion. Also assert the archive tables (`ACCOUNT_INFO_STATE_ARCHIVE`/`ACCOUNT_STORAGE_ARCHIVE`, if present in the fixture) are completely untouched (no keys written) after the heal, to lock in the "archive columns don't need to be touched" requirement as a regression test, not just an assumption.
- `DbUpgradeFlatDbCommandTest`: extend the existing "refuses when not Bonsai format" test to check `BONSAI_ARCHIVE` no longer triggers that rejection (mock `getFormat()` returning `BONSAI_ARCHIVE`, confirm execution proceeds past the guard), while `FOREST`/`UNKNOWN` still do.

## Out of scope

Anything involving the archive tables themselves (`ACCOUNT_INFO_STATE_ARCHIVE`/`ACCOUNT_STORAGE_ARCHIVE`) — healing historical entries, verifying them against per-block trie state, etc. — is explicitly not part of this change, per the stated requirement that archive columns don't need to be touched.
