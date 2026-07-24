# Flat Database Heal (Partial → Full Upgrade)

**Date:** 2026-07-24
**Status:** Approved

## Goal

Add a bric subcommand that upgrades a Bonsai database's flat DB from PARTIAL to FULL by reconciling `ACCOUNT_INFO_STATE` / `ACCOUNT_STORAGE_STORAGE` against the canonical account/storage tries, reusing Besu's own trie-walk-and-diff building blocks rather than reimplementing trie traversal from scratch.

### Why this needs its own implementation, not just a Besu CLI flag

Besu's `BonsaiWorldStateKeyValueStorage.upgradeToFullFlatDbMode()` / the `--Xbonsai-full-flat-db-enabled` flag only flip the persisted `flatDbStatus` metadata byte — they do **not** backfill missing or divergent flat entries. The actual backfill ("flat database healing") lives in `SnapWorldDownloadState.startFlatDatabaseHeal()` / `AccountFlatDatabaseHealingRangeRequest` / `StorageFlatDatabaseHealingRangeRequest`, in Besu's `besu-ethereum-eth` module. That code is peer-network-driven (snap sync range requests + Merkle proofs) and not part of Besu's plugin API — it can't be invoked directly from an external tool. What *is* reusable is the underlying trie/storage machinery those classes are built on (`StoredMerklePatriciaTrie`, `RangeStorageEntriesCollector`/`TrieIterator`, `RangeManager`, `BonsaiWorldStateKeyValueStorage`), all in `besu-ethereum-trie`/`besu-ethereum-core`, which bric already depends on.

Out of scope: Bonsai Archive flat DB healing (`ARCHIVE` mode), Forest databases (no flat DB concept), parallelizing range processing.

## User-facing surface

New flat subcommand of `db`, following the `db compact` / `db drop-cf` pattern:

`db upgrade-flatdb [--dry-run]`

- Requires the database open (`db open <path> --write`) unless `--dry-run`, which works read-only (no writes to skip guarding against).
- Only supported for `BesuDatabaseManager.DatabaseFormat.BONSAI` (rejects `BONSAI_ARCHIVE`, `FOREST`, `UNKNOWN`).
- Prints per-range progress and a final summary; resumable if interrupted (Ctrl-C, crash) via a checkpoint stored in `VARIABLES`.

### Output example

```
> db upgrade-flatdb
Healing flat database against state root 0xabc123... (block 19234567)
Range 1/16 — 8,412 accounts checked, 3 fixed (2 updated, 1 removed)
Range 2/16 — 8,390 accounts checked, 0 fixed
...
Range 16/16 — 8,401 accounts checked, 1 fixed (1 added)
Healing storage for 4 flagged accounts...
Storage: 512 slots checked, 6 fixed (4 updated, 2 removed)
Flat DB mode upgraded to FULL.
Done in 00:04:12. Accounts: 4 fixed. Storage slots: 6 fixed.
```

```
> db upgrade-flatdb --dry-run
Dry run — no changes will be written.
Range 1/16 — 8,412 accounts checked, 3 divergent (2 to update, 1 to remove)
...
Would fix 4 accounts and 6 storage slots. Flat DB mode is currently PARTIAL.
```

## Architecture

### New classes (all under `net.consensys.bric.besu`)

#### `NoOpKeyValueStorage`

Minimal `implements org.hyperledger.besu.plugin.services.storage.KeyValueStorage` where every method either no-ops or throws `UnsupportedOperationException`. Needed only because `BonsaiWorldStateKeyValueStorage`'s constructor requires a `KeyValueStorage` for trie logs; healing never touches `TRIE_LOG_STORAGE`, so a real implementation isn't needed.

#### `FlatDbHealer`

Owns the heal algorithm. Constructed with `BesuDatabaseManager dbManager`.

```java
public FlatDbHealer(BesuDatabaseManager dbManager) {
  this.storage = new RocksDBSegmentedStorage(dbManager);
  var flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
      new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
  flatDbStrategyProvider.loadFlatDbStrategy(storage); // derives current FlatDbMode from TRIE_BRANCH_STORAGE
  this.worldState = new BonsaiWorldStateKeyValueStorage(
      flatDbStrategyProvider, storage, new NoOpKeyValueStorage(),
      FlatDbCacheManager.NO_OP_CACHE, 0L);
}
```

This is Besu's real `BonsaiWorldStateKeyValueStorage` (the "raw" 5-arg constructor, which takes a `SegmentedKeyValueStorage` directly rather than Besu's heavier `StorageProvider` abstraction), driven by bric's existing `RocksDBSegmentedStorage` adapter. It gives us, for free, Besu's own:
- `getWorldStateRootHash()` — reads the persisted target root from key `"worldRoot"` in `TRIE_BRANCH_STORAGE` (`PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY`). This is the state root a live Besu node's flat DB should match — using it means no block-header RLP parsing is needed.
- `getAccountStateTrieNode(Bytes location, Bytes32 nodeHash)` / `getAccountStorageTrieNode(Hash accountHash, Bytes location, Bytes32 nodeHash)` — trie node loaders reading `TRIE_BRANCH_STORAGE`.
- `updater()` → `BonsaiWorldStateKeyValueStorage.Updater` with `putAccountInfoState(Hash, Bytes)`, `removeAccountInfoState(Hash)`, `putStorageValueBySlotHash(Hash, Hash, Bytes)`, `removeStorageValueBySlotHash(Hash, Hash)`.
- `upgradeToFullFlatDbMode()` — the metadata flag-flip, called once at the end.

**`heal(boolean dryRun, ProgressListener listener) -> HealResult`** — the entry point.

1. `Bytes32 stateRoot = storage's worldState.getWorldStateRootHash()...orElseThrow(...)` — fail with a clear error if absent (empty database).
2. Load an existing checkpoint (§ Resumability). If present and its recorded state root still matches, resume from the checkpoint's next-range-start; otherwise start fresh from range 0.
3. `Map<Bytes32, Bytes32> ranges = RangeManager.generateAllRanges(16)` (Besu's own class, reused directly — same 16-way split Besu's snap sync heal uses).
4. For each remaining range `[start, end]`, call `healAccountRange(stateRoot, start, end, dryRun)`, returning counts and the set of account hashes that diverged. Accumulate diverged accounts across all ranges into `accountsToHealStorage`.
5. After all account ranges complete, for each account in `accountsToHealStorage`, call `healAccountStorage(accountHash, storageRoot, dryRun)` (storage root taken from the now-corrected account RLP).
6. If not dry-run: call `worldState.upgradeToFullFlatDbMode()`, clear the checkpoint, and print the mode confirmation.
7. Return a `HealResult` (accounts added/updated/removed, slots added/updated/removed, elapsed time).

**`healAccountRange(stateRoot, startKeyHash, endKeyHash, dryRun) -> RangeResult`** — mirrors `AccountFlatDatabaseHealingRangeRequest.doPersist()` minus the peer-proof step:

```java
MerkleTrie<Bytes, Bytes> accountTrie = new StoredMerklePatriciaTrie<>(
    worldState::getAccountStateTrieNode, stateRoot, Function.identity(), Function.identity());

RangeStorageEntriesCollector collector = RangeStorageEntriesCollector.createCollector(
    startKeyHash, endKeyHash, Integer.MAX_VALUE, Integer.MAX_VALUE);
TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
NavigableMap<Bytes32, Bytes> trieAccounts = (NavigableMap<Bytes32, Bytes>) accountTrie.entriesFrom(
    root -> RangeStorageEntriesCollector.collectEntries(collector, visitor, root, startKeyHash));

NavigableMap<Bytes32, Bytes> flatAccounts = readFlatRange(ACCOUNT_INFO_STATE, startKeyHash, endKeyHash);
```

Diff: for each `(hash, trieValue)` in `trieAccounts` where `flatAccounts.get(hash)` is absent or different → `updater.putAccountInfoState(Hash.wrap(hash), trieValue)`, mark divergent. For each `hash` in `flatAccounts` not present in `trieAccounts` → `updater.removeAccountInfoState(Hash.wrap(hash))`, mark divergent (its storage is not re-healed, since the account no longer exists). Skip both write calls when `dryRun`. Commit the range's updater transaction (if not dry-run) before returning, and persist the checkpoint (§ Resumability).

Unlike Besu's networked version — which caps each request at `getLocalFlatAccountCountToHealPerRequest()` (128) and recurses via `getChildRequests()` because it's paying a network round-trip per batch — bric reads and diffs an entire range in one local pass (`Integer.MAX_VALUE` limit above), since there's no round-trip cost to amortize. A single range in production-sized state could still be tens of millions of accounts; if that proves memory-heavy in practice, this is the one place to add sub-batching (see Design decisions).

**`healAccountStorage(accountHash, storageRoot, dryRun) -> StorageResult`** — identical shape, but:
- Trie: `new StoredMerklePatriciaTrie<>((location, hash) -> worldState.getAccountStorageTrieNode(accountHash, location, hash), storageRoot, ...)`, walked over the full range (`RangeManager.MIN_RANGE`..`MAX_RANGE`) since a single account's storage is bounded.
- Flat side: `ACCOUNT_STORAGE_STORAGE`, keyed on disk by `accountHash + slotHash` (bric's existing `SegmentReader.computeStorageKey` convention, 64 bytes total). Reading this range means seeking `streamFromKey` between `accountHash+MIN_RANGE` and `accountHash+MAX_RANGE`, then **stripping the leading 32-byte `accountHash` from each returned key** to get a bare `slotHash` — the storage trie's leaf keys (from `entriesFrom`) are bare 32-byte slot hashes, so the two maps must be keyed the same way (`Bytes32` slot hash) before they can be diffed.
- Writes: `updater.putStorageValueBySlotHash(accountHash, Hash.wrap(slotHash), value)` / `removeStorageValueBySlotHash(accountHash, Hash.wrap(slotHash))`.

`readFlatRange(segment, startKeyHash, endKeyHash)` — reads existing flat entries for a key range via `RocksDBSegmentedStorage.streamFromKey(segment, startKey, endKey)` (new; see below), building a `NavigableMap<Bytes32, Bytes>`. For `ACCOUNT_INFO_STATE` the on-disk key *is* the account hash, so no stripping is needed; for `ACCOUNT_STORAGE_STORAGE` the caller (`healAccountStorage`) strips the `accountHash` prefix as above so both maps end up keyed by the same bare 32-byte hash and can be diffed directly.

### `RocksDBSegmentedStorage` changes

Currently every write/stream method throws `UnsupportedOperationException` (bric is read-only today). This feature is the first write consumer, so implement, each guarded by `dbManager.isWritable()` (throw the existing message otherwise):

- **`startTransaction()`** — returns a `SegmentedKeyValueStorageTransaction` wrapping a `org.rocksdb.WriteBatch`: `put(segment, key, value)` → `batch.put(cfHandle, key, value)`, `remove(segment, key)` → `batch.delete(cfHandle, key)`, `commit()` → `dbManager.getDatabase().write(writeOptions, batch)` then `batch.close()`, `rollback()` → `batch.clear()`.
- **`tryDelete(segment, key)`** — direct `db.delete(cfHandle, key)`, returns `true`/`false` on success/`RocksDBException`.
- **`clear(segment)`** — out of scope for this feature (no caller needs it); leave throwing, matching today's behavior.
- **`streamFromKey(segment, startKey, endKey)`** — bounded `RocksIterator` scan (`iterator.seek(startKey)`, advance while `key <= endKey`), same iteration style as `SegmentReader`'s existing `iterateKeyValueFrom`, returned as `Stream<Pair<byte[], byte[]>>`.

`stream()` / `streamKeys()` / `getAllKeysThat` / `getAllValuesFromKeysThat` stay unimplemented — nothing in this feature calls them.

### Resumability

Checkpoint stored as a single key in `VARIABLES` (e.g. `"bricFlatDbHealCheckpoint"`), value = RLP or simple concatenation of `[stateRoot (32 bytes)][nextRangeIndex (1 byte, 0-16)][nextStartKeyWithinRange (32 bytes, optional)]`. Written after each range commits (account phase) and after each account's storage heal commits (storage phase — tracked as a pending-accounts-to-heal list, so a crash mid-storage-phase doesn't force re-walking all 16 account ranges).

On start: read the checkpoint; if its `stateRoot` no longer matches `worldState.getWorldStateRootHash()` (chain has moved on since the interrupted run), discard it and start over — resuming against a stale root would produce wrong results. Otherwise resume from the recorded position. Cleared on successful full completion.

`--dry-run` never reads or writes the checkpoint — it's a single-pass report, independent of any in-progress real heal.

### `commands/DbUpgradeFlatDbCommand`

`db upgrade-flatdb [--dry-run]`

1. Guard: DB open. Guard: format is `BONSAI` (error otherwise — Archive/Forest unsupported).
2. If not `--dry-run`: guard DB writable (same message pattern as `DbCompactCommand`).
3. Construct `FlatDbHealer`, call `heal(dryRun, progressListener)` where `progressListener` prints the per-range/per-phase lines shown above.
4. Print the final summary from `HealResult`.

### Wiring

- `DbCommand`: new field `DbUpgradeFlatDbCommand`, new switch case `"upgrade-flatdb"`, `getUsage()` entry.
- `BricCommandProcessor` autocomplete: add `upgrade-flatdb` to the `db` subcommand list.

## Data flow

1. User runs `db open /path/to/db --write` then `db upgrade-flatdb`.
2. `FlatDbHealer` reads the target state root from `TRIE_BRANCH_STORAGE["worldRoot"]`.
3. For each of 16 key ranges: walk the account trie locally (reading `TRIE_BRANCH_STORAGE`), read the same key range from `ACCOUNT_INFO_STATE`, diff, write corrections through a `SegmentedKeyValueStorageTransaction` (RocksDB `WriteBatch`), commit, checkpoint.
4. For every account found divergent, walk its storage trie the same way against `ACCOUNT_STORAGE_STORAGE`, diff, write, commit, checkpoint.
5. Once complete, flip `flatDbStatus` to `FULL` via `worldState.upgradeToFullFlatDbMode()`, clear the checkpoint, print the summary.

If interrupted at any point (Ctrl-C, crash, `kill`): the last-committed range/account is durable (RocksDB `WriteBatch` commits are atomic), and the checkpoint records exactly where to resume. Re-running `db upgrade-flatdb` continues from there, provided the state root hasn't changed (i.e., provided Besu hasn't been restarted and advanced the chain in the meantime).

## Error handling

| Condition | Behavior |
|---|---|
| No database open | `Error: No database is open. Use 'db open <path>' first.` |
| Format is not BONSAI | `Error: Flat DB healing is only supported for Bonsai databases. Current format: <FORMAT>` |
| Not `--dry-run` and DB read-only | `Error: Database is open in read-only mode. Reopen with 'db open <path> --write'.` |
| No persisted world state root (`"worldRoot"` key absent) | `Error: No world state root found; database may be empty or not yet synced.` |
| Checkpoint's state root doesn't match current world state root | Discard checkpoint, log `Chain has advanced since the last interrupted run; restarting heal from the beginning.`, proceed from range 0 |
| Interrupted mid-run (Ctrl-C / process kill) | No special handling needed — last commit is durable, checkpoint reflects it; next invocation resumes |

## Testing

JUnit 5 + AssertJ + Mockito, matching existing test conventions (`BesuFlatDbReaderTest`, `DbCompactCommandTest`).

### `RocksDBSegmentedStorageTest` (extend existing)

- `startTransaction()` + `put`/`remove` + `commit()` against a temp RocksDB instance actually persists/removes the expected keys.
- All write methods throw when `dbManager.isWritable()` is false (preserves today's read-only guarantee).
- `streamFromKey(segment, start, end)` returns exactly the entries in `[start, end]`, in key order.

### `FlatDbHealerTest`

- Build a small synthetic Bonsai DB (temp RocksDB, real column families) with a complete account+storage trie but deliberately-corrupted flat entries: one missing account, one account with a stale/wrong flat value, one flat entry with no corresponding trie leaf (orphan to remove), one account with a corrupted storage slot.
- `heal(dryRun=false, ...)`: assert `ACCOUNT_INFO_STATE`/`ACCOUNT_STORAGE_STORAGE` now exactly match trie-derived truth; assert `flatDbStatus` is `FULL` afterward; assert `HealResult` counts match.
- `heal(dryRun=true, ...)`: assert flat DB is byte-for-byte unchanged; assert reported counts match the real-run counts; assert `flatDbStatus` unchanged.
- Resume: run `heal()`, simulate interruption after the first range commits (inject a listener that throws after range 1), assert the checkpoint was written; run `heal()` again, assert it resumes from range 2 rather than re-processing range 1 (spy/verify no redundant writes to range-1 keys).
- Stale checkpoint: write a checkpoint recording a state root that no longer matches, assert the next `heal()` call discards it and starts from range 0.
- Rejects non-Bonsai formats at the command layer (`DbUpgradeFlatDbCommandTest`).

### `DbUpgradeFlatDbCommandTest`

- Guard order: closed DB → error; non-Bonsai format → error; read-only + non-dry-run → error; `--dry-run` succeeds read-only.
- Happy path prints expected progress/summary lines (mock `FlatDbHealer`).

### Manual verification (no automated coverage)

- Run against a real partial-flat-db Bonsai snapshot (if one is available), confirm `db info`'s `Flat DB Mode` changes from `PARTIAL` to `FULL` and spot-check a few accounts/slots via `account`/`storage` commands before and after.

## Design decisions

**Why drive Besu's trie/storage classes directly instead of trying to reuse `SnapWorldDownloadState`/`AccountFlatDatabaseHealingRangeRequest` as-is.** Those classes live in `besu-ethereum-eth`, are not part of the plugin API, and are tightly coupled to snap-sync's networking and peer-proof machinery (`EthContext`, `SnapSyncStatePersistenceManager`, `WorldStateProofProvider` proof verification against peer-supplied data). None of that fits a local, single-process CLI tool reading its own already-complete-except-for-flat-db data. The actual repair logic inside `doPersist()` — trie walk, diff, write — depends only on `besu-ethereum-trie`/`besu-ethereum-core` classes bric already has on its classpath, so reusing *those* gives the same correctness guarantees without the networking baggage.

**Why not use `SegmentedKeyValueStorage.stream()`/proof verification at all.** Besu's networked heal reads local data and *verifies* it against a peer-supplied Merkle proof before deciding whether to trust it or re-derive from the trie. Bric has no peer and no need to distrust its own local trie — the trie itself (read from `TRIE_BRANCH_STORAGE`) already contains all the information needed to know what the flat DB should look like. Skipping the proof step isn't cutting a corner; the proof step exists to answer a question (can I trust the data I got over the network?) bric doesn't have.

**Why whole-range reads instead of Besu's 128-account-per-batch pagination.** Besu paginates because each batch costs a network round-trip to a peer. Bric's reads are all local RocksDB iterator calls, so there is no round-trip cost to amortize — one pass per range is simpler and about as fast. If profiling on a large mainnet-size database shows the whole-range in-memory diff is a memory problem, adding sub-batching within `healAccountRange` is a contained follow-up (the collector/visitor already support a `limit` parameter for exactly this).

**Why per-range/per-account checkpointing in `VARIABLES` instead of no persistence.** A full mainnet-size state walk is a genuinely long-running operation; unlike `db compact` (which can simply be re-submitted if interrupted, since compaction is idempotent and RocksDB tracks its own progress), an interrupted heal with no checkpoint would force restarting the entire trie walk from scratch. Storing a small checkpoint costs one key and a few writes per range/account and avoids that.

**Why `--dry-run` works without `--write` but the real heal doesn't.** Dry-run performs no writes at all, so there's nothing for the existing read-only safety guarantee to protect against. Letting users check the scale of drift without first committing to a write-mode session (which the existing `--write` warning flags as a mode change) is strictly more convenient with no added risk.

**Why no new locking logic.** RocksDB's own file lock already prevents opening the same data directory writable from two processes at once, so bric attempting `db open ... --write` while a real Besu node has the same database open will already fail cleanly at `openDatabase()` — no bric-specific concurrency guard is needed. Operators should stop Besu before running this command; that's a usage note, not a code path.
