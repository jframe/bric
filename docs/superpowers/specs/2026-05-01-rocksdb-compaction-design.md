# RocksDB Compaction Subcommands

**Date:** 2026-05-01
**Status:** Approved

## Goal

Add bric subcommands that let a user force a RocksDB manual compaction on one or more column families, see the status of running and historical jobs, and cancel a running job. Compactions run asynchronously so the REPL stays usable.

Out of scope: range-based compaction, persisting job state across `bric` restarts, a non-interactive CLI batch mode (the user can wrap `bric` in `tmux`/`screen` for long jobs that need to survive SSH drops).

## User-facing surface

Three new flat subcommands of `db`, mirroring the existing pattern (`db drop-cf`, `db stats`):

- `db compact <segment...|--all>` — submit one async compaction job per resolved column family. Prints the assigned job ID for each and returns immediately.
- `db compact-status [<job-id>]` — without an argument, list every job (RUNNING / DONE / FAILED / CANCELLED) submitted in the current DB session. With a job ID, print the same row plus a per-CF detail block including RocksDB's `compaction-stats` property.
- `db compact-cancel <job-id>` — interrupt a running job. RocksDB's `CompactRangeOptions.setCanceled(true)` is per-compaction, so cancelling one job does not affect any others.

`db compact` and `db compact-cancel` require the database to be open in `--write` mode (compaction mutates SST files). `db compact-status` works in any mode — it reads in-memory job state and a RocksDB property, neither of which requires write access. In read-only mode the job list will simply be empty.

### Output examples

`db compact ACCOUNT_INFO_STATE ACCOUNT_STORAGE_STORAGE`:

```
Submitted job 1: ACCOUNT_INFO_STATE
Submitted job 2: ACCOUNT_STORAGE_STORAGE
```

`db compact-status`:

```
JOB  CF                       STATE    STARTED              ELAPSED   PENDING_BYTES
1    ACCOUNT_INFO_STATE       RUNNING  2026-05-01 14:22:01  00:03:12  1.2 GB
2    ACCOUNT_STORAGE_STORAGE  DONE     2026-05-01 14:21:55  00:00:47  -
```

`db compact-status 1` includes the table row plus the full RocksDB `compaction-stats` text dump for that CF.

## Architecture

### New classes

All under `net.consensys.bric`.

#### `db/CompactionJob`

Value-ish class holding per-job state.

Fields:
- `id` (int)
- `cfName` (String)
- `state` (enum `State { RUNNING, DONE, FAILED, CANCELLED }`)
- `startedAt` (Instant)
- `finishedAt` (Instant, nullable)
- `error` (String, nullable — populated when state is FAILED)
- `options` (`CompactRangeOptions`) — per-job options object passed to `compactRange`; `setCanceled(true)` on this is how cancellation is signalled. Closed (`options.close()`) by the worker after `compactRange` returns, regardless of outcome (the native handle would otherwise leak).

State transitions are guarded by `synchronized` on the job instance. The state field is the single source of truth for what happened to a job.

#### `db/CompactionJobManager`

Owns the executor, the job map, and the ID counter. Single instance per `BesuDatabaseManager` lifetime; reset on every `closeDatabase()`.

Internals:
- `AtomicInteger nextId` — starts at 1.
- `Map<Integer, CompactionJob> jobs` — `ConcurrentHashMap`, never removes entries during a session (history retained until close).
- `ExecutorService executor` — `Executors.newCachedThreadPool` with daemon threads. Submitting many concurrent compactions is fine; RocksDB's `max_background_compactions` will internally throttle.
- A reference to the open `RocksDB` (passed at construction).

Methods:
- `int submit(String cfName, ColumnFamilyHandle handle)` — allocate ID, construct a fresh `CompactRangeOptions`, create a RUNNING job holding both, submit a `Runnable` that calls `db.compactRange(handle, null, null, options)` and updates state on completion. Returns the job ID.
- `void cancel(int jobId)` — calls `setCanceled(true)` on the job's `CompactRangeOptions`. RocksDB's compaction loop polls this flag and aborts. The method waits up to 5s for the worker to flip the job to CANCELLED, then returns regardless. **No effect on sibling jobs** — cancellation is per-compaction.
- `void cancelAll()` — calls `setCanceled(true)` on every RUNNING job's options, waits up to 5s for all of them to flip. Used by REPL exit. Idempotent: safe to call when no jobs are running.
- `Optional<CompactionJob> get(int jobId)`.
- `List<CompactionJob> list()` — sorted by ID.
- `boolean hasRunning()`, `List<Integer> runningJobIds()`.
- `void shutdownAndClear()` — called by `closeDatabase()` on a clean close (no running jobs); shuts down the executor and clears the map.

Worker `Runnable` body and exception handling:
- Try: `db.compactRange(handle, null, null, job.options)`.
- After the call returns or throws, in a `finally` block: close `job.options` (releases the native handle).
- Outcome mapping:
  - Returns normally **and** `job.options.canceled() == true` → mark CANCELLED. (Some RocksDB builds return cleanly rather than throwing on cancel; we check the flag explicitly.)
  - Returns normally → mark DONE.
  - `RocksDBException` with `Status.Code.Incomplete` **or** the options' canceled flag is set → mark CANCELLED.
  - Any other `RocksDBException` → mark FAILED, store `e.getMessage()`.
  - Any other `Throwable` → mark FAILED, store `e.toString()`. Never let an exception escape the worker.
- In all cases, set `finishedAt = Instant.now()`.

#### `commands/DbCompactCommand`

`db compact <segment...|--all>`.

1. Guard: DB open + writable.
2. Resolve targets:
   - `--all` → iterate `dbManager.getColumnFamilyNames()`, skip empty CFs (predicate matches `DbStatsCommand`: `estimatedKeys == 0 && totalSize == 0`). If zero non-empty, print `No non-empty column families to compact.` and return.
   - Otherwise → resolve each arg via `ColumnFamilyResolver.resolveColumnFamily`. If *any* fails, abort the entire command before submitting any jobs (atomic-ish; we don't want partial submission).
3. For each resolved CF, call `jobManager.submit(...)` and print `Submitted job <id>: <CF>`.

#### `commands/DbCompactStatusCommand`

`db compact-status [<job-id>]`.

- No arg: print the table (header + one row per job in ID order). For RUNNING jobs, populate `PENDING_BYTES` from `db.getProperty(handle, "rocksdb.estimate-pending-compaction-bytes")` formatted with the existing byte-formatting helper. For non-RUNNING, render `-`.
- With arg: same row plus a `--- Compaction Stats ---` block from `db.getProperty(handle, "rocksdb.compaction-stats")`. If the job is FAILED, also print the error message.

Errors: `Error: No such job: <id>` if the ID isn't in the manager.

#### `commands/DbCompactCancelCommand`

`db compact-cancel <job-id>`.

1. Guard: DB open + writable; job ID is numeric.
2. Look up job. If not RUNNING, `Error: Job <id> is already <state>` and return.
3. Call `jobManager.cancel(jobId)`. Print `Cancelled job <id>` (or `Cancel signalled for job <id> (worker did not transition within timeout)` if the wait timed out).

### Wiring

#### `DbCommand`

Three new fields, three new switch cases (`compact`, `compact-status`, `compact-cancel`), updated `getUsage()` text. Pattern is identical to existing `db stats` integration (commit `c52c77d`).

#### `BesuDatabaseManager`

- New field: `CompactionJobManager jobManager` (constructed inside `openDatabase` once `db` is non-null; cleared in `closeDatabase`).
- New accessor: `getCompactionJobManager()`.
- `closeDatabase()` change: at the top, if `jobManager.hasRunning()`, throw `IllegalStateException("Cannot close: compaction jobs still running: " + runningIds + ". Cancel them first with 'db compact-cancel <job-id>'.")` before closing handles. The `DbCloseCommand` catches this and prints the message.

#### `BricApplication`

REPL-exit path (Ctrl-D / `quit`): before invoking `closeDatabase`, if `hasRunning()`, call `jobManager.cancelAll()`, then proceed with close. This keeps shutdown clean — the user is exiting the process intentionally and a single batch cancel is preferable to refusing exit.

Interactive `db close` path stays strict (refuses with the running-jobs message). Interactive close is recoverable; process exit is not.

#### `BricCommandProcessor` autocomplete

Add `compact`, `compact-status`, `compact-cancel` to the `db` subcommand autocomplete list (same pattern as the recent `db stats` change in commit `80ea47b`).

## Data flow

### Submitting a compaction

1. User types `db compact ACCOUNT_INFO_STATE`.
2. `DbCompactCommand` resolves the CF, calls `jobManager.submit("ACCOUNT_INFO_STATE", handle)`.
3. Manager allocates ID 1, stores a RUNNING `CompactionJob`, submits a worker.
4. Worker calls `db.compactRange(handle)` (blocks the worker thread; returns when done).
5. On return: synchronized on the job, set state DONE + `finishedAt`. (Or FAILED / CANCELLED per exception type.)
6. Command prints `Submitted job 1: ACCOUNT_INFO_STATE` and returns to the prompt.

### Cancellation

1. User types `db compact-cancel 1`.
2. Command verifies job 1 is RUNNING.
3. Manager calls `setCanceled(true)` on job 1's `CompactRangeOptions`.
4. RocksDB's compaction loop polls the flag and aborts. The worker either gets `RocksDBException(Status.Incomplete)` or returns normally with `options.canceled() == true`. Either way the worker transitions the job to CANCELLED.
5. Manager polls the job's state for up to 5s; returns once CANCELLED (or timeout — sibling jobs are unaffected, so a stuck cancel is recoverable).
6. Command prints `Cancelled job 1`.

### Close while running

- Interactive `db close` → manager reports running jobs, close throws, command prints `Cannot close: ...`.
- REPL exit → application loops cancel-all, then closes normally.

## Error handling

### Per-command guard order (first failure short-circuits)

| Check | `compact` | `compact-status` | `compact-cancel` |
|---|---|---|---|
| DB open | yes | yes | yes |
| DB writable | yes | no | yes |
| Args present | yes (CFs or `--all`) | no (id optional) | yes (id required, numeric) |
| CF resolves | yes | n/a | n/a |
| Job exists | n/a | yes (when id given) | yes |
| Job is RUNNING | n/a | n/a | yes |

Error messages match existing precedents (`DbDropCfCommand`, `DbStatsCommand`).

### Worker thread

All exceptions are caught inside the `Runnable` and recorded on the job. Nothing is printed to the console from the worker — the user has long since left the prompt that submitted the job, and they'll see results via `compact-status`.

### Cancel timeout

If the worker doesn't transition to CANCELLED within 5s of `setCanceled(true)`, the manager returns anyway and the command reports the timeout (without claiming success). The cancellation flag is still set on the job's options, so the worker will eventually transition when its compaction loop next polls — sibling jobs are untouched, and future jobs are unaffected. There is no DB-wide state to clean up.

## Testing

JUnit 5 + AssertJ + Mockito; `BesuDatabaseManager` and `RocksDB` are mocked (matches existing test pattern).

### `CompactionJobManagerTest`

- `submit` returns monotonically increasing IDs.
- `submit` records job in RUNNING state immediately.
- On worker success → DONE, `finishedAt` set.
- On `RocksDBException` (non-Incomplete) → FAILED with error captured.
- On `Status.Incomplete` → CANCELLED.
- `hasRunning` / `runningJobIds` reflect live state.
- `shutdownAndClear` interrupts running jobs and empties the map.
- `cancelAll` flips every RUNNING job to CANCELLED and is a no-op when nothing is running.
- Worker timing controlled with a `CountDownLatch` so we can assert intermediate states without flakes.

### `DbCompactCommandTest`

- Refuses when DB closed; refuses when read-only; refuses when no CFs and no `--all`.
- Resolves CF names through all three forms supported by `ColumnFamilyResolver` (predefined enum, UTF-8 name, hex ID).
- `--all` skips empty CFs (mock `getStats` returning zero-key/zero-size).
- A single bad CF name in a multi-CF call aborts the entire command — no jobs submitted.
- Prints `Submitted job <id>: <CF>` per submission.

### `DbCompactStatusCommandTest`

- No-arg lists all jobs in ID order with correct columns.
- With ID, prints detail block plus mocked `compaction-stats` text.
- `No such job` error path.
- FAILED job's detail block includes the error message.

### `DbCompactCancelCommandTest`

- Cancels a single RUNNING job (verify `setCanceled(true)` is called on that job's options).
- Errors on already-finished job (DONE / FAILED / CANCELLED).
- Reports timeout when the worker doesn't transition within the wait window (worker held by a latch in the test).
- Cancelling job N does not call `setCanceled` on job M's options (sibling-isolation regression test).

### `DbCommandTest` (existing)

Extended with three cases routing to the new subcommands.

### `BesuDatabaseManagerTest`

`closeDatabase` refuses when a `CompactionJobManager` reports running jobs; succeeds when none are running.

### Manual verification (no automated coverage)

- Open a real Besu DB in `--write`, run `db compact ACCOUNT_INFO_STATE`, observe progress via `db compact-status`, observe DONE on completion.
- Kick off a long compaction, run `db compact-cancel`, confirm CANCELLED state and that subsequent `compact` jobs still execute (proving `enableManualCompaction` ran).

## Design decisions

**Why three flat subcommands instead of a nested `db compact` parent.** The existing `db drop-cf` / `db stats` commands are flat siblings of `db open` / `db close`, not nested under a parent. Three flat subcommands match that pattern. A nested parent would add a layer of indirection without grouping anything that other db subcommands don't already share.

**Why `compact-status` doesn't require write mode.** Status only reads in-memory job state and a read-only RocksDB property. The job map will be empty in a read-only session (since you couldn't have submitted anything), so the command is a no-op there — but a no-op is more user-friendly than a refusal, and there's no safety reason to gate it behind write mode.

**Why per-job cancel (via `CompactRangeOptions.setCanceled`).** rocksdbjni 10.6.2 doesn't expose `disableManualCompaction()` / `enableManualCompaction()` (DB-wide on/off switches that exist in the C++ API). It does expose `CompactRangeOptions.setCanceled(boolean)`, which signals the specific compaction associated with that options object. Each job constructs its own options, so cancellation is naturally isolated per-job — no warning is needed for sibling jobs.

**Why no persistence and no CLI batch mode.** Persisting jobs to disk would let a new bric instance see historical jobs but couldn't actually resume them (RocksDB doesn't support that), and only one bric process can hold the write lock anyway. CLI batch mode would help wrap with `nohup` for very long jobs, but `tmux`/`screen` already solves session-drop survival without adding code. Both can be added later if needed.

**Why job history isn't bounded.** Jobs are wiped on `db close`. A single REPL session won't accumulate enough jobs to matter. If it ever does, bounding is a trivial follow-up.
