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
- `db compact-cancel <job-id>` — interrupt a running job. Because RocksDB's `disableManualCompaction()` is DB-wide, cancelling one running job cancels all sibling running jobs; the command prints a warning when this happens.

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

State transitions are guarded by `synchronized` on the job instance. The state field is the single source of truth for what happened to a job.

#### `db/CompactionJobManager`

Owns the executor, the job map, and the ID counter. Single instance per `BesuDatabaseManager` lifetime; reset on every `closeDatabase()`.

Internals:
- `AtomicInteger nextId` — starts at 1.
- `Map<Integer, CompactionJob> jobs` — `ConcurrentHashMap`, never removes entries during a session (history retained until close).
- `ExecutorService executor` — `Executors.newCachedThreadPool` with daemon threads. Submitting many concurrent compactions is fine; RocksDB's `max_background_compactions` will internally throttle.
- A reference to the open `RocksDB` (passed at construction).

Methods:
- `int submit(String cfName, ColumnFamilyHandle handle)` — allocate ID, create RUNNING job, submit a `Runnable` that calls `db.compactRange(handle)` and updates state on completion. Returns the job ID.
- `void cancel(int jobId)` — calls `db.disableManualCompaction()` (DB-wide), waits up to 5s for the worker to flip to CANCELLED, then calls `db.enableManualCompaction()`. If the worker hasn't transitioned within the timeout, still re-enables (we don't want manual compaction permanently disabled). **Side effect:** because `disableManualCompaction()` is DB-wide, every other RUNNING job is also cancelled. Callers handle the warning (see `DbCompactCancelCommand`).
- `void cancelAll()` — same mechanism (`disableManualCompaction()` once, wait for all RUNNING jobs to flip, `enableManualCompaction()`). Used by REPL exit. Idempotent: safe to call when no jobs are running.
- `Optional<CompactionJob> get(int jobId)`.
- `List<CompactionJob> list()` — sorted by ID.
- `boolean hasRunning()`, `List<Integer> runningJobIds()`.
- `void shutdownAndClear()` — called by `closeDatabase()` on a clean close (no running jobs); shuts down the executor and clears the map.

Worker `Runnable` exception handling:
- `RocksDBException` with `Status.Code.Incomplete` → mark CANCELLED.
- Any other `RocksDBException` → mark FAILED, store `e.getMessage()`.
- Any other `Throwable` → mark FAILED, store `e.toString()`. Never let an exception escape the worker.

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
3. If `runningJobIds().size() > 1`, print to stderr: `Warning: cancelling job <id> will also cancel sibling running jobs: [<ids>]. Proceeding.`
4. Call `jobManager.cancel(jobId)`. Print `Cancelled job <id>`.

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
2. Command verifies job 1 is RUNNING. If sibling RUNNING jobs exist, prints the warning.
3. Manager calls `db.disableManualCompaction()`. RocksDB raises `Status.Incomplete` from the worker's `compactRange`.
4. Worker catches, transitions job to CANCELLED.
5. Manager polls the job's state for up to 5s; once CANCELLED (or timeout), calls `db.enableManualCompaction()`.
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

If the worker doesn't transition to CANCELLED within 5s of `disableManualCompaction()`, the manager still calls `enableManualCompaction()` and returns. The job will eventually transition (whenever the worker actually returns from `compactRange`); meanwhile, future jobs aren't blocked.

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

- Cancels a single RUNNING job (verify `disableManualCompaction` then `enableManualCompaction` calls in order).
- Multi-job warning printed when sibling RUNNING jobs exist.
- Errors on already-finished job.
- `enableManualCompaction` is still called when the worker doesn't transition within the timeout.

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

**Why DB-wide cancel semantics with a warning rather than per-job cancel.** RocksDB's `disableManualCompaction()` is DB-wide; there is no API to cancel a specific manual compaction. Implementing per-job cancel would require either re-issuing the surviving compactions (complex, error-prone) or refusing cancel when more than one job runs (frustrating). A warning when sibling jobs exist makes the behavior explicit and rarely matters in practice — multi-CF concurrent manual compaction is uncommon.

**Why no persistence and no CLI batch mode.** Persisting jobs to disk would let a new bric instance see historical jobs but couldn't actually resume them (RocksDB doesn't support that), and only one bric process can hold the write lock anyway. CLI batch mode would help wrap with `nohup` for very long jobs, but `tmux`/`screen` already solves session-drop survival without adding code. Both can be added later if needed.

**Why job history isn't bounded.** Jobs are wiped on `db close`. A single REPL session won't accumulate enough jobs to matter. If it ever does, bounding is a trivial follow-up.
