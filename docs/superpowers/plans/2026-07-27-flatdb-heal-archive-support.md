# Flat DB Heal: Bonsai Archive Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `db upgrade-flatdb` to accept `BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE` databases in addition to plain `BONSAI`, healing only the current-state flat tables and leaving archive-specific historical tables untouched.

**Architecture:** `FlatDbHealer`'s constructor picks `DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG` instead of `DEFAULT_BONSAI_CONFIG` when the target database's format is `BONSAI_ARCHIVE`, so Besu's own `BonsaiFlatDbStrategyProvider`/`BonsaiArchiveFlatDbStrategy` machinery — already reused unchanged for plain Bonsai — does the right thing automatically: writes still go only to `ACCOUNT_INFO_STATE`/`ACCOUNT_STORAGE_STORAGE`, and the end-of-heal mode flip persists `FlatDbMode.ARCHIVE` instead of `FlatDbMode.FULL`. No change to the trie-walk, diff, or checkpoint logic.

**Tech Stack:** Java 25, Gradle, Hyperledger Besu libraries (`besu-ethereum-core` 25.12.0), JUnit 5, AssertJ, Mockito.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-27-flatdb-heal-archive-support-design.md` — every requirement in that doc must be traceable to a task below.
- Archive-specific tables (`ACCOUNT_INFO_STATE_ARCHIVE`, `ACCOUNT_STORAGE_ARCHIVE`) must never be read or written by this feature, on either format — this is load-bearing, not incidental, and needs its own regression test, not just an assumption.
- Only `BONSAI` and `BONSAI_ARCHIVE` are accepted formats; `FOREST` and `UNKNOWN` remain rejected with the existing error message.
- No other behavior changes: the trie walk, diff/write logic in `healAccountRange`/`healAccountStorage`, and checkpoint/resumability mechanics are unchanged by this plan.
- Follow existing code style: no Javadoc-heavy comments, package `net.consensys.bric.besu` for storage/trie classes, `net.consensys.bric.commands` for CLI commands.

---

### Task 1: `FlatDbHealer` becomes format-aware

**Files:**
- Modify: `src/main/java/net/consensys/bric/besu/FlatDbHealer.java:52-69` (constructor)
- Modify: `src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java` (new fixture helper + new test)

**Interfaces:**
- Consumes: `BesuDatabaseManager.getFormat()` (existing method, returns `BesuDatabaseManager.DatabaseFormat`), `BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE` (existing enum constant), `DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG` (existing Besu constant, confirmed present in `besu-ethereum-core-25.12.0.jar` via `javap` alongside the already-used `DEFAULT_BONSAI_CONFIG`).
- Produces: no change to `FlatDbHealer`'s public surface (`getTargetStateRoot()`, `heal(boolean, FlatDbHealProgressListener)` keep their exact signatures) — only the constructor's internal config selection changes. Consumed by Task 2 unchanged.

- [ ] **Step 1: Write the failing test**

Read the current `src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java` first — it already has `createFixtureDatabaseSchema()`, `openWritableFixtureDatabase()`, `buildWorldState(BesuDatabaseManager)`, `accountRlp(long, Hash)`, `seedAccountTrie(BonsaiWorldStateKeyValueStorage, Map<Hash, byte[]>)` helpers from earlier tasks — reuse them, don't redefine them.

Add a new fixture helper and test to `FlatDbHealerTest.java` (append after the existing `openWritableFixtureDatabase()`/`buildWorldState()` methods, e.g. right after line 104):

```java
    /**
     * Same column families as createFixtureDatabaseSchema(), plus ACCOUNT_INFO_STATE_ARCHIVE —
     * enough for BesuDatabaseManager.detectDatabaseFormat() to classify this as BONSAI_ARCHIVE.
     * Opens the database writable and returns a BonsaiWorldStateKeyValueStorage built the same
     * way FlatDbHealer builds its own, for seeding fixture data.
     */
    private BonsaiWorldStateKeyValueStorage openWritableArchiveFixtureDatabase() throws Exception {
        List<ColumnFamilyDescriptor> descriptors = List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.CODE_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.VARIABLES.getId()));
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        try (RocksDB db = RocksDB.open(options, tempDir.toString(), descriptors, handles)) {
            for (ColumnFamilyHandle handle : handles) {
                handle.close();
            }
        }

        dbManager.openDatabase(tempDir.toString(), true);
        return buildWorldState(dbManager);
    }

    @Test
    void constructor_healsArchiveDatabaseAndLeavesArchiveTableUntouched() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableArchiveFixtureDatabase();
        assertThat(dbManager.getFormat()).isEqualTo(BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE);

        Hash missingHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        byte[] missingRlp = accountRlp(1, Hash.EMPTY_TRIE_HASH);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(missingHash, missingRlp));

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(false, FlatDbHealProgressListener.NO_OP);

        assertThat(result.accountsAdded).isEqualTo(1);

        // Current-state table got the fix, exactly like a plain Bonsai database.
        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(missingHash)).contains(Bytes.wrap(missingRlp));

        // The archive-specific table is never touched: still completely empty.
        RocksDBSegmentedStorage verifyStorage = new RocksDBSegmentedStorage(dbManager);
        List<org.apache.commons.lang3.tuple.Pair<byte[], byte[]>> archiveEntries = verifyStorage.streamFromKey(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE_ARCHIVE,
            RangeManager.MIN_RANGE.toArrayUnsafe(), RangeManager.MAX_RANGE.toArrayUnsafe()).toList();
        assertThat(archiveEntries).isEmpty();

        // The persisted flat DB mode is ARCHIVE (0x02), not FULL (0x01) — this is the one
        // behavior that's actually format-conditional in Besu's own upgradeToFullFlatDbMode().
        net.consensys.bric.db.SegmentReader segmentReader = new net.consensys.bric.db.SegmentReader(dbManager);
        Optional<byte[]> flatDbMode = segmentReader.get(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, "flatDbStatus".getBytes());
        assertThat(flatDbMode).isPresent();
        assertThat(flatDbMode.get()[0]).isEqualTo((byte) 0x02);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: FAIL — with the constructor still hardcoded to `DEFAULT_BONSAI_CONFIG`, `BonsaiFlatDbStrategyProvider` derives/writes `FlatDbMode.FULL` regardless of the database's actual archive-capable schema, so the last assertion (`flatDbMode.get()[0]).isEqualTo((byte) 0x02)`) fails — the persisted byte is `0x01` (FULL), not `0x02` (ARCHIVE).

- [ ] **Step 3: Make the constructor format-aware**

In `src/main/java/net/consensys/bric/besu/FlatDbHealer.java`, replace the constructor body (currently lines 52-69):

```java
    public FlatDbHealer(BesuDatabaseManager dbManager) {
        this.storage = new RocksDBSegmentedStorage(dbManager);
        DataStorageConfiguration dataStorageConfiguration =
            dbManager.getFormat() == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE
                ? DataStorageConfiguration.DEFAULT_BONSAI_ARCHIVE_CONFIG
                : DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;
        BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
            new NoOpMetricsSystem(), dataStorageConfiguration);
        try {
            flatDbStrategyProvider.loadFlatDbStrategy(storage);
        } catch (UnsupportedOperationException e) {
            // Besu's loadFlatDbStrategy() persists the flat DB mode byte via
            // storage.startTransaction() whenever that byte isn't already on disk (e.g. a
            // database created before this metadata key existed) — which RocksDBSegmentedStorage
            // correctly rejects when the underlying database was opened read-only. Skipping it
            // is harmless here: dry-run callers only need to read the derived strategy, not
            // persist it, and bric's own heal logic doesn't depend on this write having happened.
            LOG.debug("Skipping flat DB metadata write-back; database is read-only.", e);
        }
        this.worldState = new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider, storage, new NoOpKeyValueStorage());
    }
```

No import changes needed — `BesuDatabaseManager` is already imported (line 3), and `DataStorageConfiguration` is already imported (line 18); `DEFAULT_BONSAI_ARCHIVE_CONFIG` is a static field access on the same already-imported class.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: PASS (all tests, including the new `constructor_healsArchiveDatabaseAndLeavesArchiveTableUntouched`)

- [ ] **Step 5: Run the full suite to confirm no regression to the plain-Bonsai path**

Run: `./gradlew test`
Expected: PASS (zero regressions — the constructor change only takes effect for `BONSAI_ARCHIVE`; plain `BONSAI`/`UNKNOWN`-format fixtures still get `DEFAULT_BONSAI_CONFIG` exactly as before)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/FlatDbHealer.java src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java
git commit -m "feat: make FlatDbHealer format-aware for Bonsai Archive databases"
```

---

### Task 2: `DbUpgradeFlatDbCommand` accepts Bonsai Archive

**Files:**
- Modify: `src/main/java/net/consensys/bric/commands/DbUpgradeFlatDbCommand.java:32-37,71-72`
- Modify: `src/test/java/net/consensys/bric/commands/DbUpgradeFlatDbCommandTest.java`

**Interfaces:**
- Consumes: Task 1's format-aware `FlatDbHealer` constructor (no signature change — this task doesn't need to know about it beyond it now working for `BONSAI_ARCHIVE`).
- Produces: no change to `DbUpgradeFlatDbCommand`'s public surface (`execute(String[])`, `getHelp()`, `getUsage()` unchanged) — only the internal format guard and the success message change.

- [ ] **Step 1: Write the failing tests**

Read the current `src/test/java/net/consensys/bric/commands/DbUpgradeFlatDbCommandTest.java` first — it already has `refusesWhenNotBonsaiFormat()` (mocks `getFormat()` returning `FOREST`), `refusesWhenReadOnlyAndNotDryRun()`, `dryRunDoesNotRequireWritable()` (both mock `BONSAI`). Reuse the same mock-setup style (`mockDbManager = mock(BesuDatabaseManager.class)` from `@BeforeEach`).

Add these two tests to `DbUpgradeFlatDbCommandTest.java` (near `refusesWhenNotBonsaiFormat()`):

```java
    @Test
    void allowsBonsaiArchiveFormat() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{"--dry-run"});
        // Fails later (no real database behind the mock), but must get past the format guard —
        // BONSAI_ARCHIVE is no longer rejected as "not a Bonsai database".
        assertThat(errorStream.toString()).doesNotContain("only supported for Bonsai databases");
    }

    @Test
    void stillRefusesForestAndUnknownFormats() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.UNKNOWN);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("only supported for Bonsai databases");
        assertThat(errorStream.toString()).contains("UNKNOWN");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbUpgradeFlatDbCommandTest"`
Expected: FAIL on `allowsBonsaiArchiveFormat` — the current guard (`format != BesuDatabaseManager.DatabaseFormat.BONSAI`) rejects `BONSAI_ARCHIVE` too, so the error output still contains "only supported for Bonsai databases".

- [ ] **Step 3: Relax the format guard and make the success message format-aware**

In `src/main/java/net/consensys/bric/commands/DbUpgradeFlatDbCommand.java`, replace the format guard (currently lines 32-37):

```java
        BesuDatabaseManager.DatabaseFormat format = dbManager.getFormat();
        boolean isBonsai = format == BesuDatabaseManager.DatabaseFormat.BONSAI;
        boolean isBonsaiArchive = format == BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE;
        if (!isBonsai && !isBonsaiArchive) {
            System.err.println(
                "Error: Flat DB healing is only supported for Bonsai databases. Current format: " + format);
            return;
        }
```

Then replace the non-dry-run success message (currently line 72, `System.out.println("Flat DB mode upgraded to FULL.");`) with a format-aware version, since Besu's own `upgradeToFullFlatDbMode()` persists `ARCHIVE` (not `FULL`) for an archive database (verified in the design spec):

```java
            System.out.println("Flat DB mode upgraded to " + (isBonsaiArchive ? "ARCHIVE" : "FULL") + ".");
```

`isBonsai`/`isBonsaiArchive` are computed once near the top of `execute()` and are still in scope at the point the success message prints, since both live in the same method body with no intervening early return that would put them out of scope.

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbUpgradeFlatDbCommandTest"`
Expected: PASS (all tests, including the two new ones)

- [ ] **Step 5: Run the full suite as a final regression check**

Run: `./gradlew test`
Expected: PASS (no regressions across the whole project — this is the last task of this plan)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbUpgradeFlatDbCommand.java src/test/java/net/consensys/bric/commands/DbUpgradeFlatDbCommandTest.java
git commit -m "feat: accept Bonsai Archive databases in db upgrade-flatdb"
```

---

## Manual Verification (not automated)

If a real Bonsai Archive database snapshot is available:

1. `db open /path/to/archive/db --write`
2. `db info` — confirm format shows `BONSAI_ARCHIVE`
3. `db upgrade-flatdb --dry-run` — confirm it runs (no longer rejected) and reports plausible counts
4. `db upgrade-flatdb` — let it complete
5. `db info` — confirm `Flat DB Mode: ARCHIVE` (not `FULL`)
6. Spot-check that `ACCOUNT_INFO_STATE_ARCHIVE`/`ACCOUNT_STORAGE_ARCHIVE` entry counts (via `db stats`) are unchanged before/after

This step has no automated coverage — say so explicitly rather than claiming it's verified.

## Self-Review Notes

- **Spec coverage:** the spec's two required code changes (format-aware `FlatDbHealer` constructor, relaxed `DbUpgradeFlatDbCommand` guard) map to Task 1 and Task 2 respectively; the spec's testing section (archive fixture heals correctly, persisted mode is ARCHIVE not FULL, archive table untouched, CLI guard accepts `BONSAI_ARCHIVE`/still rejects `FOREST`/`UNKNOWN`) is covered by Task 1's and Task 2's test steps.
- **Placeholder scan:** none found — every step shows complete code.
- **Type consistency:** `BesuDatabaseManager.DatabaseFormat.BONSAI_ARCHIVE` is used identically in Task 1's test and Task 2's guard/tests; `FlatDbHealResult`/`FlatDbHealProgressListener.NO_OP` (Task 1's test) match their existing definitions from the prior plan, unchanged by this one.
