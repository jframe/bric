# Flat DB Heal (Partial → Full) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `db upgrade-flatdb [--dry-run]` subcommand that reconciles a Bonsai database's `ACCOUNT_INFO_STATE`/`ACCOUNT_STORAGE_STORAGE` flat tables against the canonical account/storage tries, then flips the flat DB mode to FULL.

**Architecture:** A new `FlatDbHealer` class drives Besu's own trie/storage classes (`StoredMerklePatriciaTrie`, `RangeStorageEntriesCollector`, `RangeManager`, `BonsaiWorldStateKeyValueStorage`) directly against bric's existing `RocksDBSegmentedStorage` adapter (extended here to support real writes and ranged reads for the first time). The walk splits the account keyspace into 16 ranges (mirroring Besu's own snap-sync heal), diffs trie-truth against flat-truth per range, writes corrections, and — for every account found to differ — repeats the same walk-and-diff against that account's storage trie. Progress is checkpointed in the existing `VARIABLES` column family so an interrupted run resumes instead of restarting.

**Tech Stack:** Java 25, Gradle, RocksDB (rocksdbjni), Hyperledger Besu libraries (`besu-ethereum-core`, `besu-ethereum-trie`, `besu-datatypes`, `besu-plugin-api`), JUnit 5, AssertJ, Mockito.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-24-flatdb-heal-design.md` — every requirement in that doc must be traceable to a task below.
- Only `BesuDatabaseManager.DatabaseFormat.BONSAI` is supported (not `BONSAI_ARCHIVE`, `FOREST`, `UNKNOWN`).
- Non-dry-run requires the database open in `--write` mode (existing convention, see `DbCompactCommand`).
- `--dry-run` performs zero writes and must work read-only.
- No new column families — checkpoint state goes in the existing `VARIABLES` CF.
- Follow existing code style: no Javadoc-heavy comments, package `net.consensys.bric.besu` for storage/trie classes, `net.consensys.bric.commands` for CLI commands.
- Every new/changed write path in `RocksDBSegmentedStorage` must throw the existing `UnsupportedOperationException` messages when `dbManager.isWritable()` is false — do not weaken today's read-only guarantee.

---

### Task 1: `NoOpKeyValueStorage`

**Files:**
- Create: `src/main/java/net/consensys/bric/besu/NoOpKeyValueStorage.java`
- Test: `src/test/java/net/consensys/bric/besu/NoOpKeyValueStorageTest.java`

**Interfaces:**
- Produces: `net.consensys.bric.besu.NoOpKeyValueStorage implements org.hyperledger.besu.plugin.services.storage.KeyValueStorage` — a working no-op trie-log storage, consumed by Task 4's `FlatDbHealer` constructor (`BonsaiWorldStateKeyValueStorage`'s `trieLogStorage` parameter).

- [ ] **Step 1: Write the failing test**

```java
package net.consensys.bric.besu;

import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NoOpKeyValueStorageTest {

    @Test
    void allReadsReturnEmptyOrFalse() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();

        assertThat(storage.containsKey("key".getBytes())).isFalse();
        assertThat(storage.get("key".getBytes())).isEmpty();
        assertThat(storage.stream()).isEmpty();
        assertThat(storage.streamFromKey("key".getBytes())).isEmpty();
        assertThat(storage.streamFromKey("a".getBytes(), "z".getBytes())).isEmpty();
        assertThat(storage.streamKeys()).isEmpty();
        assertThat(storage.getAllKeysThat(k -> true)).isEmpty();
        assertThat(storage.getAllValuesFromKeysThat(k -> true)).isEmpty();
        assertThat(storage.isClosed()).isFalse();
    }

    @Test
    void clearAndCloseDoNotThrow() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();
        storage.clear();
        storage.close();
    }

    @Test
    void tryDeleteReturnsTrue() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();
        assertThat(storage.tryDelete("key".getBytes())).isTrue();
    }

    @Test
    void transactionOperationsAreAllNoOps() {
        NoOpKeyValueStorage storage = new NoOpKeyValueStorage();
        KeyValueStorageTransaction transaction = storage.startTransaction();

        transaction.put("key".getBytes(), "value".getBytes());
        transaction.remove("key".getBytes());
        transaction.commit();
        transaction.rollback();
        transaction.close();
        // No exception means the no-op contract held.
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "net.consensys.bric.besu.NoOpKeyValueStorageTest"`
Expected: FAIL — compilation error, `NoOpKeyValueStorage` does not exist.

- [ ] **Step 3: Write the implementation**

```java
package net.consensys.bric.besu;

import org.apache.commons.lang3.tuple.Pair;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorageTransaction;

import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Stand-in for Besu's trie-log KeyValueStorage. Flat DB healing never reads or writes
 * trie logs, so every method either no-ops or reports empty/absent.
 */
public class NoOpKeyValueStorage implements KeyValueStorage {

    @Override
    public void clear() {
    }

    @Override
    public boolean containsKey(byte[] key) {
        return false;
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        return Optional.empty();
    }

    @Override
    public Stream<Pair<byte[], byte[]>> stream() {
        return Stream.empty();
    }

    @Override
    public Stream<Pair<byte[], byte[]>> streamFromKey(byte[] startKey) {
        return Stream.empty();
    }

    @Override
    public Stream<Pair<byte[], byte[]>> streamFromKey(byte[] startKey, byte[] endKey) {
        return Stream.empty();
    }

    @Override
    public Stream<byte[]> streamKeys() {
        return Stream.empty();
    }

    @Override
    public boolean tryDelete(byte[] key) {
        return true;
    }

    @Override
    public Set<byte[]> getAllKeysThat(Predicate<byte[]> returnCondition) {
        return Set.of();
    }

    @Override
    public Set<byte[]> getAllValuesFromKeysThat(Predicate<byte[]> returnCondition) {
        return Set.of();
    }

    @Override
    public KeyValueStorageTransaction startTransaction() {
        return new NoOpTransaction();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void close() {
    }

    private static final class NoOpTransaction implements KeyValueStorageTransaction {
        @Override
        public void put(byte[] key, byte[] value) {
        }

        @Override
        public void remove(byte[] key) {
        }

        @Override
        public void commit() {
        }

        @Override
        public void rollback() {
        }

        @Override
        public void close() {
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "net.consensys.bric.besu.NoOpKeyValueStorageTest"`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/NoOpKeyValueStorage.java src/test/java/net/consensys/bric/besu/NoOpKeyValueStorageTest.java
git commit -m "feat: add no-op KeyValueStorage for flat db healing's unused trie-log slot"
```

---

### Task 2: `RocksDBSegmentedStorage` write and ranged-read support

**Files:**
- Modify: `src/main/java/net/consensys/bric/besu/RocksDBSegmentedStorage.java`
- Test: `src/test/java/net/consensys/bric/besu/RocksDBSegmentedStorageTest.java` (new)

**Interfaces:**
- Consumes: `net.consensys.bric.db.BesuDatabaseManager` — existing `isWritable()`, `getDatabase()` (returns `org.rocksdb.RocksDB`), `getColumnFamily(KeyValueSegmentIdentifier)`.
- Produces: `RocksDBSegmentedStorage.startTransaction()`, `.tryDelete(SegmentIdentifier, byte[])`, `.streamFromKey(SegmentIdentifier, byte[], byte[])` all functional (previously all threw `UnsupportedOperationException`). Consumed by Task 5's `FlatDbHealer` (via `BonsaiWorldStateKeyValueStorage.updater()`, which calls `startTransaction()` internally) and Task 6/7's range reads (`streamFromKey`).

This task touches an existing production class read by `BesuFlatDbReader` (Task 1's dependency chain) — do not change its constructor or the already-working `get`/`getNearestAfter`/`getNearestBefore`/`close`/`isClosed` methods.

- [ ] **Step 1: Write the failing tests**

```java
package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.hyperledger.besu.plugin.services.storage.SegmentedKeyValueStorageTransaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RocksDBSegmentedStorageTest {

    @TempDir
    Path tempDir;

    private BesuDatabaseManager dbManager;
    private RocksDBSegmentedStorage storage;

    @BeforeEach
    void setUp() {
        RocksDB.loadLibrary();
        dbManager = new BesuDatabaseManager();
    }

    @AfterEach
    void tearDown() {
        if (dbManager.isOpen()) {
            dbManager.closeDatabase();
        }
    }

    private void createTestDatabase() throws Exception {
        List<ColumnFamilyDescriptor> descriptors = List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()));
        List<ColumnFamilyHandle> handles = new ArrayList<>();
        DBOptions options = new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true);
        try (RocksDB db = RocksDB.open(options, tempDir.toString(), descriptors, handles)) {
            for (ColumnFamilyHandle handle : handles) {
                handle.close();
            }
        }
    }

    @Test
    void startTransaction_putAndCommit_persistsValue() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        transaction.commit();
        transaction.close();

        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes()))
            .contains("value".getBytes());
    }

    @Test
    void startTransaction_putAndRollback_doesNotPersist() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        transaction.rollback();
        transaction.close();

        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes())).isEmpty();
    }

    @Test
    void startTransaction_removeAndCommit_deletesValue() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction seed = storage.startTransaction();
        seed.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        seed.commit();
        seed.close();

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.remove(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes());
        transaction.commit();
        transaction.close();

        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes())).isEmpty();
    }

    @Test
    void startTransaction_throwsWhenReadOnly() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), false);
        storage = new RocksDBSegmentedStorage(dbManager);

        assertThatThrownBy(() -> storage.startTransaction())
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("read-only");
    }

    @Test
    void tryDelete_removesKeyWhenWritable() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction seed = storage.startTransaction();
        seed.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes(), "value".getBytes());
        seed.commit();
        seed.close();

        boolean deleted = storage.tryDelete(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes());

        assertThat(deleted).isTrue();
        assertThat(storage.get(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes())).isEmpty();
    }

    @Test
    void tryDelete_throwsWhenReadOnly() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), false);
        storage = new RocksDBSegmentedStorage(dbManager);

        assertThatThrownBy(() -> storage.tryDelete(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, "key".getBytes()))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("read-only");
    }

    @Test
    void streamFromKey_returnsOnlyEntriesWithinRange() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), true);
        storage = new RocksDBSegmentedStorage(dbManager);

        SegmentedKeyValueStorageTransaction transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x01}, "a".getBytes());
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x05}, "b".getBytes());
        transaction.put(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x09}, "c".getBytes());
        transaction.commit();
        transaction.close();

        List<Pair<byte[], byte[]>> results = storage.streamFromKey(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, new byte[]{0x02}, new byte[]{0x08}).toList();

        assertThat(results).hasSize(1);
        assertThat(results.get(0).getLeft()).isEqualTo(new byte[]{0x05});
        assertThat(results.get(0).getRight()).isEqualTo("b".getBytes());
    }

    @Test
    void streamFromKey_returnsEmptyForUnknownSegment() throws Exception {
        createTestDatabase();
        dbManager.openDatabase(tempDir.toString(), false);
        storage = new RocksDBSegmentedStorage(dbManager);

        List<Pair<byte[], byte[]>> results = storage.streamFromKey(
            KeyValueSegmentIdentifier.TRIE_LOG_STORAGE, new byte[]{0x00}, new byte[]{(byte) 0xff}).toList();

        assertThat(results).isEmpty();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.besu.RocksDBSegmentedStorageTest"`
Expected: FAIL — `startTransaction`/`tryDelete` throw `UnsupportedOperationException` unconditionally today; `streamFromKey` throws too.

- [ ] **Step 3: Implement the write path and ranged read**

Replace these four methods in `src/main/java/net/consensys/bric/besu/RocksDBSegmentedStorage.java` (currently at lines 71-99 for the `getNearestBefore` neighbor, 107-145 for the throwing stubs):

```java
    @Override
    public Stream<Pair<byte[], byte[]>> streamFromKey(SegmentIdentifier segment, byte[] startKey, byte[] endKey) {
        ColumnFamilyHandle cfHandle = getColumnFamilyHandle(segment);
        if (cfHandle == null) {
            return Stream.empty();
        }

        List<Pair<byte[], byte[]>> results = new ArrayList<>();
        try (RocksIterator iterator = dbManager.getDatabase().newIterator(cfHandle)) {
            iterator.seek(startKey);
            while (iterator.isValid() && Arrays.compareUnsigned(iterator.key(), endKey) <= 0) {
                results.add(Pair.of(iterator.key(), iterator.value()));
                iterator.next();
            }
        }
        return results.stream();
    }
```

```java
    @Override
    public SegmentedKeyValueStorageTransaction startTransaction() {
        if (!dbManager.isWritable()) {
            throw new UnsupportedOperationException("Transactions not supported in read-only mode");
        }
        return new RocksDBSegmentedTransaction();
    }

    @Override
    public boolean tryDelete(SegmentIdentifier segment, byte[] key) {
        if (!dbManager.isWritable()) {
            throw new UnsupportedOperationException("Delete not supported in read-only mode");
        }
        try {
            ColumnFamilyHandle cfHandle = getColumnFamilyHandle(segment);
            if (cfHandle == null) {
                return false;
            }
            dbManager.getDatabase().delete(cfHandle, key);
            return true;
        } catch (RocksDBException e) {
            return false;
        }
    }
```

Add this inner class at the bottom of the file, before the closing brace of `RocksDBSegmentedStorage`:

```java
    /** Batches puts/removes across segments and commits them atomically via a RocksDB WriteBatch. */
    private final class RocksDBSegmentedTransaction implements SegmentedKeyValueStorageTransaction {
        private final WriteBatch batch = new WriteBatch();

        @Override
        public void put(SegmentIdentifier segment, byte[] key, byte[] value) {
            try {
                batch.put(getColumnFamilyHandle(segment), key, value);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to stage put", e);
            }
        }

        @Override
        public void remove(SegmentIdentifier segment, byte[] key) {
            try {
                batch.delete(getColumnFamilyHandle(segment), key);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to stage remove", e);
            }
        }

        @Override
        public void commit() {
            try (WriteOptions writeOptions = new WriteOptions()) {
                dbManager.getDatabase().write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new StorageException("Failed to commit transaction", e);
            }
        }

        @Override
        public void rollback() {
            batch.clear();
        }

        @Override
        public void close() {
            batch.close();
        }
    }
```

Update the imports at the top of the file (add to the existing import block):

```java
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.Arrays;
```

(`RocksDBException`/`RocksIterator` are likely already imported for the existing `get`/`getNearestBefore` methods — check before duplicating an import.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.besu.RocksDBSegmentedStorageTest"`
Expected: PASS (8 tests)

- [ ] **Step 5: Run the full existing test suite to confirm no regression**

Run: `./gradlew test --tests "net.consensys.bric.besu.BesuFlatDbReaderTest"`
Expected: PASS (unchanged — this class only reads, confirming the modify didn't break `get`/`getNearestBefore`)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/RocksDBSegmentedStorage.java src/test/java/net/consensys/bric/besu/RocksDBSegmentedStorageTest.java
git commit -m "feat: add write and ranged-read support to RocksDBSegmentedStorage"
```

---

### Task 3: `FlatDbHealCheckpoint`

**Files:**
- Create: `src/main/java/net/consensys/bric/besu/FlatDbHealCheckpoint.java`
- Test: `src/test/java/net/consensys/bric/besu/FlatDbHealCheckpointTest.java`

**Interfaces:**
- Produces: `FlatDbHealCheckpoint(Bytes32 stateRoot, Phase phase, int nextRangeIndex)`, `.encode() -> byte[]`, `static .decode(byte[]) -> FlatDbHealCheckpoint`, `.stateRoot()`, `.phase()`, `.nextRangeIndex()`, nested `enum Phase { ACCOUNTS, STORAGE }`. Consumed by Task 5 (`FlatDbHealer`'s checkpoint read/write) and Task 7 (`heal()` orchestration).

- [ ] **Step 1: Write the failing test**

```java
package net.consensys.bric.besu;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlatDbHealCheckpointTest {

    private static final Bytes32 STATE_ROOT = Bytes32.fromHexString(
        "0x1111111111111111111111111111111111111111111111111111111111111111");

    @Test
    void encodeThenDecode_roundTripsAllFields() {
        FlatDbHealCheckpoint checkpoint =
            new FlatDbHealCheckpoint(STATE_ROOT, FlatDbHealCheckpoint.Phase.ACCOUNTS, 7);

        FlatDbHealCheckpoint decoded = FlatDbHealCheckpoint.decode(checkpoint.encode());

        assertThat(decoded.stateRoot()).isEqualTo(STATE_ROOT);
        assertThat(decoded.phase()).isEqualTo(FlatDbHealCheckpoint.Phase.ACCOUNTS);
        assertThat(decoded.nextRangeIndex()).isEqualTo(7);
        assertThat(decoded).isEqualTo(checkpoint);
    }

    @Test
    void encodeThenDecode_storagePhase() {
        FlatDbHealCheckpoint checkpoint =
            new FlatDbHealCheckpoint(STATE_ROOT, FlatDbHealCheckpoint.Phase.STORAGE, 16);

        FlatDbHealCheckpoint decoded = FlatDbHealCheckpoint.decode(checkpoint.encode());

        assertThat(decoded.phase()).isEqualTo(FlatDbHealCheckpoint.Phase.STORAGE);
        assertThat(decoded.nextRangeIndex()).isEqualTo(16);
    }

    @Test
    void decode_rejectsWrongLength() {
        assertThatThrownBy(() -> FlatDbHealCheckpoint.decode(new byte[]{1, 2, 3}))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid checkpoint length");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealCheckpointTest"`
Expected: FAIL — `FlatDbHealCheckpoint` does not exist.

- [ ] **Step 3: Write the implementation**

```java
package net.consensys.bric.besu;

import org.apache.tuweni.bytes.Bytes32;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Durable progress marker for an in-progress flat DB heal, persisted as a single value
 * under {@code VARIABLES["bricFlatDbHealCheckpoint"]}.
 */
public class FlatDbHealCheckpoint {

    public enum Phase {
        ACCOUNTS,
        STORAGE
    }

    private static final int ENCODED_LENGTH = 32 + 1 + 4;

    private final Bytes32 stateRoot;
    private final Phase phase;
    private final int nextRangeIndex;

    public FlatDbHealCheckpoint(Bytes32 stateRoot, Phase phase, int nextRangeIndex) {
        this.stateRoot = Objects.requireNonNull(stateRoot);
        this.phase = Objects.requireNonNull(phase);
        this.nextRangeIndex = nextRangeIndex;
    }

    public Bytes32 stateRoot() {
        return stateRoot;
    }

    public Phase phase() {
        return phase;
    }

    public int nextRangeIndex() {
        return nextRangeIndex;
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_LENGTH);
        buffer.put(stateRoot.toArrayUnsafe());
        buffer.put((byte) phase.ordinal());
        buffer.putInt(nextRangeIndex);
        return buffer.array();
    }

    public static FlatDbHealCheckpoint decode(byte[] bytes) {
        if (bytes.length != ENCODED_LENGTH) {
            throw new IllegalArgumentException(
                "Invalid checkpoint length: expected " + ENCODED_LENGTH + " but was " + bytes.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] rootBytes = new byte[32];
        buffer.get(rootBytes);
        Bytes32 stateRoot = Bytes32.wrap(rootBytes);
        Phase phase = Phase.values()[buffer.get()];
        int nextRangeIndex = buffer.getInt();
        return new FlatDbHealCheckpoint(stateRoot, phase, nextRangeIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlatDbHealCheckpoint that)) {
            return false;
        }
        return nextRangeIndex == that.nextRangeIndex
            && stateRoot.equals(that.stateRoot)
            && phase == that.phase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateRoot, phase, nextRangeIndex);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealCheckpointTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/FlatDbHealCheckpoint.java src/test/java/net/consensys/bric/besu/FlatDbHealCheckpointTest.java
git commit -m "feat: add FlatDbHealCheckpoint for resumable flat db healing"
```

---

### Task 4: `FlatDbHealer` construction and target state root

**Files:**
- Create: `src/main/java/net/consensys/bric/besu/FlatDbHealer.java`
- Test: `src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java` (new — extended by Tasks 5-7)

**Interfaces:**
- Consumes: `RocksDBSegmentedStorage` (Task 2), `NoOpKeyValueStorage` (Task 1), Besu's `BonsaiWorldStateKeyValueStorage`, `BonsaiFlatDbStrategyProvider`, `DataStorageConfiguration.DEFAULT_BONSAI_CONFIG`, `FlatDbCacheManager.NO_OP_CACHE`, `NoOpMetricsSystem`.
- Produces: `FlatDbHealer(BesuDatabaseManager dbManager)`, `.getTargetStateRoot() -> Bytes32` (throws `IllegalStateException` if absent). Consumed by Tasks 5-8.

This task also establishes the shared test fixture builder (`buildFixtureWorldState`) that Tasks 5-7 extend with more accounts/mismatches — write it once here with the pieces needed for this task's tests (a database with a persisted `worldRoot` and nothing else), and note where later tasks will add to it.

- [ ] **Step 1: Write the failing tests**

```java
package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.PathBasedWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlatDbHealerTest {

    @TempDir
    Path tempDir;

    private BesuDatabaseManager dbManager;

    @BeforeEach
    void setUp() {
        RocksDB.loadLibrary();
        dbManager = new BesuDatabaseManager();
    }

    @AfterEach
    void tearDown() {
        if (dbManager.isOpen()) {
            dbManager.closeDatabase();
        }
    }

    /**
     * Creates the column families a real Bonsai database has, opens it via
     * BesuDatabaseManager in write mode, and returns a BonsaiWorldStateKeyValueStorage
     * built the same way FlatDbHealer builds its own — used only to seed fixture data
     * (trie nodes, flat entries, the worldRoot key) that FlatDbHealer will later read.
     */
    private BonsaiWorldStateKeyValueStorage openWritableFixtureDatabase() throws Exception {
        List<ColumnFamilyDescriptor> descriptors = List.of(
            new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE.getId()),
            new ColumnFamilyDescriptor(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE.getId()),
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

    static BonsaiWorldStateKeyValueStorage buildWorldState(BesuDatabaseManager dbManager) {
        RocksDBSegmentedStorage storage = new RocksDBSegmentedStorage(dbManager);
        BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
            new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
        flatDbStrategyProvider.loadFlatDbStrategy(storage);
        return new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider, storage, new NoOpKeyValueStorage(), FlatDbCacheManager.NO_OP_CACHE, 0L);
    }

    @Test
    void getTargetStateRoot_returnsPersistedWorldRoot() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Bytes32 expectedRoot = Bytes32.fromHexString(
            "0x2222222222222222222222222222222222222222222222222222222222222222");
        BonsaiWorldStateKeyValueStorage.Updater seedUpdater = fixtureWorldState.updater();
        seedUpdater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY,
            expectedRoot.toArrayUnsafe());
        seedUpdater.commit();

        FlatDbHealer healer = new FlatDbHealer(dbManager);

        assertThat(healer.getTargetStateRoot()).isEqualTo(expectedRoot);
    }

    @Test
    void getTargetStateRoot_throwsWhenNoWorldRootPersisted() throws Exception {
        openWritableFixtureDatabase();

        FlatDbHealer healer = new FlatDbHealer(dbManager);

        assertThatThrownBy(healer::getTargetStateRoot)
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("No world state root found");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: FAIL — `FlatDbHealer` does not exist.

- [ ] **Step 3: Write the implementation**

```java
package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.trie.pathbased.common.storage.cache.FlatDbCacheManager;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

/**
 * Reconciles a Bonsai database's flat account/storage tables against the canonical
 * account and storage tries, driving Besu's own trie/storage classes directly rather
 * than Besu's peer-network-driven snap-sync healing (which isn't reusable outside a
 * running node — see docs/superpowers/specs/2026-07-24-flatdb-heal-design.md).
 */
public class FlatDbHealer {

    private final RocksDBSegmentedStorage storage;
    private final BonsaiWorldStateKeyValueStorage worldState;

    public FlatDbHealer(BesuDatabaseManager dbManager) {
        this.storage = new RocksDBSegmentedStorage(dbManager);
        BonsaiFlatDbStrategyProvider flatDbStrategyProvider = new BonsaiFlatDbStrategyProvider(
            new NoOpMetricsSystem(), DataStorageConfiguration.DEFAULT_BONSAI_CONFIG);
        flatDbStrategyProvider.loadFlatDbStrategy(storage);
        this.worldState = new BonsaiWorldStateKeyValueStorage(
            flatDbStrategyProvider, storage, new NoOpKeyValueStorage(), FlatDbCacheManager.NO_OP_CACHE, 0L);
    }

    public Bytes32 getTargetStateRoot() {
        return worldState.getWorldStateRootHash()
            .map(Bytes32::wrap)
            .orElseThrow(() -> new IllegalStateException(
                "No world state root found; database may be empty or not yet synced."));
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/FlatDbHealer.java src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java
git commit -m "feat: add FlatDbHealer construction and target state root lookup"
```

---

### Task 5: `FlatDbHealer.healAccountRange` — account-level diff and write

**Files:**
- Modify: `src/main/java/net/consensys/bric/besu/FlatDbHealer.java`
- Modify: `src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java`

**Interfaces:**
- Consumes: `RangeManager` (`besu-ethereum-trie`), `RangeStorageEntriesCollector`, `TrieIterator`, `StoredMerklePatriciaTrie`, `BonsaiWorldStateKeyValueStorage.getAccountStateTrieNode`/`.updater()`/`Updater.putAccountInfoState`/`.removeAccountInfoState`.
- Produces: package-private `FlatDbHealer.AccountRangeOutcome healAccountRange(Bytes32 stateRoot, Bytes32 startKeyHash, Bytes32 endKeyHash, boolean dryRun)` with fields `accountsChecked`, `added`, `updated`, `removed` (all `long`), `divergentAccounts` (`List<org.hyperledger.besu.datatypes.Hash>`). Consumed by Task 7's `heal()` orchestration.

Fixture note: this task builds a real account trie (via `StoredMerklePatriciaTrie.put`/`.commit(NodeUpdater)`) with four accounts — one whose flat entry matches (no-op), one missing from flat, one whose flat entry is stale, plus one flat-only "orphan" entry with no trie counterpart — and asserts `healAccountRange` reconciles all three mismatches and leaves the matching one untouched.

- [ ] **Step 1: Write the failing test**

Add to `FlatDbHealerTest.java` (new imports needed: see below; new test methods appended to the class body):

```java
    private static byte[] accountRlp(long nonce, Hash storageRoot) {
        return new BonsaiAccount(
                null, Address.ZERO, Hash.ZERO, nonce, Wei.ZERO, storageRoot, Hash.EMPTY, false, null)
            .serializeAccount().toArrayUnsafe();
    }

    /** Builds a real, persisted account trie with the given (accountHash -> accountRlp) leaves. */
    private static Bytes32 seedAccountTrie(
            BonsaiWorldStateKeyValueStorage worldState, Map<Hash, byte[]> accounts) {
        StoredMerklePatriciaTrie<Bytes, Bytes> trie = new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(), Function.identity(), Function.identity());
        accounts.forEach((accountHash, rlp) -> trie.put(accountHash, Bytes.wrap(rlp)));

        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        trie.commit((location, hash, node) -> updater.putAccountStateTrieNode(location, hash, node));
        Bytes32 rootHash = trie.getRootHash();
        updater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE,
            PathBasedWorldStateKeyValueStorage.WORLD_ROOT_HASH_KEY,
            rootHash.toArrayUnsafe());
        updater.commit();
        return rootHash;
    }

    private static void seedFlatAccount(
            BonsaiWorldStateKeyValueStorage worldState, Hash accountHash, byte[] rlp) {
        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        updater.putAccountInfoState(accountHash, Bytes.wrap(rlp));
        updater.commit();
    }

    @Test
    void healAccountRange_addsMissingUpdatesStaleAndRemovesOrphan() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();

        Hash matchingHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        Hash missingHash = Hash.wrap(Bytes32.leftPad(Bytes.of(2)));
        Hash staleHash = Hash.wrap(Bytes32.leftPad(Bytes.of(3)));
        Hash orphanHash = Hash.wrap(Bytes32.leftPad(Bytes.of(4)));

        byte[] matchingRlp = accountRlp(1, Hash.EMPTY);
        byte[] missingRlp = accountRlp(2, Hash.EMPTY);
        byte[] staleRlpInTrie = accountRlp(3, Hash.EMPTY);
        byte[] staleRlpInFlat = accountRlp(999, Hash.EMPTY);
        byte[] orphanRlp = accountRlp(4, Hash.EMPTY);

        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(
            matchingHash, matchingRlp,
            missingHash, missingRlp,
            staleHash, staleRlpInTrie));

        seedFlatAccount(fixtureWorldState, matchingHash, matchingRlp);
        seedFlatAccount(fixtureWorldState, staleHash, staleRlpInFlat);
        seedFlatAccount(fixtureWorldState, orphanHash, orphanRlp);

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealer.AccountRangeOutcome outcome = healer.healAccountRange(
            stateRoot, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, false);

        assertThat(outcome.accountsChecked).isEqualTo(3);
        assertThat(outcome.added).isEqualTo(1);
        assertThat(outcome.updated).isEqualTo(1);
        assertThat(outcome.removed).isEqualTo(1);
        assertThat(outcome.divergentAccounts).containsExactlyInAnyOrder(missingHash, staleHash);

        // Re-open a fresh reader-side world state and verify the flat table now matches the trie.
        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(matchingHash)).contains(Bytes.wrap(matchingRlp));
        assertThat(verifyWorldState.getAccount(missingHash)).contains(Bytes.wrap(missingRlp));
        assertThat(verifyWorldState.getAccount(staleHash)).contains(Bytes.wrap(staleRlpInTrie));
        assertThat(verifyWorldState.getAccount(orphanHash)).isEmpty();
    }

    @Test
    void healAccountRange_dryRun_reportsWithoutWriting() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash missingHash = Hash.wrap(Bytes32.leftPad(Bytes.of(2)));
        byte[] missingRlp = accountRlp(2, Hash.EMPTY);

        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(missingHash, missingRlp));

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealer.AccountRangeOutcome outcome = healer.healAccountRange(
            stateRoot, RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, true);

        assertThat(outcome.added).isEqualTo(1);

        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(missingHash)).isEmpty();
    }
```

New imports to add at the top of `FlatDbHealerTest.java`:

```java
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.RangeManager;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiAccount;

import java.util.Function;
import java.util.Map;
import java.util.Optional;
```

(`assertThat` is already imported by Task 4's Step 1 — do not add it again, `javac` rejects duplicate single-type imports.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: FAIL — `healAccountRange` and `AccountRangeOutcome` do not exist.

- [ ] **Step 3: Implement `healAccountRange`**

Add to `FlatDbHealer.java` (new imports listed after):

```java
    static final class AccountRangeOutcome {
        final long accountsChecked;
        final long added;
        final long updated;
        final long removed;
        final List<Hash> divergentAccounts;

        AccountRangeOutcome(
                long accountsChecked, long added, long updated, long removed, List<Hash> divergentAccounts) {
            this.accountsChecked = accountsChecked;
            this.added = added;
            this.updated = updated;
            this.removed = removed;
            this.divergentAccounts = divergentAccounts;
        }
    }

    AccountRangeOutcome healAccountRange(
            Bytes32 stateRoot, Bytes32 startKeyHash, Bytes32 endKeyHash, boolean dryRun) {
        MerkleTrie<Bytes, Bytes> accountTrie = new StoredMerklePatriciaTrie<>(
            worldState::getAccountStateTrieNode, stateRoot, Function.identity(), Function.identity());

        RangeStorageEntriesCollector collector = RangeStorageEntriesCollector.createCollector(
            startKeyHash, endKeyHash, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
        NavigableMap<Bytes32, Bytes> trieAccounts = new TreeMap<>(accountTrie.entriesFrom(
            root -> RangeStorageEntriesCollector.collectEntries(collector, visitor, root, startKeyHash)));

        NavigableMap<Bytes32, Bytes> flatAccounts = readFlatRange(
            KeyValueSegmentIdentifier.ACCOUNT_INFO_STATE, startKeyHash.toArrayUnsafe(), endKeyHash.toArrayUnsafe());

        List<Bytes32> toAdd = new ArrayList<>();
        List<Bytes32> toUpdate = new ArrayList<>();
        List<Bytes32> toRemove = new ArrayList<>();

        for (Map.Entry<Bytes32, Bytes> entry : trieAccounts.entrySet()) {
            Bytes flatValue = flatAccounts.get(entry.getKey());
            if (flatValue == null) {
                toAdd.add(entry.getKey());
            } else if (!flatValue.equals(entry.getValue())) {
                toUpdate.add(entry.getKey());
            }
        }
        for (Bytes32 accountHash : flatAccounts.keySet()) {
            if (!trieAccounts.containsKey(accountHash)) {
                toRemove.add(accountHash);
            }
        }

        if (!dryRun && !(toAdd.isEmpty() && toUpdate.isEmpty() && toRemove.isEmpty())) {
            BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
            for (Bytes32 accountHash : toAdd) {
                updater.putAccountInfoState(Hash.wrap(accountHash), trieAccounts.get(accountHash));
            }
            for (Bytes32 accountHash : toUpdate) {
                updater.putAccountInfoState(Hash.wrap(accountHash), trieAccounts.get(accountHash));
            }
            for (Bytes32 accountHash : toRemove) {
                updater.removeAccountInfoState(Hash.wrap(accountHash));
            }
            updater.commit();
        }

        List<Hash> divergentAccounts = new ArrayList<>();
        toAdd.forEach(hash -> divergentAccounts.add(Hash.wrap(hash)));
        toUpdate.forEach(hash -> divergentAccounts.add(Hash.wrap(hash)));

        return new AccountRangeOutcome(
            trieAccounts.size(), toAdd.size(), toUpdate.size(), toRemove.size(), divergentAccounts);
    }

    private NavigableMap<Bytes32, Bytes> readFlatRange(
            KeyValueSegmentIdentifier segment, byte[] startKey, byte[] endKey) {
        NavigableMap<Bytes32, Bytes> result = new TreeMap<>();
        for (Pair<byte[], byte[]> entry : storage.streamFromKey(segment, startKey, endKey).toList()) {
            result.put(Bytes32.wrap(entry.getLeft()), Bytes.wrap(entry.getRight()));
        }
        return result;
    }
```

Add these imports to `FlatDbHealer.java`:

```java
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/FlatDbHealer.java src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java
git commit -m "feat: implement FlatDbHealer.healAccountRange account diff and write"
```

---

### Task 6: `FlatDbHealer.healAccountStorage` — storage-level diff and write

**Files:**
- Modify: `src/main/java/net/consensys/bric/besu/FlatDbHealer.java`
- Modify: `src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java`

**Interfaces:**
- Consumes: `BonsaiWorldStateKeyValueStorage.getAccountStorageTrieNode(Hash, Bytes, Bytes32)`, `.Updater.putStorageValueBySlotHash`/`.removeStorageValueBySlotHash`.
- Produces: package-private `FlatDbHealer.StorageRangeOutcome healAccountStorage(Hash accountHash, Hash storageRoot, boolean dryRun)` with fields `slotsChecked`, `added`, `updated`, `removed` (all `long`). Consumed by Task 7's `heal()` orchestration.

- [ ] **Step 1: Write the failing test**

Add to `FlatDbHealerTest.java`:

```java
    /** Builds a real, persisted storage trie for one account and returns its root. */
    private static Hash seedStorageTrie(
            BonsaiWorldStateKeyValueStorage worldState, Hash accountHash, Map<Bytes32, Bytes> slots) {
        StoredMerklePatriciaTrie<Bytes, Bytes> trie = new StoredMerklePatriciaTrie<>(
            (location, hash) -> Optional.empty(), Function.identity(), Function.identity());
        slots.forEach(trie::put);

        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        trie.commit((location, hash, node) ->
            updater.putAccountStorageTrieNode(accountHash, location, hash, node));
        Bytes32 rootHash = trie.getRootHash();
        updater.commit();
        return Hash.wrap(rootHash);
    }

    private static void seedFlatStorage(
            BonsaiWorldStateKeyValueStorage worldState, Hash accountHash, Hash slotHash, Bytes value) {
        BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
        updater.putStorageValueBySlotHash(accountHash, slotHash, value);
        updater.commit();
    }

    @Test
    void healAccountStorage_addsMissingUpdatesStaleAndRemovesOrphan() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));

        Hash matchingSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(10)));
        Hash missingSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(20)));
        Hash staleSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(30)));
        Hash orphanSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(40)));

        Bytes matchingValue = Bytes.of(1);
        Bytes missingValue = Bytes.of(2);
        Bytes staleValueInTrie = Bytes.of(3);
        Bytes staleValueInFlat = Bytes.of(99);
        Bytes orphanValue = Bytes.of(4);

        Hash storageRoot = seedStorageTrie(fixtureWorldState, accountHash, Map.of(
            matchingSlot, matchingValue,
            missingSlot, missingValue,
            staleSlot, staleValueInTrie));

        seedFlatStorage(fixtureWorldState, accountHash, matchingSlot, matchingValue);
        seedFlatStorage(fixtureWorldState, accountHash, staleSlot, staleValueInFlat);
        seedFlatStorage(fixtureWorldState, accountHash, orphanSlot, orphanValue);

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealer.StorageRangeOutcome outcome =
            healer.healAccountStorage(accountHash, storageRoot, false);

        assertThat(outcome.slotsChecked).isEqualTo(3);
        assertThat(outcome.added).isEqualTo(1);
        assertThat(outcome.updated).isEqualTo(1);
        assertThat(outcome.removed).isEqualTo(1);

        // Read the flat table directly (not via getStorageValueByStorageSlotKey, which derives
        // storageRoot from getAccount(accountHash) — this test never seeds a flat account entry,
        // since it's testing storage healing in isolation from account healing).
        RocksDBSegmentedStorage verifyStorage = new RocksDBSegmentedStorage(dbManager);
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, matchingSlot).toArrayUnsafe()))
            .contains(matchingValue.toArrayUnsafe());
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, missingSlot).toArrayUnsafe()))
            .contains(missingValue.toArrayUnsafe());
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, staleSlot).toArrayUnsafe()))
            .contains(staleValueInTrie.toArrayUnsafe());
        assertThat(verifyStorage.get(KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE,
                Bytes.concatenate(accountHash, orphanSlot).toArrayUnsafe()))
            .isEmpty();
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: FAIL — `healAccountStorage` and `StorageRangeOutcome` do not exist.

- [ ] **Step 3: Implement `healAccountStorage`**

Add to `FlatDbHealer.java`:

```java
    static final class StorageRangeOutcome {
        final long slotsChecked;
        final long added;
        final long updated;
        final long removed;

        StorageRangeOutcome(long slotsChecked, long added, long updated, long removed) {
            this.slotsChecked = slotsChecked;
            this.added = added;
            this.updated = updated;
            this.removed = removed;
        }
    }

    StorageRangeOutcome healAccountStorage(Hash accountHash, Hash storageRoot, boolean dryRun) {
        MerkleTrie<Bytes, Bytes> storageTrie = new StoredMerklePatriciaTrie<>(
            (location, hash) -> worldState.getAccountStorageTrieNode(accountHash, location, hash),
            storageRoot, Function.identity(), Function.identity());

        RangeStorageEntriesCollector collector = RangeStorageEntriesCollector.createCollector(
            RangeManager.MIN_RANGE, RangeManager.MAX_RANGE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        TrieIterator<Bytes> visitor = RangeStorageEntriesCollector.createVisitor(collector);
        NavigableMap<Bytes32, Bytes> trieSlots = new TreeMap<>(storageTrie.entriesFrom(
            root -> RangeStorageEntriesCollector.collectEntries(collector, visitor, root, RangeManager.MIN_RANGE)));

        NavigableMap<Bytes32, Bytes> flatSlots = readFlatStorageRange(accountHash);

        List<Bytes32> toAdd = new ArrayList<>();
        List<Bytes32> toUpdate = new ArrayList<>();
        List<Bytes32> toRemove = new ArrayList<>();

        for (Map.Entry<Bytes32, Bytes> entry : trieSlots.entrySet()) {
            Bytes flatValue = flatSlots.get(entry.getKey());
            if (flatValue == null) {
                toAdd.add(entry.getKey());
            } else if (!flatValue.equals(entry.getValue())) {
                toUpdate.add(entry.getKey());
            }
        }
        for (Bytes32 slotHash : flatSlots.keySet()) {
            if (!trieSlots.containsKey(slotHash)) {
                toRemove.add(slotHash);
            }
        }

        if (!dryRun && !(toAdd.isEmpty() && toUpdate.isEmpty() && toRemove.isEmpty())) {
            BonsaiWorldStateKeyValueStorage.Updater updater = worldState.updater();
            for (Bytes32 slotHash : toAdd) {
                updater.putStorageValueBySlotHash(accountHash, Hash.wrap(slotHash), trieSlots.get(slotHash));
            }
            for (Bytes32 slotHash : toUpdate) {
                updater.putStorageValueBySlotHash(accountHash, Hash.wrap(slotHash), trieSlots.get(slotHash));
            }
            for (Bytes32 slotHash : toRemove) {
                updater.removeStorageValueBySlotHash(accountHash, Hash.wrap(slotHash));
            }
            updater.commit();
        }

        return new StorageRangeOutcome(trieSlots.size(), toAdd.size(), toUpdate.size(), toRemove.size());
    }

    private NavigableMap<Bytes32, Bytes> readFlatStorageRange(Hash accountHash) {
        byte[] startKey = Bytes.concatenate(accountHash, RangeManager.MIN_RANGE).toArrayUnsafe();
        byte[] endKey = Bytes.concatenate(accountHash, RangeManager.MAX_RANGE).toArrayUnsafe();

        NavigableMap<Bytes32, Bytes> result = new TreeMap<>();
        for (Pair<byte[], byte[]> entry : storage.streamFromKey(
                KeyValueSegmentIdentifier.ACCOUNT_STORAGE_STORAGE, startKey, endKey).toList()) {
            byte[] key = entry.getLeft();
            Bytes32 slotHash = Bytes32.wrap(key, 32);
            result.put(slotHash, Bytes.wrap(entry.getRight()));
        }
        return result;
    }
```

Add this import to `FlatDbHealer.java`:

```java
import org.hyperledger.besu.ethereum.trie.RangeManager;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/FlatDbHealer.java src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java
git commit -m "feat: implement FlatDbHealer.healAccountStorage storage diff and write"
```

---

### Task 7: `FlatDbHealer.heal()` orchestration, resumability, and dry-run

**Files:**
- Modify: `src/main/java/net/consensys/bric/besu/FlatDbHealer.java`
- Create: `src/main/java/net/consensys/bric/besu/FlatDbHealResult.java`
- Create: `src/main/java/net/consensys/bric/besu/FlatDbHealProgressListener.java`
- Modify: `src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java`

**Interfaces:**
- Consumes: `FlatDbHealCheckpoint` (Task 3), `healAccountRange`/`healAccountStorage` (Tasks 5-6), `RangeManager.generateAllRanges(int)`, `BonsaiWorldStateKeyValueStorage.getAccount(Hash)`, `.upgradeToFullFlatDbMode()`, `PmtStateTrieAccountValue.readFrom(RLP.input(...))`.
- Produces: `FlatDbHealer.heal(boolean dryRun, FlatDbHealProgressListener listener) -> FlatDbHealResult`. Consumed by Task 8's `DbUpgradeFlatDbCommand`.

`FlatDbHealResult` fields: `accountsAdded`, `accountsUpdated`, `accountsRemoved`, `slotsAdded`, `slotsUpdated`, `slotsRemoved` (all `long`), `dryRun` (`boolean`), plus `totalAccountsFixed()`/`totalSlotsFixed()`.

`FlatDbHealProgressListener` methods: `onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed)`, `onStorageAccountComplete(int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed)`.

- [ ] **Step 1: Write `FlatDbHealResult`**

```java
package net.consensys.bric.besu;

public class FlatDbHealResult {
    public final long accountsAdded;
    public final long accountsUpdated;
    public final long accountsRemoved;
    public final long slotsAdded;
    public final long slotsUpdated;
    public final long slotsRemoved;
    public final boolean dryRun;

    public FlatDbHealResult(
            long accountsAdded, long accountsUpdated, long accountsRemoved,
            long slotsAdded, long slotsUpdated, long slotsRemoved, boolean dryRun) {
        this.accountsAdded = accountsAdded;
        this.accountsUpdated = accountsUpdated;
        this.accountsRemoved = accountsRemoved;
        this.slotsAdded = slotsAdded;
        this.slotsUpdated = slotsUpdated;
        this.slotsRemoved = slotsRemoved;
        this.dryRun = dryRun;
    }

    public long totalAccountsFixed() {
        return accountsAdded + accountsUpdated + accountsRemoved;
    }

    public long totalSlotsFixed() {
        return slotsAdded + slotsUpdated + slotsRemoved;
    }
}
```

- [ ] **Step 2: Write `FlatDbHealProgressListener`**

```java
package net.consensys.bric.besu;

public interface FlatDbHealProgressListener {
    void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed);

    void onStorageAccountComplete(int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed);

    /** Listener that discards every event — used by tests and dry-run callers that don't print progress. */
    FlatDbHealProgressListener NO_OP = new FlatDbHealProgressListener() {
        @Override
        public void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed) {
        }

        @Override
        public void onStorageAccountComplete(
                int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed) {
        }
    };
}
```

- [ ] **Step 3: Write the failing tests**

Add to `FlatDbHealerTest.java`:

```java
    @Test
    void heal_reconcilesAccountsAndTheirStorageThenUpgradesToFull() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();

        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        Hash missingSlot = Hash.wrap(Bytes32.leftPad(Bytes.of(20)));
        Bytes missingSlotValue = Bytes.of(7);

        Hash storageRoot = seedStorageTrie(fixtureWorldState, accountHash, Map.of(missingSlot, missingSlotValue));
        byte[] accountRlp = accountRlp(1, storageRoot);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));
        // No flat account entry seeded at all: account is "missing", so its storage must be healed too.

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(false, FlatDbHealProgressListener.NO_OP);

        assertThat(result.accountsAdded).isEqualTo(1);
        assertThat(result.slotsAdded).isEqualTo(1);
        assertThat(result.dryRun).isFalse();

        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountHash)).contains(Bytes.wrap(accountRlp));
        assertThat(verifyWorldState.getStorageValueByStorageSlotKey(
                accountHash, new org.hyperledger.besu.datatypes.StorageSlotKey(missingSlot, Optional.empty())))
            .contains(missingSlotValue);

        Optional<byte[]> flatDbMode = new net.consensys.bric.db.SegmentReader(dbManager)
            .get(KeyValueSegmentIdentifier.TRIE_BRANCH_STORAGE, "flatDbStatus".getBytes());
        assertThat(flatDbMode).isPresent();
        assertThat(flatDbMode.get()[0]).isEqualTo((byte) 0x01); // FULL, per FlatDbMode encoding
    }

    @Test
    void heal_dryRun_reportsWithoutTouchingCheckpointOrData() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        byte[] accountRlp = accountRlp(1, Hash.EMPTY);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(true, FlatDbHealProgressListener.NO_OP);

        assertThat(result.accountsAdded).isEqualTo(1);
        assertThat(result.dryRun).isTrue();

        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountHash)).isEmpty();
        assertThat(new net.consensys.bric.db.SegmentReader(dbManager)
            .get(KeyValueSegmentIdentifier.VARIABLES, "bricFlatDbHealCheckpoint".getBytes())).isEmpty();
    }

    @Test
    void heal_resumesFromCheckpointAfterSimulatedInterruption() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        byte[] accountRlp = accountRlp(1, Hash.EMPTY);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));

        // Simulate an interruption after range 0 of 16 completed by writing the checkpoint directly,
        // without ever writing accountHash's flat entry (as if the process died mid-range-1).
        BonsaiWorldStateKeyValueStorage.Updater updater = fixtureWorldState.updater();
        updater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.VARIABLES, "bricFlatDbHealCheckpoint".getBytes(),
            new FlatDbHealCheckpoint(stateRoot, FlatDbHealCheckpoint.Phase.ACCOUNTS, 1).encode());
        updater.commit();

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(false, FlatDbHealProgressListener.NO_OP);

        // accountHash falls in range 0 (its hash starts with 0x00...01, the very first range),
        // which the simulated checkpoint marks already-done, so it must NOT be healed.
        BonsaiWorldStateKeyValueStorage verifyWorldState = buildWorldState(dbManager);
        assertThat(verifyWorldState.getAccount(accountHash)).isEmpty();
        assertThat(result.accountsAdded).isEqualTo(0);
    }

    @Test
    void heal_discardsStaleCheckpointWhenStateRootHasMoved() throws Exception {
        BonsaiWorldStateKeyValueStorage fixtureWorldState = openWritableFixtureDatabase();
        Hash accountHash = Hash.wrap(Bytes32.leftPad(Bytes.of(1)));
        byte[] accountRlp = accountRlp(1, Hash.EMPTY);
        Bytes32 stateRoot = seedAccountTrie(fixtureWorldState, Map.of(accountHash, accountRlp));

        Bytes32 staleRoot = Bytes32.leftPad(Bytes.of(0x7f));
        BonsaiWorldStateKeyValueStorage.Updater updater = fixtureWorldState.updater();
        updater.getWorldStateTransaction().put(
            KeyValueSegmentIdentifier.VARIABLES, "bricFlatDbHealCheckpoint".getBytes(),
            new FlatDbHealCheckpoint(staleRoot, FlatDbHealCheckpoint.Phase.ACCOUNTS, 1).encode());
        updater.commit();

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        FlatDbHealResult result = healer.heal(false, FlatDbHealProgressListener.NO_OP);

        // Stale checkpoint discarded, so the full walk runs and finds accountHash missing.
        assertThat(result.accountsAdded).isEqualTo(1);
    }
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: FAIL — `heal`, `FlatDbHealResult`, `FlatDbHealProgressListener` don't exist yet.

- [ ] **Step 5: Implement `heal()` and its checkpoint helpers**

Add to `FlatDbHealer.java`:

```java
    private static final int RANGE_COUNT = 16;
    private static final byte[] CHECKPOINT_KEY = "bricFlatDbHealCheckpoint".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PENDING_STORAGE_ACCOUNTS_KEY =
        "bricFlatDbHealPendingStorageAccounts".getBytes(StandardCharsets.UTF_8);

    public FlatDbHealResult heal(boolean dryRun, FlatDbHealProgressListener listener) {
        Bytes32 stateRoot = getTargetStateRoot();

        int startRangeIndex = 0;
        FlatDbHealCheckpoint.Phase resumePhase = FlatDbHealCheckpoint.Phase.ACCOUNTS;
        List<Hash> divergentAccounts = new ArrayList<>();

        if (!dryRun) {
            Optional<FlatDbHealCheckpoint> checkpoint = readCheckpoint();
            if (checkpoint.isPresent() && checkpoint.get().stateRoot().equals(stateRoot)) {
                startRangeIndex = checkpoint.get().nextRangeIndex();
                resumePhase = checkpoint.get().phase();
                divergentAccounts.addAll(readPendingStorageAccounts());
            } else if (checkpoint.isPresent()) {
                LOG.info("Chain has advanced since the last interrupted run; "
                    + "restarting heal from the beginning.");
            }
        }

        long accountsAdded = 0;
        long accountsUpdated = 0;
        long accountsRemoved = 0;

        if (resumePhase == FlatDbHealCheckpoint.Phase.ACCOUNTS) {
            List<Map.Entry<Bytes32, Bytes32>> ranges =
                new ArrayList<>(RangeManager.generateAllRanges(RANGE_COUNT).entrySet());
            for (int i = startRangeIndex; i < ranges.size(); i++) {
                Map.Entry<Bytes32, Bytes32> range = ranges.get(i);
                AccountRangeOutcome outcome = healAccountRange(stateRoot, range.getKey(), range.getValue(), dryRun);
                accountsAdded += outcome.added;
                accountsUpdated += outcome.updated;
                accountsRemoved += outcome.removed;
                divergentAccounts.addAll(outcome.divergentAccounts);
                listener.onRangeComplete(
                    i + 1, ranges.size(), outcome.accountsChecked, outcome.added + outcome.updated + outcome.removed);
                if (!dryRun) {
                    persistCheckpoint(new FlatDbHealCheckpoint(stateRoot, FlatDbHealCheckpoint.Phase.ACCOUNTS, i + 1));
                }
            }
            if (!dryRun) {
                persistPendingStorageAccounts(divergentAccounts);
                persistCheckpoint(new FlatDbHealCheckpoint(stateRoot, FlatDbHealCheckpoint.Phase.STORAGE, RANGE_COUNT));
            }
        }

        long slotsAdded = 0;
        long slotsUpdated = 0;
        long slotsRemoved = 0;
        int totalAccountsToHeal = divergentAccounts.size();
        List<Hash> remainingAccounts = new ArrayList<>(divergentAccounts);
        int accountsHealed = 0;

        while (!remainingAccounts.isEmpty()) {
            Hash accountHash = remainingAccounts.remove(0);
            Optional<Bytes> accountValue = worldState.getAccount(accountHash);
            if (accountValue.isPresent()) {
                Hash storageRoot = PmtStateTrieAccountValue.readFrom(RLP.input(accountValue.get())).getStorageRoot();
                StorageRangeOutcome outcome = healAccountStorage(accountHash, storageRoot, dryRun);
                slotsAdded += outcome.added;
                slotsUpdated += outcome.updated;
                slotsRemoved += outcome.removed;
                accountsHealed++;
                listener.onStorageAccountComplete(
                    accountsHealed, totalAccountsToHeal, outcome.slotsChecked,
                    outcome.added + outcome.updated + outcome.removed);
            }
            if (!dryRun) {
                persistPendingStorageAccounts(remainingAccounts);
            }
        }

        if (!dryRun) {
            clearCheckpoint();
            worldState.upgradeToFullFlatDbMode();
        }

        return new FlatDbHealResult(
            accountsAdded, accountsUpdated, accountsRemoved, slotsAdded, slotsUpdated, slotsRemoved, dryRun);
    }

    private Optional<FlatDbHealCheckpoint> readCheckpoint() {
        return storage.get(KeyValueSegmentIdentifier.VARIABLES, CHECKPOINT_KEY)
            .map(FlatDbHealCheckpoint::decode);
    }

    private List<Hash> readPendingStorageAccounts() {
        return storage.get(KeyValueSegmentIdentifier.VARIABLES, PENDING_STORAGE_ACCOUNTS_KEY)
            .map(FlatDbHealer::decodeAccountList)
            .orElseGet(ArrayList::new);
    }

    private void persistCheckpoint(FlatDbHealCheckpoint checkpoint) {
        var transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.VARIABLES, CHECKPOINT_KEY, checkpoint.encode());
        transaction.commit();
        transaction.close();
    }

    private void persistPendingStorageAccounts(List<Hash> accounts) {
        var transaction = storage.startTransaction();
        transaction.put(KeyValueSegmentIdentifier.VARIABLES, PENDING_STORAGE_ACCOUNTS_KEY, encodeAccountList(accounts));
        transaction.commit();
        transaction.close();
    }

    private void clearCheckpoint() {
        var transaction = storage.startTransaction();
        transaction.remove(KeyValueSegmentIdentifier.VARIABLES, CHECKPOINT_KEY);
        transaction.remove(KeyValueSegmentIdentifier.VARIABLES, PENDING_STORAGE_ACCOUNTS_KEY);
        transaction.commit();
        transaction.close();
    }

    private static byte[] encodeAccountList(List<Hash> accounts) {
        ByteBuffer buffer = ByteBuffer.allocate(accounts.size() * 32);
        for (Hash account : accounts) {
            buffer.put(account.toArrayUnsafe());
        }
        return buffer.array();
    }

    private static List<Hash> decodeAccountList(byte[] bytes) {
        List<Hash> accounts = new ArrayList<>();
        for (int offset = 0; offset < bytes.length; offset += 32) {
            accounts.add(Hash.wrap(Bytes32.wrap(bytes, offset)));
        }
        return accounts;
    }
```

Add these imports and the logger field to `FlatDbHealer.java`:

```java
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.trie.common.PmtStateTrieAccountValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
```

and, inside the class body near the top:

```java
    private static final Logger LOG = LoggerFactory.getLogger(FlatDbHealer.class);
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.besu.FlatDbHealerTest"`
Expected: PASS (9 tests)

- [ ] **Step 7: Commit**

```bash
git add src/main/java/net/consensys/bric/besu/FlatDbHealer.java src/main/java/net/consensys/bric/besu/FlatDbHealResult.java src/main/java/net/consensys/bric/besu/FlatDbHealProgressListener.java src/test/java/net/consensys/bric/besu/FlatDbHealerTest.java
git commit -m "feat: implement FlatDbHealer.heal orchestration with dry-run and resume"
```

---

### Task 8: `db upgrade-flatdb` CLI command and wiring

**Files:**
- Create: `src/main/java/net/consensys/bric/commands/DbUpgradeFlatDbCommand.java`
- Test: `src/test/java/net/consensys/bric/commands/DbUpgradeFlatDbCommandTest.java`
- Modify: `src/main/java/net/consensys/bric/commands/DbCommand.java`
- Modify: `src/test/java/net/consensys/bric/commands/DbCommandTest.java`
- Modify: `src/main/java/net/consensys/bric/completion/BricCompleter.java`

**Interfaces:**
- Consumes: `FlatDbHealer` (Task 7), `BesuDatabaseManager.isOpen()`/`.isWritable()`/`.getFormat()`.
- Produces: `DbUpgradeFlatDbCommand(BesuDatabaseManager dbManager) implements Command`, wired into `DbCommand` as subcommand `"upgrade-flatdb"`.

- [ ] **Step 1: Write the failing tests**

```java
package net.consensys.bric.commands;

import net.consensys.bric.db.BesuDatabaseManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DbUpgradeFlatDbCommandTest {

    private BesuDatabaseManager mockDbManager;
    private DbUpgradeFlatDbCommand command;
    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;

    @BeforeEach
    void setUp() {
        mockDbManager = mock(BesuDatabaseManager.class);
        command = new DbUpgradeFlatDbCommand(mockDbManager);

        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    void refusesWhenDbClosed() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("Error: No database is open");
    }

    @Test
    void refusesWhenNotBonsaiFormat() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.FOREST);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("only supported for Bonsai databases");
        assertThat(errorStream.toString()).contains("FOREST");
    }

    @Test
    void refusesWhenReadOnlyAndNotDryRun() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{});
        assertThat(errorStream.toString()).contains("read-only mode");
    }

    @Test
    void dryRunDoesNotRequireWritable() {
        when(mockDbManager.isOpen()).thenReturn(true);
        when(mockDbManager.getFormat()).thenReturn(BesuDatabaseManager.DatabaseFormat.BONSAI);
        when(mockDbManager.isWritable()).thenReturn(false);
        command.execute(new String[]{"--dry-run"});
        // Fails later (no real database behind the mock), but must get past the write-mode guard.
        assertThat(errorStream.toString()).doesNotContain("read-only mode");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbUpgradeFlatDbCommandTest"`
Expected: FAIL — `DbUpgradeFlatDbCommand` does not exist.

- [ ] **Step 3: Write the implementation**

```java
package net.consensys.bric.commands;

import net.consensys.bric.besu.FlatDbHealProgressListener;
import net.consensys.bric.besu.FlatDbHealResult;
import net.consensys.bric.besu.FlatDbHealer;
import net.consensys.bric.db.BesuDatabaseManager;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

/**
 * Reconciles a Bonsai database's flat account/storage tables against the account and
 * storage tries, then marks the flat DB mode FULL. See
 * docs/superpowers/specs/2026-07-24-flatdb-heal-design.md for the algorithm.
 */
public class DbUpgradeFlatDbCommand implements Command {

    private final BesuDatabaseManager dbManager;

    public DbUpgradeFlatDbCommand(BesuDatabaseManager dbManager) {
        this.dbManager = dbManager;
    }

    @Override
    public void execute(String[] args) {
        if (!dbManager.isOpen()) {
            System.err.println("Error: No database is open. Use 'db open <path>' first.");
            return;
        }

        BesuDatabaseManager.DatabaseFormat format = dbManager.getFormat();
        if (format != BesuDatabaseManager.DatabaseFormat.BONSAI) {
            System.err.println(
                "Error: Flat DB healing is only supported for Bonsai databases. Current format: " + format);
            return;
        }

        boolean dryRun = Arrays.asList(args).contains("--dry-run");
        if (!dryRun && !dbManager.isWritable()) {
            System.err.println(
                "Error: Database is open in read-only mode. Reopen with 'db open <path> --write'.");
            return;
        }

        if (dryRun) {
            System.out.println("Dry run - no changes will be written.");
        }

        FlatDbHealer healer = new FlatDbHealer(dbManager);
        Instant start = Instant.now();
        FlatDbHealResult result;
        try {
            result = healer.heal(dryRun, new PrintingProgressListener());
        } catch (IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }
        Duration elapsed = Duration.between(start, Instant.now());

        if (dryRun) {
            System.out.println("Would fix " + result.totalAccountsFixed() + " accounts and "
                + result.totalSlotsFixed() + " storage slots.");
        } else {
            System.out.println("Flat DB mode upgraded to FULL.");
            System.out.println(String.format(
                "Done in %02d:%02d:%02d. Accounts: %d fixed. Storage slots: %d fixed.",
                elapsed.toHoursPart(), elapsed.toMinutesPart(), elapsed.toSecondsPart(),
                result.totalAccountsFixed(), result.totalSlotsFixed()));
        }
    }

    private static final class PrintingProgressListener implements FlatDbHealProgressListener {
        @Override
        public void onRangeComplete(int rangeIndex, int totalRanges, long accountsChecked, long accountsFixed) {
            System.out.println("Range " + rangeIndex + "/" + totalRanges + " - " + accountsChecked
                + " accounts checked, " + accountsFixed + " fixed");
        }

        @Override
        public void onStorageAccountComplete(
                int accountsHealed, int totalAccountsToHeal, long slotsChecked, long slotsFixed) {
            System.out.println("Storage " + accountsHealed + "/" + totalAccountsToHeal + " accounts - "
                + slotsChecked + " slots checked, " + slotsFixed + " fixed");
        }
    }

    @Override
    public String getHelp() {
        return "Upgrade a Bonsai database's flat DB from PARTIAL to FULL by reconciling it against the tries";
    }

    @Override
    public String getUsage() {
        return "db upgrade-flatdb [--dry-run]\n"
             + "                               Reconciles ACCOUNT_INFO_STATE/ACCOUNT_STORAGE_STORAGE\n"
             + "                               against the account/storage tries. Requires --write mode\n"
             + "                               unless --dry-run (which only reports, never writes).";
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbUpgradeFlatDbCommandTest"`
Expected: PASS (4 tests)

- [ ] **Step 5: Wire into `DbCommand`**

In `src/main/java/net/consensys/bric/commands/DbCommand.java`, add the field (after line 28, alongside `compactCancelCommand`):

```java
    private final DbUpgradeFlatDbCommand upgradeFlatDbCommand;
```

Add to the constructor (after line 42, alongside `this.compactCancelCommand = ...`):

```java
        this.upgradeFlatDbCommand = new DbUpgradeFlatDbCommand(dbManager);
```

Add a switch case (after line 89, alongside `case "compact-cancel":`):

```java
            case "upgrade-flatdb":
                upgradeFlatDbCommand.execute(subArgs);
                break;
```

Add a usage line (after the `db compact-cancel` line, before the closing `"\n"` in `getUsage()`):

```java
               "                                 db upgrade-flatdb [--dry-run]              - Upgrade flat DB from PARTIAL to FULL\n" +
```

- [ ] **Step 6: Extend `DbCommandTest` routing coverage**

Add to `DbCommandTest.java`:

```java
    @Test
    void testExecuteUpgradeFlatDbSubcommand() {
        when(mockDbManager.isOpen()).thenReturn(false);
        command.execute(new String[]{"upgrade-flatdb"});
        assertThat(errorStream.toString()).contains("No database is open");
    }

    @Test
    void testUsageMentionsUpgradeFlatDb() {
        assertThat(command.getUsage()).contains("db upgrade-flatdb");
    }
```

- [ ] **Step 7: Wire into `BricCompleter` autocomplete**

In `src/main/java/net/consensys/bric/completion/BricCompleter.java`, replace the `DB_SUBCOMMANDS` set (lines 25-27):

```java
    private static final Set<String> DB_SUBCOMMANDS = Set.of(
        "open", "close", "info", "get", "put", "scan", "drop-cf", "stats",
        "compact", "compact-status", "compact-cancel", "upgrade-flatdb");
```

- [ ] **Step 8: Run the full affected test suite**

Run: `./gradlew test --tests "net.consensys.bric.commands.DbCommandTest" --tests "net.consensys.bric.commands.DbUpgradeFlatDbCommandTest"`
Expected: PASS

- [ ] **Step 9: Run the entire project test suite as a final regression check**

Run: `./gradlew test`
Expected: PASS (no regressions across the whole project)

- [ ] **Step 10: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbUpgradeFlatDbCommand.java \
        src/main/java/net/consensys/bric/commands/DbCommand.java \
        src/main/java/net/consensys/bric/completion/BricCompleter.java \
        src/test/java/net/consensys/bric/commands/DbUpgradeFlatDbCommandTest.java \
        src/test/java/net/consensys/bric/commands/DbCommandTest.java
git commit -m "feat: add db upgrade-flatdb subcommand"
```

---

## Manual Verification (not automated)

After Task 8, if a real partial-flat-db Bonsai snapshot is available:

1. `db open /path/to/partial/db --write`
2. `db info` — note current `Flat DB Mode: PARTIAL`
3. `db upgrade-flatdb --dry-run` — confirm reported counts look plausible for the snapshot's known state
4. `db upgrade-flatdb` — let it run to completion
5. `db info` — confirm `Flat DB Mode: FULL`
6. Spot-check a few known accounts/slots via `account <address>` / `storage <address> <slot>` before and after to confirm values are unchanged for already-correct entries

This step has no automated coverage — say so explicitly rather than claiming it's verified.

## Self-Review Notes

- **Spec coverage:** every section of `docs/superpowers/specs/2026-07-24-flatdb-heal-design.md` maps to a task — `NoOpKeyValueStorage`/`RocksDBSegmentedStorage` (Tasks 1-2), `FlatDbHealCheckpoint`/resumability (Tasks 3, 7), `FlatDbHealer` core algorithm (Tasks 4-7), CLI surface + wiring (Task 8), manual verification (final section).
- **Placeholder scan:** the one inline TODO-shaped note in Task 7 Step 3 (removing the stray sanity assertion) is not a plan placeholder — it's a real correction to test code shown in Step 3, spelled out with the exact replacement text, not a "fill in later" gap.
- **Type consistency:** `AccountRangeOutcome`/`StorageRangeOutcome` field names (`accountsChecked`/`added`/`updated`/`removed`, `slotsChecked`/`added`/`updated`/`removed`) are used identically in Tasks 5-7's test and implementation code. `FlatDbHealResult`/`FlatDbHealProgressListener` method signatures introduced in Task 7 match their single call site in Task 8's `DbUpgradeFlatDbCommand`.
