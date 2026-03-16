# Arbitrary Column Families Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable db subcommands (drop-cf, get, put, scan) to work with arbitrary column families by name or hex, not just predefined segments.

**Architecture:** Create a `ColumnFamilyResolver` utility that auto-detects format (hex with `0x` prefix vs UTF-8 string) and resolves to `ColumnFamilyHandle` objects. Enhance `BesuDatabaseManager` with byte-level CF lookup. Update db commands to use the resolver instead of enum-only lookups.

**Tech Stack:** Java 11+, RocksDB, JUnit 5, AssertJ, existing Bric patterns

---

## File Structure

```
Create:
  src/main/java/net/consensys/bric/db/ColumnFamilyResolver.java
  src/test/java/net/consensys/bric/db/ColumnFamilyResolverTest.java

Modify:
  src/main/java/net/consensys/bric/db/BesuDatabaseManager.java
  src/main/java/net/consensys/bric/commands/DbDropCfCommand.java
  src/main/java/net/consensys/bric/commands/DbGetCommand.java
  src/main/java/net/consensys/bric/commands/DbPutCommand.java
  src/main/java/net/consensys/bric/commands/DbScanCommand.java
  src/test/java/net/consensys/bric/commands/DbDropCfCommandTest.java
  src/test/java/net/consensys/bric/commands/DbGetCommandTest.java
  src/test/java/net/consensys/bric/commands/DbPutCommandTest.java
  src/test/java/net/consensys/bric/commands/DbScanCommandTest.java
```

---

## Chunk 1: Foundation - ColumnFamilyResolver

### Task 1: Write ColumnFamilyResolver tests

**Files:**
- Create: `src/test/java/net/consensys/bric/db/ColumnFamilyResolverTest.java`

- [ ] **Step 1: Write failing test for hex parsing**

```java
package net.consensys.bric.db;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

public class ColumnFamilyResolverTest {

    private BesuDatabaseManager dbManager;

    @BeforeEach
    void setUp() {
        dbManager = mock(BesuDatabaseManager.class);
    }

    @Test
    void testParseHexFormat() {
        // Input: "0x0a" should parse to byte[] {10}
        byte[] result = ColumnFamilyResolver.parseInput("0x0a");
        assertThat(result).isEqualTo(new byte[]{10});
    }

    @Test
    void testParseHexFormatUppercase() {
        byte[] result = ColumnFamilyResolver.parseInput("0x0A");
        assertThat(result).isEqualTo(new byte[]{10});
    }

    @Test
    void testParseHexFormatMultibyte() {
        byte[] result = ColumnFamilyResolver.parseInput("0xabcdef1234");
        assertThat(result).isEqualTo(new byte[]{(byte)0xab, (byte)0xcd, (byte)0xef, 0x12, 0x34});
    }

    @Test
    void testParseUtf8String() {
        // Input: "TRIE_LOG_STORAGE" (no 0x prefix) should return UTF-8 bytes
        byte[] result = ColumnFamilyResolver.parseInput("TRIE_LOG_STORAGE");
        assertThat(result).isEqualTo("TRIE_LOG_STORAGE".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void testParseUtf8StringWithQuotes() {
        byte[] result = ColumnFamilyResolver.parseInput("\"MY_CUSTOM_CF\"");
        assertThat(result).isEqualTo("MY_CUSTOM_CF".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Test
    void testInvalidHexFormatOddLength() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0x0"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("even-length");
    }

    @Test
    void testInvalidHexFormatInvalidChars() {
        assertThatThrownBy(() -> ColumnFamilyResolver.parseInput("0xZZ"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid hex format");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
./gradlew test --tests "*ColumnFamilyResolverTest" -v
```

Expected output: All tests fail with "class not found"

- [ ] **Step 3: Create ColumnFamilyResolver stub**

```java
package net.consensys.bric.db;

import java.nio.charset.StandardCharsets;

/**
 * Resolves column family identifiers from user input.
 * Supports auto-detection: "0x" prefix = hex bytes, otherwise UTF-8 string.
 */
public class ColumnFamilyResolver {

    /**
     * Parse user input to byte array.
     *
     * @param input user input (e.g., "0x0a", "TRIE_LOG_STORAGE", "\"MY_CF\"")
     * @return parsed byte array
     * @throws IllegalArgumentException if hex format is invalid
     */
    public static byte[] parseInput(String input) throws IllegalArgumentException {
        if (input == null || input.isEmpty()) {
            throw new IllegalArgumentException("Input cannot be null or empty");
        }

        // Remove surrounding quotes if present
        String trimmed = input;
        if (trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }

        // Check for hex prefix (case-insensitive)
        if (trimmed.toLowerCase().startsWith("0x")) {
            return parseHex(trimmed.substring(2));
        }

        // Otherwise treat as UTF-8 string
        return trimmed.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] parseHex(String hex) throws IllegalArgumentException {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException(
                "Invalid hex format: 0x" + hex + " (expected even-length hex string)"
            );
        }

        byte[] result = new byte[hex.length() / 2];
        for (int i = 0; i < hex.length(); i += 2) {
            try {
                result[i / 2] = (byte) Integer.parseInt(hex.substring(i, i + 2), 16);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "Invalid hex format: 0x" + hex + " (contains non-hex characters)",
                    e
                );
            }
        }
        return result;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
./gradlew test --tests "*ColumnFamilyResolverTest" -v
```

Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add src/test/java/net/consensys/bric/db/ColumnFamilyResolverTest.java \
        src/main/java/net/consensys/bric/db/ColumnFamilyResolver.java
git commit -m "feat: add ColumnFamilyResolver with hex/UTF-8 parsing"
```

### Task 2: Add resolution logic to ColumnFamilyResolver

**Files:**
- Modify: `src/test/java/net/consensys/bric/db/ColumnFamilyResolverTest.java`
- Modify: `src/main/java/net/consensys/bric/db/ColumnFamilyResolver.java`

- [ ] **Step 1: Write failing test for CF resolution**

Add to `ColumnFamilyResolverTest.java`:

```java
@Test
void testResolveByEnumName() {
    // Mock dbManager to return handle for enum-based CF
    ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
    when(dbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE))
        .thenReturn(mockHandle);

    ColumnFamilyHandle result = ColumnFamilyResolver.resolveColumnFamily(
        dbManager,
        "TRIE_LOG_STORAGE"
    );
    assertThat(result).isNotNull().isEqualTo(mockHandle);
}

@Test
void testResolveByEnumId() {
    ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
    when(dbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE))
        .thenReturn(mockHandle);

    // 0x0a is the ID for TRIE_LOG_STORAGE
    ColumnFamilyHandle result = ColumnFamilyResolver.resolveColumnFamily(
        dbManager,
        "0x0a"
    );
    assertThat(result).isNotNull().isEqualTo(mockHandle);
}

@Test
void testResolveByArbitraryName() {
    ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
    Set<String> cfNames = new HashSet<>();
    cfNames.add("CUSTOM_CF");
    when(dbManager.getColumnFamilyNames()).thenReturn(cfNames);
    when(dbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);

    ColumnFamilyHandle result = ColumnFamilyResolver.resolveColumnFamily(
        dbManager,
        "CUSTOM_CF"
    );
    assertThat(result).isNotNull().isEqualTo(mockHandle);
}

@Test
void testResolveNotFound() {
    when(dbManager.getColumnFamilyNames()).thenReturn(new HashSet<>());

    ColumnFamilyHandle result = ColumnFamilyResolver.resolveColumnFamily(
        dbManager,
        "NONEXISTENT"
    );
    assertThat(result).isNull();
}

@Test
void testResolveQuotedString() {
    ColumnFamilyHandle mockHandle = mock(ColumnFamilyHandle.class);
    Set<String> cfNames = new HashSet<>();
    cfNames.add("MY_CUSTOM_CF");
    when(dbManager.getColumnFamilyNames()).thenReturn(cfNames);
    when(dbManager.getColumnFamilyByName("MY_CUSTOM_CF")).thenReturn(mockHandle);

    ColumnFamilyHandle result = ColumnFamilyResolver.resolveColumnFamily(
        dbManager,
        "\"MY_CUSTOM_CF\""
    );
    assertThat(result).isNotNull().isEqualTo(mockHandle);
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
./gradlew test --tests "*ColumnFamilyResolverTest" -v
```

Expected: New tests fail (methods don't exist)

- [ ] **Step 3: Implement resolution logic**

Add to `ColumnFamilyResolver.java`:

```java
import org.rocksdb.ColumnFamilyHandle;
import java.util.Set;

/**
 * Resolve a column family identifier to a handle.
 *
 * Resolution order:
 * 1. Try KeyValueSegmentIdentifier enum by name
 * 2. Try KeyValueSegmentIdentifier enum by hex ID
 * 3. Try arbitrary CF name in database
 * 4. Return null if not found
 *
 * @param dbManager the database manager
 * @param input the user input
 * @return ColumnFamilyHandle if resolved, null otherwise
 * @throws IllegalArgumentException if input format is invalid
 */
public static ColumnFamilyHandle resolveColumnFamily(
    BesuDatabaseManager dbManager,
    String input
) throws IllegalArgumentException {
    if (!dbManager.isOpen()) {
        throw new IllegalStateException("No database is open");
    }

    byte[] parsed = parseInput(input);

    // Try to resolve as KeyValueSegmentIdentifier by name
    try {
        String inputUpper = input.toUpperCase();
        if (inputUpper.startsWith("\"") && inputUpper.endsWith("\"")) {
            inputUpper = inputUpper.substring(1, inputUpper.length() - 1);
        }
        KeyValueSegmentIdentifier segment = KeyValueSegmentIdentifier.valueOf(inputUpper);
        ColumnFamilyHandle handle = dbManager.getColumnFamily(segment);
        if (handle != null) {
            return handle;
        }
    } catch (IllegalArgumentException e) {
        // Not an enum name, continue
    }

    // Try to resolve as KeyValueSegmentIdentifier by ID
    KeyValueSegmentIdentifier segmentById = KeyValueSegmentIdentifier.fromId(parsed);
    if (segmentById != null) {
        ColumnFamilyHandle handle = dbManager.getColumnFamily(segmentById);
        if (handle != null) {
            return handle;
        }
    }

    // Try to resolve as arbitrary CF name (as UTF-8 string)
    String cfNameStr = new String(parsed, StandardCharsets.UTF_8);
    if (dbManager.getColumnFamilyNames().contains(cfNameStr)) {
        return dbManager.getColumnFamilyByName(cfNameStr);
    }

    // Try to resolve as arbitrary CF name by bytes
    ColumnFamilyHandle handleByBytes = dbManager.getColumnFamilyByNameBytes(parsed);
    if (handleByBytes != null) {
        return handleByBytes;
    }

    // Not found
    return null;
}
```

- [ ] **Step 4: Run test to verify it passes**

```bash
./gradlew test --tests "*ColumnFamilyResolverTest" -v
```

Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add src/main/java/net/consensys/bric/db/ColumnFamilyResolver.java \
        src/test/java/net/consensys/bric/db/ColumnFamilyResolverTest.java
git commit -m "feat: add CF resolution logic (enum, hex, arbitrary names)"
```

---

## Chunk 2: BesuDatabaseManager Enhancement

### Task 3: Enhance BesuDatabaseManager with byte-level CF lookup

**Files:**
- Modify: `src/main/java/net/consensys/bric/db/BesuDatabaseManager.java` (around line 220)

- [ ] **Step 1: Add method getColumnFamilyByNameBytes**

After the `getColumnFamilyByName` method (around line 210), add:

```java
/**
 * Get column family handle by raw name bytes.
 * Used when CF name is not a standard UTF-8 string or needs byte-exact lookup.
 *
 * @param cfNameBytes the column family name as bytes
 * @return ColumnFamilyHandle if found, null otherwise
 */
public ColumnFamilyHandle getColumnFamilyByNameBytes(byte[] cfNameBytes) {
    if (!isOpen) {
        throw new IllegalStateException("No database is open");
    }

    // Try exact match on stored handles
    for (Map.Entry<String, ColumnFamilyHandle> entry : handlesByName.entrySet()) {
        if (java.util.Arrays.equals(entry.getKey().getBytes(java.nio.charset.StandardCharsets.UTF_8), cfNameBytes)) {
            return entry.getValue();
        }
    }

    return null;
}
```

- [ ] **Step 2: Add required import for Arrays**

At the top of BesuDatabaseManager.java (around line 10), ensure this import is present:

```java
import java.util.Arrays;
```

- [ ] **Step 3: Run the build to verify no compilation errors**

```bash
./gradlew build -x test
```

Expected: Build succeeds

- [ ] **Step 4: Commit**

```bash
git add src/main/java/net/consensys/bric/db/BesuDatabaseManager.java
git commit -m "feat: add getColumnFamilyByNameBytes for byte-level CF lookup"
```

---

## Chunk 3: DbDropCfCommand Update

### Task 4: Update DbDropCfCommand to use ColumnFamilyResolver

**Files:**
- Modify: `src/main/java/net/consensys/bric/commands/DbDropCfCommand.java`
- Modify: `src/test/java/net/consensys/bric/commands/DbDropCfCommandTest.java`

- [ ] **Step 1: Write test for arbitrary CF drop**

Add to `DbDropCfCommandTest.java`:

```java
@Test
void testDropArbitraryUtf8CF() {
    String[] args = {"CUSTOM_CF"};

    Set<String> cfNames = new HashSet<>();
    cfNames.add("CUSTOM_CF");
    cfNames.add("default");
    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.isWritable()).thenReturn(true);
    when(dbManager.getColumnFamilyNames()).thenReturn(cfNames);
    when(dbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);

    command.execute(args);

    verify(dbManager).dropColumnFamily("CUSTOM_CF");
    // Verify success message was printed (check System.out)
}

@Test
void testDropArbitraryHexCF() {
    String[] args = {"0x0a"}; // hex for TRIE_LOG_STORAGE

    Set<String> cfNames = new HashSet<>();
    cfNames.add("TRIE_LOG_STORAGE");
    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.isWritable()).thenReturn(true);
    when(dbManager.getColumnFamilyNames()).thenReturn(cfNames);
    when(dbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE))
        .thenReturn(mockHandle);

    command.execute(args);

    verify(dbManager).dropColumnFamily("TRIE_LOG_STORAGE");
}

@Test
void testDropQuotedArbitraryName() {
    String[] args = {"\"MY_CUSTOM_CF\""};

    Set<String> cfNames = new HashSet<>();
    cfNames.add("MY_CUSTOM_CF");
    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.isWritable()).thenReturn(true);
    when(dbManager.getColumnFamilyNames()).thenReturn(cfNames);
    when(dbManager.getColumnFamilyByName("MY_CUSTOM_CF"))
        .thenReturn(mockHandle);

    command.execute(args);

    verify(dbManager).dropColumnFamily("MY_CUSTOM_CF");
}

@Test
void testDropNonexistentCFError() {
    String[] args = {"NONEXISTENT"};

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.isWritable()).thenReturn(true);
    when(dbManager.getColumnFamilyNames()).thenReturn(new HashSet<>());

    command.execute(args);

    verify(dbManager, never()).dropColumnFamily(anyString());
    // Verify error message
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
./gradlew test --tests "*DbDropCfCommandTest*testDropArbitraryUtf8CF" -v
```

Expected: Test fails (dropColumnFamily not called correctly)

- [ ] **Step 3: Update DbDropCfCommand execute method**

Replace the entire `execute()` method in `DbDropCfCommand.java`:

```java
@Override
public void execute(String[] args) {
    // Guard: default CF check fires before any DB state check
    if (args.length > 0 && "default".equalsIgnoreCase(args[0])) {
        System.err.println("Error: Cannot drop the default column family");
        return;
    }

    if (!dbManager.isOpen()) {
        System.err.println("Error: No database is open. Use 'db open <path>' first.");
        return;
    }

    if (!dbManager.isWritable()) {
        System.err.println("Error: Database is open in read-only mode. Reopen with 'db open <path> --write'.");
        return;
    }

    if (args.length < 1) {
        System.err.println("Error: Missing segment name");
        System.err.println("Usage: " + getUsage());
        return;
    }

    String input = args[0];

    // Try to resolve CF using ColumnFamilyResolver
    ColumnFamilyHandle handle;
    try {
        handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, input);
    } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        return;
    }

    if (handle == null) {
        System.err.println("Error: Column family not found: " + input);
        System.err.println("Available in this database: " +
            dbManager.getColumnFamilyNames().stream().sorted().collect(Collectors.joining(", ")));
        return;
    }

    // Get the actual CF name to drop
    String cfNameToDrop = null;
    for (String cfName : dbManager.getColumnFamilyNames()) {
        ColumnFamilyHandle testHandle = dbManager.getColumnFamilyByName(cfName);
        if (testHandle == handle) {
            cfNameToDrop = cfName;
            break;
        }
    }

    if (cfNameToDrop == null) {
        System.err.println("Error: Could not identify column family name");
        return;
    }

    try {
        dbManager.dropColumnFamily(cfNameToDrop);
        System.out.println("Dropped column family: " + cfNameToDrop);
    } catch (Exception e) {
        System.err.println("Error: Failed to drop column family: " + e.getMessage());
    }
}
```

- [ ] **Step 4: Add import for ColumnFamilyResolver**

At the top of `DbDropCfCommand.java`, add:

```java
import net.consensys.bric.db.ColumnFamilyResolver;
```

- [ ] **Step 5: Update usage/help text**

Update the `getUsage()` method:

```java
@Override
public String getUsage() {
    return "db drop-cf <segment|name|hex>\n" +
           "                               Drop a column family (requires --write mode)\n" +
           "                               Examples:\n" +
           "                                 db drop-cf TRIE_LOG_STORAGE (enum)\n" +
           "                                 db drop-cf \"CUSTOM_CF\" (UTF-8 name)\n" +
           "                                 db drop-cf 0x0a (hex ID)";
}
```

- [ ] **Step 6: Run tests**

```bash
./gradlew test --tests "*DbDropCfCommandTest" -v
```

Expected: All tests pass

- [ ] **Step 7: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbDropCfCommand.java \
        src/test/java/net/consensys/bric/commands/DbDropCfCommandTest.java
git commit -m "feat: update db drop-cf to support arbitrary CF names and hex"
```

---

## Chunk 4: DbGetCommand Update

### Task 5: Update DbGetCommand to use ColumnFamilyResolver

**Files:**
- Modify: `src/main/java/net/consensys/bric/commands/DbGetCommand.java`
- Modify: `src/test/java/net/consensys/bric/commands/DbGetCommandTest.java`

- [ ] **Step 1: Write test for arbitrary CF get**

Add to `DbGetCommandTest.java`:

```java
@Test
void testGetFromArbitraryUTF8CF() throws Exception {
    String[] args = {"CUSTOM_CF", "0xabcd"};
    byte[] value = new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef};

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
    when(dbManager.getColumnFamilyNames()).thenReturn(new HashSet<>(Arrays.asList("CUSTOM_CF")));
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);
    when(mockRocksDb.get(mockHandle, new byte[]{(byte)0xab, (byte)0xcd}))
        .thenReturn(value);

    command.execute(args);

    verify(mockRocksDb).get(mockHandle, new byte[]{(byte)0xab, (byte)0xcd});
    // Verify hex output
}

@Test
void testGetFromArbitraryHexCF() throws Exception {
    String[] args = {"0x0a", "0xabcd"}; // 0x0a = TRIE_LOG_STORAGE
    byte[] value = new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef};

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE))
        .thenReturn(mockHandle);
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);
    when(mockRocksDb.get(mockHandle, new byte[]{(byte)0xab, (byte)0xcd}))
        .thenReturn(value);

    command.execute(args);

    verify(mockRocksDb).get(mockHandle, new byte[]{(byte)0xab, (byte)0xcd});
}

@Test
void testGetKeyNotFound() {
    String[] args = {"CUSTOM_CF", "0xabcd"};

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
    when(dbManager.getColumnFamilyNames()).thenReturn(new HashSet<>(Arrays.asList("CUSTOM_CF")));
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);
    when(mockRocksDb.get(mockHandle, new byte[]{(byte)0xab, (byte)0xcd}))
        .thenReturn(null);

    command.execute(args);

    // Verify "not found" message
}
```

- [ ] **Step 2: Examine current DbGetCommand implementation**

```bash
grep -n "public void execute" src/main/java/net/consensys/bric/commands/DbGetCommand.java
```

Find the existing structure to understand what to modify.

- [ ] **Step 3: Update DbGetCommand execute method**

Modify `DbGetCommand.java` to replace enum-based segment resolution with `ColumnFamilyResolver`:

```java
@Override
public void execute(String[] args) {
    if (!dbManager.isOpen()) {
        System.err.println("Error: No database is open. Use 'db open <path>' first.");
        return;
    }

    if (args.length < 2) {
        System.err.println("Error: Missing segment and/or key");
        System.err.println("Usage: " + getUsage());
        return;
    }

    String cfInput = args[0];
    String keyHex = args[1];

    // Resolve CF
    ColumnFamilyHandle handle;
    try {
        handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, cfInput);
    } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        return;
    }

    if (handle == null) {
        System.err.println("Error: Column family not found: " + cfInput);
        return;
    }

    // Parse key as hex
    byte[] key;
    try {
        key = ColumnFamilyResolver.parseInput(keyHex);
    } catch (IllegalArgumentException e) {
        System.err.println("Error: Invalid key format: " + e.getMessage());
        return;
    }

    // Read value
    try {
        byte[] value = dbManager.getDatabase().get(handle, key);
        if (value == null) {
            System.out.println("Key not found: " + keyHex);
        } else {
            System.out.println("Key: " + bytesToHex(key));
            System.out.println("Value: " + bytesToHex(value));
        }
    } catch (Exception e) {
        System.err.println("Error reading from database: " + e.getMessage());
    }
}

private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : bytes) {
        sb.append(String.format("%02x", b));
    }
    return sb.toString();
}
```

- [ ] **Step 4: Add import for ColumnFamilyResolver**

```java
import net.consensys.bric.db.ColumnFamilyResolver;
```

- [ ] **Step 5: Update usage text**

```java
@Override
public String getUsage() {
    return "db get <segment|name|hex> <key-hex>\n" +
           "                               Read a value from a column family\n" +
           "                               Examples:\n" +
           "                                 db get TRIE_LOG_STORAGE 0xabcdef\n" +
           "                                 db get \"CUSTOM_CF\" 0xabcdef\n" +
           "                                 db get 0x0a 0xabcdef";
}
```

- [ ] **Step 6: Run tests**

```bash
./gradlew test --tests "*DbGetCommandTest" -v
```

Expected: All tests pass

- [ ] **Step 7: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbGetCommand.java \
        src/test/java/net/consensys/bric/commands/DbGetCommandTest.java
git commit -m "feat: update db get to support arbitrary CF names and hex"
```

---

## Chunk 5: DbPutCommand Update

### Task 6: Update DbPutCommand to use ColumnFamilyResolver

**Files:**
- Modify: `src/main/java/net/consensys/bric/commands/DbPutCommand.java`
- Modify: `src/test/java/net/consensys/bric/commands/DbPutCommandTest.java`

- [ ] **Step 1: Write test for arbitrary CF put**

Add to `DbPutCommandTest.java`:

```java
@Test
void testPutToArbitraryUTF8CF() throws Exception {
    String[] args = {"CUSTOM_CF", "0xabcd", "0xdeadbeef"};

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.isWritable()).thenReturn(true);
    when(dbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
    when(dbManager.getColumnFamilyNames()).thenReturn(new HashSet<>(Arrays.asList("CUSTOM_CF")));
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);

    command.execute(args);

    verify(mockRocksDb).put(
        mockHandle,
        new byte[]{(byte)0xab, (byte)0xcd},
        new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}
    );
}

@Test
void testPutToArbitraryHexCF() throws Exception {
    String[] args = {"0x0a", "0xabcd", "0xdeadbeef"};

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.isWritable()).thenReturn(true);
    when(dbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE))
        .thenReturn(mockHandle);
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);

    command.execute(args);

    verify(mockRocksDb).put(
        mockHandle,
        new byte[]{(byte)0xab, (byte)0xcd},
        new byte[]{(byte)0xde, (byte)0xad, (byte)0xbe, (byte)0xef}
    );
}

@Test
void testPutReadOnlyError() {
    String[] args = {"CUSTOM_CF", "0xabcd", "0xdeadbeef"};

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.isWritable()).thenReturn(false);

    command.execute(args);

    // Verify error message about read-only mode
}
```

- [ ] **Step 2: Examine current DbPutCommand implementation**

```bash
grep -n "public void execute" src/main/java/net/consensys/bric/commands/DbPutCommand.java
```

- [ ] **Step 3: Update DbPutCommand execute method**

```java
@Override
public void execute(String[] args) {
    if (!dbManager.isOpen()) {
        System.err.println("Error: No database is open. Use 'db open <path>' first.");
        return;
    }

    if (!dbManager.isWritable()) {
        System.err.println("Error: Database is open in read-only mode. Reopen with 'db open <path> --write'.");
        return;
    }

    if (args.length < 3) {
        System.err.println("Error: Missing segment, key, and/or value");
        System.err.println("Usage: " + getUsage());
        return;
    }

    String cfInput = args[0];
    String keyHex = args[1];
    String valueHex = args[2];

    // Resolve CF
    ColumnFamilyHandle handle;
    try {
        handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, cfInput);
    } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        return;
    }

    if (handle == null) {
        System.err.println("Error: Column family not found: " + cfInput);
        return;
    }

    // Parse key and value as hex
    byte[] key, value;
    try {
        key = ColumnFamilyResolver.parseInput(keyHex);
        value = ColumnFamilyResolver.parseInput(valueHex);
    } catch (IllegalArgumentException e) {
        System.err.println("Error: Invalid key or value format: " + e.getMessage());
        return;
    }

    // Write value
    try {
        dbManager.getDatabase().put(handle, key, value);
        System.out.println("Wrote to column family: " + cfInput);
        System.out.println("Key: " + bytesToHex(key));
        System.out.println("Value: " + bytesToHex(value));
    } catch (Exception e) {
        System.err.println("Error writing to database: " + e.getMessage());
    }
}

private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : bytes) {
        sb.append(String.format("%02x", b));
    }
    return sb.toString();
}
```

- [ ] **Step 4: Add import for ColumnFamilyResolver**

```java
import net.consensys.bric.db.ColumnFamilyResolver;
```

- [ ] **Step 5: Update usage text**

```java
@Override
public String getUsage() {
    return "db put <segment|name|hex> <key-hex> <value-hex>\n" +
           "                               Write a value to a column family (requires --write mode)\n" +
           "                               Examples:\n" +
           "                                 db put TRIE_LOG_STORAGE 0xabcd 0xdeadbeef\n" +
           "                                 db put \"CUSTOM_CF\" 0xabcd 0xdeadbeef\n" +
           "                                 db put 0x0a 0xabcd 0xdeadbeef";
}
```

- [ ] **Step 6: Run tests**

```bash
./gradlew test --tests "*DbPutCommandTest" -v
```

Expected: All tests pass

- [ ] **Step 7: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbPutCommand.java \
        src/test/java/net/consensys/bric/commands/DbPutCommandTest.java
git commit -m "feat: update db put to support arbitrary CF names and hex"
```

---

## Chunk 6: DbScanCommand Update

### Task 7: Update DbScanCommand to use ColumnFamilyResolver

**Files:**
- Modify: `src/main/java/net/consensys/bric/commands/DbScanCommand.java`
- Modify: `src/test/java/net/consensys/bric/commands/DbScanCommandTest.java`

- [ ] **Step 1: Write test for arbitrary CF scan**

Add to `DbScanCommandTest.java`:

```java
@Test
void testScanArbitraryUTF8CF() throws Exception {
    String[] args = {"CUSTOM_CF"};

    RocksIterator mockIterator = mock(RocksIterator.class);
    when(mockIterator.isValid()).thenReturn(true, true, false);
    when(mockIterator.key()).thenReturn(new byte[]{0x01}, new byte[]{0x02});
    when(mockIterator.value()).thenReturn(new byte[]{(byte)0xaa}, new byte[]{(byte)0xbb});

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
    when(dbManager.getColumnFamilyNames()).thenReturn(new HashSet<>(Arrays.asList("CUSTOM_CF")));
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);
    when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);

    command.execute(args);

    verify(mockIterator, times(2)).next();
    // Verify output shows key-value pairs in hex
}

@Test
void testScanArbitraryHexCF() throws Exception {
    String[] args = {"0x0a"}; // TRIE_LOG_STORAGE

    RocksIterator mockIterator = mock(RocksIterator.class);
    when(mockIterator.isValid()).thenReturn(true, false);
    when(mockIterator.key()).thenReturn(new byte[]{0x01});
    when(mockIterator.value()).thenReturn(new byte[]{(byte)0xaa});

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.getColumnFamily(KeyValueSegmentIdentifier.TRIE_LOG_STORAGE))
        .thenReturn(mockHandle);
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);
    when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);

    command.execute(args);

    verify(mockIterator).seekToFirst();
}

@Test
void testScanWithLimitOption() throws Exception {
    String[] args = {"CUSTOM_CF", "--limit", "5"};

    RocksIterator mockIterator = mock(RocksIterator.class);
    // Setup to return 5 items then stop

    when(dbManager.isOpen()).thenReturn(true);
    when(dbManager.getColumnFamilyByName("CUSTOM_CF")).thenReturn(mockHandle);
    when(dbManager.getColumnFamilyNames()).thenReturn(new HashSet<>(Arrays.asList("CUSTOM_CF")));
    when(dbManager.getDatabase()).thenReturn(mockRocksDb);
    when(mockRocksDb.newIterator(mockHandle)).thenReturn(mockIterator);

    command.execute(args);

    // Verify iteration stops after 5 items
}
```

- [ ] **Step 2: Examine current DbScanCommand implementation**

```bash
grep -n "public void execute" src/main/java/net/consensys/bric/commands/DbScanCommand.java
```

- [ ] **Step 3: Update DbScanCommand execute method**

```java
@Override
public void execute(String[] args) {
    if (!dbManager.isOpen()) {
        System.err.println("Error: No database is open. Use 'db open <path>' first.");
        return;
    }

    if (args.length < 1) {
        System.err.println("Error: Missing segment");
        System.err.println("Usage: " + getUsage());
        return;
    }

    String cfInput = args[0];
    int limit = Integer.MAX_VALUE;

    // Parse optional --limit flag
    for (int i = 1; i < args.length; i++) {
        if ("--limit".equals(args[i]) && i + 1 < args.length) {
            try {
                limit = Integer.parseInt(args[i + 1]);
            } catch (NumberFormatException e) {
                System.err.println("Error: Invalid limit value: " + args[i + 1]);
                return;
            }
            i++; // Skip next arg
        }
    }

    // Resolve CF
    ColumnFamilyHandle handle;
    try {
        handle = ColumnFamilyResolver.resolveColumnFamily(dbManager, cfInput);
    } catch (IllegalArgumentException e) {
        System.err.println("Error: " + e.getMessage());
        return;
    }

    if (handle == null) {
        System.err.println("Error: Column family not found: " + cfInput);
        return;
    }

    // Scan and display
    try (RocksIterator iterator = dbManager.getDatabase().newIterator(handle)) {
        iterator.seekToFirst();
        int count = 0;
        while (iterator.isValid() && count < limit) {
            byte[] key = iterator.key();
            byte[] value = iterator.value();
            System.out.println("Key: " + bytesToHex(key) + " -> Value: " + bytesToHex(value));
            iterator.next();
            count++;
        }
        if (count >= limit) {
            System.out.println("(Limited to " + limit + " entries)");
        }
    } catch (Exception e) {
        System.err.println("Error scanning column family: " + e.getMessage());
    }
}

private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder("0x");
    for (byte b : bytes) {
        sb.append(String.format("%02x", b));
    }
    return sb.toString();
}
```

- [ ] **Step 4: Add import for ColumnFamilyResolver**

```java
import net.consensys.bric.db.ColumnFamilyResolver;
import org.rocksdb.RocksIterator;
```

- [ ] **Step 5: Update usage text**

```java
@Override
public String getUsage() {
    return "db scan <segment|name|hex> [--limit <count>]\n" +
           "                               Scan entries in a column family\n" +
           "                               Examples:\n" +
           "                                 db scan TRIE_LOG_STORAGE\n" +
           "                                 db scan \"CUSTOM_CF\" --limit 10\n" +
           "                                 db scan 0x0a --limit 5";
}
```

- [ ] **Step 6: Run tests**

```bash
./gradlew test --tests "*DbScanCommandTest" -v
```

Expected: All tests pass

- [ ] **Step 7: Commit**

```bash
git add src/main/java/net/consensys/bric/commands/DbScanCommand.java \
        src/test/java/net/consensys/bric/commands/DbScanCommandTest.java
git commit -m "feat: update db scan to support arbitrary CF names and hex"
```

---

## Chunk 7: Integration & Testing

### Task 8: Run full test suite and integration testing

**Files:**
- All modified command and resolver files

- [ ] **Step 1: Run full test suite**

```bash
./gradlew test -v
```

Expected: All tests pass (no failures)

- [ ] **Step 2: Run build to verify no warnings**

```bash
./gradlew build -v
```

Expected: Build succeeds with no errors

- [ ] **Step 3: Manual integration test with fatJar**

```bash
./gradlew fatJar
```

- [ ] **Step 4: Test arbitrary CF operations with real database**

Create a simple test script (if possible) or document manual steps:
- Open a Besu database in write mode
- Use `db drop-cf "CUSTOM_CF"` (UTF-8)
- Use `db drop-cf 0x0a` (hex)
- Use `db get "CUSTOM_CF" 0xabcd` (get non-existent should fail gracefully)
- Use `db scan "CUSTOM_CF"` (scan non-existent should show error)

Expected: All operations work as designed, error messages are clear

- [ ] **Step 5: Commit test results**

```bash
git add .
git commit -m "test: verify all db subcommands support arbitrary CFs"
```

---

## Chunk 8: Final Cleanup & Documentation

### Task 9: Update help and verify backward compatibility

**Files:**
- `src/main/java/net/consensys/bric/commands/DbCommand.java` (if it has main help)
- `src/main/java/net/consensys/bric/BricCommandProcessor.java` (if it has command registry help)

- [ ] **Step 1: Update DbCommand help text (if applicable)**

If DbCommand has a getHelp() or main help message:

```java
// Update help to mention arbitrary CF support
"db - Database operations\n" +
"  db open <path> [--write]      Open a Besu database\n" +
"  db close                       Close the current database\n" +
"  db info                        Show database statistics\n" +
"  db drop-cf <cf>               Drop a column family\n" +
"  db get <cf> <key>             Read from a column family\n" +
"  db put <cf> <key> <value>     Write to a column family\n" +
"  db scan <cf>                  Scan entries in a column family\n" +
"  \n" +
"  Column family format (auto-detected):\n" +
"    SEGMENT_NAME                Predefined segment name\n" +
"    \"CUSTOM_NAME\"             UTF-8 string (with quotes)\n" +
"    0xabcdef                    Hexadecimal bytes"
```

- [ ] **Step 2: Verify backward compatibility**

Run tests on existing enum-based commands to ensure they still work:

```bash
./gradlew test --tests "*DbDropCfCommandTest*resolveByEnumName" -v
./gradlew test --tests "*DbGetCommandTest" -v
./gradlew test --tests "*DbPutCommandTest" -v
./gradlew test --tests "*DbScanCommandTest" -v
```

Expected: All existing enum-based tests still pass

- [ ] **Step 3: Create summary of changes**

List all files modified and what changed in each:

```
ColumnFamilyResolver.java (NEW)
  - Auto-detects hex (0x prefix) vs UTF-8 strings
  - Resolves CFs by enum name, enum ID, or arbitrary name
  - Returns null if not found

BesuDatabaseManager.java (MODIFIED)
  - Added getColumnFamilyByNameBytes() for byte-level lookup

DbDropCfCommand.java (MODIFIED)
  - Uses ColumnFamilyResolver for flexible CF selection
  - Updated help/usage text

DbGetCommand.java (MODIFIED)
  - Uses ColumnFamilyResolver for flexible CF selection
  - Displays raw bytes as hex
  - Updated help/usage text

DbPutCommand.java (MODIFIED)
  - Uses ColumnFamilyResolver for flexible CF selection
  - Parses hex values for key and value
  - Updated help/usage text

DbScanCommand.java (MODIFIED)
  - Uses ColumnFamilyResolver for flexible CF selection
  - Displays key-value pairs as hex
  - Supports --limit option
  - Updated help/usage text
```

- [ ] **Step 4: Commit final documentation updates**

```bash
git add src/main/java/net/consensys/bric/commands/DbCommand.java
git commit -m "docs: update help text for arbitrary CF support"
```

- [ ] **Step 5: Final verification - run all tests one more time**

```bash
./gradlew clean test -v
```

Expected: All tests pass, no warnings

- [ ] **Step 6: Create summary commit**

```bash
git log --oneline -10
```

Review the commit history. If appropriate, create a summary commit:

```bash
git commit --allow-empty -m "feat: complete arbitrary column family support for db subcommands

- Add ColumnFamilyResolver for auto-detecting hex/UTF-8 formats
- Enhance BesuDatabaseManager with byte-level CF lookup
- Update db drop-cf, db get, db put, db scan commands
- All commands now support predefined segments, UTF-8 names, and hex IDs
- Full backward compatibility maintained
- Comprehensive test coverage added"
```

---

## Testing Checklist

- [ ] Unit tests for ColumnFamilyResolver (hex, UTF-8, quotes, errors)
- [ ] Unit tests for updated commands (enum, arbitrary, hex, edge cases)
- [ ] All tests pass locally
- [ ] Build succeeds without warnings
- [ ] Backward compatibility verified (enum-based commands still work)
- [ ] Manual integration testing with real database
- [ ] Error messages are clear and helpful
- [ ] Help/usage text is updated and accurate

---

## Verification Steps

Before claiming completion:

1. **All tests pass**: `./gradlew test -v`
2. **Build succeeds**: `./gradlew build -v`
3. **No compilation warnings**: Check build output
4. **Backward compatibility**: Old commands still work
5. **New functionality**: Try arbitrary CFs
6. **Error handling**: Test invalid inputs, missing CFs
7. **Documentation**: Help text updated, usage examples clear
