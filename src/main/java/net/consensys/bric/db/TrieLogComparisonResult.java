package net.consensys.bric.db;

import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

import java.util.ArrayList;
import java.util.List;

/**
 * Container for comparison results between trielog and archive storage.
 */
public class TrieLogComparisonResult {
    public long blockNumber;
    public Hash blockHash;

    // Summary statistics
    public int totalAccountComparisons;
    public int accountMatches;
    public int accountMismatches;

    public int totalStorageComparisons;
    public int storageMatches;
    public int storageMismatches;

    public int totalCodeComparisons;
    public int codeMatches;
    public int codeMismatches;

    // Detailed mismatches
    public List<AccountMismatch> accountMismatchList = new ArrayList<>();
    public List<StorageMismatch> storageMismatchList = new ArrayList<>();
    public List<CodeMismatch> codeMismatchList = new ArrayList<>();

    // Optional: matched items (for verbose mode)
    public List<AccountMatch> accountMatchList = new ArrayList<>();
    public List<StorageMatch> storageMatchList = new ArrayList<>();
    public List<CodeMatch> codeMatchList = new ArrayList<>();

    /**
     * Represents an account field mismatch.
     */
    public static class AccountMismatch {
        public Address address;
        public String field;  // "nonce", "balance", "storageRoot", "codeHash", or "account" for missing
        public String expectedValue;  // from trielog
        public String actualValue;  // from archive
        public Long archiveBlockNumber;  // block number found in archive
    }

    /**
     * Represents a storage slot mismatch.
     */
    public static class StorageMismatch {
        public Address address;
        public Hash slotHash;
        public UInt256 expectedValue;  // from trielog
        public UInt256 actualValue;  // from archive
        public Long archiveBlockNumber;
    }

    /**
     * Represents a code mismatch.
     */
    public static class CodeMismatch {
        public Address address;
        public Hash expectedCodeHash;  // from trielog
        public Hash actualCodeHash;  // from archive
        public int expectedSize;
        public int actualSize;
    }

    /**
     * Represents a matched account (for verbose mode).
     */
    public static class AccountMatch {
        public Address address;
        public Long archiveBlockNumber;
    }

    /**
     * Represents a matched storage slot (for verbose mode).
     */
    public static class StorageMatch {
        public Address address;
        public Hash slotHash;
        public UInt256 value;
        public Long archiveBlockNumber;
    }

    /**
     * Represents matched code (for verbose mode).
     */
    public static class CodeMatch {
        public Address address;
        public Hash codeHash;
        public int size;
    }
}
