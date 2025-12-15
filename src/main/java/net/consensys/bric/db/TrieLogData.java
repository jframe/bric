package net.consensys.bric.db;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.pathbased.common.trielog.TrieLogLayer;

/**
 * Wrapper for Besu's TrieLogLayer with additional metadata.
 * Provides convenient access to parsed trie log (state diff) data.
 */
public class TrieLogData {
    public Hash blockHash;
    public long blockNumber;
    public TrieLogLayer trieLogLayer;

    public boolean isEmpty() {
        if (trieLogLayer == null) {
            return true;
        }
        return trieLogLayer.getAccountChanges().isEmpty()
            && trieLogLayer.getCodeChanges().isEmpty()
            && trieLogLayer.getStorageChanges().isEmpty();
    }

    public boolean hasAccountChanges() {
        return trieLogLayer != null && !trieLogLayer.getAccountChanges().isEmpty();
    }

    public boolean hasCodeChanges() {
        return trieLogLayer != null && !trieLogLayer.getCodeChanges().isEmpty();
    }

    public boolean hasStorageChanges() {
        return trieLogLayer != null && !trieLogLayer.getStorageChanges().isEmpty();
    }
}
