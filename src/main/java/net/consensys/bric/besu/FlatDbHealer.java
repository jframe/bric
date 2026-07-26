package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import net.consensys.bric.db.KeyValueSegmentIdentifier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.trie.MerkleTrie;
import org.hyperledger.besu.ethereum.trie.RangeStorageEntriesCollector;
import org.hyperledger.besu.ethereum.trie.TrieIterator;
import org.hyperledger.besu.ethereum.trie.patricia.StoredMerklePatriciaTrie;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
import org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

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
            flatDbStrategyProvider, storage, new NoOpKeyValueStorage());
    }

    public Bytes32 getTargetStateRoot() {
        return worldState.getWorldStateRootHash()
            .map(Bytes32::wrap)
            .orElseThrow(() -> new IllegalStateException(
                "No world state root found; database may be empty or not yet synced."));
    }

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
}
