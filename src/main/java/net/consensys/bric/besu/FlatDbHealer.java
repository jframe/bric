package net.consensys.bric.besu;

import net.consensys.bric.db.BesuDatabaseManager;
import org.apache.tuweni.bytes.Bytes32;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.BonsaiWorldStateKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.flat.BonsaiFlatDbStrategyProvider;
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
            flatDbStrategyProvider, storage, new NoOpKeyValueStorage());
    }

    public Bytes32 getTargetStateRoot() {
        return worldState.getWorldStateRootHash()
            .map(Bytes32::wrap)
            .orElseThrow(() -> new IllegalStateException(
                "No world state root found; database may be empty or not yet synced."));
    }
}
