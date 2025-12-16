package net.consensys.bric.formatters;

import net.consensys.bric.db.TrieLogData;
import org.hyperledger.besu.datatypes.AccountValue;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.StorageSlotKey;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.trie.pathbased.common.PathBasedValue;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;

/**
 * Formats trie log (state diff) data for human-readable display.
 */
public class TrieLogFormatter {

    private static final BigInteger WEI_PER_ETH = new BigInteger("1000000000000000000");

    /**
     * Format trie log as a multi-section diff display.
     */
    public String format(TrieLogData trieLog) {
        StringBuilder sb = new StringBuilder();

        sb.append("\nTrie Log (State Diff):\n");
        sb.append("  Block Hash: ").append(trieLog.blockHash).append("\n");

        if (trieLog.blockNumber > 0) {
            sb.append("  Block Number: ").append(String.format("%,d", trieLog.blockNumber)).append("\n");
        }

        sb.append("\n");

        if (trieLog.trieLogLayer == null) {
            sb.append("(No trie log data available)\n");
            return sb.toString();
        }

        // Display summary
        int accountChanges = trieLog.trieLogLayer.getAccountChanges().size();
        int codeChanges = trieLog.trieLogLayer.getCodeChanges().size();
        int storageChanges = trieLog.trieLogLayer.getStorageChanges().values().stream()
            .mapToInt(Map::size)
            .sum();

        sb.append("Summary:\n");
        sb.append("  Account Changes: ").append(accountChanges).append("\n");
        sb.append("  Code Changes:    ").append(codeChanges).append("\n");
        sb.append("  Storage Changes: ").append(storageChanges).append("\n");
        sb.append("\n");

        // Display account changes
        if (trieLog.hasAccountChanges()) {
            sb.append("═══ Account Changes ═══\n\n");
            for (Map.Entry<Address, PathBasedValue<AccountValue>> entry :
                trieLog.trieLogLayer.getAccountChanges().entrySet()) {
                formatAccountChange(sb, entry.getKey(), entry.getValue());
                sb.append("\n");
            }
        }

        // Display code changes
        if (trieLog.hasCodeChanges()) {
            sb.append("═══ Code Changes ═══\n\n");
            for (Map.Entry<Address, PathBasedValue<Bytes>> entry :
                trieLog.trieLogLayer.getCodeChanges().entrySet()) {
                formatCodeChange(sb, entry.getKey(), entry.getValue());
                sb.append("\n");
            }
        }

        // Display storage changes
        if (trieLog.hasStorageChanges()) {
            sb.append("═══ Storage Changes ═══\n\n");
            for (Map.Entry<Address, Map<StorageSlotKey, PathBasedValue<UInt256>>> entry :
                trieLog.trieLogLayer.getStorageChanges().entrySet()) {
                formatStorageChanges(sb, entry.getKey(), entry.getValue());
                sb.append("\n");
            }
        }

        if (trieLog.isEmpty()) {
            sb.append("(Empty block - no state changes)\n");
        }

        return sb.toString();
    }

    private void formatAccountChange(StringBuilder sb, Address address,
                                     PathBasedValue<AccountValue> change) {
        sb.append("Address: ").append(address.toHexString()).append("\n");

        AccountValue prior = change.getPrior();
        AccountValue updated = change.getUpdated();

        if (prior == null && updated != null) {
            sb.append("  Status: CREATED\n");
            formatAccountValue(sb, "  ", updated);
        } else if (prior != null && updated == null) {
            sb.append("  Status: DELETED\n");
            formatAccountValue(sb, "  ", prior);
        } else if (prior != null && updated != null) {
            sb.append("  Status: UPDATED\n");
            formatAccountDiff(sb, prior, updated);
        }
    }

    private void formatAccountValue(StringBuilder sb, String indent, AccountValue account) {
        sb.append(indent).append("Nonce:       ").append(account.getNonce()).append("\n");
        sb.append(indent).append("Balance:     ").append(formatWei(account.getBalance())).append("\n");
        sb.append(indent).append("Storage Root: ").append(account.getStorageRoot()).append("\n");
        sb.append(indent).append("Code Hash:   ").append(account.getCodeHash()).append("\n");
    }

    private void formatAccountDiff(StringBuilder sb, AccountValue prior, AccountValue updated) {
        if (prior.getNonce() != updated.getNonce()) {
            sb.append("  Nonce:       ").append(prior.getNonce())
              .append(" → ").append(updated.getNonce()).append("\n");
        }
        if (!prior.getBalance().equals(updated.getBalance())) {
            sb.append("  Balance:     ").append(formatWei(prior.getBalance()))
              .append(" → ").append(formatWei(updated.getBalance())).append("\n");
        }
        if (!prior.getStorageRoot().equals(updated.getStorageRoot())) {
            sb.append("  Storage Root: ").append(prior.getStorageRoot())
              .append(" → ").append(updated.getStorageRoot()).append("\n");
        }
        if (!prior.getCodeHash().equals(updated.getCodeHash())) {
            sb.append("  Code Hash:   ").append(prior.getCodeHash())
              .append(" → ").append(updated.getCodeHash()).append("\n");
        }
    }

    private void formatCodeChange(StringBuilder sb, Address address, PathBasedValue<Bytes> change) {
        sb.append("Address: ").append(address.toHexString()).append("\n");

        Bytes prior = change.getPrior();
        Bytes updated = change.getUpdated();

        if (prior == null && updated != null) {
            sb.append("  Status: DEPLOYED\n");
            sb.append("  Code Size: ").append(updated.size()).append(" bytes\n");
            if (updated.size() <= 100) {
                sb.append("  Bytecode: ").append(updated.toHexString()).append("\n");
            } else {
                sb.append("  Bytecode (first 50 bytes): ")
                  .append(updated.slice(0, 50).toHexString()).append("...\n");
            }
        } else if (prior != null && updated == null) {
            sb.append("  Status: CLEARED\n");
            sb.append("  Prior Code Size: ").append(prior.size()).append(" bytes\n");
        } else if (prior != null && updated != null && !prior.equals(updated)) {
            sb.append("  Status: UPDATED\n");
            sb.append("  Prior Size:   ").append(prior.size()).append(" bytes\n");
            sb.append("  Updated Size: ").append(updated.size()).append(" bytes\n");
        }
    }

    private void formatStorageChanges(StringBuilder sb, Address address,
                                      Map<StorageSlotKey, PathBasedValue<UInt256>> changes) {
        sb.append("Address: ").append(address.toHexString()).append("\n");
        sb.append("  Storage Slots (").append(changes.size()).append(" changes):\n");

        for (Map.Entry<StorageSlotKey, PathBasedValue<UInt256>> entry : changes.entrySet()) {
            StorageSlotKey slotKey = entry.getKey();
            PathBasedValue<UInt256> change = entry.getValue();

            sb.append("    Slot: ").append(slotKey.getSlotHash().toHexString()).append("\n");

            Optional<UInt256> prior = Optional.ofNullable(change.getPrior())
                .filter(v -> !v.isZero());
            Optional<UInt256> updated = Optional.ofNullable(change.getUpdated())
                .filter(v -> !v.isZero());

            String priorStr = prior.map(UInt256::toHexString).orElse("0x0");
            String updatedStr = updated.map(UInt256::toHexString).orElse("0x0");

            if (prior.isEmpty() && updated.isPresent()) {
                sb.append("      Value: ").append(priorStr).append(" → ").append(updatedStr).append("\n");
            } else if (prior.isPresent() && updated.isEmpty()) {
                sb.append("      Value: ").append(priorStr).append(" → ").append(updatedStr).append(" (cleared)\n");
            } else if (prior.isPresent() && updated.isPresent()) {
                sb.append("      Value: ").append(priorStr).append(" → ").append(updatedStr).append("\n");
            } else {
                // Both are empty/zero - edge case, but show it
                sb.append("      Value: ").append(priorStr).append(" → ").append(updatedStr).append(" (no change)\n");
            }
        }
    }

    private String formatWei(Wei wei) {
        BigInteger weiValue = wei.toBigInteger();
        BigDecimal eth = new BigDecimal(weiValue).divide(new BigDecimal(WEI_PER_ETH));
        return String.format("%,d Wei (%.18f ETH)", weiValue, eth);
    }

    /**
     * Format a compact single-line summary.
     */
    public String formatCompact(TrieLogData trieLog) {
        String hashPrefix = trieLog.blockHash.toHexString().substring(0, 10) + "...";

        if (trieLog.trieLogLayer == null) {
            return String.format("Block %s | No trie log data", hashPrefix);
        }

        int accountChanges = trieLog.trieLogLayer.getAccountChanges().size();
        int codeChanges = trieLog.trieLogLayer.getCodeChanges().size();
        int storageChanges = trieLog.trieLogLayer.getStorageChanges().values().stream()
            .mapToInt(Map::size)
            .sum();

        return String.format("Block %s | %d account, %d code, %d storage changes",
            hashPrefix, accountChanges, codeChanges, storageChanges);
    }

    /**
     * Check if trie log is empty (no data).
     */
    public boolean isEmpty(TrieLogData trieLog) {
        return trieLog.isEmpty();
    }
}
