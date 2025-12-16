package net.consensys.bric.formatters;

import org.apache.tuweni.units.bigints.UInt256;
import net.consensys.bric.db.BesuDatabaseReader.AccountData;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

/**
 * Formats account data for human-readable display.
 */
public class AccountFormatter {

    private static final BigInteger WEI_PER_ETH = new BigInteger("1000000000000000000"); // 10^18
    private static final Hash EMPTY_CODE_HASH = Hash.fromHexString(
        "0xc5d2460186f7233c927e7db2dcc703c0e6b061b9d7daad5c4b9b1cd0b6d9c5c6"
    );

    /**
     * Format account data as a multi-line string.
     */
    public String format(AccountData account) {
        StringBuilder sb = new StringBuilder();

        sb.append("\nAccount Information:\n");

        if (account.address != null) {
            sb.append("  Address:      ").append(account.address.toHexString()).append("\n");
        }
        sb.append("  Account Hash: ").append(account.accountHash.toHexString()).append("\n");

        // Show block number if available (archive databases only)
        if (account.blockNumber != null) {
            sb.append("  Block Number: ").append(String.format("%,d", account.blockNumber)).append("\n");
        }

        sb.append("  Nonce:        ").append(formatNonce(account.nonce)).append("\n");
        sb.append("  Balance:      ").append(formatBalance(account.balance)).append("\n");
        sb.append("  Storage Root: ").append(account.storageRoot.toHexString()).append("\n");
        sb.append("  Code Hash:    ").append(formatCodeHash(account.codeHash)).append("\n");

        return sb.toString();
    }

    /**
     * Format nonce with comma separators.
     */
    private String formatNonce(long nonce) {
        return String.format("%,d", nonce);
    }

    /**
     * Format balance showing both Wei and ETH.
     * Example: "1500000000000000000 Wei (1.5 ETH)"
     */
    private String formatBalance(Wei balance) {
        BigInteger weiValue = balance.toBigInteger();

        if (weiValue.equals(BigInteger.ZERO)) {
            return "0 Wei (0 ETH)";
        }

        // Convert to ETH
        BigDecimal ethValue = new BigDecimal(weiValue)
            .divide(new BigDecimal(WEI_PER_ETH), 18, RoundingMode.DOWN)
            .stripTrailingZeros();

        String ethStr = ethValue.toPlainString();
        if (ethStr.equals("0")) {
            return String.format("%,d Wei (< 0.000000000000000001 ETH)", weiValue);
        }

        return String.format("%,d Wei (%s ETH)", weiValue, ethStr);
    }

    /**
     * Format code hash with indication if it's an empty account (no code).
     */
    private String formatCodeHash(Hash codeHash) {
        if (codeHash.equals(EMPTY_CODE_HASH)) {
            return codeHash.toHexString() + " (no code - EOA)";
        }
        return codeHash.toHexString() + " (contract)";
    }

    /**
     * Format a compact single-line summary.
     */
    public String formatCompact(AccountData account) {
        String addressStr = account.address != null
            ? account.address.toHexString()
            : account.accountHash.toHexString();

        BigDecimal ethValue = new BigDecimal(account.balance.toBigInteger())
            .divide(new BigDecimal(WEI_PER_ETH), 4, RoundingMode.DOWN);

        String codeIndicator = account.codeHash.equals(EMPTY_CODE_HASH) ? "EOA" : "Contract";

        return String.format("%s | %s ETH | nonce=%d | %s",
            addressStr, ethValue.toPlainString(), account.nonce, codeIndicator);
    }
}
