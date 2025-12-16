package net.consensys.bric.db;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.Wei;

/**
 * Account data container.
 */
public class AccountData {
    public Address address;
    public Hash accountHash;
    public long nonce;
    public Wei balance;
    public Hash storageRoot;
    public Hash codeHash;
    public Long blockNumber;  // Block number when account was retrieved (archive databases only)
}
