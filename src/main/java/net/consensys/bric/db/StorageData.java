package net.consensys.bric.db;

import org.apache.tuweni.units.bigints.UInt256;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

/**
 * Storage data container.
 */
public class StorageData {
    public Address address;
    public UInt256 slot;
    public Hash accountHash;
    public Hash slotHash;
    public UInt256 value;
    public Long blockNumber;  // Block number when storage was retrieved (archive databases only)
}
