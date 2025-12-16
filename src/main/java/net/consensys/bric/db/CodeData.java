package net.consensys.bric.db;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;

/**
 * Code data container.
 */
public class CodeData {
    public Address address;
    public Hash codeHash;
    public byte[] bytecode;
}
