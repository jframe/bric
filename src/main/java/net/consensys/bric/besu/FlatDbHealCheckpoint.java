package net.consensys.bric.besu;

import org.apache.tuweni.bytes.Bytes32;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Durable progress marker for an in-progress flat DB heal, persisted as a single value
 * under {@code VARIABLES["bricFlatDbHealCheckpoint"]}.
 */
public class FlatDbHealCheckpoint {

    public enum Phase {
        ACCOUNTS,
        STORAGE
    }

    private static final int ENCODED_LENGTH = 32 + 1 + 4;

    private final Bytes32 stateRoot;
    private final Phase phase;
    private final int nextRangeIndex;

    public FlatDbHealCheckpoint(Bytes32 stateRoot, Phase phase, int nextRangeIndex) {
        this.stateRoot = Objects.requireNonNull(stateRoot);
        this.phase = Objects.requireNonNull(phase);
        this.nextRangeIndex = nextRangeIndex;
    }

    public Bytes32 stateRoot() {
        return stateRoot;
    }

    public Phase phase() {
        return phase;
    }

    public int nextRangeIndex() {
        return nextRangeIndex;
    }

    public byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(ENCODED_LENGTH);
        buffer.put(stateRoot.toArrayUnsafe());
        buffer.put((byte) phase.ordinal());
        buffer.putInt(nextRangeIndex);
        return buffer.array();
    }

    public static FlatDbHealCheckpoint decode(byte[] bytes) {
        if (bytes.length != ENCODED_LENGTH) {
            throw new IllegalArgumentException(
                "Invalid checkpoint length: expected " + ENCODED_LENGTH + " but was " + bytes.length);
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] rootBytes = new byte[32];
        buffer.get(rootBytes);
        Bytes32 stateRoot = Bytes32.wrap(rootBytes);
        Phase phase = Phase.values()[buffer.get()];
        int nextRangeIndex = buffer.getInt();
        return new FlatDbHealCheckpoint(stateRoot, phase, nextRangeIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlatDbHealCheckpoint that)) {
            return false;
        }
        return nextRangeIndex == that.nextRangeIndex
            && stateRoot.equals(that.stateRoot)
            && phase == that.phase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateRoot, phase, nextRangeIndex);
    }
}
