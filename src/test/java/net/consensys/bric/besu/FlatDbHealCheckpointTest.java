package net.consensys.bric.besu;

import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlatDbHealCheckpointTest {

    private static final Bytes32 STATE_ROOT = Bytes32.fromHexString(
        "0x1111111111111111111111111111111111111111111111111111111111111111");

    @Test
    void encodeThenDecode_roundTripsAllFields() {
        FlatDbHealCheckpoint checkpoint =
            new FlatDbHealCheckpoint(STATE_ROOT, FlatDbHealCheckpoint.Phase.ACCOUNTS, 7);

        FlatDbHealCheckpoint decoded = FlatDbHealCheckpoint.decode(checkpoint.encode());

        assertThat(decoded.stateRoot()).isEqualTo(STATE_ROOT);
        assertThat(decoded.phase()).isEqualTo(FlatDbHealCheckpoint.Phase.ACCOUNTS);
        assertThat(decoded.nextRangeIndex()).isEqualTo(7);
        assertThat(decoded).isEqualTo(checkpoint);
    }

    @Test
    void encodeThenDecode_storagePhase() {
        FlatDbHealCheckpoint checkpoint =
            new FlatDbHealCheckpoint(STATE_ROOT, FlatDbHealCheckpoint.Phase.STORAGE, 16);

        FlatDbHealCheckpoint decoded = FlatDbHealCheckpoint.decode(checkpoint.encode());

        assertThat(decoded.phase()).isEqualTo(FlatDbHealCheckpoint.Phase.STORAGE);
        assertThat(decoded.nextRangeIndex()).isEqualTo(16);
    }

    @Test
    void decode_rejectsWrongLength() {
        assertThatThrownBy(() -> FlatDbHealCheckpoint.decode(new byte[]{1, 2, 3}))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Invalid checkpoint length");
    }
}
