package net.consensys.bric.besu;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class HeartbeatThrottleTest {

    @Test
    void shouldFire_falseBeforeIntervalElapses() {
        AtomicLong clock = new AtomicLong(0);
        HeartbeatThrottle throttle = new HeartbeatThrottle(60_000, clock::get);

        clock.set(59_999);

        assertThat(throttle.shouldFire()).isFalse();
    }

    @Test
    void shouldFire_trueOnceIntervalHasFullyElapsed() {
        AtomicLong clock = new AtomicLong(0);
        HeartbeatThrottle throttle = new HeartbeatThrottle(60_000, clock::get);

        clock.set(60_000);

        assertThat(throttle.shouldFire()).isTrue();
    }

    @Test
    void shouldFire_requiresAFullIntervalAgainAfterFiring() {
        AtomicLong clock = new AtomicLong(0);
        HeartbeatThrottle throttle = new HeartbeatThrottle(60_000, clock::get);

        clock.set(60_000);
        assertThat(throttle.shouldFire()).isTrue();

        clock.set(90_000);
        assertThat(throttle.shouldFire()).isFalse();

        clock.set(120_000);
        assertThat(throttle.shouldFire()).isTrue();
    }
}
