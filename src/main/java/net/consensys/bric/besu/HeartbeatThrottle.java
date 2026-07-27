package net.consensys.bric.besu;

import java.util.function.LongSupplier;

/**
 * Rate-limits a periodic progress heartbeat during a long-running flat DB heal, so a single
 * account range or one huge contract's storage walk — either of which can span many batches —
 * reports at most once per interval instead of once per batch.
 */
class HeartbeatThrottle {

    private final long intervalMillis;
    private final LongSupplier clock;
    private long lastFiredMillis;

    HeartbeatThrottle(long intervalMillis) {
        this(intervalMillis, System::currentTimeMillis);
    }

    /** Package-private: lets tests inject a fake clock to verify the interval deterministically. */
    HeartbeatThrottle(long intervalMillis, LongSupplier clock) {
        this.intervalMillis = intervalMillis;
        this.clock = clock;
        this.lastFiredMillis = clock.getAsLong();
    }

    /**
     * Returns {@code true} at most once per {@code intervalMillis}. The first eligible call is only
     * after a full interval has elapsed since construction — there's no immediate fire on startup.
     */
    boolean shouldFire() {
        long now = clock.getAsLong();
        if (now - lastFiredMillis >= intervalMillis) {
            lastFiredMillis = now;
            return true;
        }
        return false;
    }
}
