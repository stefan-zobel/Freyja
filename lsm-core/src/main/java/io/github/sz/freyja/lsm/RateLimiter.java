package io.github.sz.freyja.lsm;

import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import io.github.sz.freyja.common.Clock;

/**
 * Limits background I/O to a number of bytes per second, so that compaction does not starve
 * foreground writes (docs/spec/storage-engine.md, section 3.2).
 *
 * <p>Each caller announces the bytes it is about to write. The limiter keeps the time at
 * which the bytes announced so far are paid for at the configured rate, and makes a caller
 * wait until then. Unused time is not saved up: after a pause the rate starts again from the
 * present.
 *
 * <p>While the bypass condition holds, nobody waits. The engine sets it while writes are
 * held back, because then compaction has to catch up as fast as it can.
 *
 * <p>Safe for concurrent use.
 */
final class RateLimiter {

    /** Pauses the calling thread. */
    @FunctionalInterface
    interface Sleeper {
        void sleepNanos(long nanos);
    }

    /** Sleeps for real. */
    static final Sleeper PARK = nanos -> {
        long deadline = System.nanoTime() + nanos;
        long remaining = nanos;
        while (remaining > 0 && !Thread.currentThread().isInterrupted()) {
            LockSupport.parkNanos(remaining);
            remaining = deadline - System.nanoTime();
        }
    };

    private final long bytesPerSecond;
    private final Clock clock;
    private final Sleeper sleeper;
    private final BooleanSupplier bypass;
    private long paidUntil;      // guarded by this
    private boolean started;     // guarded by this
    private long throttledNanos; // guarded by this

    /**
     * @param bytesPerSecond the rate; 0 means no limit
     * @param bypass         while this returns true, nobody waits
     */
    RateLimiter(long bytesPerSecond, Clock clock, Sleeper sleeper, BooleanSupplier bypass) {
        if (bytesPerSecond < 0) {
            throw new IllegalArgumentException("negative rate");
        }
        this.bytesPerSecond = bytesPerSecond;
        this.clock = clock;
        this.sleeper = sleeper;
        this.bypass = bypass;
    }

    /** Announces {@code bytes} of I/O and waits until the rate allows them. */
    void acquire(long bytes) {
        if (bytesPerSecond == 0 || bytes <= 0) {
            return;
        }
        long wait;
        synchronized (this) {
            long now = clock.nanoTime();
            if (!started || paidUntil - now < 0 || bypass.getAsBoolean()) {
                // Start from the present: no credit for idle time, no debt during a bypass.
                paidUntil = now;
                started = true;
            }
            wait = bypass.getAsBoolean() ? 0 : paidUntil - now;
            paidUntil += costNanos(bytes);
            throttledNanos += wait;
        }
        if (wait > 0) {
            sleeper.sleepNanos(wait);
        }
    }

    /** Returns the total time callers were made to wait. */
    synchronized long throttledNanos() {
        return throttledNanos;
    }

    private long costNanos(long bytes) {
        // bytes * 1e9 / rate, without overflow for large byte counts.
        long seconds = bytes / bytesPerSecond;
        long rest = bytes % bytesPerSecond;
        return seconds * 1_000_000_000L + rest * 1_000_000_000L / bytesPerSecond;
    }
}
