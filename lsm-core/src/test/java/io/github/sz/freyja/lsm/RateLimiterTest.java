package io.github.sz.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

/** The I/O rate limiter, on a clock that the test moves. */
class RateLimiterTest {

    private long now = 1_000;
    private long slept;
    private boolean bypass;

    /** A limiter whose sleeps advance the test clock. */
    private RateLimiter limiter(long bytesPerSecond) {
        return new RateLimiter(bytesPerSecond, () -> now, nanos -> {
            slept += nanos;
            now += nanos;
        }, () -> bypass);
    }

    @Test
    void steadyUseIsHeldToTheRate() {
        RateLimiter limiter = limiter(1_000_000); // one byte per microsecond
        for (int i = 0; i < 100; i++) {
            limiter.acquire(10_000); // 10 ms worth each
        }
        // The first call starts at once; each later one waits for the one before it.
        assertEquals(99 * 10_000_000L, slept);
        assertEquals(slept, limiter.throttledNanos());
    }

    @Test
    void timeThatPassesAnywayCountsTowardTheRate() {
        RateLimiter limiter = limiter(1_000_000);
        limiter.acquire(10_000);
        now += 4_000_000;          // the caller spent 4 ms on its own work
        limiter.acquire(10_000);
        assertEquals(6_000_000L, slept);
    }

    @Test
    void idleTimeIsNotSavedUp() {
        RateLimiter limiter = limiter(1_000_000);
        limiter.acquire(10_000);
        now += 60_000_000_000L;    // a minute without any I/O
        limiter.acquire(10_000);   // starts from the present ...
        assertEquals(0, slept);
        limiter.acquire(10_000);   // ... so the next one waits its full 10 ms
        assertEquals(10_000_000L, slept);
    }

    @Test
    void nobodyWaitsDuringABypassAndNoDebtBuildsUp() {
        RateLimiter limiter = limiter(1_000_000);
        limiter.acquire(10_000);
        bypass = true;
        for (int i = 0; i < 1000; i++) {
            limiter.acquire(1_000_000);
        }
        assertEquals(0, slept);
        bypass = false;
        limiter.acquire(10_000);
        // Only the last bypassed request is still being paid for: one second.
        assertEquals(1_000_000_000L, slept);
    }

    @Test
    void aRateOfZeroMeansNoLimit() {
        RateLimiter limiter = limiter(0);
        for (int i = 0; i < 1000; i++) {
            limiter.acquire(Long.MAX_VALUE / 2);
        }
        assertEquals(0, slept);
        assertThrows(IllegalArgumentException.class, () -> limiter(-1));
    }

    @Test
    void largeAndTinyRequestsAreCostedExactly() {
        RateLimiter limiter = limiter(3);
        limiter.acquire(1);                    // a third of a second
        limiter.acquire(0);                    // nothing
        limiter.acquire(-5);                   // nothing
        limiter.acquire(7_000_000_000L);       // waits for the first request
        assertEquals(333_333_333L, slept);
        limiter.acquire(1);                    // waits for the seven billion bytes
        assertEquals(333_333_333L + 2_333_333_333_333_333_333L, slept);
    }
}
