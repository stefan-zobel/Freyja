package io.github.sz.freyja.common.testing;

import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Named probes for rare situations (docs/spec/verification.md, section 3.1). The code calls
 * {@link #hit} where such a situation occurs; a simulation counts the hits and fails if a
 * probe is never reached across its seed set.
 *
 * <p>Outside tests nobody switches the probes on, and a hit does nothing. A probe never
 * changes what the code does: it is counted, nothing else.
 *
 * <p>The counters are shared by everything in the process. Whoever wants the hits of one run
 * takes the difference of {@link #count} before and after it.
 */
public final class Probes {

    /** The situations that are counted where they occur, below the node. */
    public enum Probe {
        /** A log file ended in data that was never synced, and the reader left that data out. */
        TORN_TAIL_DROPPED,
        /** Invalid data in the last file of a log was damage, because a sync point proved it synced. */
        DAMAGE_PROVEN_BY_SYNC_POINT,
        /** A write of the storage engine was held back until a flush or a compaction had made room. */
        WRITE_HELD_BACK
    }

    private static volatile AtomicLongArray counters;

    private Probes() {
    }

    /** Switches the probes on, for the rest of the life of the process. For tests. */
    public static synchronized void enable() {
        if (counters == null) {
            counters = new AtomicLongArray(Probe.values().length);
        }
    }

    /** Notes that the situation occurred. Does nothing unless the probes are switched on. */
    public static void hit(Probe probe) {
        AtomicLongArray current = counters;
        if (current != null) {
            current.incrementAndGet(probe.ordinal());
        }
    }

    /** How often the situation occurred since the probes were switched on. */
    public static long count(Probe probe) {
        AtomicLongArray current = counters;
        return current == null ? 0 : current.get(probe.ordinal());
    }
}
