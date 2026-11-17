package io.github.stefan_zobel.freyja.lsm;

import java.io.Closeable;

/**
 * A read view of the engine at one sequence number. Reads through a snapshot see exactly the
 * writes that had returned when the snapshot was taken.
 *
 * <p>A live snapshot keeps the engine from reclaiming the versions it can see, so it should
 * be closed as soon as it is no longer needed. A snapshot does not survive closing or
 * reopening the engine.
 */
public final class Snapshot implements Closeable {

    private final LsmEngine engine;
    private final long sequence;
    private boolean closed;

    Snapshot(LsmEngine engine, long sequence) {
        this.engine = engine;
        this.sequence = sequence;
    }

    /** Returns the sequence number this snapshot reads at. */
    public long sequence() {
        return sequence;
    }

    /** Returns the sequence number for a read by {@code engine}, after checking the snapshot. */
    long sequenceFor(LsmEngine reader) {
        synchronized (this) {
            if (closed) {
                throw new IllegalStateException("snapshot is closed");
            }
        }
        if (reader != engine) {
            throw new IllegalArgumentException("snapshot belongs to another engine");
        }
        return sequence;
    }

    /** Releases the snapshot. Closing twice has no effect. */
    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }
        engine.releaseSnapshot(this);
    }
}
