package io.github.stefan_zobel.freyja.lsm;

import java.util.Arrays;

import io.github.stefan_zobel.freyja.lsm.table.InternalKey;

/**
 * Decides which entries a compaction keeps (docs/spec/storage-engine.md, section 8).
 *
 * <p>For each user key the compaction keeps the newest version, plus every version that is
 * the newest one visible to some live snapshot. A tombstone that would be kept is dropped
 * after all if no snapshot is older than it and no deeper level can hold the key: then
 * nothing remains that it would have to hide.
 *
 * <p>Entries must be offered in internal-key order, that is, per user key from the newest
 * version to the oldest.
 */
final class Retention {

    private static final long NONE = -1;

    private final long[] snapshots; // ascending
    private final long oldestSnapshot;
    private byte[] currentUserKey;
    private long newerSequence = NONE; // sequence of the previous, newer version of the key

    /**
     * @param liveSnapshots the sequence numbers of the snapshots that are live when the
     *                      compaction starts, in any order
     */
    Retention(long[] liveSnapshots) {
        this.snapshots = liveSnapshots.clone();
        Arrays.sort(this.snapshots);
        this.oldestSnapshot = snapshots.length > 0 ? snapshots[0] : Long.MAX_VALUE;
    }

    /**
     * Returns whether the compaction writes this entry to its output.
     *
     * @param internalKey         the entry's key
     * @param deeperLevelsMayHold whether a level below the output level may hold the user key
     */
    boolean keep(byte[] internalKey, boolean deeperLevelsMayHold) {
        if (currentUserKey == null || !InternalKey.hasUserKey(internalKey, currentUserKey)) {
            currentUserKey = InternalKey.userKey(internalKey);
            newerSequence = NONE;
        }
        long sequence = InternalKey.sequence(internalKey);
        // Needed if it is the newest version, or the newest one some snapshot sees: a
        // snapshot at or after this version and before the next newer one.
        boolean needed = newerSequence == NONE || snapshotIn(sequence, newerSequence);
        newerSequence = sequence;
        if (!needed) {
            return false;
        }
        // A tombstone exists to hide older versions. Every snapshot is at or after it, so
        // all older versions in this compaction are dropped; if no deeper level holds the
        // key either, the tombstone hides nothing.
        return !(InternalKey.type(internalKey) == InternalKey.TYPE_TOMBSTONE
                && sequence <= oldestSnapshot && !deeperLevelsMayHold);
    }

    /** Returns whether a snapshot lies in {@code [from, toExclusive)}. */
    private boolean snapshotIn(long from, long toExclusive) {
        int index = Arrays.binarySearch(snapshots, from);
        if (index < 0) {
            index = -index - 1; // first snapshot greater than from
        }
        return index < snapshots.length && snapshots[index] < toExclusive;
    }
}
