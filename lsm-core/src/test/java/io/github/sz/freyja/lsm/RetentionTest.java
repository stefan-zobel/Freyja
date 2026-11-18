package io.github.sz.freyja.lsm;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.sz.freyja.lsm.table.InternalKey;

/**
 * Exhaustive check of the compaction retention rules: for every history of up to five
 * versions of a key and every set of snapshots, dropping what the rules allow changes
 * nothing that any reader can see, and nothing is kept that no reader can see.
 */
class RetentionTest {

    /** Versions get the sequence numbers 2, 4, 6, ...; snapshots can sit on and between them. */
    private static final int MAX_VERSIONS = 5;

    private record Entry(long sequence, boolean tombstone) {
    }

    /** The entry a reader at {@code viewpoint} sees: the newest one at or below it, or null. */
    private static Entry visible(List<Entry> newestFirst, long viewpoint) {
        for (Entry entry : newestFirst) {
            if (entry.sequence() <= viewpoint) {
                return entry;
            }
        }
        return null;
    }

    private static List<Entry> retained(List<Entry> newestFirst, long[] snapshots, boolean deeperLevelsMayHold,
                                        byte[] userKey) {
        Retention retention = new Retention(snapshots);
        // Another key first, to show that state does not leak from one key to the next.
        retention.keep(InternalKey.encode(new byte[] {0x01}, 99, InternalKey.TYPE_VALUE), deeperLevelsMayHold);
        List<Entry> kept = new ArrayList<>();
        for (Entry entry : newestFirst) {
            byte[] key = InternalKey.encode(userKey, entry.sequence(),
                    entry.tombstone() ? InternalKey.TYPE_TOMBSTONE : InternalKey.TYPE_VALUE);
            if (retention.keep(key, deeperLevelsMayHold)) {
                kept.add(entry);
            }
        }
        return kept;
    }

    @Test
    void retentionPreservesEveryViewAndKeepsNothingElse() {
        byte[] userKey = {0x42};
        long cases = 0;
        int maxSequence = 2 * MAX_VERSIONS + 1;
        for (int versions = 1; versions <= MAX_VERSIONS; versions++) {
            for (int types = 0; types < (1 << versions); types++) {
                List<Entry> newestFirst = new ArrayList<>();
                for (int i = versions - 1; i >= 0; i--) {
                    newestFirst.add(new Entry(2L * (i + 1), (types & (1 << i)) != 0));
                }
                for (int snapshotBits = 0; snapshotBits < (1 << maxSequence); snapshotBits++) {
                    long[] snapshots = new long[Integer.bitCount(snapshotBits)];
                    int n = 0;
                    for (int bit = 0; bit < maxSequence; bit++) {
                        if ((snapshotBits & (1 << bit)) != 0) {
                            snapshots[n++] = bit + 1;
                        }
                    }
                    for (boolean deeper : new boolean[] {false, true}) {
                        check(newestFirst, snapshots, deeper, userKey);
                        cases++;
                    }
                }
            }
        }
        assertEquals(2L * (2 + 4 + 8 + 16 + 32) * (1 << maxSequence), cases);
    }

    private static void check(List<Entry> newestFirst, long[] snapshots, boolean deeper, byte[] userKey) {
        List<Entry> kept = retained(newestFirst, snapshots, deeper, userKey);
        String what = newestFirst + " with snapshots " + java.util.Arrays.toString(snapshots)
                + (deeper ? ", deeper levels may hold the key" : ", no deeper data") + ": kept " + kept;

        // Viewpoints: every snapshot, and the present.
        long[] viewpoints = java.util.Arrays.copyOf(snapshots, snapshots.length + 1);
        viewpoints[snapshots.length] = Long.MAX_VALUE;

        boolean[] seen = new boolean[kept.size()];
        for (long viewpoint : viewpoints) {
            Entry before = visible(newestFirst, viewpoint);
            Entry after = visible(kept, viewpoint);
            boolean beforeHasValue = before != null && !before.tombstone();
            if (deeper || beforeHasValue) {
                // The reader must find the very same entry. With deeper data that includes
                // tombstones and "nothing here": both decide what the reader sees below.
                if (before == null ? after != null : !before.equals(after)) {
                    fail("view at " + viewpoint + " changed from " + before + " to " + after + ": " + what);
                }
            } else if (after != null && !after.tombstone()) {
                // Deleted or never written, and nothing below: it must stay invisible.
                fail("view at " + viewpoint + " shows " + after + " instead of nothing: " + what);
            }
            if (after != null) {
                seen[kept.indexOf(after)] = true;
            }
        }
        for (int i = 0; i < seen.length; i++) {
            if (!seen[i]) {
                fail("kept " + kept.get(i) + " although no reader sees it: " + what);
            }
        }
    }

    @Test
    void knownCases() {
        byte[] k = {0x42};
        List<Entry> history = List.of(new Entry(6, false), new Entry(4, true), new Entry(2, false));

        // No snapshot: only the newest version survives.
        assertEquals(List.of(new Entry(6, false)), retained(history, new long[0], false, k));
        // A snapshot between the tombstone and the newest value: the tombstone is what it
        // sees. Without deeper data the tombstone hides nothing and goes too.
        assertEquals(List.of(new Entry(6, false)), retained(history, new long[] {5}, false, k));
        assertEquals(List.of(new Entry(6, false), new Entry(4, true)), retained(history, new long[] {5}, true, k));
        // A snapshot that sees the oldest value keeps it. The tombstone goes: no reader sees
        // it, because the present sees the newest value.
        assertEquals(List.of(new Entry(6, false), new Entry(2, false)), retained(history, new long[] {3}, false, k));
        // With a second snapshot that does see the tombstone, it must stay to hide the
        // oldest value from that snapshot.
        assertEquals(history, retained(history, new long[] {3, 5}, false, k));

        // A newest tombstone: dropped only when nothing below can hold the key.
        List<Entry> deleted = List.of(new Entry(4, true), new Entry(2, false));
        assertTrue(retained(deleted, new long[0], false, k).isEmpty());
        assertEquals(List.of(new Entry(4, true)), retained(deleted, new long[0], true, k));
        assertFalse(retained(deleted, new long[] {2}, false, k).isEmpty());
    }
}
