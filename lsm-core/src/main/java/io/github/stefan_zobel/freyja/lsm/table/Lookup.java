package io.github.stefan_zobel.freyja.lsm.table;

/**
 * Result of looking up one user key at a snapshot in one source (a MemTable or a table file).
 *
 * @param kind     what the source holds for the key
 * @param value    the value if {@code kind} is {@code VALUE}, otherwise {@code null}
 * @param sequence the sequence number of the entry found, or -1 if absent
 */
public record Lookup(Kind kind, byte[] value, long sequence) {

    /** What a source holds for a key. */
    public enum Kind {
        /** The source has no entry for the key that is visible at the snapshot. */
        ABSENT,
        /** The newest visible entry is a value. */
        VALUE,
        /** The newest visible entry is a deletion marker. */
        TOMBSTONE
    }

    /** The result for a key the source does not hold. */
    public static final Lookup NOT_FOUND = new Lookup(Kind.ABSENT, null, -1);

    /** Returns the result for an internal key and the value stored with it. */
    public static Lookup of(byte[] internalKey, byte[] value) {
        long sequence = InternalKey.sequence(internalKey);
        if (InternalKey.type(internalKey) == InternalKey.TYPE_TOMBSTONE) {
            return new Lookup(Kind.TOMBSTONE, null, sequence);
        }
        return new Lookup(Kind.VALUE, value, sequence);
    }
}
