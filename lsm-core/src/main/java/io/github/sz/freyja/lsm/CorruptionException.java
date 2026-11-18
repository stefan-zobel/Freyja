package io.github.sz.freyja.lsm;

import java.io.IOException;

/**
 * Stored data failed verification: a checksum mismatch, an impossible length, or a structure
 * that contradicts the format. Damaged data is never returned to a caller.
 */
public final class CorruptionException extends IOException {

    private static final long serialVersionUID = 1L;

    private final String file;
    private final long offset;

    /**
     * @param file   the damaged file
     * @param offset the offset of the damaged unit in that file
     * @param detail what was found
     */
    public CorruptionException(String file, long offset, String detail) {
        super("corruption in " + file + " at offset " + offset + ": " + detail);
        this.file = file;
        this.offset = offset;
    }

    public CorruptionException(String file, long offset, String detail, Throwable cause) {
        this(file, offset, detail);
        initCause(cause);
    }

    /** Returns the damaged file. */
    public String file() {
        return file;
    }

    /** Returns the offset of the damaged unit. */
    public long offset() {
        return offset;
    }
}
