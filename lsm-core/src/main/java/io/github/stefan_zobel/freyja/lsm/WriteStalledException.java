package io.github.stefan_zobel.freyja.lsm;

import java.io.IOException;

/**
 * A write waited for flush or compaction to catch up and gave up when its time was over.
 * Nothing of the write was stored. The engine is intact; the caller may retry later
 * (docs/spec/storage-engine.md, section 7).
 */
public final class WriteStalledException extends IOException {

    private static final long serialVersionUID = 1L;

    public WriteStalledException(String message) {
        super(message);
    }
}
