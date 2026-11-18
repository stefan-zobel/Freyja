package io.github.stefan_zobel.freyja.lsm;

import java.io.IOException;

/**
 * A write was rejected because the file system has too little free space. Nothing of the
 * write was stored. The engine stays readable, and flushes and compactions continue, because
 * they are the only way to free space (docs/spec/storage-engine.md, section 3).
 */
public final class DiskFullException extends IOException {

    private static final long serialVersionUID = 1L;

    public DiskFullException(String message) {
        super(message);
    }

    public DiskFullException(String message, Throwable cause) {
        super(message, cause);
    }
}
