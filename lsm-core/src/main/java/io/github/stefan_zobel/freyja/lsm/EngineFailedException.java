package io.github.stefan_zobel.freyja.lsm;

import java.io.IOException;

/**
 * The engine instance has stopped accepting writes because a write to its log failed. The
 * durable state of the log is unknown after such a failure, so the engine never retries; the
 * directory must be reopened (docs/spec/storage-engine.md, section 3).
 */
public final class EngineFailedException extends IOException {

    private static final long serialVersionUID = 1L;

    public EngineFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
