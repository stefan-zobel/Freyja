package io.github.sz.freyja.lsm;

import java.io.IOException;

/**
 * The data is intact but uses a format version or feature this release does not know.
 * A reader never guesses at such data.
 */
public final class UnsupportedFormatException extends IOException {

    private static final long serialVersionUID = 1L;

    public UnsupportedFormatException(String message) {
        super(message);
    }
}
