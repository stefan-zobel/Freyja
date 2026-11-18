package io.github.sz.freyja.lsm.fs;

import java.io.IOException;

/**
 * An append failed because the file system is full. Part of the data may have been written.
 */
public final class NoSpaceException extends IOException {

    private static final long serialVersionUID = 1L;

    public NoSpaceException(String message) {
        super(message);
    }
}
