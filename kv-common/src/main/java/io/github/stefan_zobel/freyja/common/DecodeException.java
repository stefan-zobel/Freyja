package io.github.stefan_zobel.freyja.common;

/**
 * Thrown when bytes cannot be decoded because they are not a valid encoding.
 *
 * <p>Callers that read stored or received data translate this into their own corruption
 * error, which names the file and offset or the connection.
 */
public final class DecodeException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public DecodeException(String message) {
        super(message);
    }
}
