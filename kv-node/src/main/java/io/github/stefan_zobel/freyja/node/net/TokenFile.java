package io.github.stefan_zobel.freyja.node.net;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The tokens with which clients that have no certificate prove themselves
 * (docs/spec/operations.md, section 5): a file on the machine of the node, one token per
 * line. Empty lines and lines that start with {@code #} do not count.
 *
 * <p>The file is read again when its content has changed. A file that cannot be used leaves
 * the last good tokens in force.
 */
public final class TokenFile {

    /** A shorter token is too easy to guess; a file with one is not used. */
    public static final int MIN_BYTES = 16;
    /** A longer token does not fit into the handshake. */
    public static final int MAX_BYTES = 255;

    private static final System.Logger LOG = System.getLogger("freyja.tls");

    private final Path file;
    private final long recheckNanos;
    private final AtomicLong reloads = new AtomicLong();
    private final AtomicLong reloadFailures = new AtomicLong();
    private byte[] content;
    private List<byte[]> tokens;
    private long checkedNanos;

    private TokenFile(Path file, long recheckMillis) throws IOException {
        this.file = file;
        this.recheckNanos = recheckMillis * 1_000_000L;
        byte[] bytes = Files.readAllBytes(file);
        tokens = parse(bytes);
        content = bytes;
        checkedNanos = System.nanoTime();
    }

    /**
     * Reads the file. It is looked at again at most once a second.
     *
     * @throws IOException if the file cannot be read, holds no token, or holds one that is
     *                     too short or too long
     */
    public static TokenFile open(Path file) throws IOException {
        return new TokenFile(file, 1_000);
    }

    /** As {@link #open(Path)}, with the time after which the file is looked at again. */
    public static TokenFile open(Path file, long recheckMillis) throws IOException {
        return new TokenFile(file, recheckMillis);
    }

    /** How often a changed file was taken into use. */
    public long reloadCount() {
        return reloads.get();
    }

    /** How often a changed file could not be used. */
    public long reloadFailureCount() {
        return reloadFailures.get();
    }

    /** Whether the file, as it is now, holds this token. */
    public synchronized boolean accepts(String token) {
        long now = System.nanoTime();
        if (now - checkedNanos >= recheckNanos) {
            checkedNanos = now;
            try {
                byte[] bytes = Files.readAllBytes(file);
                if (!Arrays.equals(bytes, content)) {
                    tokens = parse(bytes);
                    content = bytes;
                    reloads.incrementAndGet();
                    LOG.log(System.Logger.Level.INFO, "the tokens were read again: " + file);
                }
            } catch (IOException e) {
                reloadFailures.incrementAndGet();
                LOG.log(System.Logger.Level.WARNING, "the changed token file cannot be used, the last good tokens"
                        + " stay in force (" + file + "): " + e.getMessage());
            }
        }
        byte[] given = token.getBytes(StandardCharsets.UTF_8);
        boolean found = false;
        // Every token is compared, and each in a time that does not depend on where it differs.
        for (byte[] known : tokens) {
            found |= MessageDigest.isEqual(known, given);
        }
        return found;
    }

    private List<byte[]> parse(byte[] bytes) throws IOException {
        List<byte[]> found = new ArrayList<>();
        for (String line : new String(bytes, StandardCharsets.UTF_8).split("\\R")) {
            String token = line.strip();
            if (token.isEmpty() || token.startsWith("#")) {
                continue;
            }
            byte[] encoded = token.getBytes(StandardCharsets.UTF_8);
            if (encoded.length < MIN_BYTES || encoded.length > MAX_BYTES) {
                throw new IOException(file + " holds a token of " + encoded.length + " bytes; a token has "
                        + MIN_BYTES + " to " + MAX_BYTES);
            }
            found.add(encoded);
        }
        if (found.isEmpty()) {
            throw new IOException(file + " holds no token");
        }
        return found;
    }
}
