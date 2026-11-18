package io.github.stefan_zobel.freyja.common.kv;

import java.util.Objects;

/**
 * What a client asks of a node (docs/spec/node.md, sections 4.3, 8 and 9).
 *
 * <p>Keys and values are treated as immutable once they are part of a request.
 */
public sealed interface ClientRequest {

    /**
     * Run a command through the log: a batch, a conditional batch, or a command of a session
     * (register, keep alive, close). Commands of an administrator are not accepted here.
     */
    record Execute(Command command) implements ClientRequest {
        public Execute {
            Objects.requireNonNull(command, "command");
        }
    }

    /**
     * Read one key.
     *
     * @param stale {@code false} for a linearizable read; {@code true} to read whatever the
     *              node that is asked holds, which may be behind
     */
    record Get(byte[] key, boolean stale) implements ClientRequest {
        public Get {
            Objects.requireNonNull(key, "key");
        }
    }

    /**
     * Read a range of keys in order.
     *
     * @param start   the first key, inclusive; empty for no lower bound
     * @param end     the key to stop before; empty for no upper bound
     * @param limit   the largest number of pairs to return
     * @param reverse whether the range is read from its end to its start
     * @param stale   as for {@link Get}
     */
    record Scan(byte[] start, byte[] end, int limit, boolean reverse, boolean stale) implements ClientRequest {
        public Scan {
            Objects.requireNonNull(start, "start");
            Objects.requireNonNull(end, "end");
            if (limit < 1) {
                throw new IllegalArgumentException("limit " + limit);
            }
        }
    }
}
