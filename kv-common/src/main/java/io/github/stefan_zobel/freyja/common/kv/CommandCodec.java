package io.github.stefan_zobel.freyja.common.kv;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.common.kv.Command.CloseSession;
import io.github.stefan_zobel.freyja.common.kv.Command.Condition;
import io.github.stefan_zobel.freyja.common.kv.Command.ConditionKind;
import io.github.stefan_zobel.freyja.common.kv.Command.Conditional;
import io.github.stefan_zobel.freyja.common.kv.Command.Delete;
import io.github.stefan_zobel.freyja.common.kv.Command.KeepAlive;
import io.github.stefan_zobel.freyja.common.kv.Command.Mutation;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;
import io.github.stefan_zobel.freyja.common.kv.Command.RegisterSession;
import io.github.stefan_zobel.freyja.common.kv.Command.Write;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failed;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failure;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.SessionOk;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Written;

/**
 * The encoding of commands and of their results. All integers are little-endian.
 *
 * <p>A command in the log:
 *
 * <pre>
 * version    uint8    1
 * timestamp  uint64   the clock of the leader when it proposed the command, in milliseconds
 * type       uint8    1 Write, 2 Conditional, 3 RegisterSession, 4 KeepAlive, 5 CloseSession
 * body       ...
 * </pre>
 *
 * <ul>
 *   <li>Write: session id, sequence ({@code uint64} each), mutations.</li>
 *   <li>Conditional: session id, sequence, conditions, mutations if they hold, mutations
 *       otherwise.</li>
 *   <li>KeepAlive, CloseSession: session id.</li>
 *   <li>A list is a count ({@code uint32}) and its elements. A byte string is a length
 *       ({@code uint32}) and its bytes.</li>
 *   <li>Mutation: kind ({@code uint8}; 1 put, 2 delete), key, and for a put the value.</li>
 *   <li>Condition: kind ({@code uint8}, in the order of {@link ConditionKind}), key, value,
 *       mod index ({@code uint64}).</li>
 * </ul>
 *
 * <p>A result: kind ({@code uint8}; 1 written, 2 conditional, 3 session registered, 4 session
 * ok, 5 failed) and one field where the kind has one.
 */
public final class CommandCodec {

    public static final int VERSION = 1;

    /** A command with the time stamp its leader gave it. */
    public record Stamped(long timestampMillis, Command command) {
    }

    private CommandCodec() {
    }

    // ------------------------------------------------------------------ commands

    public static byte[] encode(long timestampMillis, Command command) {
        Out out = new Out();
        out.u8(VERSION);
        out.u64(timestampMillis);
        switch (command) {
            case Write w -> {
                out.u8(1);
                out.u64(w.sessionId());
                out.u64(w.sequence());
                mutations(out, w.mutations());
            }
            case Conditional c -> {
                out.u8(2);
                out.u64(c.sessionId());
                out.u64(c.sequence());
                out.u32(c.conditions().size());
                for (Condition condition : c.conditions()) {
                    out.u8(condition.kind().ordinal());
                    out.bytes(condition.key());
                    out.bytes(condition.value());
                    out.u64(condition.modIndex());
                }
                mutations(out, c.then());
                mutations(out, c.otherwise());
            }
            case RegisterSession _ -> out.u8(3);
            case KeepAlive k -> {
                out.u8(4);
                out.u64(k.sessionId());
            }
            case CloseSession c -> {
                out.u8(5);
                out.u64(c.sessionId());
            }
        }
        return out.buffer.toByteArray();
    }

    private static void mutations(Out out, List<Mutation> mutations) {
        out.u32(mutations.size());
        for (Mutation mutation : mutations) {
            switch (mutation) {
                case Put put -> {
                    out.u8(1);
                    out.bytes(put.key());
                    out.bytes(put.value());
                }
                case Delete delete -> {
                    out.u8(2);
                    out.bytes(delete.key());
                }
            }
        }
    }

    /**
     * @throws DecodeException if the bytes are not one command of this format version
     */
    public static Stamped decode(byte[] bytes) {
        In in = new In(bytes);
        int version = in.u8();
        if (version != VERSION) {
            throw new DecodeException("command format version " + version + "; this software reads version "
                    + VERSION);
        }
        long timestamp = in.u64();
        int type = in.u8();
        Command command = switch (type) {
            case 1 -> new Write(in.u64(), in.u64(), mutations(in));
            case 2 -> {
                long session = in.u64();
                long sequence = in.u64();
                int count = in.count(1 + 4 + 4 + 8);
                ConditionKind[] kinds = ConditionKind.values();
                List<Condition> conditions = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    int kind = in.u8();
                    if (kind >= kinds.length) {
                        throw new DecodeException("unknown condition kind " + kind);
                    }
                    conditions.add(new Condition(in.bytes(), kinds[kind], in.bytes(), in.u64()));
                }
                yield new Conditional(session, sequence, conditions, mutations(in), mutations(in));
            }
            case 3 -> new RegisterSession();
            case 4 -> new KeepAlive(in.u64());
            case 5 -> new CloseSession(in.u64());
            default -> throw new DecodeException("unknown command type " + type);
        };
        in.end();
        return new Stamped(timestamp, command);
    }

    private static List<Mutation> mutations(In in) {
        int count = in.count(1 + 4);
        List<Mutation> mutations = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int kind = in.u8();
            switch (kind) {
                case 1 -> mutations.add(new Put(in.bytes(), in.bytes()));
                case 2 -> mutations.add(new Delete(in.bytes()));
                default -> throw new DecodeException("unknown mutation kind " + kind);
            }
        }
        return mutations;
    }

    // ------------------------------------------------------------------ results

    public static byte[] encode(CommandResult result) {
        Out out = new Out();
        switch (result) {
            case Written _ -> out.u8(1);
            case ConditionalApplied c -> {
                out.u8(2);
                out.u8(c.conditionsHeld() ? 1 : 0);
            }
            case SessionRegistered s -> {
                out.u8(3);
                out.u64(s.sessionId());
            }
            case SessionOk _ -> out.u8(4);
            case Failed f -> {
                out.u8(5);
                out.u8(f.failure().ordinal());
            }
        }
        return out.buffer.toByteArray();
    }

    public static CommandResult decodeResult(byte[] bytes) {
        In in = new In(bytes);
        int kind = in.u8();
        CommandResult result = switch (kind) {
            case 1 -> new Written();
            case 2 -> {
                int held = in.u8();
                if (held > 1) {
                    throw new DecodeException("a boolean with the value " + held);
                }
                yield new ConditionalApplied(held == 1);
            }
            case 3 -> new SessionRegistered(in.u64());
            case 4 -> new SessionOk();
            case 5 -> {
                int failure = in.u8();
                Failure[] failures = Failure.values();
                if (failure >= failures.length) {
                    throw new DecodeException("unknown failure " + failure);
                }
                yield new Failed(failures[failure]);
            }
            default -> throw new DecodeException("unknown result kind " + kind);
        };
        in.end();
        return result;
    }

    // ------------------------------------------------------------------ bytes

    private static final class Out {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream(64);
        private final byte[] scratch = new byte[8];

        void u8(int value) {
            buffer.write(value);
        }

        void u32(int value) {
            LittleEndian.putInt32(scratch, 0, value);
            buffer.write(scratch, 0, 4);
        }

        void u64(long value) {
            LittleEndian.putUint64(scratch, 0, value);
            buffer.write(scratch, 0, 8);
        }

        void bytes(byte[] bytes) {
            u32(bytes.length);
            buffer.write(bytes, 0, bytes.length);
        }
    }

    private static final class In {
        private final byte[] bytes;
        private int at;

        In(byte[] bytes) {
            this.bytes = bytes;
        }

        private void need(int count) {
            if (count < 0 || count > bytes.length - at) {
                throw new DecodeException("the encoding ends early");
            }
        }

        int u8() {
            need(1);
            return bytes[at++] & 0xFF;
        }

        long u64() {
            need(8);
            long value = LittleEndian.getUint64(bytes, at);
            at += 8;
            return value;
        }

        /** A count of elements that each take at least {@code minimumSize} bytes. */
        int count(int minimumSize) {
            need(4);
            int count = LittleEndian.getInt32(bytes, at);
            at += 4;
            if (count < 0 || count > (bytes.length - at) / minimumSize) {
                throw new DecodeException("a list of " + count + " elements in " + (bytes.length - at) + " bytes");
            }
            return count;
        }

        byte[] bytes() {
            need(4);
            int length = LittleEndian.getInt32(bytes, at);
            at += 4;
            need(length);
            byte[] value = new byte[length];
            System.arraycopy(bytes, at, value, 0, length);
            at += length;
            return value;
        }

        void end() {
            if (at != bytes.length) {
                throw new DecodeException((bytes.length - at) + " bytes after the end");
            }
        }
    }
}
