package io.github.sz.freyja.common.kv;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.common.kv.ClientRequest.Execute;
import io.github.sz.freyja.common.kv.ClientRequest.Get;
import io.github.sz.freyja.common.kv.ClientRequest.Scan;
import io.github.sz.freyja.common.kv.ClientResponse.NotLeader;
import io.github.sz.freyja.common.kv.ClientResponse.Pair;
import io.github.sz.freyja.common.kv.ClientResponse.Pairs;
import io.github.sz.freyja.common.kv.ClientResponse.Reason;
import io.github.sz.freyja.common.kv.ClientResponse.Rejected;
import io.github.sz.freyja.common.kv.ClientResponse.Result;
import io.github.sz.freyja.common.kv.ClientResponse.Value;
import io.github.sz.freyja.common.kv.CommandCodec.In;
import io.github.sz.freyja.common.kv.CommandCodec.Out;

/**
 * The encoding of the messages between a client and a node (docs/spec/node.md, section 8).
 * Every message starts with a format version byte and ends with a checksum over everything
 * before it. All integers are little-endian; a boolean is one byte, 0 or 1; a byte string is
 * a length ({@code uint32}) and its bytes.
 *
 * <pre>
 * request:   version uint8 (1), type uint8, request id uint64, timeout uint32 (milliseconds),
 *            body, CRC32C uint32
 * response:  version uint8 (1), type uint8, request id uint64, body, CRC32C uint32
 * </pre>
 *
 * <ul>
 *   <li>1 Execute: the command as a byte string, in the encoding of the log with the time
 *       stamp 0 (the node that proposes it sets the time stamp).</li>
 *   <li>2 Get: key, stale.</li>
 *   <li>3 Scan: start, end, limit ({@code uint32}), reverse, stale.</li>
 *   <li>65 Result: index ({@code uint64}), the encoded result as a byte string.</li>
 *   <li>66 Value: found, value, mod index, applied index ({@code uint64} each).</li>
 *   <li>67 Pairs: applied index, number of pairs ({@code uint32}), and per pair key, value,
 *       mod index.</li>
 *   <li>68 NotLeader: leader id ({@code uint32}; all bits set for none), its address in
 *       UTF-8 as a byte string.</li>
 *   <li>69 Rejected: reason ({@code uint8}, in the order of {@link Reason}).</li>
 * </ul>
 *
 * <p>The response to a request carries its request id; a client may have several requests on
 * one connection.
 */
public final class ClientCodec {

    public static final int VERSION = 1;
    private static final int CHECKSUM = 4;

    /** A decoded request with its id and the time its sender allows for it. */
    public record RequestEnvelope(long requestId, int timeoutMillis, ClientRequest request) {
    }

    /** A decoded response with the id of its request. */
    public record ResponseEnvelope(long requestId, ClientResponse response) {
    }

    private ClientCodec() {
    }

    // ------------------------------------------------------------------ requests

    public static byte[] encode(long requestId, int timeoutMillis, ClientRequest request) {
        if (timeoutMillis < 0) {
            throw new IllegalArgumentException("timeout " + timeoutMillis);
        }
        Out out = new Out();
        out.u8(VERSION);
        out.u8(switch (request) {
            case Execute _ -> 1;
            case Get _ -> 2;
            case Scan _ -> 3;
        });
        out.u64(requestId);
        out.u32(timeoutMillis);
        switch (request) {
            case Execute e -> out.bytes(CommandCodec.encode(0, e.command()));
            case Get g -> {
                out.bytes(g.key());
                out.u8(g.stale() ? 1 : 0);
            }
            case Scan s -> {
                out.bytes(s.start());
                out.bytes(s.end());
                out.u32(s.limit());
                out.u8(s.reverse() ? 1 : 0);
                out.u8(s.stale() ? 1 : 0);
            }
        }
        return seal(out);
    }

    /**
     * @throws DecodeException if the bytes are not one valid request of this format version
     */
    public static RequestEnvelope decodeRequest(byte[] frame) {
        In in = open(frame);
        int type = in.u8();
        long requestId = in.u64();
        int timeout = in.u32();
        if (timeout < 0) {
            throw new DecodeException("a negative timeout");
        }
        ClientRequest request = switch (type) {
            case 1 -> new Execute(CommandCodec.decode(in.bytes()).command());
            case 2 -> new Get(in.bytes(), bool(in));
            case 3 -> {
                byte[] start = in.bytes();
                byte[] end = in.bytes();
                int limit = in.u32();
                if (limit < 1) {
                    throw new DecodeException("a scan with the limit " + limit);
                }
                yield new Scan(start, end, limit, bool(in), bool(in));
            }
            default -> throw new DecodeException("unknown request type " + type);
        };
        in.end();
        return new RequestEnvelope(requestId, timeout, request);
    }

    // ------------------------------------------------------------------ responses

    public static byte[] encode(long requestId, ClientResponse response) {
        Out out = new Out();
        out.u8(VERSION);
        out.u8(switch (response) {
            case Result _ -> 65;
            case Value _ -> 66;
            case Pairs _ -> 67;
            case NotLeader _ -> 68;
            case Rejected _ -> 69;
        });
        out.u64(requestId);
        switch (response) {
            case Result r -> {
                out.u64(r.index());
                out.bytes(CommandCodec.encode(r.result()));
            }
            case Value v -> {
                out.u8(v.found() ? 1 : 0);
                out.bytes(v.value());
                out.u64(v.modIndex());
                out.u64(v.appliedIndex());
            }
            case Pairs p -> {
                out.u64(p.appliedIndex());
                out.u32(p.pairs().size());
                for (Pair pair : p.pairs()) {
                    out.bytes(pair.key());
                    out.bytes(pair.value());
                    out.u64(pair.modIndex());
                }
            }
            case NotLeader n -> {
                out.u32(n.leaderId());
                out.bytes(n.leaderAddress().getBytes(StandardCharsets.UTF_8));
            }
            case Rejected r -> out.u8(r.reason().ordinal());
        }
        return seal(out);
    }

    /**
     * @throws DecodeException if the bytes are not one valid response of this format version
     */
    public static ResponseEnvelope decodeResponse(byte[] frame) {
        In in = open(frame);
        int type = in.u8();
        long requestId = in.u64();
        ClientResponse response = switch (type) {
            case 65 -> {
                long index = in.u64();
                yield new Result(CommandCodec.decodeResult(in.bytes()), index);
            }
            case 66 -> new Value(bool(in), in.bytes(), in.u64(), in.u64());
            case 67 -> {
                long applied = in.u64();
                int count = in.count(4 + 4 + 8);
                List<Pair> pairs = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    pairs.add(new Pair(in.bytes(), in.bytes(), in.u64()));
                }
                yield new Pairs(pairs, applied);
            }
            case 68 -> new NotLeader(in.u32(), new String(in.bytes(), StandardCharsets.UTF_8));
            case 69 -> {
                int reason = in.u8();
                Reason[] reasons = Reason.values();
                if (reason >= reasons.length) {
                    throw new DecodeException("unknown reason " + reason);
                }
                yield new Rejected(reasons[reason]);
            }
            default -> throw new DecodeException("unknown response type " + type);
        };
        in.end();
        return new ResponseEnvelope(requestId, response);
    }

    // ------------------------------------------------------------------ frame

    private static byte[] seal(Out out) {
        byte[] body = out.buffer.toByteArray();
        byte[] frame = new byte[body.length + CHECKSUM];
        System.arraycopy(body, 0, frame, 0, body.length);
        LittleEndian.putInt32(frame, body.length, Checksums.crc32c(body, 0, body.length));
        return frame;
    }

    /** Checks the checksum and the version, and returns a reader behind the version byte. */
    private static In open(byte[] frame) {
        if (frame.length < 1 + 1 + 8 + CHECKSUM) {
            throw new DecodeException("a message of " + frame.length + " bytes is too short");
        }
        int end = frame.length - CHECKSUM;
        if (LittleEndian.getInt32(frame, end) != Checksums.crc32c(frame, 0, end)) {
            throw new DecodeException("message checksum mismatch");
        }
        byte[] body = new byte[end];
        System.arraycopy(frame, 0, body, 0, end);
        In in = new In(body);
        int version = in.u8();
        if (version != VERSION) {
            throw new DecodeException("client protocol version " + version + "; this software speaks version "
                    + VERSION);
        }
        return in;
    }

    private static boolean bool(In in) {
        int value = in.u8();
        if (value > 1) {
            throw new DecodeException("a boolean with the value " + value);
        }
        return value == 1;
    }
}
