package io.github.sz.freyja.node.wire;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.RaftMessage;
import io.github.sz.freyja.raft.RaftMessage.AppendEntries;
import io.github.sz.freyja.raft.RaftMessage.AppendResponse;
import io.github.sz.freyja.raft.RaftMessage.Heartbeat;
import io.github.sz.freyja.raft.RaftMessage.HeartbeatResponse;
import io.github.sz.freyja.raft.RaftMessage.RequestVote;
import io.github.sz.freyja.raft.RaftMessage.TimeoutNow;
import io.github.sz.freyja.raft.RaftMessage.VoteResponse;

/**
 * The encoding of the messages between the Raft cores of two nodes (docs/spec/node.md,
 * section 8). Every encoded message starts with a format version byte and ends with a
 * checksum over everything before it. All integers are little-endian.
 *
 * <pre>
 * version   uint8    1
 * type      uint8    1 RequestVote, 2 VoteResponse, 3 AppendEntries, 4 AppendResponse,
 *                    5 Heartbeat, 6 HeartbeatResponse, 7 TimeoutNow; the types from 32 on
 *                    belong to the transfer of snapshots ({@link TransferCodec})
 * from      uint32   the sending node
 * to        uint32   the node the message is meant for
 * term      uint64
 * body      ...      the remaining fields of the message, in the order of its record
 * checksum  uint32   CRC32C over everything before it
 * </pre>
 *
 * <p>A boolean is one byte, 0 or 1. An entry is index, term, type ({@code uint8}), payload
 * length ({@code uint32}), payload. A receiver rejects what it does not know: another
 * version, an unknown type, a wrong length, a wrong checksum.
 */
public final class MessageCodec {

    public static final int VERSION = 1;
    static final int HEAD = 1 + 1 + 4 + 4 + 8;
    static final int CHECKSUM = 4;

    /** A decoded message with its sender and its addressee. */
    public record Envelope(int from, int to, RaftMessage message) {
    }

    private MessageCodec() {
    }

    public static byte[] encode(int from, int to, RaftMessage message) {
        Writer w = new Writer();
        w.u8(VERSION);
        w.u8(switch (message) {
            case RequestVote _ -> 1;
            case VoteResponse _ -> 2;
            case AppendEntries _ -> 3;
            case AppendResponse _ -> 4;
            case Heartbeat _ -> 5;
            case HeartbeatResponse _ -> 6;
            case TimeoutNow _ -> 7;
        });
        w.u32(from);
        w.u32(to);
        w.u64(message.term());
        switch (message) {
            case RequestVote m -> {
                w.u64(m.lastLogIndex());
                w.u64(m.lastLogTerm());
                w.bool(m.preVote());
                w.bool(m.transfer());
            }
            case VoteResponse m -> {
                w.bool(m.preVote());
                w.bool(m.granted());
            }
            case AppendEntries m -> {
                w.u64(m.prevLogIndex());
                w.u64(m.prevLogTerm());
                w.u64(m.leaderCommit());
                w.u64(m.followerEpoch());
                w.u32(m.entries().size());
                for (LogEntry entry : m.entries()) {
                    w.u64(entry.index());
                    w.u64(entry.term());
                    w.u8(entry.type().ordinal());
                    w.u32(entry.payload().length);
                    w.bytes(entry.payload());
                }
            }
            case AppendResponse m -> {
                w.bool(m.success());
                w.u64(m.index());
                w.u64(m.hintIndex());
                w.u64(m.hintTerm());
                w.u64(m.logEpoch());
            }
            case Heartbeat m -> {
                w.u64(m.commit());
                w.u64(m.round());
            }
            case HeartbeatResponse m -> {
                w.u64(m.round());
                w.bool(m.recovering());
            }
            case TimeoutNow _ -> {
            }
        }
        byte[] body = w.out.toByteArray();
        byte[] frame = new byte[body.length + CHECKSUM];
        System.arraycopy(body, 0, frame, 0, body.length);
        LittleEndian.putInt32(frame, body.length, Checksums.crc32c(body, 0, body.length));
        return frame;
    }

    /**
     * The node a frame is meant for, without any check of the frame; -1 if the frame is too
     * short to say. For whoever routes frames without reading them.
     */
    public static int addressee(byte[] frame) {
        return frame.length < HEAD ? -1 : LittleEndian.getInt32(frame, 6);
    }

    /**
     * @throws DecodeException if the bytes are not one valid message of this format version
     */
    public static Envelope decode(byte[] frame) {
        if (frame.length < HEAD + CHECKSUM) {
            throw new DecodeException("a message of " + frame.length + " bytes is too short");
        }
        int end = frame.length - CHECKSUM;
        if (LittleEndian.getInt32(frame, end) != Checksums.crc32c(frame, 0, end)) {
            throw new DecodeException("message checksum mismatch");
        }
        Reader r = new Reader(frame, end);
        int version = r.u8();
        if (version != VERSION) {
            throw new DecodeException("message format version " + version + "; this software reads version "
                    + VERSION);
        }
        int type = r.u8();
        int from = r.u32();
        int to = r.u32();
        long term = r.u64();
        RaftMessage message = switch (type) {
            case 1 -> new RequestVote(term, r.u64(), r.u64(), r.bool(), r.bool());
            case 2 -> new VoteResponse(term, r.bool(), r.bool());
            case 3 -> {
                long prevIndex = r.u64();
                long prevTerm = r.u64();
                long commit = r.u64();
                long epoch = r.u64();
                int count = r.u32();
                if (count < 0 || count > r.remaining() / 21) {
                    throw new DecodeException("an append with " + count + " entries in " + r.remaining() + " bytes");
                }
                EntryType[] types = EntryType.values();
                List<LogEntry> entries = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    long index = r.u64();
                    long entryTerm = r.u64();
                    int entryType = r.u8();
                    int length = r.u32();
                    if (entryType >= types.length || index < 1 || entryTerm < 1) {
                        throw new DecodeException("an append with an invalid entry");
                    }
                    entries.add(new LogEntry(index, entryTerm, types[entryType], r.bytes(length)));
                }
                yield new AppendEntries(term, prevIndex, prevTerm, entries, commit, epoch);
            }
            case 4 -> new AppendResponse(term, r.bool(), r.u64(), r.u64(), r.u64(), r.u64());
            case 5 -> new Heartbeat(term, r.u64(), r.u64());
            case 6 -> new HeartbeatResponse(term, r.u64(), r.bool());
            case 7 -> new TimeoutNow(term);
            default -> throw new DecodeException("unknown message type " + type);
        };
        if (r.remaining() != 0) {
            throw new DecodeException(r.remaining() + " bytes after the end of a message of type " + type);
        }
        if (from < 0 || to < 0 || term < 0) {
            throw new DecodeException("a message with a negative node id or term");
        }
        return new Envelope(from, to, message);
    }

    static final class Writer {
        final ByteArrayOutputStream out = new ByteArrayOutputStream(64);
        private final byte[] scratch = new byte[8];

        void u8(int value) {
            out.write(value);
        }

        void bool(boolean value) {
            out.write(value ? 1 : 0);
        }

        void u32(int value) {
            LittleEndian.putInt32(scratch, 0, value);
            out.write(scratch, 0, 4);
        }

        void u64(long value) {
            LittleEndian.putUint64(scratch, 0, value);
            out.write(scratch, 0, 8);
        }

        void bytes(byte[] bytes) {
            out.write(bytes, 0, bytes.length);
        }
    }

    static final class Reader {
        private final byte[] bytes;
        private final int end;
        private int at;

        Reader(byte[] bytes, int end) {
            this.bytes = bytes;
            this.end = end;
        }

        int remaining() {
            return end - at;
        }

        private void need(int count) {
            if (count < 0 || count > end - at) {
                throw new DecodeException("a message that ends early");
            }
        }

        int u8() {
            need(1);
            return bytes[at++] & 0xFF;
        }

        boolean bool() {
            int value = u8();
            if (value > 1) {
                throw new DecodeException("a boolean with the value " + value);
            }
            return value == 1;
        }

        int u32() {
            need(4);
            int value = LittleEndian.getInt32(bytes, at);
            at += 4;
            return value;
        }

        long u64() {
            need(8);
            long value = LittleEndian.getUint64(bytes, at);
            at += 8;
            return value;
        }

        byte[] bytes(int length) {
            need(length);
            byte[] value = new byte[length];
            System.arraycopy(bytes, at, value, 0, length);
            at += length;
            return value;
        }
    }
}
