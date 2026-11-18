package io.github.stefan_zobel.freyja.node.wire;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import io.github.stefan_zobel.freyja.common.Checksums;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.LittleEndian;
import io.github.stefan_zobel.freyja.node.wire.MessageCodec.Reader;
import io.github.stefan_zobel.freyja.node.wire.MessageCodec.Writer;
import io.github.stefan_zobel.freyja.node.wire.TransferMessage.Abort;
import io.github.stefan_zobel.freyja.node.wire.TransferMessage.Chunk;
import io.github.stefan_zobel.freyja.node.wire.TransferMessage.Done;
import io.github.stefan_zobel.freyja.node.wire.TransferMessage.Fetch;
import io.github.stefan_zobel.freyja.node.wire.TransferMessage.FileInfo;
import io.github.stefan_zobel.freyja.node.wire.TransferMessage.Offer;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;

/**
 * The encoding of the messages of a snapshot transfer. The frame is that of
 * {@link MessageCodec}, with the transfer id in the place of the term:
 *
 * <pre>
 * version      uint8    1
 * type         uint8    32 Offer, 33 Fetch, 34 Chunk, 35 Done, 36 Abort
 * from         uint32   the sending node
 * to           uint32   the node the message is meant for
 * transfer id  uint64
 * body         ...
 * checksum     uint32   CRC32C over everything before it
 * </pre>
 *
 * <ul>
 *   <li>Offer: leader term, last included index, last included term ({@code uint64} each),
 *       cluster version ({@code uint32}), length of the encoded configuration
 *       ({@code uint32}) and the configuration, number of files ({@code uint32}), and per
 *       file the length of its name ({@code uint32}), the name in UTF-8, and its size
 *       ({@code uint64}).</li>
 *   <li>Fetch: file number ({@code uint32}), offset ({@code uint64}).</li>
 *   <li>Chunk: file number, offset, number of bytes ({@code uint32}), the bytes.</li>
 *   <li>Done: accepted (one byte, 0 or 1).</li>
 *   <li>Abort: nothing.</li>
 * </ul>
 */
public final class TransferCodec {

    /** Message types from this number on are transfer messages. */
    public static final int FIRST_TYPE = 32;

    /** A decoded message with its sender and its addressee. */
    public record Envelope(int from, int to, TransferMessage message) {
    }

    private TransferCodec() {
    }

    /** Whether a frame claims to hold a transfer message. Says nothing about its validity. */
    public static boolean isTransfer(byte[] frame) {
        return frame.length > 1 && (frame[1] & 0xFF) >= FIRST_TYPE && (frame[1] & 0xFF) < AlarmCodec.TYPE;
    }

    public static byte[] encode(int from, int to, TransferMessage message) {
        Writer w = new Writer();
        w.u8(MessageCodec.VERSION);
        w.u8(switch (message) {
            case Offer _ -> 32;
            case Fetch _ -> 33;
            case Chunk _ -> 34;
            case Done _ -> 35;
            case Abort _ -> 36;
        });
        w.u32(from);
        w.u32(to);
        w.u64(message.transferId());
        switch (message) {
            case Offer m -> {
                w.u64(m.leaderTerm());
                w.u64(m.meta().lastIncludedIndex());
                w.u64(m.meta().lastIncludedTerm());
                w.u32(m.clusterVersion());
                byte[] config = m.meta().config().encode();
                w.u32(config.length);
                w.bytes(config);
                w.u32(m.files().size());
                for (FileInfo file : m.files()) {
                    byte[] name = file.name().getBytes(StandardCharsets.UTF_8);
                    w.u32(name.length);
                    w.bytes(name);
                    w.u64(file.size());
                }
            }
            case Fetch m -> {
                w.u32(m.file());
                w.u64(m.offset());
            }
            case Chunk m -> {
                w.u32(m.file());
                w.u64(m.offset());
                w.u32(m.data().length);
                w.bytes(m.data());
            }
            case Done m -> w.bool(m.accepted());
            case Abort _ -> {
            }
        }
        byte[] body = w.out.toByteArray();
        byte[] frame = new byte[body.length + MessageCodec.CHECKSUM];
        System.arraycopy(body, 0, frame, 0, body.length);
        LittleEndian.putInt32(frame, body.length, Checksums.crc32c(body, 0, body.length));
        return frame;
    }

    /**
     * @throws DecodeException if the bytes are not one valid transfer message of this format
     *                         version
     */
    public static Envelope decode(byte[] frame) {
        if (frame.length < MessageCodec.HEAD + MessageCodec.CHECKSUM) {
            throw new DecodeException("a message of " + frame.length + " bytes is too short");
        }
        int end = frame.length - MessageCodec.CHECKSUM;
        if (LittleEndian.getInt32(frame, end) != Checksums.crc32c(frame, 0, end)) {
            throw new DecodeException("message checksum mismatch");
        }
        Reader r = new Reader(frame, end);
        int version = r.u8();
        if (version != MessageCodec.VERSION) {
            throw new DecodeException("message format version " + version + "; this software reads version "
                    + MessageCodec.VERSION);
        }
        int type = r.u8();
        int from = r.u32();
        int to = r.u32();
        long id = r.u64();
        TransferMessage message = switch (type) {
            case 32 -> {
                long leaderTerm = r.u64();
                long index = r.u64();
                long term = r.u64();
                int clusterVersion = r.u32();
                ClusterConfig config;
                try {
                    config = ClusterConfig.decode(r.bytes(r.u32()));
                } catch (DecodeException e) {
                    throw e;
                } catch (RuntimeException e) {
                    throw new DecodeException("an offer with an invalid configuration");
                }
                int count = r.u32();
                if (count < 0 || count > r.remaining() / 12 || leaderTerm < 0 || index < 0 || term < 0) {
                    throw new DecodeException("an invalid offer");
                }
                List<FileInfo> files = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    String name = new String(r.bytes(r.u32()), StandardCharsets.UTF_8);
                    long size = r.u64();
                    if (size < 0) {
                        throw new DecodeException("a file of negative size");
                    }
                    files.add(new FileInfo(name, size));
                }
                yield new Offer(id, leaderTerm, new SnapshotMeta(index, term, config), clusterVersion, files);
            }
            case 33 -> new Fetch(id, r.u32(), r.u64());
            case 34 -> new Chunk(id, r.u32(), r.u64(), r.bytes(r.u32()));
            case 35 -> new Done(id, r.bool());
            case 36 -> new Abort(id);
            default -> throw new DecodeException("unknown message type " + type);
        };
        if (r.remaining() != 0) {
            throw new DecodeException(r.remaining() + " bytes after the end of a message of type " + type);
        }
        if (from < 0 || to < 0) {
            throw new DecodeException("a message with a negative node id");
        }
        if (message instanceof Fetch f && (f.file() < 0 || f.offset() < 0)
                || message instanceof Chunk c && (c.file() < 0 || c.offset() < 0)) {
            throw new DecodeException("a negative file number or offset");
        }
        return new Envelope(from, to, message);
    }
}
