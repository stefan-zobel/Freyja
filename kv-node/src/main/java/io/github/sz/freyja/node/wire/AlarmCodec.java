package io.github.sz.freyja.node.wire;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.common.kv.Command.Alarm;
import io.github.sz.freyja.node.wire.MessageCodec.Reader;
import io.github.sz.freyja.node.wire.MessageCodec.Writer;

/**
 * The message with which a node asks the leader to set an alarm in the replicated state
 * (docs/spec/node.md, section 4.4). The frame is that of {@link MessageCodec}; the place of
 * the term is not used and holds 0.
 *
 * <pre>
 * version   uint8    1
 * type      uint8    40
 * from      uint32   the sending node
 * to        uint32   the node the message is meant for
 * unused    uint64   0
 * alarm     uint8    0 NOSPACE
 * checksum  uint32   CRC32C over everything before it
 * </pre>
 */
public final class AlarmCodec {

    public static final int TYPE = 40;

    /** A decoded request with its sender and its addressee. */
    public record Request(int from, int to, Alarm alarm) {
    }

    private AlarmCodec() {
    }

    /** Whether a frame claims to hold a request for an alarm. Says nothing about its validity. */
    public static boolean isAlarm(byte[] frame) {
        return frame.length > 1 && (frame[1] & 0xFF) == TYPE;
    }

    public static byte[] encode(int from, int to, Alarm alarm) {
        Writer w = new Writer();
        w.u8(MessageCodec.VERSION);
        w.u8(TYPE);
        w.u32(from);
        w.u32(to);
        w.u64(0);
        w.u8(alarm.ordinal());
        byte[] body = w.out.toByteArray();
        byte[] frame = new byte[body.length + MessageCodec.CHECKSUM];
        System.arraycopy(body, 0, frame, 0, body.length);
        LittleEndian.putInt32(frame, body.length, Checksums.crc32c(body, 0, body.length));
        return frame;
    }

    /**
     * @throws DecodeException if the bytes are not one valid request of this format version
     */
    public static Request decode(byte[] frame) {
        if (frame.length != MessageCodec.HEAD + 1 + MessageCodec.CHECKSUM) {
            throw new DecodeException("a request for an alarm of " + frame.length + " bytes");
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
        long unused = r.u64();
        int alarm = r.u8();
        Alarm[] alarms = Alarm.values();
        if (type != TYPE || from < 0 || to < 0 || unused != 0 || alarm >= alarms.length) {
            throw new DecodeException("an invalid request for an alarm");
        }
        return new Request(from, to, alarms[alarm]);
    }
}
