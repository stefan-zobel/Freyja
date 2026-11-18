package io.github.sz.freyja.node.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Test;

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

/** The message encoding (docs/spec/node.md, section 8). */
class MessageCodecTest {

    private static List<RaftMessage> samples() {
        byte[] big = new byte[70_000];
        new Random(3).nextBytes(big);
        List<RaftMessage> messages = new ArrayList<>();
        messages.add(new RequestVote(7, 123, 6, false, true));
        messages.add(new RequestVote(Long.MAX_VALUE, 0, 0, true, false));
        messages.add(new VoteResponse(7, true, false));
        messages.add(new VoteResponse(8, false, true));
        messages.add(new AppendEntries(9, 10, 8, List.of(), 5, -1));
        messages.add(new AppendEntries(9, 10, 8, List.of(
                new LogEntry(11, 9, EntryType.NOOP, new byte[0]),
                new LogEntry(12, 9, EntryType.COMMAND, big),
                new LogEntry(13, 9, EntryType.CONFIG, new byte[] {1, 2, 3})), 12, 4));
        messages.add(new AppendResponse(9, true, 13, 0, 0, 2));
        messages.add(new AppendResponse(9, false, 13, 7, 3, 0));
        messages.add(new Heartbeat(9, 12, 99));
        messages.add(new HeartbeatResponse(9, 99, true));
        messages.add(new HeartbeatResponse(9, 100, false));
        messages.add(new TimeoutNow(9));
        return messages;
    }

    /** Entries compare their payload by identity; this compares the content. */
    private static void assertSame(RaftMessage expected, RaftMessage actual) {
        if (expected instanceof AppendEntries a && actual instanceof AppendEntries b) {
            assertEquals(a.term(), b.term());
            assertEquals(a.prevLogIndex(), b.prevLogIndex());
            assertEquals(a.prevLogTerm(), b.prevLogTerm());
            assertEquals(a.leaderCommit(), b.leaderCommit());
            assertEquals(a.followerEpoch(), b.followerEpoch());
            assertEquals(a.entries().size(), b.entries().size());
            for (int i = 0; i < a.entries().size(); i++) {
                assertEquals(a.entries().get(i).index(), b.entries().get(i).index());
                assertEquals(a.entries().get(i).term(), b.entries().get(i).term());
                assertEquals(a.entries().get(i).type(), b.entries().get(i).type());
                assertArrayEquals(a.entries().get(i).payload(), b.entries().get(i).payload());
            }
        } else {
            assertEquals(expected, actual);
        }
    }

    @Test
    void everyMessageSurvivesEncoding() {
        for (RaftMessage message : samples()) {
            byte[] frame = MessageCodec.encode(3, 4, message);
            assertEquals(MessageCodec.VERSION, frame[0]);
            MessageCodec.Envelope envelope = MessageCodec.decode(frame);
            assertEquals(3, envelope.from());
            assertEquals(4, envelope.to());
            assertSame(message, envelope.message());
            assertArrayEquals(frame, MessageCodec.encode(3, 4, envelope.message()), "one encoding per message");
        }
    }

    @Test
    void damagedTruncatedAndExtendedMessagesAreRejected() {
        Random random = new Random(11);
        for (RaftMessage message : samples()) {
            byte[] frame = MessageCodec.encode(1, 2, message);
            // Every single bit of a short message, a sample of the bits of a long one.
            int bits = frame.length * 8;
            for (int n = 0; n < Math.min(bits, 2000); n++) {
                int bit = bits <= 2000 ? n : random.nextInt(bits);
                byte[] damaged = frame.clone();
                damaged[bit / 8] ^= (byte) (1 << (bit % 8));
                assertThrows(DecodeException.class, () -> MessageCodec.decode(damaged), "bit " + bit);
            }
            for (int length = 0; length < Math.min(frame.length, 60); length++) {
                byte[] cut = java.util.Arrays.copyOf(frame, length);
                assertThrows(DecodeException.class, () -> MessageCodec.decode(cut));
            }
            byte[] longer = java.util.Arrays.copyOf(frame, frame.length + 1);
            assertThrows(DecodeException.class, () -> MessageCodec.decode(longer));
        }
    }

    /** Puts a valid checksum on changed bytes, as a sender of another version would. */
    private static byte[] resealed(byte[] frame) {
        byte[] copy = frame.clone();
        LittleEndian.putInt32(copy, copy.length - 4, Checksums.crc32c(copy, 0, copy.length - 4));
        return copy;
    }

    @Test
    void whatIsNotKnownIsRejectedAlsoWithAValidChecksum() {
        byte[] frame = MessageCodec.encode(1, 2, new Heartbeat(5, 6, 7));
        byte[] otherVersion = frame.clone();
        otherVersion[0] = 2;
        DecodeException version = assertThrows(DecodeException.class,
                () -> MessageCodec.decode(resealed(otherVersion)));
        assertTrue(version.getMessage().contains("version 2"), version.getMessage());

        byte[] otherType = frame.clone();
        otherType[1] = 42;
        assertThrows(DecodeException.class, () -> MessageCodec.decode(resealed(otherType)));

        // A body that is too long for its type.
        byte[] padded = java.util.Arrays.copyOf(frame, frame.length + 3);
        assertThrows(DecodeException.class, () -> MessageCodec.decode(resealed(padded)));

        // A boolean that is neither 0 nor 1.
        byte[] vote = MessageCodec.encode(1, 2, new VoteResponse(5, true, true));
        vote[18] = 2;
        assertThrows(DecodeException.class, () -> MessageCodec.decode(resealed(vote)));

        // An entry count that cannot fit.
        byte[] append = MessageCodec.encode(1, 2, new AppendEntries(5, 1, 1, List.of(), 0, 0));
        LittleEndian.putInt32(append, append.length - 8, 1_000_000);
        assertThrows(DecodeException.class, () -> MessageCodec.decode(resealed(append)));
    }
}
