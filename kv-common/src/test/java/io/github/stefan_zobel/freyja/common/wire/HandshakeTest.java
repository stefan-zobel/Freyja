package io.github.stefan_zobel.freyja.common.wire;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.DecodeException;

/** The first frame of a connection (docs/spec/node.md, section 8). */
class HandshakeTest {

    @Test
    void aHandshakeIsReadAsItWasWritten() {
        Handshake peer = new Handshake(Handshake.PEER, 0xABCDEFL, 3, "1.0");
        assertEquals(peer, Handshake.decode(peer.encode()));
        assertEquals("", peer.token());
        Handshake client = new Handshake(Handshake.CLIENT, 0, -1, "1.0", "a token of sixteen bytes or more");
        assertEquals(client, Handshake.decode(client.encode()));
        Handshake longest = new Handshake(Handshake.ADMIN, -1L, -1, "v".repeat(255), "t".repeat(255));
        assertEquals(longest, Handshake.decode(longest.encode()));
        assertEquals(Handshake.MAX_BYTES, longest.encode().length);
    }

    @Test
    void theBytesAreTheOnesTheSpecificationNames() {
        byte[] frame = new Handshake(Handshake.CLIENT, 0x0102030405060708L, 0x0A0B0C0D, "ab", "xyz").encode();
        assertArrayEquals(new byte[] {'F', 'R', 'Y', 'A', 1, 2, 8, 7, 6, 5, 4, 3, 2, 1, 0x0D, 0x0C, 0x0B, 0x0A, 2,
                'a', 'b', 3, 'x', 'y', 'z'}, Arrays.copyOf(frame, frame.length - 4));
    }

    @Test
    void whatIsNotAHandshakeIsRejected() {
        byte[] good = new Handshake(Handshake.CLIENT, 7, -1, "1.0", "a token").encode();
        for (int i = 0; i < good.length; i++) {
            byte[] damaged = good.clone();
            damaged[i] ^= 0x10;
            assertThrows(DecodeException.class, () -> Handshake.decode(damaged), "byte " + i);
        }
        for (int length = 0; length < good.length; length++) {
            byte[] cut = Arrays.copyOf(good, length);
            assertThrows(DecodeException.class, () -> Handshake.decode(cut), "length " + length);
        }
        assertThrows(DecodeException.class, () -> Handshake.decode(Arrays.copyOf(good, good.length + 1)));
        assertThrows(IllegalArgumentException.class, () -> new Handshake(Handshake.CLIENT, 0, -1, "1", "t".repeat(256)));
        assertThrows(IllegalArgumentException.class, () -> new Handshake(0, 0, -1, "1"));
    }

    @Test
    void theTokenIsNotPrinted() {
        String printed = new Handshake(Handshake.CLIENT, 7, -1, "1.0", "a secret token").toString();
        assertFalse(printed.contains("secret"));
        assertTrue(printed.contains("with a token"));
    }
}
