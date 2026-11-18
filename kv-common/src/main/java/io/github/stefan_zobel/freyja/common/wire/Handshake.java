package io.github.stefan_zobel.freyja.common.wire;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import io.github.stefan_zobel.freyja.common.Checksums;
import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.LittleEndian;

/**
 * The first frame on every connection, in both directions (docs/spec/node.md, section 8):
 * who is speaking, and in which protocol. The side that connects speaks first.
 *
 * <pre>
 * magic            uint32   "FRYA"
 * protocol         uint8    1
 * role             uint8    1 peer, 2 client, 3 administrator
 * cluster id       uint64   0 from a client or an administrator that does not know it
 * node id          uint32   of a peer and of the node that answers; all bits set otherwise
 * software         uint8 + bytes   the software version, UTF-8
 * token            uint8 + bytes   what a client without a certificate proves itself with,
 *                                  UTF-8; empty from everybody else
 * checksum         uint32   CRC32C over everything before it
 * </pre>
 *
 * @param role            see {@link #PEER}, {@link #CLIENT}, {@link #ADMIN}
 * @param clusterId       the cluster of the speaker; 0 if it does not know one
 * @param nodeId          the id of the node that speaks; -1 for a client or an administrator
 * @param softwareVersion the version of the software that speaks
 * @param token           the token of a client that has no certificate; empty otherwise
 */
public record Handshake(int role, long clusterId, int nodeId, String softwareVersion, String token) {

    public static final int PROTOCOL = 1;
    public static final int PEER = 1;
    public static final int CLIENT = 2;
    public static final int ADMIN = 3;
    /** The bytes "FRYA" read as a little-endian integer. */
    private static final int MAGIC = 0x41595246;
    /** A handshake is never longer than this. */
    public static final int MAX_BYTES = 4 + 1 + 1 + 8 + 4 + 1 + 255 + 1 + 255 + 4;

    public Handshake {
        Objects.requireNonNull(softwareVersion, "softwareVersion");
        if (role < PEER || role > ADMIN) {
            throw new IllegalArgumentException("role " + role);
        }
        if (softwareVersion.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("a software version of more than 255 bytes");
        }
        Objects.requireNonNull(token, "token");
        if (token.getBytes(StandardCharsets.UTF_8).length > 255) {
            throw new IllegalArgumentException("a token of more than 255 bytes");
        }
    }

    /** A handshake without a token. */
    public Handshake(int role, long clusterId, int nodeId, String softwareVersion) {
        this(role, clusterId, nodeId, softwareVersion, "");
    }

    /** The token is a secret and is not printed. */
    @Override
    public String toString() {
        return "Handshake[role=" + role + ", clusterId=" + Long.toHexString(clusterId) + ", nodeId=" + nodeId
                + ", softwareVersion=" + softwareVersion + (token.isEmpty() ? "" : ", with a token") + "]";
    }

    public byte[] encode() {
        byte[] software = softwareVersion.getBytes(StandardCharsets.UTF_8);
        byte[] secret = token.getBytes(StandardCharsets.UTF_8);
        byte[] frame = new byte[19 + software.length + 1 + secret.length + 4];
        LittleEndian.putInt32(frame, 0, MAGIC);
        frame[4] = (byte) PROTOCOL;
        frame[5] = (byte) role;
        LittleEndian.putUint64(frame, 6, clusterId);
        LittleEndian.putInt32(frame, 14, nodeId);
        frame[18] = (byte) software.length;
        System.arraycopy(software, 0, frame, 19, software.length);
        frame[19 + software.length] = (byte) secret.length;
        System.arraycopy(secret, 0, frame, 20 + software.length, secret.length);
        LittleEndian.putInt32(frame, frame.length - 4, Checksums.crc32c(frame, 0, frame.length - 4));
        return frame;
    }

    /**
     * @throws DecodeException if the bytes are not a handshake of a protocol this software
     *                         speaks
     */
    public static Handshake decode(byte[] frame) {
        if (frame.length < 19 + 1 + 4 || LittleEndian.getInt32(frame, 0) != MAGIC) {
            throw new DecodeException("not a handshake");
        }
        if (LittleEndian.getInt32(frame, frame.length - 4) != Checksums.crc32c(frame, 0, frame.length - 4)) {
            throw new DecodeException("handshake checksum mismatch");
        }
        int protocol = frame[4] & 0xFF;
        if (protocol != PROTOCOL) {
            throw new DecodeException("protocol " + protocol + "; this software speaks protocol " + PROTOCOL);
        }
        int role = frame[5] & 0xFF;
        int length = frame[18] & 0xFF;
        if (role < PEER || role > ADMIN || frame.length < 19 + length + 1 + 4) {
            throw new DecodeException("an invalid handshake");
        }
        int secret = frame[19 + length] & 0xFF;
        if (frame.length != 19 + length + 1 + secret + 4) {
            throw new DecodeException("an invalid handshake");
        }
        return new Handshake(role, LittleEndian.getUint64(frame, 6), LittleEndian.getInt32(frame, 14),
                new String(frame, 19, length, StandardCharsets.UTF_8),
                new String(frame, 20 + length, secret, StandardCharsets.UTF_8));
    }
}
