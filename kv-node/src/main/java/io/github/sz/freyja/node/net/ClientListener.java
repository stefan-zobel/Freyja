package io.github.sz.freyja.node.net;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.wire.Frames;
import io.github.sz.freyja.common.wire.Handshake;
import io.github.sz.freyja.common.kv.ClientCodec;
import io.github.sz.freyja.common.kv.ClientResponse;
import io.github.sz.freyja.common.tls.Tls;
import io.github.sz.freyja.node.concurrent.Scopes;
import io.github.sz.freyja.node.service.ClientService;

/**
 * The listener for clients (docs/spec/node.md, section 8): after the handshake, every frame
 * of a connection is a request, and every answer goes back on the same connection.
 *
 * <p>Two virtual threads serve a connection. One reads requests and hands them on. The other
 * writes answers, which arrive from the thread of the node through a bounded queue; a client
 * that does not read its answers fills that queue and loses its connection, and the node
 * never waits for it.
 *
 * <p>With TLS, a connection must prove itself before its handshake is answered: by a
 * certificate, or, where the listener takes tokens, by a token in its handshake
 * (docs/spec/operations.md, section 5).
 */
public final class ClientListener implements Closeable {

    /** Takes the requests of clients. */
    @FunctionalInterface
    public interface Sink {
        /**
         * @return {@code false} if the request could not be taken because the node has too
         *         much to do
         */
        boolean submit(byte[] request, ClientService.Reply reply);
    }

    /**
     * @param maxFrameBytes          the largest request that is accepted
     * @param answerQueue            how many answers may wait for one connection
     * @param handshakeTimeoutMillis how long a client may take for its handshake
     */
    public record Options(int maxFrameBytes, int answerQueue, int handshakeTimeoutMillis) {
        public Options {
            if (maxFrameBytes < 1024 || answerQueue < 1 || handshakeTimeoutMillis < 1) {
                throw new IllegalArgumentException("invalid listener settings");
            }
        }

        /** The defaults: requests up to 8 MiB, 1,024 waiting answers, 5 s for the handshake. */
        public static Options defaults() {
            return new Options(8 << 20, 1024, 5_000);
        }
    }

    /** Marks the end of the answers of a connection. */
    private static final byte[] END = new byte[0];

    private final long clusterId;
    private final int nodeId;
    private final String software;
    private final int role;
    private final Options options;
    private final Sink sink;
    private final Tls tls;
    private final Tls.ClientCertificate certificate;
    private final TokenFile tokens;
    private final AtomicLong connections = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();
    private ServerSocket server;
    private Thread acceptor;

    /**
     * @param role the role a connection must name in its handshake: {@link Handshake#CLIENT}
     *             or {@link Handshake#ADMIN}
     */
    public ClientListener(long clusterId, int nodeId, String software, int role, Options options, Sink sink) {
        this(clusterId, nodeId, software, role, options, sink, null, Tls.ClientCertificate.NONE, null);
    }

    /**
     * @param role        the role a connection must name in its handshake
     * @param tls         TLS of this listener; {@code null} for none, and then nobody has to
     *                    prove anything
     * @param certificate whether a connection is asked for a certificate
     * @param tokens      the tokens that are taken in place of a certificate; {@code null}
     *                    for none
     */
    public ClientListener(long clusterId, int nodeId, String software, int role, Options options, Sink sink,
                          Tls tls, Tls.ClientCertificate certificate, TokenFile tokens) {
        if (tls != null && certificate != Tls.ClientCertificate.NEEDED && tokens == null) {
            throw new IllegalArgumentException("a listener with TLS that asks for no proof");
        }
        this.tls = tls;
        this.certificate = certificate;
        this.tokens = tokens;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.software = software;
        this.role = role;
        this.options = options;
        this.sink = sink;
    }

    /**
     * Starts to accept connections.
     *
     * @param bind the address to listen on; port 0 for any free port
     * @return the port that was bound
     */
    public synchronized int listen(InetSocketAddress bind) throws IOException {
        if (server != null) {
            throw new IllegalStateException("the listener listens already");
        }
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(bind);
        ServerSocket listening = server;
        acceptor = Thread.ofVirtual().name("freyja-listener-" + role + "-" + nodeId).start(() -> {
            try {
                Scopes.serve(listening, this::accepted);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return server.getLocalPort();
    }

    private void accepted(Socket raw) throws Exception {
        raw.setTcpNoDelay(true);
        raw.setSoTimeout(options.handshakeTimeoutMillis());
        Socket socket = raw;
        boolean certified = false;
        if (tls != null) {
            try {
                javax.net.ssl.SSLSocket secured = tls.accept(raw, certificate);
                certified = Tls.certified(secured);
                socket = secured;
            } catch (IOException e) {
                refused.incrementAndGet();
                return;
            }
        }
        InputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
        byte[] first = Frames.read(in, Handshake.MAX_BYTES);
        Handshake other;
        try {
            other = first == null ? null : Handshake.decode(first);
        } catch (DecodeException e) {
            other = null;
        }
        // A client need not know the cluster; one that names a cluster must name this one.
        if (other == null || other.role() != role || (other.clusterId() != 0 && other.clusterId() != clusterId)) {
            refused.incrementAndGet();
            return;
        }
        if (tls != null && !certified && (tokens == null || !tokens.accepts(other.token()))) {
            refused.incrementAndGet();
            return; // neither a certificate nor a token that is known
        }
        OutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
        Frames.write(out, new Handshake(role, clusterId, nodeId, software).encode());
        out.flush();
        raw.setSoTimeout(0);
        connections.incrementAndGet();

        ArrayBlockingQueue<byte[]> answers = new ArrayBlockingQueue<>(options.answerQueue());
        ClientService.Reply reply = frame -> {
            if (!answers.offer(frame)) {
                // The client does not read. It loses its connection; the node does not wait.
                try {
                    raw.close();
                } catch (IOException e) {
                    // closed already
                }
            }
        };
        Scopes.either(() -> {
            for (byte[] frame = Frames.read(in, options.maxFrameBytes()); frame != null;
                    frame = Frames.read(in, options.maxFrameBytes())) {
                if (!sink.submit(frame, reply)) {
                    try {
                        reply.send(ClientCodec.encode(ClientCodec.decodeRequest(frame).requestId(),
                                new ClientResponse.Rejected(ClientResponse.Reason.OVERLOADED)));
                    } catch (DecodeException e) {
                        // not a request: nobody to answer
                    }
                }
            }
            answers.offer(END);
        }, () -> {
            while (true) {
                byte[] frame = answers.take();
                if (frame == END) {
                    return;
                }
                Frames.write(out, frame);
                if (answers.isEmpty()) {
                    out.flush();
                }
            }
        });
    }

    /** The port the listener listens on; -1 if it does not listen. */
    public synchronized int port() {
        return server == null ? -1 : server.getLocalPort();
    }

    /** Connections that completed their handshake. */
    public long connectionCount() {
        return connections.get();
    }

    /** Connections that were turned away at the handshake. */
    public long refusedCount() {
        return refused.get();
    }

    /** Stops listening and waits until every connection is closed. */
    @Override
    public void close() throws IOException {
        ServerSocket listening;
        Thread accepting;
        synchronized (this) {
            listening = server;
            accepting = acceptor;
        }
        if (listening != null) {
            listening.close();
        }
        if (accepting != null) {
            try {
                accepting.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
