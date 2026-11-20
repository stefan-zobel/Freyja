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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.wire.Frames;
import io.github.sz.freyja.common.wire.Handshake;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.common.Version;
import io.github.sz.freyja.common.tls.Tls;
import io.github.sz.freyja.node.Transport;
import io.github.sz.freyja.node.concurrent.Scopes;

/**
 * The transport between nodes over TCP (docs/spec/node.md, section 8).
 *
 * <p>Every node keeps one connection to each peer it sends to, and accepts one from each
 * peer that sends to it. A connection starts with a handshake in both directions; a peer of
 * another cluster is turned away. The transport remembers the highest cluster version each
 * peer named in its latest handshake. Frames are length-prefixed.
 *
 * <p>With TLS, both sides of a connection show a certificate, and each checks that of the
 * other before the handshake of the protocol (docs/spec/operations.md, section 5).
 *
 * <p>Sending never blocks the caller: a frame goes into a bounded queue of its peer, and a
 * frame that does not fit, or that is on its way when the connection breaks, is lost. The
 * protocols above send again what matters. A broken connection is opened again after a pause.
 *
 * <p>One virtual thread writes to each peer, one reads from each accepted connection.
 */
public final class TcpTransport implements Transport, Closeable {

    /**
     * @param maxFrameBytes          the largest frame that is accepted
     * @param queueCapacity          how many frames may wait for one peer
     * @param connectTimeoutMillis   how long connecting to a peer may take
     * @param retryMillis            the pause before a broken connection is opened again
     * @param handshakeTimeoutMillis how long the other side may take for its handshake
     */
    public record Options(int maxFrameBytes, int queueCapacity, int connectTimeoutMillis, int retryMillis,
                          int handshakeTimeoutMillis) {
        public Options {
            if (maxFrameBytes < 1024 || queueCapacity < 1 || connectTimeoutMillis < 1 || retryMillis < 1
                    || handshakeTimeoutMillis < 1) {
                throw new IllegalArgumentException("invalid transport settings");
            }
        }

        /** The defaults: frames up to 16 MiB, 1,024 waiting frames, 2 s to connect, 200 ms pause, 5 s. */
        public static Options defaults() {
            return new Options(16 << 20, 1024, 2_000, 200, 5_000);
        }
    }

    /** The connection to one peer and the frames that wait for it. */
    private final class Link implements Runnable {
        final int peer;
        final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(options.queueCapacity());
        private Socket socket;
        private OutputStream out;

        Link(int peer) {
            this.peer = peer;
        }

        @Override
        public void run() {
            while (!closed) {
                try {
                    byte[] frame = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (frame == null) {
                        continue;
                    }
                    try {
                        connect();
                        if (out == null) {
                            dropped.incrementAndGet(); // no address is known for the peer
                            continue;
                        }
                        Frames.write(out, frame);
                        sent.incrementAndGet();
                        // What has piled up goes out with the same flush.
                        for (byte[] next = queue.poll(); next != null; next = queue.poll()) {
                            Frames.write(out, next);
                            sent.incrementAndGet();
                        }
                        out.flush();
                    } catch (IOException | DecodeException e) {
                        dropped.incrementAndGet();
                        disconnect();
                        Thread.sleep(options.retryMillis());
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }
            disconnect();
        }

        private void connect() throws IOException {
            if (socket != null) {
                return;
            }
            InetSocketAddress address = addresses.get(peer);
            if (address == null) {
                return;
            }
            Socket fresh = new Socket();
            try {
                fresh.setTcpNoDelay(true);
                fresh.connect(new InetSocketAddress(address.getHostString(), address.getPort()),
                        options.connectTimeoutMillis());
                fresh.setSoTimeout(options.handshakeTimeoutMillis());
                Socket secured = tls == null ? fresh : tls.connect(fresh, address.getHostString(), address.getPort());
                OutputStream stream = new BufferedOutputStream(secured.getOutputStream(), 64 * 1024);
                Frames.write(stream, new Handshake(Handshake.PEER, clusterId, nodeId, software, clusterVersion)
                        .encode());
                stream.flush();
                byte[] answer = Frames.read(secured.getInputStream(), Handshake.MAX_BYTES);
                if (answer == null) {
                    throw new IOException("node " + peer + " closed the connection during the handshake");
                }
                Handshake other = Handshake.decode(answer);
                if (other.clusterId() != clusterId || other.nodeId() != peer) {
                    throw new IOException("the node at " + address + " is node " + other.nodeId() + " of cluster "
                            + Long.toHexString(other.clusterId()) + ", not node " + peer + " of this cluster");
                }
                peerVersions.put(peer, other.clusterVersion());
                fresh.setSoTimeout(0);
                socket = secured;
                out = stream;
            } catch (IOException | DecodeException e) {
                fresh.close();
                throw e;
            }
        }

        private void disconnect() {
            Socket old = socket;
            socket = null;
            out = null;
            if (old != null) {
                try {
                    old.close();
                } catch (IOException e) {
                    // closed already
                }
            }
        }
    }

    private final long clusterId;
    private final int nodeId;
    private final String software;
    private final int clusterVersion;
    /** The highest cluster version each peer named in its latest handshake. */
    private final Map<Integer, Integer> peerVersions = new ConcurrentHashMap<>();
    private final Options options;
    private final Consumer<byte[]> inbound;
    private final Tls tls;
    private final Map<Integer, InetSocketAddress> addresses = new ConcurrentHashMap<>();
    private final Map<Integer, Link> links = new ConcurrentHashMap<>();
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();
    private volatile boolean closed;
    private ServerSocket server;
    private Thread acceptor;

    /**
     * @param inbound takes every frame that a peer of this cluster sends; called from the
     *                thread of its connection, it must not block
     */
    public TcpTransport(long clusterId, int nodeId, String software, Options options, Consumer<byte[]> inbound) {
        this(clusterId, nodeId, software, options, inbound, null);
    }

    /**
     * @param inbound takes every frame that a peer of this cluster sends; called from the
     *                thread of its connection, it must not block
     * @param tls     TLS between the nodes; {@code null} for none
     */
    public TcpTransport(long clusterId, int nodeId, String software, Options options, Consumer<byte[]> inbound,
                        Tls tls) {
        this(clusterId, nodeId, software, Version.CLUSTER, options, inbound, tls);
    }

    /**
     * @param inbound takes every frame that a peer of this cluster sends; called from the
     *                thread of its connection, it must not block
     * @param tls     TLS between the nodes; {@code null} for none
     * @param clusterVersion the highest cluster version the software of this node supports
     */
    public TcpTransport(long clusterId, int nodeId, String software, int clusterVersion, Options options,
                        Consumer<byte[]> inbound, Tls tls) {
        this.tls = tls;
        this.clusterId = clusterId;
        this.nodeId = nodeId;
        this.software = software;
        this.clusterVersion = clusterVersion;
        this.options = options;
        this.inbound = inbound;
    }

    /**
     * Starts to accept connections of peers.
     *
     * @param bind the address to listen on; port 0 for any free port
     * @return the port that was bound
     */
    public synchronized int listen(InetSocketAddress bind) throws IOException {
        if (server != null) {
            throw new IllegalStateException("the transport listens already");
        }
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(bind);
        ServerSocket listening = server;
        acceptor = Thread.ofVirtual().name("freyja-peers-" + nodeId).start(() -> {
            try {
                Scopes.serve(listening, this::accepted);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        return server.getLocalPort();
    }

    /** Sets or changes the address at which a peer accepts connections of peers. */
    public void setPeer(int peer, InetSocketAddress address) {
        addresses.put(peer, address);
    }

    /** Forgets a peer: nothing is sent to it any more. */
    public void removePeer(int peer) {
        addresses.remove(peer);
    }

    @Override
    public void send(int toNode, byte[] message) {
        if (closed) {
            return;
        }
        Link link = links.computeIfAbsent(toNode, peer -> {
            Link created = new Link(peer);
            Thread.ofVirtual().name("freyja-link-" + nodeId + "-" + peer).start(created);
            return created;
        });
        if (!link.queue.offer(message)) {
            dropped.incrementAndGet();
        }
    }

    /** One accepted connection: the handshake, then frames until the peer closes. */
    private void accepted(Socket raw) throws IOException {
        raw.setTcpNoDelay(true);
        raw.setSoTimeout(options.handshakeTimeoutMillis());
        Socket socket;
        try {
            socket = tls == null ? raw : tls.accept(raw, Tls.ClientCertificate.NEEDED);
        } catch (IOException e) {
            refused.incrementAndGet();
            return; // no certificate of a node of this cluster
        }
        InputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);
        byte[] first = Frames.read(in, Handshake.MAX_BYTES);
        Handshake other;
        try {
            other = first == null ? null : Handshake.decode(first);
        } catch (DecodeException e) {
            other = null;
        }
        if (other == null || other.role() != Handshake.PEER || other.clusterId() != clusterId
                || other.nodeId() < 0 || other.nodeId() == nodeId) {
            refused.incrementAndGet();
            return; // not a peer of this cluster
        }
        OutputStream out = socket.getOutputStream();
        Frames.write(out, new Handshake(Handshake.PEER, clusterId, nodeId, software, clusterVersion).encode());
        out.flush();
        raw.setSoTimeout(0);
        int peer = other.nodeId();
        peerVersions.put(peer, other.clusterVersion());
        for (byte[] frame = Frames.read(in, options.maxFrameBytes()); frame != null && !closed;
                frame = Frames.read(in, options.maxFrameBytes())) {
            // Every message between nodes names its sender in the same place. A frame that
            // names another node than the one that shook hands is not passed on.
            if (frame.length >= 6 && LittleEndian.getInt32(frame, 2) == peer) {
                received.incrementAndGet();
                inbound.accept(frame);
            } else {
                refused.incrementAndGet();
            }
        }
    }

    @Override
    public int clusterVersionOf(int node) {
        return node == nodeId ? clusterVersion : peerVersions.getOrDefault(node, -1);
    }

    /** The port the transport listens on; -1 if it does not listen. */
    public synchronized int port() {
        return server == null ? -1 : server.getLocalPort();
    }

    /** Frames written to a connection. */
    public long sentCount() {
        return sent.get();
    }

    /** Frames lost: the queue of their peer was full, its address unknown, or its connection broke. */
    public long droppedCount() {
        return dropped.get();
    }

    /** Frames received from peers and passed on. */
    public long receivedCount() {
        return received.get();
    }

    /** Connections and frames that were turned away. */
    public long refusedCount() {
        return refused.get();
    }

    /** Stops listening and sending, and waits until every connection is closed. */
    @Override
    public void close() throws IOException {
        closed = true;
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
