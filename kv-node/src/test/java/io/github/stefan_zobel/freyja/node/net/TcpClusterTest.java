package io.github.stefan_zobel.freyja.node.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.github.stefan_zobel.freyja.common.Clock;
import io.github.stefan_zobel.freyja.common.Version;
import io.github.stefan_zobel.freyja.common.kv.ClientCodec;
import io.github.stefan_zobel.freyja.common.kv.ClientRequest;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.NotLeader;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Result;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Value;
import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Written;
import io.github.stefan_zobel.freyja.common.wire.Frames;
import io.github.stefan_zobel.freyja.common.wire.Handshake;
import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.NodeRuntime;
import io.github.stefan_zobel.freyja.node.service.ClientService;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.snapshot.SnapshotTransfers;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.RaftConfig;

/**
 * Three nodes in one process that talk over real sockets on the loopback interface, each run
 * by its own thread in real time (docs/spec/node.md, sections 1 and 8).
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class TcpClusterTest {

    private static final long CLUSTER = 0xC1A5;
    private static final Path DATA = Path.of("data");
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    /** One node with its network. Its files outlive a stop. */
    private final class Server implements Closeable {
        final int id;
        final MemoryFileSystem memory = new MemoryFileSystem();
        NodeRuntime runtime;
        TcpTransport transport;
        ClientListener listener;

        Server(int id) throws IOException {
            this.id = id;
            Node.initialize(memory, DATA, new NodeIdentity(CLUSTER, id), ClusterConfig.ofVoters(0, 1, 2), 1);
        }

        /** Opens the listeners. The node starts when every address is known. */
        void listen() throws IOException {
            Node.Options options = new Node.Options(RaftConfig.defaults(0, 5), 64 * 1024, 4096, 256 * 1024, 20,
                    SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                    Clock.SYSTEM);
            StateMachineStore.Options machine = new StateMachineStore.Options(new EngineOptions().reserveSize(0)
                    .minFreeSpace(0).deterministic(true).memTableSize(64 * 1024).blockCacheCapacity(1024 * 1024),
                    60_000);
            runtime = new NodeRuntime(memory, new NodeRuntime.Config(DATA, options, machine,
                    ClientService.Options.defaults(), 20, 4096, 50, System::currentTimeMillis,
                    node -> "127.0.0.1:" + clientPorts[node]));
            transport = new TcpTransport(CLUSTER, id, Version.SOFTWARE, new TcpTransport.Options(16 << 20, 1024,
                    1_000, 50, 2_000), runtime::peerFrame);
            peerPorts[id] = transport.listen(new InetSocketAddress(LOOPBACK, 0));
            listener = new ClientListener(CLUSTER, id, Version.SOFTWARE, Handshake.CLIENT,
                    ClientListener.Options.defaults(), runtime::clientRequest);
            clientPorts[id] = listener.listen(new InetSocketAddress(LOOPBACK, 0));
        }

        void start() {
            for (int peer = 0; peer < 3; peer++) {
                if (peer != id) {
                    transport.setPeer(peer, new InetSocketAddress(LOOPBACK, peerPorts[peer]));
                }
            }
            runtime.start(transport);
        }

        @Override
        public void close() throws IOException {
            if (runtime != null) {
                listener.close();
                runtime.close();
                transport.close();
                runtime = null;
            }
        }
    }

    /** A connection of a client, speaking the protocol by hand. */
    private static final class Wire implements Closeable {
        final Socket socket;
        final InputStream in;
        final OutputStream out;
        final Handshake server;
        long requestId;

        Wire(int port, int role, long cluster) throws IOException {
            socket = new Socket(LOOPBACK, port);
            socket.setSoTimeout(10_000);
            in = socket.getInputStream();
            out = socket.getOutputStream();
            Frames.write(out, new Handshake(role, cluster, -1, "test").encode());
            out.flush();
            byte[] answer = Frames.read(in, Handshake.MAX_BYTES);
            server = answer == null ? null : Handshake.decode(answer);
        }

        ClientResponse call(ClientRequest request, int timeoutMillis) throws IOException {
            long id = ++requestId;
            Frames.write(out, ClientCodec.encode(id, timeoutMillis, request));
            out.flush();
            ClientCodec.ResponseEnvelope envelope = ClientCodec.decodeResponse(Frames.read(in, 1 << 20));
            assertEquals(id, envelope.requestId());
            return envelope.response();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private final int[] peerPorts = new int[3];
    private final int[] clientPorts = new int[3];
    private final List<Server> servers = new ArrayList<>();

    private void startCluster() throws IOException {
        for (int id = 0; id < 3; id++) {
            servers.add(new Server(id));
            servers.get(id).listen();
        }
        for (Server server : servers) {
            server.start();
        }
    }

    @AfterEach
    void stopAll() throws IOException {
        for (Server server : servers) {
            server.close();
            assertEquals(0, server.memory.openHandleCount(), "open handles on node " + server.id);
        }
    }

    private static void await(String what, BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("not within 30 seconds: " + what);
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Sends a request to the leader, wherever it is: follows redirects, and asks again when
     * a node has no leader or no time.
     */
    private ClientResponse onLeader(ClientRequest request) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        int target = 0;
        while (System.nanoTime() < deadline) {
            Server server = servers.get(target);
            if (server.runtime != null) {
                try (Wire wire = new Wire(clientPorts[target], Handshake.CLIENT, 0)) {
                    ClientResponse response = wire.call(request, 2_000);
                    if (response instanceof NotLeader redirect && redirect.leaderId() >= 0) {
                        assertEquals("127.0.0.1:" + clientPorts[redirect.leaderId()], redirect.leaderAddress());
                        target = redirect.leaderId();
                        continue;
                    }
                    if (!(response instanceof NotLeader) && !(response instanceof ClientResponse.Rejected)) {
                        return response;
                    }
                } catch (IOException e) {
                    // the node is going down or coming up
                }
            }
            target = (target + 1) % 3;
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("no answer of a leader within 30 seconds to " + request);
    }

    private long register() throws IOException {
        Result result = assertInstanceOf(Result.class, onLeader(new ClientRequest.Execute(new Command.RegisterSession())));
        return assertInstanceOf(SessionRegistered.class, result.result()).sessionId();
    }

    private void put(long session, long sequence, String key, String value) throws IOException {
        Result result = assertInstanceOf(Result.class, onLeader(new ClientRequest.Execute(
                new Command.Write(session, sequence, List.of(new Put(b(key), b(value)))))));
        assertEquals(new Written(), result.result());
    }

    private Value staleRead(int node, String key) throws IOException {
        try (Wire wire = new Wire(clientPorts[node], Handshake.CLIENT, CLUSTER)) {
            return assertInstanceOf(Value.class, wire.call(new ClientRequest.Get(b(key), true), 2_000));
        }
    }

    @Test
    void threeNodesServeClientsOverSockets() throws IOException {
        startCluster();
        long session = register();
        for (int i = 1; i <= 40; i++) {
            put(session, i, "key-" + i, "value-" + i);
        }
        Value read = assertInstanceOf(Value.class, onLeader(new ClientRequest.Get(b("key-17"), false)));
        assertTrue(read.found());
        assertArrayEquals(b("value-17"), read.value());

        // Every node has it after a moment; a stale read asks only the node itself.
        for (int node = 0; node < 3; node++) {
            int at = node;
            await("node " + node + " has applied the last write", () -> {
                try {
                    Value value = staleRead(at, "key-40");
                    return value.found() && new String(value.value(), StandardCharsets.UTF_8).equals("value-40");
                } catch (IOException e) {
                    return false;
                }
            });
        }
        for (Server server : servers) {
            assertTrue(server.runtime.running());
            assertNull(server.runtime.fatal());
            assertEquals(0, server.runtime.restarts());
            assertTrue(server.transport.receivedCount() > 0 && server.transport.sentCount() > 0);
            assertEquals(0, server.transport.refusedCount());
        }
        // The handshake of the node names the cluster and the node.
        try (Wire wire = new Wire(clientPorts[1], Handshake.CLIENT, 0)) {
            assertEquals(new Handshake(Handshake.CLIENT, CLUSTER, 1, Version.SOFTWARE), wire.server);
        }
    }

    @Test
    void connectionsThatDoNotBelongAreTurnedAway() throws IOException {
        startCluster();
        // A peer of another cluster.
        try (Wire wire = new Wire(peerPorts[0], Handshake.PEER, CLUSTER + 1)) {
            assertNull(wire.server, "no handshake comes back; the connection is closed");
        }
        // A client on the listener for peers, and a peer on the listener for clients.
        try (Wire wire = new Wire(peerPorts[0], Handshake.CLIENT, CLUSTER)) {
            assertNull(wire.server);
        }
        try (Wire wire = new Wire(clientPorts[0], Handshake.PEER, CLUSTER)) {
            assertNull(wire.server);
        }
        // A client that names another cluster.
        try (Wire wire = new Wire(clientPorts[0], Handshake.CLIENT, CLUSTER + 1)) {
            assertNull(wire.server);
        }
        // Bytes that are no handshake.
        try (Socket socket = new Socket(LOOPBACK, clientPorts[0])) {
            socket.setSoTimeout(10_000);
            Frames.write(socket.getOutputStream(), new byte[] {1, 2, 3, 4, 5, 6, 7, 8});
            socket.getOutputStream().flush();
            assertNull(Frames.read(socket.getInputStream(), 100));
        }
        await("the refusals are counted", () -> servers.get(0).transport.refusedCount() == 2
                && servers.get(0).listener.refusedCount() == 3);
        // The cluster is not disturbed.
        long session = register();
        put(session, 1, "k", "v");
    }

    @Test
    void theClusterSurvivesTheLossOfItsLeaderAndTakesItBack() throws IOException {
        startCluster();
        long session = register();
        put(session, 1, "before", "1");
        int[] leader = {-1};
        await("a leader is known", () -> {
            try (Wire wire = new Wire(clientPorts[0], Handshake.CLIENT, 0)) {
                ClientResponse response = wire.call(new ClientRequest.Get(b("before"), false), 1_000);
                if (response instanceof Value) {
                    leader[0] = 0;
                } else if (response instanceof NotLeader redirect) {
                    leader[0] = redirect.leaderId();
                }
                return leader[0] >= 0;
            } catch (IOException e) {
                return false;
            }
        });
        Server stopped = servers.get(leader[0]);
        stopped.close();

        // The two that are left elect a leader; the session goes on.
        put(session, 2, "during", "2");
        for (int i = 3; i <= 20; i++) {
            put(session, i, "key-" + i, "v" + i);
        }
        // The node comes back, on new ports, and catches up.
        stopped.listen();
        for (Server server : servers) {
            if (server != stopped) {
                server.transport.setPeer(stopped.id, new InetSocketAddress(LOOPBACK, peerPorts[stopped.id]));
            }
        }
        stopped.start();
        await("the node that came back has caught up", () -> {
            try {
                Value value = staleRead(stopped.id, "key-20");
                return value.found();
            } catch (IOException e) {
                return false;
            }
        });
        put(session, 21, "after", "3");
        assertTrue(assertInstanceOf(Value.class, onLeader(new ClientRequest.Get(b("during"), false))).found());
    }
}
