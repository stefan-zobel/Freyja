package io.github.stefan_zobel.freyja.node.net;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import io.github.stefan_zobel.freyja.common.Clock;
import io.github.stefan_zobel.freyja.common.Version;
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
 * by its own thread in real time. The files of a node are in memory and outlive a stop.
 */
final class TcpCluster implements Closeable {

    static final long CLUSTER = 0xC1A5;
    static final Path DATA = Path.of("data");
    static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();

    final class Server {
        final int id;
        final MemoryFileSystem memory = new MemoryFileSystem();
        NodeRuntime runtime;
        TcpTransport transport;
        ClientListener listener;

        Server(int id) throws IOException {
            this.id = id;
            Node.initialize(memory, DATA, new NodeIdentity(CLUSTER, id), ClusterConfig.ofVoters(0, 1, 2), 1);
        }

        boolean running() {
            return runtime != null;
        }

        /** Opens the listeners, on new ports. The node starts when every address is known. */
        void listen() throws IOException {
            Node.Options options = new Node.Options(RaftConfig.defaults(0, 5), 64 * 1024, 4096, 256 * 1024, 20,
                    SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                    Clock.SYSTEM);
            StateMachineStore.Options machine = new StateMachineStore.Options(new EngineOptions().reserveSize(0)
                    .minFreeSpace(0).deterministic(true).memTableSize(64 * 1024).blockCacheCapacity(1024 * 1024),
                    sessionTimeoutMillis);
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

        void stop() throws IOException {
            if (runtime != null) {
                listener.close();
                runtime.close();
                transport.close();
                runtime = null;
            }
        }

        /** Starts a stopped node again and tells the others where it listens now. */
        void restart() throws IOException {
            listen();
            for (Server server : servers) {
                if (server != this && server.running()) {
                    server.transport.setPeer(id, new InetSocketAddress(LOOPBACK, peerPorts[id]));
                }
            }
            start();
        }
    }

    final int[] peerPorts = new int[3];
    final int[] clientPorts = new int[3];
    final List<Server> servers = new ArrayList<>();
    private final long sessionTimeoutMillis;

    /** @param sessionTimeoutMillis after how much idle time the state machine ends a session */
    TcpCluster(long sessionTimeoutMillis) throws IOException {
        this.sessionTimeoutMillis = sessionTimeoutMillis;
        for (int id = 0; id < 3; id++) {
            servers.add(new Server(id));
            servers.get(id).listen();
        }
        for (Server server : servers) {
            server.start();
        }
    }

    /** The address for clients of a node. */
    String endpoint(int node) {
        return "127.0.0.1:" + clientPorts[node];
    }

    static void await(String what, BooleanSupplier condition) {
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

    /** Stops every node and checks that no file handle is left open. */
    @Override
    public void close() throws IOException {
        for (Server server : servers) {
            server.stop();
            if (server.memory.openHandleCount() != 0) {
                throw new AssertionError("open handles on node " + server.id);
            }
        }
    }
}
