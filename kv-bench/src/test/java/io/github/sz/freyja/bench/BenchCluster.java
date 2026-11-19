package io.github.sz.freyja.bench;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.github.sz.freyja.common.Clock;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.node.Node;
import io.github.sz.freyja.node.NodeServer;
import io.github.sz.freyja.node.net.Security;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.snapshot.SnapshotTransfers;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.RaftConfig;

/**
 * Three nodes in this process, each on a file system in memory, with their listeners for
 * clients and administrators and their HTTP endpoints; on request also an empty spare node
 * that can join.
 */
final class BenchCluster implements AutoCloseable {

    static final long CLUSTER = 0xBE;
    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final Path DATA = Path.of("data");

    /**
     * @param queueCapacity how many events may wait for the event loop of a node
     * @param memTable      the size of a memtable of the state machines
     * @param deterministic whether the engines flush and compact inside the writes
     */
    record Settings(int queueCapacity, long memTable, boolean deterministic) {
        static Settings defaults() {
            return new Settings(4096, 1 << 20, true);
        }
    }

    final List<NodeServer> servers = new ArrayList<>();
    NodeServer spare;
    private final Settings settings;

    private BenchCluster(Settings settings) {
        this.settings = settings;
    }

    static BenchCluster start(Settings settings) throws IOException {
        BenchCluster cluster = new BenchCluster(settings);
        try {
            for (int id = 0; id < 3; id++) {
                MemoryFileSystem fs = new MemoryFileSystem();
                Node.initialize(fs, DATA, new NodeIdentity(CLUSTER, id), ClusterConfig.ofVoters(0, 1, 2), 1);
                cluster.servers.add(cluster.server(fs, Map.of()));
            }
            for (NodeServer server : cluster.servers) {
                for (NodeServer other : cluster.servers) {
                    if (other != server) {
                        server.transport().setPeer(other.identity().nodeId(),
                                new InetSocketAddress(LOOPBACK, other.peerPort()));
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            cluster.close();
            throw e;
        }
        return cluster;
    }

    private NodeServer server(MemoryFileSystem fs, Map<Integer, String> seeds) throws IOException {
        Node.Options node = new Node.Options(RaftConfig.defaults(0, 3), 1 << 20, settings.queueCapacity(), 8 << 20,
                1_000, SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                Clock.SYSTEM);
        return NodeServer.start(fs, new NodeServer.Settings(DATA, new InetSocketAddress(LOOPBACK, 0),
                new InetSocketAddress(LOOPBACK, 0), new InetSocketAddress(LOOPBACK, 0), seeds, node,
                new StateMachineStore.Options(new EngineOptions().deterministic(settings.deterministic())
                        .reserveSize(0).minFreeSpace(0).memTableSize(settings.memTable())
                        .blockCacheCapacity(8 << 20), 60_000), 20, new InetSocketAddress(LOOPBACK, 0),
                Security.INSECURE));
    }

    /** Starts an empty node, number 3, that knows where the others listen and can join. */
    String startSpare() throws IOException {
        MemoryFileSystem fs = new MemoryFileSystem();
        Node.initialize(fs, DATA, new NodeIdentity(CLUSTER, 3), ClusterConfig.EMPTY, 1);
        Map<Integer, String> seeds = new HashMap<>();
        for (NodeServer server : servers) {
            seeds.put(server.identity().nodeId(), "127.0.0.1:" + server.peerPort());
        }
        spare = server(fs, seeds);
        return "3,127.0.0.1:" + spare.peerPort() + ",127.0.0.1:" + spare.clientPort();
    }

    private static List<String> addresses(List<NodeServer> servers, java.util.function.ToIntFunction<NodeServer> port) {
        List<String> list = new ArrayList<>();
        for (NodeServer server : servers) {
            list.add("127.0.0.1:" + port.applyAsInt(server));
        }
        return list;
    }

    List<String> clients() {
        return addresses(servers, NodeServer::clientPort);
    }

    List<String> admins() {
        return addresses(servers, NodeServer::adminPort);
    }

    List<String> statuses() {
        return addresses(servers, NodeServer::statusPort);
    }

    @Override
    public void close() throws IOException {
        for (NodeServer server : servers) {
            server.close();
        }
        if (spare != null) {
            spare.close();
        }
    }
}
