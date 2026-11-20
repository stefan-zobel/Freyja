package io.github.sz.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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

/** The command-line tool of a user against three nodes in this process. */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class FreyjaCliTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final long CLUSTER = 0xC11;

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final List<NodeServer> servers = new ArrayList<>();

    @AfterEach
    void stop() throws IOException {
        for (NodeServer server : servers) {
            server.close();
        }
    }

    private void startCluster() throws IOException {
        Node.Options node = new Node.Options(RaftConfig.defaults(0, 3), 64 * 1024, 4096, 256 * 1024, 20,
                SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                Clock.SYSTEM);
        for (int id = 0; id < 3; id++) {
            Path data = Path.of("node-" + id);
            Node.initialize(fs, data, new NodeIdentity(CLUSTER, id), ClusterConfig.ofVoters(0, 1, 2), 1);
            servers.add(NodeServer.start(fs, new NodeServer.Settings(data, new InetSocketAddress(LOOPBACK, 0),
                    new InetSocketAddress(LOOPBACK, 0), new InetSocketAddress(LOOPBACK, 0), Map.of(), node,
                    new StateMachineStore.Options(new EngineOptions().deterministic(true).reserveSize(0)
                            .minFreeSpace(0).memTableSize(32 * 1024).blockCacheCapacity(1 << 20), 60_000),
                    20, null, Security.INSECURE)));
        }
        for (NodeServer server : servers) {
            for (NodeServer other : servers) {
                if (other != server) {
                    server.transport().setPeer(other.identity().nodeId(),
                            new InetSocketAddress(LOOPBACK, other.peerPort()));
                }
            }
        }
    }

    private String nodes() {
        return String.join(",", servers.stream().map(s -> "127.0.0.1:" + s.clientPort()).toList());
    }

    /** Runs the tool: its exit code, then what it printed. */
    private static String cli(String... args) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        int code = FreyjaCli.run(args, out, out);
        return (code + "\n" + bytes.toString(StandardCharsets.UTF_8)).replace("\r\n", "\n").strip();
    }

    @Test
    void keysAreWrittenReadScannedAndRemoved() throws IOException {
        startCluster();
        String nodes = nodes();
        assertEquals("0\nok", cli("--timeout-ms", "60000", nodes, "put", "greeting", "hello, Freyja"));
        assertEquals("0\nhello, Freyja", cli(nodes, "get", "greeting"));
        for (String key : List.of("fruit/apple", "fruit/banana", "fruit/cherry", "vegetable/leek")) {
            assertEquals("0\nok", cli(nodes, "put", key, key.substring(key.indexOf('/') + 1)));
        }
        assertEquals("0\nfruit/apple\tapple\nfruit/banana\tbanana\nfruit/cherry\tcherry",
                cli(nodes, "scan", "fruit/", "fruit0"));
        assertEquals("0\nfruit/apple\tapple\nfruit/banana\tbanana", cli(nodes, "scan", "fruit/", "--limit", "2"));
        assertEquals("0\nfruit/apple\tapple\nfruit/banana\tbanana\nfruit/cherry\tcherry\ngreeting\thello, Freyja"
                + "\nvegetable/leek\tleek", cli(nodes, "scan"));

        // A stale read may lag behind; the node that answers has applied the write at last.
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (!cli(nodes, "get", "fruit/cherry", "--stale").equals("0\ncherry")) {
            assertTrue(System.nanoTime() < deadline, "no node answered with the value");
        }

        assertEquals("0\nok", cli(nodes, "delete", "fruit/banana"));
        assertEquals("3\nno value", cli(nodes, "get", "fruit/banana"));
        assertEquals("0\nok", cli(nodes, "delete", "fruit/banana"), "removing what is not there");

        // Keys and values that are not text.
        assertEquals("0\nok", cli("--hex", nodes, "put", "00ff", "cafe"));
        assertEquals("0\ncafe", cli("--hex", nodes, "get", "00ff"));
        assertEquals("0\n00ff\tcafe", cli("--hex", nodes, "scan", "00", "01"));
    }

    @Test
    void wrongUsageAndAClusterThatCannotBeReachedAreSaid() {
        assertTrue(cli().startsWith("2\nerror: nodes and a command are needed"), cli());
        assertTrue(cli("127.0.0.1:1", "fetch", "k").startsWith("2\nerror: unknown command fetch"));
        assertTrue(cli("127.0.0.1:1", "put", "k").startsWith("2\nerror: wrong arguments for put"));
        assertTrue(cli("127.0.0.1:1", "delete", "k", "--stale").startsWith("2\nerror: wrong arguments for delete"));
        assertTrue(cli("--timeout-ms", "0", "127.0.0.1:1", "get", "k").startsWith("2\nerror: --timeout-ms needs"));
        assertTrue(cli("--hex", "127.0.0.1:1", "get", "xyz").startsWith("2\nerror: not hexadecimal: xyz"));
        assertTrue(cli("--token-file", "t", "127.0.0.1:1", "get", "k").startsWith("2\nerror: TLS needs --tls-trust"));
        assertTrue(cli("no-port", "get", "k").startsWith("2\nerror: "));
        String unreachable = cli("--timeout-ms", "500", "127.0.0.1:1", "get", "k");
        assertTrue(unreachable.startsWith("1\nerror: "), unreachable);
    }
}
