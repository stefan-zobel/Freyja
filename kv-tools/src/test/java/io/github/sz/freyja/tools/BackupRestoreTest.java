package io.github.sz.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.common.Clock;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.node.Node;
import io.github.sz.freyja.node.NodeServer;
import io.github.sz.freyja.node.net.Security;
import io.github.sz.freyja.node.backup.Backups;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.snapshot.SnapshotTransfers;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.RaftConfig;

/**
 * Backup, verification, restore, and the recovery from a lost majority
 * (docs/spec/operations.md, section 3), with servers in this process on one file system in
 * memory.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class BackupRestoreTest {

    private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
    private static final long CLUSTER = 0x01D;

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final List<NodeServer> servers = new ArrayList<>();
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    @AfterEach
    void stop() throws IOException {
        for (NodeServer server : servers) {
            server.close();
        }
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private NodeServer start(Path data) throws IOException {
        Node.Options node = new Node.Options(RaftConfig.defaults(0, 3), 64 * 1024, 4096, 256 * 1024, 20,
                SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                Clock.SYSTEM);
        NodeServer server = NodeServer.start(fs, new NodeServer.Settings(data, new InetSocketAddress(LOOPBACK, 0),
                new InetSocketAddress(LOOPBACK, 0), new InetSocketAddress(LOOPBACK, 0), Map.of(), node,
                new StateMachineStore.Options(new EngineOptions().deterministic(true).reserveSize(0).minFreeSpace(0)
                        .memTableSize(32 * 1024).blockCacheCapacity(1 << 20), 60_000), 20, null, Security.INSECURE));
        servers.add(server);
        return server;
    }

    private List<NodeServer> startCluster() throws IOException {
        List<NodeServer> cluster = new ArrayList<>();
        for (int id = 0; id < 3; id++) {
            Path data = Path.of("node-" + id);
            Node.initialize(fs, data, new NodeIdentity(CLUSTER, id), ClusterConfig.ofVoters(0, 1, 2), 1);
            cluster.add(start(data));
        }
        for (NodeServer server : cluster) {
            for (NodeServer other : cluster) {
                if (other != server) {
                    server.transport().setPeer(other.identity().nodeId(),
                            new InetSocketAddress(LOOPBACK, other.peerPort()));
                }
            }
        }
        return cluster;
    }

    private static FreyjaClient client(List<NodeServer> nodes) {
        return FreyjaClient.open(FreyjaClient.Options.of(nodes.stream().map(s -> "127.0.0.1:" + s.clientPort())
                .toArray(String[]::new)).withRequestTimeout(60_000).withKeepAlive(0));
    }

    private int verify(String directory) {
        bytes.reset();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        return BackupVerify.run(new String[] {directory}, fs, out, out);
    }

    private String out() {
        return bytes.toString(StandardCharsets.UTF_8);
    }

    @Test
    void aBackupIsTakenFromARunningClusterVerifiedAndRestoredAsANewCluster() throws Exception {
        List<NodeServer> cluster = startCluster();
        fs.createDirectories(Path.of("backups"));
        Path backup = Path.of("backups", "monday");
        Backups.Manifest manifest;
        try (FreyjaClient client = client(cluster)) {
            for (int i = 0; i < 300; i++) {
                client.put(b("key-" + i), b("before-" + i + "-" + "x".repeat(200)));
            }
            // The backup is taken while the cluster runs; any node will do.
            manifest = cluster.get(1).backup(backup);
            client.put(b("key-0"), b("after the backup"));
            client.put(b("late"), b("after the backup"));
        }
        assertEquals(CLUSTER, manifest.clusterId());
        assertEquals(1, manifest.nodeId());
        assertTrue(manifest.meta().lastIncludedIndex() >= 300, manifest.meta().toString());
        assertFalse(manifest.files().isEmpty());
        assertThrows(IOException.class, () -> cluster.get(1).backup(backup), "the target exists");
        assertEquals(List.of(), fs.list(Path.of("node-1", SnapshotTransfers.DIRECTORY)),
                "the checkpoint the copy was made from is removed");

        assertEquals(0, verify(backup.toString()), out());
        assertTrue(out().contains("backup of:    node 1 of cluster 1d"), out());
        assertTrue(out().contains("verdict:      the backup is whole"), out());

        // A new cluster from the backup: one node, another cluster id.
        Path restored = Path.of("restored");
        assertThrows(IOException.class, () -> Backups.restore(fs, backup, restored, new NodeIdentity(CLUSTER, 7)),
                "the old cluster id");
        Backups.restore(fs, backup, restored, new NodeIdentity(0x2ED, 7));
        assertEquals(0, new NodeVerifyRun().run(restored));
        NodeServer alone = start(restored);
        try (FreyjaClient client = client(List.of(alone))) {
            assertTrue(new String(client.get(b("key-0")).orElseThrow().value(), StandardCharsets.UTF_8)
                    .startsWith("before-0-"), "as of the backup");
            assertTrue(client.get(b("key-299")).isPresent());
            assertTrue(client.get(b("late")).isEmpty(), "written after the backup");
            assertEquals(300, client.scan(b("key-"), b("key-~"), 1_000).size());
            client.put(b("new"), b("life"));
            assertTrue(client.get(b("new")).isPresent());
        }
        assertEquals(0x2ED, alone.identity().clusterId());
        String status = adminStatus(alone);
        assertTrue(status.contains("node:            7 of cluster 2ed"), status);
        assertTrue(status.contains("member:          node 7 voter"), status);
        assertFalse(status.contains("node 0 voter"), status);
    }

    @Test
    void aBackupThatIsNotWholeIsFoundAndNotRestored() throws Exception {
        List<NodeServer> cluster = startCluster();
        fs.createDirectories(Path.of("backups"));
        Path backup = Path.of("backups", "damaged");
        try (FreyjaClient client = client(cluster)) {
            for (int i = 0; i < 200; i++) {
                client.put(b("key-" + i), b("value-" + "y".repeat(300)));
            }
        }
        Backups.Manifest manifest = cluster.get(0).backup(backup);
        Path table = backup.resolve(Backups.ENGINE).resolve(manifest.files().stream()
                .filter(f -> f.name().endsWith(".sst")).findFirst().orElseThrow().name());
        byte[] one = fs.peek(table, 40, 1);
        one[0] ^= 0x01;
        fs.overwrite(table, 40, one);
        assertEquals(1, verify(backup.toString()), out());
        assertTrue(out().contains("does not have the checksum of the manifest"), out());
        assertTrue(out().contains("must not be relied on"), out());
        assertThrows(CorruptionException.class,
                () -> Backups.restore(fs, backup, Path.of("never"), new NodeIdentity(0x2ED, 1)));
        assertFalse(fs.exists(Path.of("never")), "nothing is made of a backup that is not whole");

        // Without its manifest a directory is not a backup.
        fs.delete(backup.resolve(Backups.MANIFEST));
        assertEquals(1, verify(backup.toString()));
        assertTrue(out().contains("not a backup"), out());
        assertEquals(2, verify("--x"));
    }

    @Test
    void oneNodeThatIsLeftStartsANewClusterWhenItIsToldThatDataMayBeLost() throws Exception {
        List<NodeServer> cluster = startCluster();
        try (FreyjaClient client = client(cluster)) {
            for (int i = 0; i < 100; i++) {
                client.put(b("key-" + i), b("value-" + i));
            }
        }
        // Wait until node 0 has everything, then lose the other two for good.
        try (FreyjaClient direct = client(List.of(cluster.get(0)))) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (direct.getStale(b("key-99")).isEmpty()) {
                assertTrue(System.nanoTime() < deadline, "node 0 never caught up");
                Thread.sleep(20);
            }
        }
        for (NodeServer server : cluster) {
            server.close();
        }
        servers.clear();

        Path data = Path.of("node-0");
        assertThrows(IOException.class, () -> Backups.recoverFromLostMajority(fs, data, CLUSTER), "a new id");
        Backups.recoverFromLostMajority(fs, data, 0x105E);
        NodeServer alone = start(data);
        try (FreyjaClient client = client(List.of(alone))) {
            assertEquals("value-99", new String(client.get(b("key-99")).orElseThrow().value(), StandardCharsets.UTF_8));
            assertEquals(100, client.scan(b("key-"), b("key-~"), 1_000).size());
            client.put(b("again"), b("alive"));
        }
        assertEquals(0x105E, alone.identity().clusterId());
        String status = adminStatus(alone);
        assertTrue(status.contains("member:          node 0 voter"), status);
        assertFalse(status.contains("node 1 voter"), status);
    }

    private static String adminStatus(NodeServer server) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            bytes.reset();
            assertEquals(0, FreyjaAdmin.run(new String[] {"127.0.0.1:" + server.adminPort(), "status"}, out, out));
            String status = bytes.toString(StandardCharsets.UTF_8);
            // The membership shows once the node has applied the entry that sets it.
            if (status.contains("role:            LEADER") && status.lines().filter(l -> l.startsWith("member:")).count() == 1) {
                return status;
            }
            assertTrue(System.nanoTime() < deadline, status);
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }
    }

    /** Runs the tool that verifies a data directory, on the file system of this test. */
    private final class NodeVerifyRun {
        int run(Path directory) {
            PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
            return NodeVerify.run(new String[] {directory.toString()}, fs, quiet, quiet);
        }
    }
}
