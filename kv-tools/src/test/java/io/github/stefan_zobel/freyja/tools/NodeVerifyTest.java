package io.github.stefan_zobel.freyja.tools;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.CommandCodec;
import io.github.stefan_zobel.freyja.lsm.EngineOptions;
import io.github.stefan_zobel.freyja.lsm.fs.MemoryFileSystem;
import io.github.stefan_zobel.freyja.node.Node;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.snapshot.SnapshotTransfers;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.raft.ClusterConfig;
import io.github.stefan_zobel.freyja.raft.RaftConfig;
import io.github.stefan_zobel.freyja.raft.Role;

/** The tool that verifies the data directory of a node. */
class NodeVerifyTest {

    private static final Path DIR = Path.of("data");

    private final MemoryFileSystem fs = new MemoryFileSystem();
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    private int run(String... args) {
        bytes.reset();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        return NodeVerify.run(args, fs, out, out);
    }

    private String out() {
        return bytes.toString(StandardCharsets.UTF_8);
    }

    /** A node that is its own cluster writes some data and stops. */
    private void fill() throws IOException {
        Node.initialize(fs, DIR, new NodeIdentity(0x7E57, 0), ClusterConfig.ofVoters(0), 1);
        Node.Options options = new Node.Options(RaftConfig.defaults(0, 1), 4 * 1024, 4096, 1L << 30, 10,
                SnapshotTransfers.Options.defaults(), new Node.Limits(10_000, 1L << 40, 0, 10, Long.MAX_VALUE),
                () -> 0);
        try (Node node = Node.open(fs, DIR, options, new StateMachineStore.Options(new EngineOptions()
                .deterministic(true).reserveSize(0).minFreeSpace(0).memTableSize(16 * 1024)
                .blockCacheCapacity(1 << 20), 60_000), (to, frame) -> {
                }, notification -> {
                })) {
            for (int i = 0; i < 40 && node.view().role() != Role.LEADER; i++) {
                node.tick();
                while (node.runOnce()) {
                    // work
                }
            }
            assertEquals(Role.LEADER, node.view().role());
            for (int i = 0; i < 120; i++) {
                assertEquals(Node.Admission.ACCEPTED, node.propose(i, CommandCodec.encode(0,
                        new Command.SetAddress(i, "peer-" + "x".repeat(200) + ":1", "client:" + i)), node.now() + 100));
                while (node.runOnce()) {
                    // work
                }
            }
            assertTrue(node.store().appliedIndex() >= 120);
            node.store().flush();
        }
    }

    private List<Path> files(Path directory, String suffix) throws IOException {
        return fs.list(directory).stream().filter(p -> p.getFileName().toString().endsWith(suffix)).toList();
    }

    @Test
    void anIntactDirectoryPassesAndDamageIsFoundWhereItIs() throws IOException {
        fill();
        assertEquals(0, run(DIR.toString()), out());
        assertTrue(out().contains("none damaged"), out());
        assertTrue(out().contains("generation 1 is intact"), out());
        assertTrue(out().contains("verdict:      nothing is damaged"), out());
        assertEquals(0, fs.openHandleCount());

        // A sealed segment of the log.
        List<Path> segments = files(DIR.resolve(Node.RAFT_DIRECTORY), ".rlog");
        assertTrue(segments.size() > 3, segments.toString());
        Path segment = segments.get(1);
        byte[] one = fs.peek(segment, 200, 1);
        one[0] ^= 0x04;
        fs.overwrite(segment, 200, one);
        assertEquals(1, run(DIR.toString()), out());
        assertTrue(out().contains(segment.getFileName() + " is DAMAGED"), out());
        assertTrue(out().contains("1 damaged; the node will refill its log from the leader"), out());
        assertTrue(out().contains("generation 1 is intact"), out());
        one[0] ^= 0x04;
        fs.overwrite(segment, 200, one);
        assertEquals(0, run(DIR.toString()), out());

        // A table of the state machine.
        Path table = files(DIR.resolve(StateMachineStore.DIRECTORY).resolve("0000000001"), ".sst").getFirst();
        byte[] other = fs.peek(table, 60, 1);
        other[0] ^= 0x10;
        fs.overwrite(table, 60, other);
        assertEquals(1, run(DIR.toString()), out());
        assertTrue(out().contains("generation 1 is DAMAGED; the node will rebuild it"), out());
        assertTrue(out().contains("none damaged"), out());
        assertEquals(0, fs.openHandleCount());

        assertEquals(2, run());
        assertEquals(2, run("--x"));
    }
}
