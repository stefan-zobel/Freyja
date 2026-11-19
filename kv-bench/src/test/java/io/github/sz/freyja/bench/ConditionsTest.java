package io.github.sz.freyja.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.github.sz.freyja.common.admin.AdminResponse;

/**
 * Write and space amplification, and workloads measured while the cluster compacts, sends a
 * snapshot, or brings a node up to date (docs/spec/verification.md, section 7), against
 * nodes in this process.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class ConditionsTest {

    private BenchCluster cluster;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    @AfterEach
    void stop() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }

    /** Runs the tool; returns its exit code. What it printed is in {@link #output}. */
    private int bench(String... args) {
        bytes.reset();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        return FreyjaBench.run(args, out, out);
    }

    private String output() {
        return bytes.toString(StandardCharsets.UTF_8);
    }

    private Map<String, String> values() {
        Map<String, String> values = new HashMap<>();
        for (String line : output().split("\n")) {
            int equals = line.indexOf(" = ");
            if (equals > 0) {
                values.put(line.substring(0, equals).trim(), line.substring(equals + 3).trim());
            }
        }
        return values;
    }

    private String[] args(String workload, int rate, int seconds, String... more) {
        List<String> args = new ArrayList<>(List.of("--endpoints", String.join(",", cluster.clients()),
                "--workload", workload, "--rate", Integer.toString(rate), "--seconds", Integer.toString(seconds),
                "--clients", "8", "--keys", "2000", "--value-bytes", "200"));
        args.addAll(List.of(more));
        return args.toArray(String[]::new);
    }

    /** Fills the cluster: 2,000 keys. */
    private void fill() {
        assertEquals(0, bench(args("bulk-load", 1_000, 2)), output());
    }

    @Test
    void aResultHasTheWriteAndSpaceAmplificationOfTheNodes() throws Exception {
        cluster = BenchCluster.start(BenchCluster.Settings.defaults());
        assertEquals(0, bench(args("write-only", 400, 3, "--status", String.join(",", cluster.statuses()))),
                output());
        System.out.println(output());
        Map<String, String> values = values();
        long completed = Long.parseLong(values.get("completed"));
        // A key is "key-" and twelve digits: 16 bytes, and 200 of value.
        assertEquals(completed * 216, Long.parseLong(values.get("user.written.bytes")), output());
        long live = Long.parseLong(values.get("live.bytes"));
        assertTrue(live > 0 && live <= 2_000 * 216, output());
        // Every node writes each value into its Raft log and into its state machine.
        double write = Double.parseDouble(values.get("write.amplification"));
        assertTrue(write > 1.5 && write < 100, output());
        double space = Double.parseDouble(values.get("space.amplification"));
        assertTrue(space > 1 && space < 100, output());
        assertTrue(Long.parseLong(values.get("node.written.bytes")) > Long.parseLong(values.get("user.written.bytes")));
    }

    @Test
    void aWorkloadIsMeasuredWhileTheNodesCompact() throws Exception {
        cluster = BenchCluster.start(new BenchCluster.Settings(4096, 64 * 1024, false));
        assertEquals(0, bench(args("mixed", 200, 4, "--status", String.join(",", cluster.statuses()),
                "--during", "compaction", "--background-rate", "300")), output());
        Map<String, String> values = values();
        assertEquals("COMPACTION", values.get("during"));
        assertEquals("true", values.get("during.reached"), output());
        assertTrue(Long.parseLong(values.get("during.compactions")) > 0, output());
        // The writer that feeds the compactions counts for the amplification, not for the latencies.
        assertTrue(Long.parseLong(values.get("user.written.bytes")) > Long.parseLong(values.get("completed")) * 216,
                output());
        assertEquals("800", values.get("due"));
    }

    @Test
    void aConditionThatDoesNotComeAboutFailsTheRun() throws Exception {
        cluster = BenchCluster.start(BenchCluster.Settings.defaults());
        // One value a second does not fill a memtable of a megabyte: nothing to compact.
        assertEquals(1, bench(args("read-heavy", 100, 2, "--status", String.join(",", cluster.statuses()),
                "--during", "compaction", "--background-rate", "1")), output());
        assertEquals("false", values().get("during.reached"), output());
        assertTrue(output().contains("did not come about"), output());
        // Without the figures of the nodes it cannot be seen, and without a spare nobody joins.
        assertEquals(2, bench(args("mixed", 100, 1, "--during", "compaction")), output());
        assertEquals(2, bench(args("mixed", 100, 1, "--during", "catch-up",
                "--admin", String.join(",", cluster.admins()))), output());
        assertEquals(2, bench(args("mixed", 100, 1, "--during", "no-such")), output());
    }

    /** The members the leader knows. */
    private List<AdminResponse.Member> members() throws Exception {
        for (String admin : cluster.admins()) {
            AdminResponse.Status status = NodeAccess.status(admin);
            if (status.role().equals("LEADER")) {
                return status.members();
            }
        }
        throw new AssertionError("no leader");
    }

    @Test
    void aWorkloadIsMeasuredWhileANodeJoinsWithASnapshot() throws Exception {
        cluster = BenchCluster.start(BenchCluster.Settings.defaults());
        fill();
        String spare = cluster.startSpare();
        assertEquals(0, bench(args("mixed", 200, 4, "--during", "snapshot-transfer",
                "--admin", String.join(",", cluster.admins()), "--spare", spare)), output());
        Map<String, String> values = values();
        assertEquals("true", values.get("during.reached"), output());
        long snapshot = Long.parseLong(values.get("during.leader.snapshot.index"));
        assertTrue(snapshot >= 2_000, "every node shortened its log: " + output());
        assertTrue(Boolean.parseBoolean(values.get("during.spare.snapshot.seen"))
                || Long.parseLong(values.get("during.spare.match.index")) >= snapshot, output());
        assertFalse(members().stream().anyMatch(m -> m.nodeId() == 3), "the spare left again");
    }

    @Test
    void aWorkloadIsMeasuredWhileANodeCatchesUp() throws Exception {
        cluster = BenchCluster.start(BenchCluster.Settings.defaults());
        fill();
        String spare = cluster.startSpare();
        assertEquals(0, bench(args("read-heavy", 200, 4, "--during", "catch-up",
                "--admin", String.join(",", cluster.admins()), "--spare", spare)), output());
        Map<String, String> values = values();
        assertEquals("true", values.get("during.reached"), output());
        assertEquals("0", values.get("during.leader.snapshot.index"), "the log reaches back to the start");
        assertTrue(Long.parseLong(values.get("during.spare.match.index")) > 2_000, output());
        assertFalse(members().stream().anyMatch(m -> m.nodeId() == 3), "the spare left again");
    }
}
