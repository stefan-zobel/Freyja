package io.github.sz.freyja.tools.external;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.client.FreyjaException;
import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.tools.NodeVerify;
import io.github.sz.freyja.tools.external.ExternalCluster.NodeProcess;

/**
 * The external fault suite (docs/spec/verification.md, section 4): clusters of real processes
 * on real files and sockets, broken from outside while clients work, and the histories of the
 * clients checked for linearizability.
 *
 * <p>It runs only in its profile:
 *
 * <pre>
 * ./mvnw -pl kv-tools -am test -Pexternal-faults
 * </pre>
 *
 * <p>System properties, all optional:
 * <ul>
 *   <li>{@code freyja.external.seconds}: how long the faults of one run last (30);</li>
 *   <li>{@code freyja.external.runs}: how many runs every scenario makes, each with the next
 *       seed (1);</li>
 *   <li>{@code freyja.external.seed}: the seed of the first run (1);</li>
 *   <li>{@code freyja.external.dir}: where the data directories go (a temporary directory);
 *       the directory of a run that fails is kept;</li>
 *   <li>{@code freyja.external.porcupine}: the program that checks an exported history a
 *       second time ({@code src/test/external/porcupine}); without it only the checker of
 *       the project is asked;</li>
 *   <li>{@code freyja.external.smallfs}: an empty directory on a file system of about
 *       64 MiB for the scenario of the full disk; without it that scenario is skipped.</li>
 * </ul>
 *
 * <p>Pauses need signals and are made on Linux only.
 */
@Tag("external")
class ExternalFaultTest {

    private static final int SECONDS = Integer.getInteger("freyja.external.seconds", 30);
    private static final int RUNS = Integer.getInteger("freyja.external.runs", 1);
    private static final long SEED = Long.getLong("freyja.external.seed", 1L);
    private static final String PORCUPINE = System.getProperty("freyja.external.porcupine");
    private static final String SMALL_FS = System.getProperty("freyja.external.smallfs");

    /** What the test does to a cluster. */
    private enum Fault {
        /** A process is ended without warning and started again later. */
        KILL,
        /** A process is stopped where it is and goes on later. */
        PAUSE,
        /** Nodes cannot reach each other, in both directions or in one. */
        PARTITION,
        /** Connections are slow, and some break. */
        SLOW_NETWORK
    }

    /** Small segments and early snapshots, so that a node that was away meets both. */
    private static final List<String> SMALL_LOGS = List.of("--segment-bytes", "262144", "--snapshot-log-bytes",
            "1048576", "--snapshot-retain", "200");

    // ------------------------------------------------------------------ scenarios

    @Test
    void nodesAreKilledAndComeBack() throws Exception {
        scenario("kill", 3, EnumSet.of(Fault.KILL));
    }

    @Test
    void nodesArePausedAndGoOn() throws Exception {
        assumeTrue(ExternalCluster.linux(), "pauses need signals");
        scenario("pause", 3, EnumSet.of(Fault.PAUSE));
    }

    @Test
    void theNetworkIsPartitioned() throws Exception {
        scenario("partition", 5, EnumSet.of(Fault.PARTITION));
    }

    @Test
    void connectionsAreSlowAndBreak() throws Exception {
        scenario("slow-network", 3, EnumSet.of(Fault.SLOW_NETWORK));
    }

    @Test
    void everythingGoesWrongInTurn() throws Exception {
        EnumSet<Fault> faults = EnumSet.allOf(Fault.class);
        if (!ExternalCluster.linux()) {
            faults.remove(Fault.PAUSE);
        }
        scenario("all", 5, faults);
    }

    private void scenario(String name, int nodes, Set<Fault> faults) throws Exception {
        for (int run = 0; run < RUNS; run++) {
            long seed = SEED + run;
            System.out.println("external fault suite: scenario " + name + ", seed " + seed + ", " + SECONDS
                    + " seconds of faults");
            Path directory = workDirectory(name + "-" + seed);
            runOnce(directory, nodes, faults, seed);
            removeTree(directory); // only a run that fails leaves its files behind
        }
    }

    private void runOnce(Path directory, int count, Set<Fault> faults, long seed) throws Exception {
        List<Path> data = new ArrayList<>();
        for (int id = 0; id < count; id++) {
            data.add(directory.resolve("node-" + id));
        }
        Random random = new Random(seed);
        try (ExternalCluster cluster = new ExternalCluster(data, directory.resolve("logs"), SMALL_LOGS)) {
            cluster.start();
            Workload workload = new Workload(cluster.clientEndpoints(), 4_000);
            workload.start(6, seed);
            await("the clients get answers before anything breaks", 60, () -> workload.answered() >= 20);

            Fault[] menu = faults.toArray(Fault[]::new);
            long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(SECONDS);
            while (System.nanoTime() < end) {
                inject(cluster, menu[random.nextInt(menu.length)], random);
                cluster.heal();
                for (NodeProcess node : cluster.nodes) {
                    node.start();
                }
                Thread.sleep(500 + random.nextInt(1_500));
            }

            // Everything is well again: the cluster must serve its clients again.
            long before = workload.answered();
            await("the clients get answers again after the last fault", 90, () -> workload.answered() >= before + 20);
            workload.stop();
            System.out.println("external fault suite: " + workload.summary());
            assertEquals(List.of(), workload.failures(), "a client failed in a way it must not");

            assertSameStateEverywhere(cluster);

            Path history = directory.resolve("history.txt");
            workload.export(history);
            assertNull(workload.check(), "seed " + seed + ", history in " + history);
            if (PORCUPINE != null) {
                Process check = new ProcessBuilder(PORCUPINE, history.toString()).inheritIO().start();
                assertEquals(0, check.waitFor(), "the independent checker, seed " + seed + ", history in " + history);
            }

            // What the processes leave behind when they end in order is whole.
            for (NodeProcess node : cluster.nodes) {
                node.stop();
            }
            for (NodeProcess node : cluster.nodes) {
                assertEquals(0, verify(node.data), "the data directory of node " + node.id + " in " + node.data);
            }
        }
    }

    private static void inject(ExternalCluster cluster, Fault fault, Random random) throws Exception {
        List<NodeProcess> nodes = cluster.nodes;
        int n = nodes.size();
        switch (fault) {
            case KILL -> {
                // The leader as often as any other node; now and then more than the cluster can spare.
                int victims = random.nextInt(5) == 0 ? n / 2 + 1 : 1 + random.nextInt(Math.max(1, n / 2));
                List<NodeProcess> order = shuffled(nodes, random);
                NodeProcess leader = random.nextBoolean() ? cluster.leader() : null;
                if (leader != null) {
                    order.remove(leader);
                    order.addFirst(leader);
                }
                for (int i = 0; i < victims; i++) {
                    order.get(i).kill();
                    Thread.sleep(random.nextInt(300));
                }
                Thread.sleep(300 + random.nextInt(2_500));
            }
            case PAUSE -> {
                NodeProcess leader = random.nextBoolean() ? cluster.leader() : null;
                NodeProcess victim = leader != null ? leader : nodes.get(random.nextInt(n));
                victim.pause();
                // Longer than an election takes, so that another node leads when it wakes up.
                Thread.sleep(300 + random.nextInt(4_000));
            }
            case PARTITION -> {
                NodeProcess leader = random.nextBoolean() ? cluster.leader() : null;
                int one = leader != null ? leader.id : random.nextInt(n);
                switch (random.nextInt(4)) {
                    case 0 -> cluster.isolate(one);
                    case 1 -> { // it hears the others, they do not hear it
                        for (NodeProcess other : nodes) {
                            if (other.id != one) {
                                cluster.cutOneWay(one, other.id);
                            }
                        }
                    }
                    case 2 -> { // they hear it, it does not hear them
                        for (NodeProcess other : nodes) {
                            if (other.id != one) {
                                cluster.cutOneWay(other.id, one);
                            }
                        }
                    }
                    default -> { // two groups, the one with the chosen node the smaller
                        List<NodeProcess> order = shuffled(nodes, random);
                        order.remove(nodes.get(one));
                        order.addFirst(nodes.get(one));
                        int small = Math.max(1, (n - 1) / 2);
                        for (int a = 0; a < small; a++) {
                            for (int b = small; b < n; b++) {
                                cluster.cut(order.get(a).id, order.get(b).id);
                            }
                        }
                    }
                }
                Thread.sleep(1_000 + random.nextInt(3_000));
            }
            case SLOW_NETWORK -> {
                for (int from = 0; from < n; from++) {
                    for (int to = 0; to < n; to++) {
                        if (from != to && random.nextInt(3) != 0) {
                            cluster.proxies[from][to].delay(5 + random.nextInt(250));
                        }
                    }
                }
                long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1_000 + random.nextInt(3_000));
                while (System.nanoTime() < until) {
                    Thread.sleep(100 + random.nextInt(400));
                    int from = random.nextInt(n);
                    int to = random.nextInt(n);
                    if (from != to) {
                        cluster.proxies[from][to].reset();
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ the full disk

    /**
     * One node of three has a small disk. The cluster fills up until that node raises the
     * alarm; from then on it takes deletes only. Space is freed, the alarm is cleared, and
     * the cluster takes writes again (docs/spec/node.md, section 4.4).
     */
    @Test
    void aFullDiskStopsWritesUntilSpaceIsFreed() throws Exception {
        assumeTrue(SMALL_FS != null, "no small file system was given");
        Path small = Path.of(SMALL_FS);
        try (Stream<Path> content = Files.list(small)) {
            assertEquals(List.of(), content.toList(), "the small file system must be empty");
        }
        long capacity = Files.getFileStore(small).getTotalSpace();
        long minFree = capacity / 2;
        Path directory = workDirectory("full-disk-" + SEED);
        List<Path> data = List.of(directory.resolve("node-0"), directory.resolve("node-1"), small.resolve("node-2"));
        List<String> options = List.of("--segment-bytes", "262144", "--snapshot-log-bytes", "1048576",
                "--snapshot-retain", "50", "--min-free-bytes", Long.toString(minFree));
        try (ExternalCluster cluster = new ExternalCluster(data, directory.resolve("logs"), options)) {
            cluster.start();
            byte[] value = new byte[4 * 1024];
            Arrays.fill(value, (byte) 'v');
            int written = 0;
            try (FreyjaClient client = FreyjaClient.open(FreyjaClient.Options.of(cluster.clientEndpoints())
                    .withRequestTimeout(30_000).withKeepAlive(0))) {
                // Twice the capacity would fit nowhere: the alarm must come first.
                long limit = 2 * capacity / value.length;
                boolean refused = false;
                while (!refused && written < limit) {
                    try {
                        client.put(fill(written), value);
                        written++;
                    } catch (FreyjaException.NoSpace e) {
                        refused = true;
                    }
                }
                assertTrue(refused, "the cluster took " + written + " values and never raised the alarm");
                System.out.println("external fault suite: the alarm came after " + written + " values");
                assertTrue(cluster.awaitLeader(30).admin("status").contains("alarms:          NOSPACE"));
                assertThrows(FreyjaException.NoSpace.class, () -> client.put(fill(0), value));
                assertTrue(client.get(fill(0)).isPresent(), "reads are served");

                // Deletes are taken, and with them space can be freed.
                for (int i = 0; i < written; i++) {
                    client.delete(fill(i));
                }
                NodeProcess tight = cluster.nodes.get(2);
                await("the small disk has room again", 120, () -> {
                    tight.admin("snapshot");
                    tight.admin("compact");
                    try {
                        return Files.getFileStore(small).getUsableSpace() > minFree + (capacity / 8);
                    } catch (IOException e) {
                        return false;
                    }
                });
                await("the alarm is cleared", 30, () -> cluster.leader() != null
                        && cluster.leader().admin("clear-alarm").startsWith("0\n"));
                await("the cluster takes writes again", 30, () -> {
                    try {
                        client.put(fill(0), new byte[] {1});
                        return true;
                    } catch (FreyjaException.NoSpace e) {
                        return false;
                    }
                });
                assertTrue(cluster.awaitLeader(30).admin("status").contains("alarms:          none"));
            }
            assertSameStateEverywhere(cluster);
            for (NodeProcess node : cluster.nodes) {
                node.stop();
            }
            for (NodeProcess node : cluster.nodes) {
                assertEquals(0, verify(node.data), "the data directory of node " + node.id);
            }
        }
        removeTree(small.resolve("node-2"));
        removeTree(directory);
    }

    private static byte[] fill(int i) {
        return String.format("fill-%07d", i).getBytes(StandardCharsets.UTF_8);
    }

    // ------------------------------------------------------------------ helpers

    /**
     * Writes one last key through the cluster, waits until every node has it, and compares
     * what the nodes hold: every node must answer a stale scan of everything alike.
     */
    private static void assertSameStateEverywhere(ExternalCluster cluster) throws Exception {
        byte[] last = "zz-the-end".getBytes(StandardCharsets.UTF_8);
        try (FreyjaClient client = FreyjaClient.open(FreyjaClient.Options.of(cluster.clientEndpoints())
                .withRequestTimeout(60_000).withKeepAlive(0))) {
            client.put(last, new byte[] {1});
        }
        List<List<String>> states = new ArrayList<>();
        for (NodeProcess node : cluster.nodes) {
            try (FreyjaClient direct = FreyjaClient.open(FreyjaClient.Options.of(node.clientAddress())
                    .withRequestTimeout(2_000).withKeepAlive(0))) {
                await("node " + node.id + " has caught up", 120, () -> {
                    try {
                        return direct.getStale(last).isPresent();
                    } catch (FreyjaException e) {
                        return false;
                    }
                });
                List<String> state = new ArrayList<>();
                byte[] from = new byte[0];
                while (true) {
                    List<FreyjaClient.Entry> page = direct.scanStale(from, new byte[0], 500);
                    for (FreyjaClient.Entry entry : page) {
                        state.add(new String(entry.key(), StandardCharsets.UTF_8) + "="
                                + Arrays.hashCode(entry.value()) + "@" + entry.modIndex());
                    }
                    if (page.size() < 500) {
                        break;
                    }
                    byte[] lastKey = page.getLast().key();
                    from = Arrays.copyOf(lastKey, lastKey.length + 1); // the smallest key after it
                }
                states.add(state);
            }
        }
        for (int id = 1; id < states.size(); id++) {
            assertEquals(states.getFirst(), states.get(id), "node 0 and node " + id + " hold different states");
        }
    }

    private static int verify(Path data) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
        int code = NodeVerify.run(new String[] {data.toString()}, new RealFileSystem(), out, out);
        if (code != 0) {
            System.out.println(bytes.toString(StandardCharsets.UTF_8));
        }
        return code;
    }

    private static <T> List<T> shuffled(List<T> list, Random random) {
        List<T> copy = new ArrayList<>(list);
        java.util.Collections.shuffle(copy, random);
        return copy;
    }

    private static void await(String what, int seconds, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("not within " + seconds + " seconds: " + what);
            }
            Thread.sleep(50);
        }
    }

    private static Path workDirectory(String name) throws IOException {
        String base = System.getProperty("freyja.external.dir");
        Path root = base != null ? Path.of(base) : Path.of(System.getProperty("java.io.tmpdir"), "freyja-external");
        Path directory = root.resolve(name);
        removeTree(directory);
        Files.createDirectories(directory);
        return directory;
    }

    private static void removeTree(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }
}
