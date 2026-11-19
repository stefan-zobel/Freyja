package io.github.sz.freyja.node;

import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;

import io.github.sz.freyja.common.Clock;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.fs.RealFileSystem;
import io.github.sz.freyja.node.backup.Backups;
import io.github.sz.freyja.node.net.Security;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.snapshot.SnapshotTransfers;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.RaftConfig;

/**
 * The program that runs a node (docs/spec/operations.md).
 *
 * <pre>
 * FreyjaNode init --data DIR --cluster HEX --node N [--voters N,N,...] [--cluster-version V]
 * FreyjaNode run  --data DIR --peer HOST:PORT --client HOST:PORT --admin HOST:PORT
 *                 [--seed N=HOST:PORT]... [--tick-ms MILLIS] [--status HOST:PORT]
 *                 [--segment-bytes N] [--snapshot-log-bytes N] [--snapshot-retain N]
 *                 [--quota-bytes N] [--min-free-bytes N]
 *                 --insecure yes
 *               | --tls-key FILE [--tls-password-file FILE]
 *                 --peer-trust FILE [--peer-name ATTRIBUTE=VALUE]
 *                 [--client-trust FILE [--client-name ATTRIBUTE=VALUE]] [--client-tokens FILE]
 *                 --admin-trust FILE [--admin-name ATTRIBUTE=VALUE]
 * </pre>
 *
 * <p>A node runs with TLS on its three listeners unless it is told {@code --insecure yes}
 * (docs/spec/operations.md, section 5). The key store holds the private key of the node and
 * its certificate; a trust store holds the authorities whose certificates are accepted at
 * one listener; a name is an attribute that the subject of such a certificate must have. All
 * stores are PKCS12 files with the password in the first line of the password file. Clients
 * need a certificate, or a token of the token file, or either if both are given.
 *
 * <p>The last five set the size of a segment of the Raft log (64 MiB), the size of the log at
 * which the node takes a snapshot and shortens it (1 GiB), the number of applied entries it
 * keeps then (10,000), the size of the state machine at which the alarm for lack of space is
 * raised (16 GiB), and the free space of the file system below which it is raised (1 GiB).
 *
 * <p>{@code --status} serves the metrics and the health of the node over HTTP: {@code /metrics},
 * {@code /health}, {@code /ready}.
 *
 * <pre>
 * FreyjaNode restore --backup DIR --data DIR --cluster HEX --node N
 * FreyjaNode recover-majority --data DIR --cluster HEX --i-accept-data-loss yes
 * </pre>
 *
 * <p>{@code restore} makes the data directory of the first node of a new cluster from a
 * backup. {@code recover-majority} starts a new cluster from the data directory of the one
 * node that is left after the others are lost for good; it is not safe, says so, and wants
 * to be told that this is understood.
 *
 * <p>{@code init} prepares an empty data directory. A node of a new cluster is given the
 * voters the cluster starts with, the same on every node; a node that is to join a cluster is
 * given none. {@code run} starts the node of a prepared directory and prints one line when it
 * listens, with the ports it has bound:
 *
 * <pre>
 * READY node=N peer=PORT client=PORT admin=PORT
 * </pre>
 *
 * <p>Exit code 0: done. 1: the node could not be prepared or started. 2: wrong usage.
 */
public final class FreyjaNode {

    private FreyjaNode() {
    }

    public static void main(String[] args) {
        int code = run(args, System.out, System.err, true);
        if (code != 0) {
            System.exit(code);
        }
    }

    /**
     * @param block whether {@code run} waits until the process is told to end; a test passes
     *              {@code false} and gets the call back as soon as the node listens
     */
    public static int run(String[] args, PrintStream out, PrintStream err, boolean block) {
        if (args.length == 0) {
            return usage(err);
        }
        Map<String, List<String>> options = new HashMap<>();
        for (int i = 1; i < args.length; i += 2) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                return usage(err);
            }
            options.computeIfAbsent(args[i].substring(2), _ -> new ArrayList<>()).add(args[i + 1]);
        }
        try {
            return switch (args[0]) {
                case "init" -> init(options, out);
                case "run" -> start(options, out, block);
                case "restore" -> restore(options, out);
                case "recover-majority" -> recover(options, out, err);
                default -> usage(err);
            };
        } catch (IllegalArgumentException e) {
            err.println("error: " + e.getMessage());
            return usage(err);
        } catch (IOException e) {
            err.println("error: " + e.getMessage());
            return 1;
        }
    }

    private static int usage(PrintStream err) {
        err.println("usage: FreyjaNode init --data DIR --cluster HEX --node N [--voters N,N,...]"
                + " [--cluster-version V]");
        err.println("       FreyjaNode restore --backup DIR --data DIR --cluster HEX --node N");
        err.println("       FreyjaNode recover-majority --data DIR --cluster HEX --i-accept-data-loss yes");
        err.println("       FreyjaNode run --data DIR --peer HOST:PORT --client HOST:PORT --admin HOST:PORT"
                + " [--seed N=HOST:PORT]... [--tick-ms MILLIS] [--status HOST:PORT]");
        err.println("                      [--segment-bytes N] [--snapshot-log-bytes N] [--snapshot-retain N]"
                + " [--quota-bytes N] [--min-free-bytes N]");
        err.println("                      --insecure yes | --tls-key FILE [--tls-password-file FILE]"
                + " --peer-trust FILE [--peer-name A=V]");
        err.println("                      [--client-trust FILE [--client-name A=V]] [--client-tokens FILE]"
                + " --admin-trust FILE [--admin-name A=V]");
        return 2;
    }

    private static String one(Map<String, List<String>> options, String name, String fallback) {
        List<String> values = options.get(name);
        if (values == null) {
            if (fallback == null) {
                throw new IllegalArgumentException("--" + name + " is missing");
            }
            return fallback;
        }
        if (values.size() != 1) {
            throw new IllegalArgumentException("--" + name + " is given more than once");
        }
        return values.getFirst();
    }

    private static int init(Map<String, List<String>> options, PrintStream out) throws IOException {
        Path data = Path.of(one(options, "data", null));
        long cluster = Long.parseUnsignedLong(one(options, "cluster", null), 16);
        int node = Integer.parseInt(one(options, "node", null));
        TreeSet<Integer> voters = new TreeSet<>();
        for (String voter : one(options, "voters", "").split(",")) {
            if (!voter.isBlank()) {
                voters.add(Integer.parseInt(voter.trim()));
            }
        }
        int version = Integer.parseInt(one(options, "cluster-version", "1"));
        Node.initialize(new RealFileSystem(), data, new NodeIdentity(cluster, node),
                voters.isEmpty() ? ClusterConfig.EMPTY : ClusterConfig.ofVoters(voters.toArray(Integer[]::new)),
                version);
        out.println("prepared " + data + " for node " + node + " of cluster " + Long.toHexString(cluster)
                + (voters.isEmpty() ? ", to join a cluster" : ", voters " + voters));
        return 0;
    }

    private static int restore(Map<String, List<String>> options, PrintStream out) throws IOException {
        Path data = Path.of(one(options, "data", null));
        long cluster = Long.parseUnsignedLong(one(options, "cluster", null), 16);
        int node = Integer.parseInt(one(options, "node", null));
        Backups.Manifest manifest = Backups.restore(new RealFileSystem(), Path.of(one(options, "backup", null)),
                data, new NodeIdentity(cluster, node));
        out.println("restored " + data + " as node " + node + ", the only voter of the new cluster "
                + Long.toHexString(cluster) + ", from a backup of cluster " + Long.toHexString(manifest.clusterId())
                + " through index " + manifest.meta().lastIncludedIndex());
        out.println("add further members as learners");
        return 0;
    }

    private static int recover(Map<String, List<String>> options, PrintStream out, PrintStream err)
            throws IOException {
        Path data = Path.of(one(options, "data", null));
        long cluster = Long.parseUnsignedLong(one(options, "cluster", null), 16);
        out.println("This starts a new cluster from the one node in " + data + ".");
        out.println("It is NOT SAFE: whatever the lost nodes had acknowledged and this node never received");
        out.println("is gone, and nobody can say whether there was such a thing.");
        if (!one(options, "i-accept-data-loss", "no").equals("yes")) {
            err.println("error: not done; say --i-accept-data-loss yes if this is understood");
            return 1;
        }
        long last = Backups.recoverFromLostMajority(new RealFileSystem(), data, cluster);
        out.println("done: the node is the only voter of the new cluster " + Long.toHexString(cluster)
                + "; its log ended at index " + last);
        return 0;
    }

    private static int start(Map<String, List<String>> options, PrintStream out, boolean block) throws IOException {
        Path data = Path.of(one(options, "data", null));
        Map<Integer, String> seeds = new HashMap<>();
        for (String seed : options.getOrDefault("seed", List.of())) {
            int equals = seed.indexOf('=');
            if (equals <= 0) {
                throw new IllegalArgumentException("--seed takes N=HOST:PORT, not " + seed);
            }
            NodeServer.parse(seed.substring(equals + 1));
            seeds.put(Integer.parseInt(seed.substring(0, equals)), seed.substring(equals + 1));
        }
        int tick = Integer.parseInt(one(options, "tick-ms", "100"));
        Security security = security(options);
        Node.Limits limits = Node.Limits.defaults();
        limits = new Node.Limits(limits.maxApplyLag(), size(options, "quota-bytes", limits.quotaBytes()),
                size(options, "min-free-bytes", limits.minFreeBytes()), limits.alarmCheckTicks(),
                limits.slowSyncNanos());
        Node.Options node = new Node.Options(RaftConfig.defaults(0, System.nanoTime()),
                size(options, "segment-bytes", 64L << 20), 4096, size(options, "snapshot-log-bytes", 1L << 30),
                size(options, "snapshot-retain", 10_000), SnapshotTransfers.Options.defaults(), limits, Clock.SYSTEM);
        NodeServer server = NodeServer.start(new RealFileSystem(), new NodeServer.Settings(data,
                resolve(one(options, "peer", null)), resolve(one(options, "client", null)),
                resolve(one(options, "admin", null)), seeds, node,
                // The engine applies whatever is committed; low free space is answered by the alarm.
                new StateMachineStore.Options(new EngineOptions().minFreeSpace(0).walBytesPerSync(1024 * 1024), 60_000),
                tick,
                options.containsKey("status") ? resolve(one(options, "status", null)) : null, security));
        out.println("READY node=" + server.identity().nodeId() + " peer=" + server.peerPort() + " client="
                + server.clientPort() + " admin=" + server.adminPort()
                + (server.statusPort() < 0 ? "" : " status=" + server.statusPort()));
        out.flush();
        if (!block) {
            return 0;
        }
        CountDownLatch ended = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException e) {
                // the process ends anyway
            }
            ended.countDown();
        }, "freyja-shutdown"));
        try {
            ended.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return 0;
    }

    private static final List<String> TLS_OPTIONS = List.of("tls-key", "tls-password-file", "peer-trust",
            "peer-name", "client-trust", "client-name", "client-tokens", "admin-trust", "admin-name");

    private static Security security(Map<String, List<String>> options) throws IOException {
        if (options.containsKey("insecure")) {
            if (!one(options, "insecure", null).equals("yes")) {
                throw new IllegalArgumentException("--insecure takes yes");
            }
            for (String name : TLS_OPTIONS) {
                if (options.containsKey(name)) {
                    throw new IllegalArgumentException("--insecure and --" + name + " exclude each other");
                }
            }
            return Security.INSECURE;
        }
        if (!options.containsKey("tls-key")) {
            throw new IllegalArgumentException("--tls-key is missing; a node without TLS must be told --insecure yes");
        }
        return Security.open(new Security.Files(file(options, "tls-key"), file(options, "tls-password-file"),
                file(options, "peer-trust"), text(options, "peer-name"), file(options, "client-trust"),
                text(options, "client-name"), file(options, "client-tokens"), file(options, "admin-trust"),
                text(options, "admin-name")));
    }

    private static String text(Map<String, List<String>> options, String name) {
        return options.containsKey(name) ? one(options, name, null) : null;
    }

    private static Path file(Map<String, List<String>> options, String name) {
        return options.containsKey(name) ? Path.of(one(options, name, null)) : null;
    }

    private static long size(Map<String, List<String>> options, String name, long fallback) {
        long value = Long.parseLong(one(options, name, Long.toString(fallback)));
        if (value < 0) {
            throw new IllegalArgumentException("--" + name + " is negative");
        }
        return value;
    }

    private static InetSocketAddress resolve(String address) {
        InetSocketAddress parsed = NodeServer.parse(address);
        return new InetSocketAddress(parsed.getHostString(), parsed.getPort());
    }
}
