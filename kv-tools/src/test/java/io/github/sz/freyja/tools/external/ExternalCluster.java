package io.github.sz.freyja.tools.external;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.github.sz.freyja.node.FreyjaNode;
import io.github.sz.freyja.tools.FreyjaAdmin;

/**
 * A cluster of node processes on real files, with a proxy on every direction of every link
 * between two nodes (docs/spec/verification.md, section 4).
 *
 * <p>The ports of a node stay the same over its restarts. A node sends to a peer through the
 * proxy of that direction, which it knows as the address of the peer it was started with.
 */
final class ExternalCluster implements Closeable {

    private static final String CLUSTER_ID = "e7e";
    private static final boolean LINUX = System.getProperty("os.name").toLowerCase().contains("linux");

    /** One node: its directory, its ports, and its process while it runs. */
    final class NodeProcess {
        final int id;
        final Path data;
        final Path log;
        final int peerPort;
        final int clientPort;
        final int adminPort;
        private Process process;
        private boolean paused;

        private NodeProcess(int id, Path data, Path log, int[] ports) {
            this.id = id;
            this.data = data;
            this.log = log;
            this.peerPort = ports[0];
            this.clientPort = ports[1];
            this.adminPort = ports[2];
        }

        boolean running() {
            return process != null && process.isAlive();
        }

        boolean paused() {
            return paused;
        }

        String clientAddress() {
            return "127.0.0.1:" + clientPort;
        }

        /** Starts the process and waits until it listens for administrators. */
        void start() throws IOException, InterruptedException {
            if (running()) {
                return;
            }
            List<String> command = new ArrayList<>(List.of(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(), "--enable-preview",
                    "-Xmx192m", "-cp", System.getProperty("java.class.path"), FreyjaNode.class.getName(), "run",
                    "--data", data.toString(), "--peer", "127.0.0.1:" + peerPort, "--client",
                    "127.0.0.1:" + clientPort, "--admin", "127.0.0.1:" + adminPort, "--tick-ms", "20", "--insecure", "yes"));
            command.addAll(nodeOptions);
            for (NodeProcess other : nodes) {
                if (other != this) {
                    command.add("--seed");
                    command.add(other.id + "=127.0.0.1:" + proxies[id][other.id].port());
                }
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (true) {
                process = new ProcessBuilder(command).redirectErrorStream(true)
                        .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile())).start();
                paused = false;
                while (process.isAlive() && !listens(adminPort)) {
                    if (System.nanoTime() > deadline) {
                        throw new IOException("node " + id + " does not listen; see " + log);
                    }
                    Thread.sleep(20);
                }
                if (process.isAlive()) {
                    return;
                }
                // It ended at once: a port of its last life may not be free yet.
                if (System.nanoTime() > deadline) {
                    throw new IOException("node " + id + " does not start; see " + log);
                }
                Thread.sleep(200);
            }
        }

        /** Ends the process without any warning to it. */
        void kill() throws InterruptedException {
            if (process != null) {
                process.destroyForcibly();
                process.waitFor();
                paused = false;
            }
        }

        /** Asks the process to end and waits for it. */
        void stop() throws InterruptedException {
            if (process != null) {
                resume();
                process.destroy();
                if (!process.waitFor(30, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor();
                }
            }
        }

        /** Stops the process where it is: it keeps its sockets and its files and does nothing. */
        void pause() throws InterruptedException {
            if (running() && !paused) {
                signal("-STOP");
                paused = true;
            }
        }

        void resume() throws InterruptedException {
            if (running() && paused) {
                signal("-CONT");
                paused = false;
            }
        }

        private void signal(String signal) throws InterruptedException {
            try {
                new ProcessBuilder("kill", signal, Long.toString(process.pid())).inheritIO().start().waitFor();
            } catch (IOException e) {
                throw new IllegalStateException("cannot send a signal", e);
            }
        }

        /** Runs a command of an administrator against this node: its exit code, then its output. */
        String admin(String... command) {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(bytes, true, StandardCharsets.UTF_8);
            String[] args = new String[command.length + 1];
            args[0] = "127.0.0.1:" + adminPort;
            System.arraycopy(command, 0, args, 1, command.length);
            int code = FreyjaAdmin.run(args, out, out);
            return code + "\n" + bytes.toString(StandardCharsets.UTF_8);
        }
    }

    final List<NodeProcess> nodes = new ArrayList<>();
    /** {@code proxies[from][to]}: what {@code from} sends to {@code to} passes through it. */
    final FaultProxy[][] proxies;
    private final List<String> nodeOptions;

    /**
     * Prepares the data directories and the proxies; no process runs yet.
     *
     * @param directories the data directory of every node
     * @param logs        where the output of the nodes goes
     * @param nodeOptions further options of {@code FreyjaNode run} for every node
     */
    ExternalCluster(List<Path> directories, Path logs, List<String> nodeOptions) throws IOException {
        this.nodeOptions = List.copyOf(nodeOptions);
        int n = directories.size();
        this.proxies = new FaultProxy[n][n];
        StringBuilder voters = new StringBuilder();
        for (int id = 0; id < n; id++) {
            voters.append(id == 0 ? "" : ",").append(id);
        }
        Files.createDirectories(logs);
        // The ports of the nodes stay taken until every proxy has a port of its own: a proxy
        // that got a port a node was promised would keep that node from ever starting.
        List<ServerSocket> promised = new ArrayList<>();
        try {
            prepare(directories, logs, voters.toString(), promised);
        } finally {
            for (ServerSocket socket : promised) {
                socket.close();
            }
        }
    }

    private void prepare(List<Path> directories, Path logs, String voters, List<ServerSocket> promised)
            throws IOException {
        int n = directories.size();
        for (int id = 0; id < n; id++) {
            PrintStream quiet = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
            Path data = directories.get(id);
            if (FreyjaNode.run(new String[] {"init", "--data", data.toString(), "--cluster", CLUSTER_ID, "--node",
                    Integer.toString(id), "--voters", voters}, quiet, quiet, false) != 0) {
                throw new IOException("cannot prepare " + data);
            }
            nodes.add(new NodeProcess(id, data, logs.resolve("node-" + id + ".log"), freePorts(3, promised)));
        }
        for (int from = 0; from < n; from++) {
            for (int to = 0; to < n; to++) {
                if (from != to) {
                    proxies[from][to] = new FaultProxy(nodes.get(to).peerPort);
                }
            }
        }
    }

    static boolean linux() {
        return LINUX;
    }

    /** Starts every node and tells the cluster where clients reach each of them. */
    void start() throws IOException, InterruptedException {
        for (NodeProcess node : nodes) {
            node.start();
        }
        NodeProcess leader = awaitLeader(60);
        for (NodeProcess node : nodes) {
            // No address for peers: every node keeps the proxies it was started with.
            String answer = leader.admin("set-address", Integer.toString(node.id), "", node.clientAddress());
            if (!answer.startsWith("0\n")) {
                throw new IOException("set-address refused: " + answer);
            }
        }
    }

    /** The node that says it leads, or {@code null}. Only nodes that run and are not paused are asked. */
    NodeProcess leader() {
        for (NodeProcess node : nodes) {
            if (node.running() && !node.paused() && node.admin("status").contains("role:            LEADER")) {
                return node;
            }
        }
        return null;
    }

    NodeProcess awaitLeader(int seconds) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (true) {
            NodeProcess leader = leader();
            if (leader != null) {
                return leader;
            }
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no leader within " + seconds + " seconds");
            }
            Thread.sleep(50);
        }
    }

    String[] clientEndpoints() {
        return nodes.stream().map(NodeProcess::clientAddress).toArray(String[]::new);
    }

    /** Nothing passes between the two nodes, in either direction. */
    void cut(int a, int b) {
        proxies[a][b].block();
        proxies[b][a].block();
    }

    /** Nothing that {@code from} sends reaches {@code to}; the other direction is untouched. */
    void cutOneWay(int from, int to) {
        proxies[from][to].block();
    }

    /** The node hears nobody and nobody hears it. */
    void isolate(int node) {
        for (NodeProcess other : nodes) {
            if (other.id != node) {
                cut(node, other.id);
            }
        }
    }

    /** The network is well again, the processes that were paused run again. */
    void heal() throws InterruptedException {
        for (FaultProxy[] row : proxies) {
            for (FaultProxy proxy : row) {
                if (proxy != null) {
                    proxy.heal();
                }
            }
        }
        for (NodeProcess node : nodes) {
            node.resume();
        }
    }

    @Override
    public void close() throws IOException {
        for (NodeProcess node : nodes) {
            try {
                if (node.process != null) {
                    node.resume();
                    node.kill();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        for (FaultProxy[] row : proxies) {
            for (FaultProxy proxy : row) {
                if (proxy != null) {
                    proxy.close();
                }
            }
        }
    }

    private static boolean listens(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 200);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** Finds ports that are free and keeps them taken with sockets that the caller closes. */
    private static int[] freePorts(int count, List<ServerSocket> taken) throws IOException {
        int[] ports = new int[count];
        for (int i = 0; i < count; i++) {
            ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
            taken.add(socket);
            ports[i] = socket.getLocalPort();
        }
        return ports;
    }
}
