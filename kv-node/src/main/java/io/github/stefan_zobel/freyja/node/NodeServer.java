package io.github.stefan_zobel.freyja.node;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.Version;
import io.github.stefan_zobel.freyja.common.admin.AdminCodec;
import io.github.stefan_zobel.freyja.common.admin.AdminRequest;
import io.github.stefan_zobel.freyja.common.admin.AdminResponse;
import io.github.stefan_zobel.freyja.common.wire.Handshake;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.node.backup.Backups;
import io.github.stefan_zobel.freyja.node.metrics.MetricsRegistry;
import io.github.stefan_zobel.freyja.node.net.ClientListener;
import io.github.stefan_zobel.freyja.node.net.StatusEndpoint;
import io.github.stefan_zobel.freyja.node.net.TcpTransport;
import io.github.stefan_zobel.freyja.node.service.ClientService;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.snapshot.SnapshotTransfers;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;
import io.github.stefan_zobel.freyja.raft.SnapshotMeta;

/**
 * One node with everything around it: the runtime, the transport to its peers, and the
 * listeners for clients and for administrators (docs/spec/node.md, sections 1 and 8).
 *
 * <p>Where the other nodes listen, the server learns from the replicated state. Until the
 * node has applied that, it uses the addresses it was started with.
 */
public final class NodeServer implements Closeable {

    /**
     * @param dataDirectory the prepared data directory of the node
     * @param peerListen    where to accept connections of peers; port 0 for any free port
     * @param clientListen  where to accept connections of clients
     * @param adminListen   where to accept connections of administrators
     * @param seedPeers     where peers accept connections of peers, by node id, as
     *                      {@code host:port}: what the node knows before the replicated state
     *                      tells it
     * @param node          the settings of the node
     * @param machine       the settings of the state machine
     * @param tickMillis    how long a tick takes
     * @param statusListen  where to serve metrics and health over HTTP; {@code null} for nowhere
     */
    public record Settings(Path dataDirectory, InetSocketAddress peerListen, InetSocketAddress clientListen,
                           InetSocketAddress adminListen, Map<Integer, String> seedPeers, Node.Options node,
                           StateMachineStore.Options machine, int tickMillis, InetSocketAddress statusListen) {
        public Settings {
            seedPeers = Map.copyOf(seedPeers);
        }
    }

    private final NodeIdentity identity;
    private final NodeRuntime runtime;
    private final TcpTransport transport;
    private final ClientListener clients;
    private final ClientListener administrators;
    private final Map<Integer, String> clientAddresses = new ConcurrentHashMap<>();
    private final MetricsRegistry metrics = new MetricsRegistry();
    private final StorageFileSystem fs;
    private final Path dataDirectory;
    private final java.util.concurrent.atomic.AtomicLong backups = new java.util.concurrent.atomic.AtomicLong();
    private StatusEndpoint status;

    private NodeServer(StorageFileSystem fs, Settings settings) throws IOException {
        identity = NodeIdentity.read(fs, settings.dataDirectory());
        // The engine reports to the same registry as the node.
        settings.machine().engine().metrics(metrics);
        runtime = new NodeRuntime(fs, new NodeRuntime.Config(settings.dataDirectory(), settings.node(),
                settings.machine(), new ClientService.Options(settings.tickMillis(), 1_000, 10_000),
                settings.tickMillis(), 4096, 500, System::currentTimeMillis,
                node -> clientAddresses.getOrDefault(node, ""), this::learned));
        transport = new TcpTransport(identity.clusterId(), identity.nodeId(), Version.SOFTWARE,
                TcpTransport.Options.defaults(), runtime::peerFrame);
        clients = new ClientListener(identity.clusterId(), identity.nodeId(), Version.SOFTWARE, Handshake.CLIENT,
                ClientListener.Options.defaults(), runtime::clientRequest);
        this.fs = fs;
        this.dataDirectory = settings.dataDirectory();
        administrators = new ClientListener(identity.clusterId(), identity.nodeId(), Version.SOFTWARE,
                Handshake.ADMIN, ClientListener.Options.defaults(), this::adminRequest);
        runtime.measure(metrics);
        boolean started = false;
        try {
            if (settings.statusListen() != null) {
                status = new StatusEndpoint(settings.statusListen(), this::renderMetrics, runtime::notAlive,
                        runtime::notReady);
            }
            transport.listen(settings.peerListen());
            clients.listen(settings.clientListen());
            administrators.listen(settings.adminListen());
            settings.seedPeers().forEach((node, address) -> {
                if (node != identity.nodeId()) {
                    transport.setPeer(node, parse(address));
                }
            });
            runtime.start(transport);
            started = true;
        } finally {
            if (!started) {
                close();
            }
        }
    }

    /** Starts the node of a prepared data directory. */
    public static NodeServer start(StorageFileSystem fs, Settings settings) throws IOException {
        return new NodeServer(fs, settings);
    }

    /** The replicated state says where a node listens. Called on the thread of the node. */
    private void learned(int node, io.github.stefan_zobel.freyja.node.sm.KvStateMachine.NodeAddress address) {
        if (!address.client().isEmpty()) {
            clientAddresses.put(node, address.client());
        }
        if (node != identity.nodeId() && !address.peer().isEmpty()) {
            try {
                transport.setPeer(node, parse(address.peer()));
            } catch (IllegalArgumentException e) {
                // An address that is no address is not used; the old one stays.
            }
        }
    }

    /**
     * A request of an administrator. A backup is made here, beside the node, because it
     * copies files and must not hold the node up; everything else goes to the node.
     */
    private boolean adminRequest(byte[] frame, ClientService.Reply reply) {
        AdminCodec.RequestEnvelope envelope;
        try {
            envelope = AdminCodec.decodeRequest(frame);
        } catch (DecodeException e) {
            return true; // without a request id there is nobody to answer
        }
        if (!(envelope.request() instanceof AdminRequest.Backup wanted)) {
            return runtime.adminRequest(frame, reply);
        }
        Thread.ofVirtual().name("freyja-backup").start(() -> {
            AdminResponse answer;
            try {
                Backups.Manifest manifest = backup(Path.of(wanted.directory()));
                answer = new AdminResponse.Done("backup through index " + manifest.meta().lastIncludedIndex()
                        + " in " + wanted.directory() + ", " + manifest.files().size() + " files");
            } catch (IOException | RuntimeException e) {
                answer = new AdminResponse.Refused("the backup failed: " + e.getMessage(), -1);
            }
            reply.send(AdminCodec.encode(envelope.requestId(), answer));
        });
        return true;
    }

    /**
     * Writes a backup of the state machine into {@code target}, a directory that does not
     * exist yet (docs/spec/operations.md, section 3). The node takes a checkpoint, which is
     * quick and holds nothing up; the caller's thread copies it with checksums and writes the
     * manifest last.
     */
    public Backups.Manifest backup(Path target) throws IOException {
        Path checkpoint = dataDirectory.resolve(SnapshotTransfers.DIRECTORY)
                .resolve("backup-" + backups.incrementAndGet());
        Object[] taken;
        try {
            taken = runtime.onNode(node -> {
                try {
                    SnapshotMeta meta = node.store().meta();
                    int version = node.store().machine().clusterVersion();
                    node.store().checkpoint(checkpoint);
                    return new Object[] {meta, version};
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }).get(60, java.util.concurrent.TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while the node took its checkpoint", e);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            throw new IOException("the node could not take a checkpoint: " + e.getMessage(), e);
        }
        try {
            return Backups.write(fs, checkpoint, target, identity.clusterId(), identity.nodeId(),
                    (Integer) taken[1], (SnapshotMeta) taken[0]);
        } finally {
            StateMachineStore.removeTree(fs, checkpoint);
        }
    }

    /** Reads {@code host:port}. */
    public static InetSocketAddress parse(String address) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) {
            throw new IllegalArgumentException("not host:port: " + address);
        }
        try {
            return InetSocketAddress.createUnresolved(address.substring(0, colon),
                    Integer.parseInt(address.substring(colon + 1)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("not host:port: " + address);
        }
    }

    public NodeIdentity identity() {
        return identity;
    }

    public NodeRuntime runtime() {
        return runtime;
    }

    public TcpTransport transport() {
        return transport;
    }

    public int peerPort() {
        return transport.port();
    }

    public int clientPort() {
        return clients.port();
    }

    public int adminPort() {
        return administrators.port();
    }

    /** The port of the HTTP endpoint for metrics and health; -1 if there is none. */
    public int statusPort() {
        return status == null ? -1 : status.port();
    }

    /** The metrics registry of the node. */
    public MetricsRegistry metrics() {
        return metrics;
    }

    private String renderMetrics() {
        metrics.gauge("transport.sent", transport.sentCount());
        metrics.gauge("transport.received", transport.receivedCount());
        metrics.gauge("transport.dropped", transport.droppedCount());
        metrics.gauge("transport.refused", transport.refusedCount());
        metrics.gauge("listener.clients.connections", clients.connectionCount());
        metrics.gauge("listener.clients.refused", clients.refusedCount());
        return metrics.render();
    }

    /** Stops the listeners, the node, and the transport. */
    @Override
    public void close() throws IOException {
        IOException first = null;
        for (Closeable part : new Closeable[] {status, administrators, clients, runtime, transport}) {
            try {
                if (part == null) {
                    continue;
                }
                part.close();
            } catch (IOException e) {
                first = first == null ? e : first;
            }
        }
        if (first != null) {
            throw first;
        }
    }
}
