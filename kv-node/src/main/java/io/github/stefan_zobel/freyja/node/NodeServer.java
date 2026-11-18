package io.github.stefan_zobel.freyja.node;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.github.stefan_zobel.freyja.common.Version;
import io.github.stefan_zobel.freyja.common.wire.Handshake;
import io.github.stefan_zobel.freyja.lsm.fs.StorageFileSystem;
import io.github.stefan_zobel.freyja.node.net.ClientListener;
import io.github.stefan_zobel.freyja.node.net.TcpTransport;
import io.github.stefan_zobel.freyja.node.service.ClientService;
import io.github.stefan_zobel.freyja.node.sm.StateMachineStore;
import io.github.stefan_zobel.freyja.node.store.NodeIdentity;

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
     */
    public record Settings(Path dataDirectory, InetSocketAddress peerListen, InetSocketAddress clientListen,
                           InetSocketAddress adminListen, Map<Integer, String> seedPeers, Node.Options node,
                           StateMachineStore.Options machine, int tickMillis) {
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

    private NodeServer(StorageFileSystem fs, Settings settings) throws IOException {
        identity = NodeIdentity.read(fs, settings.dataDirectory());
        runtime = new NodeRuntime(fs, new NodeRuntime.Config(settings.dataDirectory(), settings.node(),
                settings.machine(), new ClientService.Options(settings.tickMillis(), 1_000, 10_000),
                settings.tickMillis(), 4096, 500, System::currentTimeMillis,
                node -> clientAddresses.getOrDefault(node, ""), this::learned));
        transport = new TcpTransport(identity.clusterId(), identity.nodeId(), Version.SOFTWARE,
                TcpTransport.Options.defaults(), runtime::peerFrame);
        clients = new ClientListener(identity.clusterId(), identity.nodeId(), Version.SOFTWARE, Handshake.CLIENT,
                ClientListener.Options.defaults(), runtime::clientRequest);
        administrators = new ClientListener(identity.clusterId(), identity.nodeId(), Version.SOFTWARE,
                Handshake.ADMIN, ClientListener.Options.defaults(), runtime::adminRequest);
        boolean started = false;
        try {
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

    /** Stops the listeners, the node, and the transport. */
    @Override
    public void close() throws IOException {
        IOException first = null;
        for (Closeable part : new Closeable[] {administrators, clients, runtime, transport}) {
            try {
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
