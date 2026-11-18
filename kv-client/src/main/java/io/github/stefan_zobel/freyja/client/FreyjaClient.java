package io.github.stefan_zobel.freyja.client;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongFunction;

import io.github.stefan_zobel.freyja.common.DecodeException;
import io.github.stefan_zobel.freyja.common.Version;
import io.github.stefan_zobel.freyja.common.kv.ClientCodec;
import io.github.stefan_zobel.freyja.common.kv.ClientRequest;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.NotLeader;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Pairs;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Rejected;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Result;
import io.github.stefan_zobel.freyja.common.kv.ClientResponse.Value;
import io.github.stefan_zobel.freyja.common.kv.Command;
import io.github.stefan_zobel.freyja.common.kv.Command.Condition;
import io.github.stefan_zobel.freyja.common.kv.Command.Delete;
import io.github.stefan_zobel.freyja.common.kv.Command.Mutation;
import io.github.stefan_zobel.freyja.common.kv.Command.Put;
import io.github.stefan_zobel.freyja.common.kv.CommandResult;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.Failed;
import io.github.stefan_zobel.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.stefan_zobel.freyja.common.wire.Frames;
import io.github.stefan_zobel.freyja.common.wire.Handshake;

/**
 * A client of a Freyja cluster (docs/spec/node.md, section 9).
 *
 * <p>The client finds the leader from any node it can reach and follows redirects. It opens a
 * session, numbers its commands, and sends a command again, with the same number, until it is
 * answered or its time has passed: however often a command reaches the cluster, it takes
 * effect once. A command whose outcome is not known when its time has passed is reported as
 * {@link FreyjaException.UnknownOutcome}, never as failed.
 *
 * <p>A client handles one request at a time; calls from several threads take turns. A session
 * allows no more: the cluster keeps the answer to the last command of a session only. Who
 * needs requests in parallel opens several clients.
 *
 * <p>Reads are linearizable unless the method says otherwise.
 */
public final class FreyjaClient implements Closeable {

    /**
     * @param endpoints            the addresses of nodes for clients, as {@code host:port};
     *                             one that can be reached is enough
     * @param clusterId            the cluster the client means to talk to; 0 if it does not
     *                             care
     * @param requestTimeoutMillis how long a request may take, with all its retries
     * @param attemptTimeoutMillis how long the client waits for the answer to one attempt
     * @param backoffMillis        the first pause after a node had no time; it doubles up to
     *                             {@code maxBackoffMillis}
     * @param maxBackoffMillis     the longest pause
     * @param connectTimeoutMillis how long connecting to a node may take
     * @param keepAliveMillis      after this much idle time the client tells the cluster that
     *                             it is alive, so that its session does not expire; 0 for
     *                             never
     */
    public record Options(List<String> endpoints, long clusterId, int requestTimeoutMillis,
                          int attemptTimeoutMillis, int backoffMillis, int maxBackoffMillis,
                          int connectTimeoutMillis, int keepAliveMillis) {
        public Options {
            endpoints = List.copyOf(endpoints);
            if (endpoints.isEmpty() || requestTimeoutMillis < 1 || attemptTimeoutMillis < 1 || backoffMillis < 1
                    || maxBackoffMillis < backoffMillis || connectTimeoutMillis < 1 || keepAliveMillis < 0) {
                throw new IllegalArgumentException("invalid client settings");
            }
            for (String endpoint : endpoints) {
                address(endpoint);
            }
        }

        /** The defaults: 10 s per request, 2 s per attempt, pauses of 20 to 500 ms, keep alive after 10 s. */
        public static Options of(String... endpoints) {
            return new Options(List.of(endpoints), 0, 10_000, 2_000, 20, 500, 1_000, 10_000);
        }

        public Options withRequestTimeout(int millis) {
            return new Options(endpoints, clusterId, millis, Math.min(attemptTimeoutMillis, millis), backoffMillis,
                    maxBackoffMillis, connectTimeoutMillis, keepAliveMillis);
        }

        public Options withKeepAlive(int millis) {
            return new Options(endpoints, clusterId, requestTimeoutMillis, attemptTimeoutMillis, backoffMillis,
                    maxBackoffMillis, connectTimeoutMillis, millis);
        }
    }

    /** A value with the log index of the command that wrote it. */
    public record Versioned(byte[] value, long modIndex) {
    }

    /** One pair of a scan. */
    public record Entry(byte[] key, byte[] value, long modIndex) {
    }

    private final Options options;
    private final ReentrantLock lock = new ReentrantLock();
    private Socket socket;
    private InputStream in;
    private OutputStream out;
    /** The node to ask next, as {@code host:port}. */
    private String target;
    private int nextEndpoint;
    private long session;
    private long sequence;
    private long requestId;
    private long lastUsedNanos = System.nanoTime();
    private volatile boolean closed;
    private Thread keepAlive;

    private FreyjaClient(Options options) {
        this.options = options;
        this.target = options.endpoints().getFirst();
    }

    /** Creates a client. It connects and opens its session with its first request. */
    public static FreyjaClient open(Options options) {
        FreyjaClient client = new FreyjaClient(options);
        if (options.keepAliveMillis() > 0) {
            client.keepAlive = Thread.ofVirtual().name("freyja-client-keepalive").start(client::keepAliveLoop);
        }
        return client;
    }

    // ------------------------------------------------------------------ operations

    /** Sets the value of a key. */
    public void put(byte[] key, byte[] value) {
        write(List.of(new Put(key, value)));
    }

    /** Removes a key. Removing a key that is not there has no effect. */
    public void delete(byte[] key) {
        write(List.of(new Delete(key)));
    }

    /** Applies puts and deletes as one batch: all of them or none. */
    public void write(List<Mutation> mutations) {
        List<Mutation> batch = List.copyOf(mutations);
        execute(seq -> new Command.Write(session, seq, batch));
    }

    /**
     * Applies {@code then} if every condition holds, otherwise {@code otherwise}.
     *
     * @return whether the conditions held
     */
    public boolean conditional(List<Condition> conditions, List<Mutation> then, List<Mutation> otherwise) {
        List<Condition> all = List.copyOf(conditions);
        List<Mutation> yes = List.copyOf(then);
        List<Mutation> no = List.copyOf(otherwise);
        CommandResult result = execute(seq -> new Command.Conditional(session, seq, all, yes, no));
        return ((ConditionalApplied) result).conditionsHeld();
    }

    /** Sets the value of a key if the key has no value. */
    public boolean putIfAbsent(byte[] key, byte[] value) {
        return conditional(List.of(Condition.notExists(key)), List.of(new Put(key, value)), List.of());
    }

    /** Sets the value of a key if it was last written by the command with this log index. */
    public boolean putIfUnchanged(byte[] key, byte[] value, long modIndex) {
        return conditional(List.of(Condition.modIndexEquals(key, modIndex)), List.of(new Put(key, value)),
                List.of());
    }

    /** Reads a key. */
    public Optional<Versioned> get(byte[] key) {
        return get(key, false);
    }

    /**
     * Reads a key from whatever node answers first. The answer may be older than what a
     * linearizable read gives.
     */
    public Optional<Versioned> getStale(byte[] key) {
        return get(key, true);
    }

    private Optional<Versioned> get(byte[] key, boolean stale) {
        Value value = (Value) read(new ClientRequest.Get(Objects.requireNonNull(key, "key"), stale));
        return value.found() ? Optional.of(new Versioned(value.value(), value.modIndex())) : Optional.empty();
    }

    /**
     * Reads the keys from {@code start} (inclusive) to {@code end} (exclusive) in ascending
     * order, at most {@code limit} of them.
     *
     * @param start an empty array for no lower bound
     * @param end   an empty array for no upper bound
     */
    public List<Entry> scan(byte[] start, byte[] end, int limit) {
        return scan(start, end, limit, false, false);
    }

    /**
     * Reads the keys from {@code start} (inclusive) to {@code end} (exclusive) in descending
     * order, at most {@code limit} of them: the largest keys of the range.
     *
     * @param start an empty array for no lower bound
     * @param end   an empty array for no upper bound
     */
    public List<Entry> scanReverse(byte[] start, byte[] end, int limit) {
        return scan(start, end, limit, true, false);
    }

    /**
     * Reads a range in ascending order from whatever node answers first. The answer may be
     * older than what a linearizable scan gives.
     */
    public List<Entry> scanStale(byte[] start, byte[] end, int limit) {
        return scan(start, end, limit, false, true);
    }

    private List<Entry> scan(byte[] start, byte[] end, int limit, boolean reverse, boolean stale) {
        Pairs pairs = (Pairs) read(new ClientRequest.Scan(start, end, limit, reverse, stale));
        List<Entry> entries = new ArrayList<>(pairs.pairs().size());
        for (ClientResponse.Pair pair : pairs.pairs()) {
            entries.add(new Entry(pair.key(), pair.value(), pair.modIndex()));
        }
        return entries;
    }

    /** The id of the session of this client; 0 before its first command. */
    public long sessionId() {
        lock.lock();
        try {
            return session;
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------ requests

    private ClientResponse read(ClientRequest request) {
        lock.lock();
        try {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.requestTimeoutMillis());
            Exchange exchange = new Exchange();
            ClientResponse response = exchange(request, deadline, exchange);
            if (response == null) {
                throw new FreyjaException.TimedOut("no answer to " + request.getClass().getSimpleName()
                        + " within " + options.requestTimeoutMillis() + " ms", exchange.failure);
            }
            return response;
        } finally {
            lastUsedNanos = System.nanoTime();
            lock.unlock();
        }
    }

    /** Runs a command of the session through the cluster; {@code command} builds it from its number. */
    private CommandResult execute(LongFunction<Command> command) {
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("the client is closed");
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.requestTimeoutMillis());
            ensureSession(deadline);
            long number = ++sequence;
            Exchange exchange = new Exchange();
            while (true) {
                ClientResponse response = exchange(new ClientRequest.Execute(command.apply(number)), deadline,
                        exchange);
                if (response == null) {
                    if (exchange.sent == 0) {
                        sequence--; // no node ever saw it
                        throw new FreyjaException.TimedOut("no node took the command within "
                                + options.requestTimeoutMillis() + " ms", exchange.failure);
                    }
                    throw new FreyjaException.UnknownOutcome("the command was sent " + exchange.sent
                            + " times and not answered within " + options.requestTimeoutMillis()
                            + " ms; it may have taken effect", exchange.failure);
                }
                CommandResult result = ((Result) response).result();
                if (!(result instanceof Failed failed)) {
                    return result;
                }
                switch (failed.failure()) {
                    case NO_SUCH_SESSION -> {
                        session = 0;
                        if (exchange.sent > 1) {
                            // An earlier attempt may have taken effect before the session expired.
                            throw new FreyjaException.UnknownOutcome("the session expired while the command"
                                    + " was on its way; it may have taken effect", null);
                        }
                        // The one attempt did nothing. A new session, and the command again.
                        ensureSession(deadline);
                        number = ++sequence;
                        exchange = new Exchange();
                    }
                    case NO_SPACE -> {
                        sequence--; // the command did not use its number up
                        throw new FreyjaException.NoSpace("the cluster is short of space and accepts only deletes");
                    }
                    case INVALID -> throw new FreyjaException.Refused("the cluster refused the command as invalid:"
                            + " a key or a value is too long, or a key is empty");
                    default -> throw new IllegalStateException("the cluster answered " + failed.failure()
                            + " to command " + number + " of session " + session);
                }
            }
        } finally {
            lastUsedNanos = System.nanoTime();
            lock.unlock();
        }
    }

    private void ensureSession(long deadline) {
        if (session != 0) {
            return;
        }
        Exchange exchange = new Exchange();
        ClientResponse response = exchange(new ClientRequest.Execute(new Command.RegisterSession()), deadline,
                exchange);
        if (response == null) {
            throw new FreyjaException.TimedOut("no session within " + options.requestTimeoutMillis() + " ms",
                    exchange.failure);
        }
        if (!(((Result) response).result() instanceof SessionRegistered registered)) {
            throw new IllegalStateException("the cluster answered " + response + " to the opening of a session");
        }
        session = registered.sessionId();
        sequence = 0;
    }

    /** What happened to one request across its attempts. */
    private static final class Exchange {
        /** How often the request was written to a connection. */
        int sent;
        /** The failure of the last attempt that failed. */
        Exception failure;
    }

    /**
     * Sends a request until a node answers it for good or the deadline has passed.
     *
     * @return the answer: a result, a value, or pairs; {@code null} if the deadline passed
     * @throws FreyjaException.Refused if a node refused the request for good
     */
    private ClientResponse exchange(ClientRequest request, long deadline, Exchange exchange) {
        int backoff = options.backoffMillis();
        while (!closed) {
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remaining <= 0) {
                return null;
            }
            try {
                connect((int) Math.min(remaining, options.connectTimeoutMillis()));
                int timeout = (int) Math.min(remaining, options.attemptTimeoutMillis());
                long id = ++requestId;
                Frames.write(out, ClientCodec.encode(id, timeout, request));
                out.flush();
                exchange.sent++;
                // The node drops a request whose time has passed; a little longer, and no
                // answer will come.
                socket.setSoTimeout(timeout + 250);
                ClientResponse response = null;
                while (response == null) {
                    byte[] frame = Frames.read(in, 64 << 20);
                    if (frame == null) {
                        throw new IOException("the node closed the connection");
                    }
                    ClientCodec.ResponseEnvelope envelope = ClientCodec.decodeResponse(frame);
                    if (envelope.requestId() == id) {
                        response = envelope.response();
                    }
                }
                switch (response) {
                    case NotLeader redirect -> {
                        disconnect();
                        if (redirect.leaderAddress().isEmpty()) {
                            rotate();
                            pause(backoff, deadline);
                            backoff = Math.min(backoff * 2, options.maxBackoffMillis());
                        } else {
                            target = redirect.leaderAddress();
                        }
                    }
                    case Rejected rejected -> {
                        switch (rejected.reason()) {
                            case INVALID, UNSUPPORTED -> throw new FreyjaException.Refused(
                                    "the node refused the request: " + rejected.reason());
                            case UNAVAILABLE -> {
                                disconnect();
                                rotate();
                            }
                            default -> {
                                // overloaded, or too late for this attempt: the same node again
                            }
                        }
                        pause(backoff, deadline);
                        backoff = Math.min(backoff * 2, options.maxBackoffMillis());
                    }
                    default -> {
                        return response;
                    }
                }
            } catch (IOException | DecodeException e) {
                exchange.failure = e;
                disconnect();
                rotate();
                pause(backoff, deadline);
                backoff = Math.min(backoff * 2, options.maxBackoffMillis());
            }
        }
        return null;
    }

    // ------------------------------------------------------------------ the connection

    private static InetSocketAddress address(String endpoint) {
        int colon = endpoint.lastIndexOf(':');
        if (colon <= 0 || colon == endpoint.length() - 1) {
            throw new IllegalArgumentException("not host:port: " + endpoint);
        }
        return InetSocketAddress.createUnresolved(endpoint.substring(0, colon),
                Integer.parseInt(endpoint.substring(colon + 1)));
    }

    private void connect(int timeoutMillis) throws IOException {
        if (socket != null) {
            return;
        }
        InetSocketAddress address = address(target);
        Socket fresh = new Socket();
        try {
            fresh.setTcpNoDelay(true);
            fresh.connect(new InetSocketAddress(address.getHostString(), address.getPort()), Math.max(1, timeoutMillis));
            fresh.setSoTimeout(options.connectTimeoutMillis());
            OutputStream stream = new BufferedOutputStream(fresh.getOutputStream(), 64 * 1024);
            InputStream input = new BufferedInputStream(fresh.getInputStream(), 64 * 1024);
            Frames.write(stream, new Handshake(Handshake.CLIENT, options.clusterId(), -1, Version.SOFTWARE).encode());
            stream.flush();
            byte[] answer = Frames.read(input, Handshake.MAX_BYTES);
            if (answer == null) {
                throw new IOException(target + " closed the connection during the handshake");
            }
            Handshake node = Handshake.decode(answer);
            if (options.clusterId() != 0 && node.clusterId() != options.clusterId()) {
                throw new IOException(target + " belongs to another cluster");
            }
            socket = fresh;
            in = input;
            out = stream;
        } catch (IOException | DecodeException e) {
            fresh.close();
            throw e;
        }
    }

    private void disconnect() {
        Socket old = socket;
        socket = null;
        in = null;
        out = null;
        if (old != null) {
            try {
                old.close();
            } catch (IOException e) {
                // closed already
            }
        }
    }

    /** The next node of the list is asked next. */
    private void rotate() {
        nextEndpoint = (nextEndpoint + 1) % options.endpoints().size();
        target = options.endpoints().get(nextEndpoint);
    }

    private void pause(int millis, long deadline) {
        long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        long wait = Math.min(millis, remaining);
        if (wait > 0) {
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new FreyjaException("interrupted while waiting to ask again", e);
            }
        }
    }

    // ------------------------------------------------------------------ the session

    private void keepAliveLoop() {
        long interval = TimeUnit.MILLISECONDS.toNanos(options.keepAliveMillis());
        while (!closed) {
            try {
                Thread.sleep(Math.max(10, options.keepAliveMillis() / 4));
            } catch (InterruptedException e) {
                return;
            }
            if (!lock.tryLock()) {
                continue; // a request is on its way: the session is alive
            }
            try {
                if (!closed && session != 0 && System.nanoTime() - lastUsedNanos >= interval) {
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(options.attemptTimeoutMillis());
                    ClientResponse response = exchange(
                            new ClientRequest.Execute(new Command.KeepAlive(session)), deadline, new Exchange());
                    if (response instanceof Result result && result.result() instanceof Failed) {
                        session = 0; // it has expired; the next command opens a new one
                    }
                    lastUsedNanos = System.nanoTime();
                }
            } catch (RuntimeException e) {
                // the next round tries again
            } finally {
                lock.unlock();
            }
        }
    }

    /** Ends the session and closes the connection. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        Thread alive = keepAlive;
        if (alive != null) {
            alive.interrupt();
        }
        lock.lock();
        try {
            if (session != 0) {
                try {
                    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(
                            Math.min(1_000, options.requestTimeoutMillis()));
                    exchange(new ClientRequest.Execute(new Command.CloseSession(session)), deadline, new Exchange());
                } catch (RuntimeException e) {
                    // the session expires on its own
                }
                session = 0;
            }
            closed = true;
            disconnect();
        } finally {
            lock.unlock();
        }
    }
}
