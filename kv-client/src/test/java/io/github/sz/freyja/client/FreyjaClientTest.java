package io.github.sz.freyja.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.github.sz.freyja.common.kv.ClientCodec;
import io.github.sz.freyja.common.kv.ClientRequest;
import io.github.sz.freyja.common.kv.ClientResponse;
import io.github.sz.freyja.common.kv.ClientResponse.Reason;
import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.common.kv.CommandResult.Failure;
import io.github.sz.freyja.common.wire.Frames;
import io.github.sz.freyja.common.wire.Handshake;

/**
 * The client against a node that follows a script (docs/spec/node.md, section 9): what the
 * client does when it is redirected, put off, not answered, or told that its session is gone.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class FreyjaClientTest {

    private static final long CLUSTER = 0xAB;

    /** What the scripted node does with a request instead of answering it. */
    private static final ClientResponse SILENCE = new ClientResponse.Rejected(Reason.EXPIRED);
    private static final ClientResponse HANG_UP = new ClientResponse.Rejected(Reason.UNAVAILABLE);

    /** A node that answers every request as its script says, and remembers what it was asked. */
    private static final class ScriptedNode implements Closeable {
        final ServerSocket server;
        final List<ClientRequest> requests = Collections.synchronizedList(new ArrayList<>());
        volatile Function<ClientRequest, ClientResponse> script;
        volatile int connections;

        ScriptedNode(Function<ClientRequest, ClientResponse> script) throws IOException {
            this.script = script;
            server = new ServerSocket();
            server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            Thread.ofVirtual().start(() -> {
                while (true) {
                    Socket socket;
                    try {
                        socket = server.accept();
                    } catch (IOException e) {
                        return;
                    }
                    connections++;
                    Thread.ofVirtual().start(() -> serve(socket));
                }
            });
        }

        String address() {
            return "127.0.0.1:" + server.getLocalPort();
        }

        private void serve(Socket socket) {
            try (socket) {
                InputStream in = socket.getInputStream();
                OutputStream out = socket.getOutputStream();
                Handshake hello = Handshake.decode(Frames.read(in, Handshake.MAX_BYTES));
                assertEquals(Handshake.CLIENT, hello.role());
                Frames.write(out, new Handshake(Handshake.CLIENT, CLUSTER, 0, "test").encode());
                out.flush();
                for (byte[] frame = Frames.read(in, 1 << 20); frame != null; frame = Frames.read(in, 1 << 20)) {
                    ClientCodec.RequestEnvelope envelope = ClientCodec.decodeRequest(frame);
                    requests.add(envelope.request());
                    ClientResponse response = script.apply(envelope.request());
                    if (response == HANG_UP) {
                        return;
                    }
                    if (response != SILENCE) {
                        Frames.write(out, ClientCodec.encode(envelope.requestId(), response));
                        out.flush();
                    }
                }
            } catch (IOException | RuntimeException e) {
                // the connection ends
            }
        }

        /** The commands the node was asked to execute, in order. */
        List<Command> commands() {
            List<Command> commands = new ArrayList<>();
            synchronized (requests) {
                for (ClientRequest request : requests) {
                    if (request instanceof ClientRequest.Execute execute) {
                        commands.add(execute.command());
                    }
                }
            }
            return commands;
        }

        @Override
        public void close() throws IOException {
            server.close();
        }
    }

    private final List<Closeable> open = new ArrayList<>();

    private ScriptedNode node(Function<ClientRequest, ClientResponse> script) throws IOException {
        ScriptedNode node = new ScriptedNode(script);
        open.add(node);
        return node;
    }

    private FreyjaClient client(int timeoutMillis, ScriptedNode... nodes) {
        String[] endpoints = new String[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            endpoints[i] = nodes[i].address();
        }
        FreyjaClient client = FreyjaClient.open(new FreyjaClient.Options(List.of(endpoints), CLUSTER, timeoutMillis,
                Math.min(300, timeoutMillis), 5, 40, 500, 0));
        open.add(client);
        return client;
    }

    @AfterEach
    void closeAll() throws IOException {
        for (Closeable closeable : open.reversed()) {
            closeable.close();
        }
    }

    private static byte[] b(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    /** A node that serves a session with the id 7 and applies whatever it is asked. */
    private static ClientResponse obliging(ClientRequest request) {
        if (request instanceof ClientRequest.Execute execute) {
            return new ClientResponse.Result(switch (execute.command()) {
                case Command.RegisterSession _ -> new CommandResult.SessionRegistered(7);
                case Command.Conditional _ -> new CommandResult.ConditionalApplied(true);
                case Command.Write _ -> new CommandResult.Written();
                default -> new CommandResult.SessionOk();
            }, 100);
        }
        if (request instanceof ClientRequest.Get) {
            return new ClientResponse.Value(true, b("value"), 5, 9);
        }
        return new ClientResponse.Pairs(List.of(new ClientResponse.Pair(b("a"), b("1"), 3)), 9);
    }

    @Test
    void theClientOpensASessionAndNumbersItsCommands() throws IOException {
        ScriptedNode node = node(FreyjaClientTest::obliging);
        FreyjaClient client = client(5_000, node);
        assertEquals(0, client.sessionId());
        client.put(b("k"), b("v"));
        client.delete(b("k"));
        assertTrue(client.putIfAbsent(b("k"), b("v")));
        assertEquals(7, client.sessionId());
        assertArrayEquals(b("value"), client.get(b("k")).orElseThrow().value());
        assertEquals(5, client.getStale(b("k")).orElseThrow().modIndex());
        assertEquals(1, client.scan(new byte[0], new byte[0], 10).size());

        List<Command> commands = node.commands();
        assertEquals(Command.RegisterSession.class, commands.get(0).getClass());
        assertEquals(new long[] {7, 1}[0], ((Command.Write) commands.get(1)).sessionId());
        assertEquals(1, ((Command.Write) commands.get(1)).sequence());
        assertEquals(2, ((Command.Write) commands.get(2)).sequence());
        assertEquals(3, ((Command.Conditional) commands.get(3)).sequence());
        assertEquals(1, node.connections, "one connection for all of it");

        client.close();
        assertEquals(new Command.CloseSession(7), node.commands().getLast());
        assertThrows(IllegalStateException.class, () -> client.put(b("k"), b("v")));
    }

    @Test
    void theClientFollowsARedirectAndComesBackWhenANodeHasNoTime() throws IOException {
        ScriptedNode leader = node(FreyjaClientTest::obliging);
        ScriptedNode follower = node(request -> new ClientResponse.NotLeader(1, leader.address()));
        FreyjaClient client = client(5_000, follower, leader);
        client.put(b("k"), b("v"));
        assertEquals(1, follower.requests.size(), "asked once, then it knew the leader");
        assertEquals(2, leader.commands().size());

        // The leader has no time, twice; the client waits and asks the same node again.
        int[] refusals = {2};
        leader.script = request -> refusals[0]-- > 0 ? new ClientResponse.Rejected(Reason.OVERLOADED)
                : obliging(request);
        client.put(b("k"), b("w"));
        List<Command> commands = leader.commands();
        assertEquals(5, commands.size());
        for (int i = 2; i < 5; i++) {
            assertEquals(2, ((Command.Write) commands.get(i)).sequence(), "the same command each time");
        }
        assertEquals(1, follower.requests.size());
    }

    @Test
    void aCommandThatIsSentAndNotAnsweredHasAnUnknownOutcome() throws IOException {
        ScriptedNode node = node(FreyjaClientTest::obliging);
        FreyjaClient client = client(700, node);
        client.put(b("k"), b("v"));
        node.script = request -> SILENCE;

        FreyjaException.UnknownOutcome unknown = assertThrows(FreyjaException.UnknownOutcome.class,
                () -> client.put(b("k"), b("lost?")));
        assertTrue(unknown.getMessage().contains("may have taken effect"), unknown.getMessage());
        // A read that is not answered has done nothing.
        assertThrows(FreyjaException.TimedOut.class, () -> client.get(b("k")));

        // The next command has the next number: the one before may still arrive.
        node.script = FreyjaClientTest::obliging;
        client.put(b("k"), b("next"));
        List<Command> commands = node.commands();
        assertEquals(3, ((Command.Write) commands.getLast()).sequence());
        long resent = commands.stream().filter(c -> c instanceof Command.Write w && w.sequence() == 2).count();
        assertTrue(resent >= 2, "the command without an answer was sent again: " + resent);
    }

    @Test
    void aCommandThatNoNodeTookHasNoEffectAndKeepsItsNumber() throws IOException {
        ScriptedNode node = node(FreyjaClientTest::obliging);
        node.close(); // nobody listens
        FreyjaClient client = client(600, node);
        assertThrows(FreyjaException.TimedOut.class, () -> client.put(b("k"), b("w")));
        assertThrows(FreyjaException.TimedOut.class, () -> client.put(b("k"), b("x")));
    }

    @Test
    void aBrokenConnectionIsOpenedAgainAndTheCommandSentAgain() throws IOException {
        ScriptedNode node = node(FreyjaClientTest::obliging);
        FreyjaClient client = client(5_000, node);
        client.put(b("k"), b("v"));
        boolean[] once = {true};
        node.script = request -> {
            if (once[0]) {
                once[0] = false;
                return HANG_UP;
            }
            return obliging(request);
        };
        client.put(b("k"), b("w"));
        List<Command> commands = node.commands();
        assertEquals(2, ((Command.Write) commands.get(commands.size() - 2)).sequence());
        assertEquals(2, ((Command.Write) commands.getLast()).sequence(), "the same number on the new connection");
        assertEquals(2, node.connections);
    }

    @Test
    void anExpiredSessionIsReplacedIfTheCommandCannotHaveTakenEffect() throws IOException {
        long[] session = {7};
        ScriptedNode node = node(request -> {
            if (request instanceof ClientRequest.Execute execute) {
                if (execute.command() instanceof Command.RegisterSession) {
                    return new ClientResponse.Result(new CommandResult.SessionRegistered(session[0]), 1);
                }
                if (execute.command() instanceof Command.Write write && write.sessionId() != session[0]) {
                    return new ClientResponse.Result(new CommandResult.Failed(Failure.NO_SUCH_SESSION), 2);
                }
            }
            return obliging(request);
        });
        FreyjaClient client = client(5_000, node);
        client.put(b("k"), b("v"));
        session[0] = 8; // session 7 has expired
        client.put(b("k"), b("w"));
        assertEquals(8, client.sessionId());
        Command.Write last = (Command.Write) node.commands().getLast();
        assertEquals(8, last.sessionId());
        assertEquals(1, last.sequence(), "the first command of the new session");
    }

    @Test
    void aClientThatIsToldNotToRetryReportsOverloadAtOnceAndUsesTheNumberAgain() throws IOException {
        boolean[] busy = {false};
        ScriptedNode node = node(request -> busy[0] && !(request instanceof ClientRequest.Execute execute
                && execute.command() instanceof Command.RegisterSession)
                ? new ClientResponse.Rejected(Reason.OVERLOADED) : obliging(request));
        FreyjaClient client = FreyjaClient.open(new FreyjaClient.Options(List.of(node.address()), CLUSTER, 5_000,
                300, 5, 40, 500, 0).withRetryWhenOverloaded(false));
        open.add(client);
        client.put(b("k"), b("v"));
        busy[0] = true;
        int before = node.requests.size();
        long started = System.nanoTime();
        assertThrows(FreyjaException.Overloaded.class, () -> client.put(b("k"), b("w")));
        assertThrows(FreyjaException.Overloaded.class, () -> client.get(b("k")));
        assertTrue(System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(4),
                "no retries until the deadline");
        assertEquals(before + 2, node.requests.size(), "each request was sent once");
        busy[0] = false;
        client.put(b("k"), b("x"));
        List<Command> commands = node.commands();
        assertEquals(2, ((Command.Write) commands.get(commands.size() - 2)).sequence());
        assertEquals(2, ((Command.Write) commands.getLast()).sequence(), "the refused command had no effect");

        // A redirect rules an effect out: the command refused by the leader had none.
        ScriptedNode leader = node(request -> new ClientResponse.Rejected(Reason.OVERLOADED));
        ScriptedNode follower = node(request -> request instanceof ClientRequest.Execute execute
                && execute.command() instanceof Command.RegisterSession ? obliging(request)
                : new ClientResponse.NotLeader(1, leader.address()));
        FreyjaClient redirected = FreyjaClient.open(new FreyjaClient.Options(List.of(follower.address()), CLUSTER,
                5_000, 300, 5, 40, 500, 0).withRetryWhenOverloaded(false));
        open.add(redirected);
        assertThrows(FreyjaException.Overloaded.class, () -> redirected.put(b("k"), b("v")));

        // An attempt without an answer may have taken effect.
        boolean[] first = {true};
        node.script = request -> {
            if (request instanceof ClientRequest.Execute execute && execute.command() instanceof Command.Write) {
                if (first[0]) {
                    first[0] = false;
                    return SILENCE;
                }
                return new ClientResponse.Rejected(Reason.OVERLOADED);
            }
            return obliging(request);
        };
        assertThrows(FreyjaException.UnknownOutcome.class, () -> client.put(b("k"), b("y")));
    }

    @Test
    void lackOfSpaceIsReportedAndTheNumberOfTheCommandIsUsedAgain() throws IOException {
        boolean[] full = {true};
        ScriptedNode node = node(request -> request instanceof ClientRequest.Execute execute
                && execute.command() instanceof Command.Write && full[0]
                ? new ClientResponse.Result(new CommandResult.Failed(Failure.NO_SPACE), 5) : obliging(request));
        FreyjaClient client = client(5_000, node);
        assertThrows(FreyjaException.NoSpace.class, () -> client.put(b("k"), b("v")));
        full[0] = false;
        client.put(b("k"), b("v"));
        List<Command> commands = node.commands();
        assertEquals(1, ((Command.Write) commands.get(commands.size() - 2)).sequence());
        assertEquals(1, ((Command.Write) commands.getLast()).sequence());

        node.script = request -> new ClientResponse.Rejected(Reason.UNSUPPORTED);
        assertThrows(FreyjaException.Refused.class, () -> client.scan(new byte[0], new byte[0], 5));
        assertFalse(client.sessionId() == 0);
    }

    @Test
    void aNodeOfAnotherClusterIsNotTalkedTo() throws IOException {
        ScriptedNode node = node(FreyjaClientTest::obliging);
        FreyjaClient client = FreyjaClient.open(new FreyjaClient.Options(List.of(node.address()), CLUSTER + 1, 500,
                200, 5, 40, 500, 0));
        open.add(client);
        assertThrows(FreyjaException.TimedOut.class, () -> client.put(b("k"), b("v")));
        assertTrue(node.requests.isEmpty());
        assertThrows(IllegalArgumentException.class, () -> FreyjaClient.Options.of("no-port"));
    }
}
