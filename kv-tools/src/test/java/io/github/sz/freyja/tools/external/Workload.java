package io.github.sz.freyja.tools.external;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.client.FreyjaException;
import io.github.sz.freyja.common.kv.Command.Condition;
import io.github.sz.freyja.common.kv.Command.Put;
import io.github.sz.freyja.raft.sim.LinearizabilityChecker;
import io.github.sz.freyja.raft.sim.LinearizabilityChecker.Kind;
import io.github.sz.freyja.raft.sim.LinearizabilityChecker.Operation;

/**
 * Clients that read, write, and compare-and-set registers through the client library while
 * the test breaks things, and the history of what they asked for and what they were told.
 *
 * <p>A register is a key; its value is a number, 0 while the key is not there. Every write
 * writes a number that no other operation writes. The keys in use move on as the run goes,
 * so that the history of one key stays short enough to be checked.
 *
 * <p>What a client is told decides what goes into the history:
 * <ul>
 *   <li>an answer: the operation with its result;</li>
 *   <li>{@code UnknownOutcome}, or a failure the client does not classify: a write or a
 *       compare-and-set that may have taken effect, may still take effect, or never will;</li>
 *   <li>{@code TimedOut}, {@code NoSpace}, {@code Refused}: the client says the operation
 *       had no effect, and the history takes its word. If that word is wrong, the check
 *       fails.</li>
 * </ul>
 */
final class Workload {

    private static final int KEYS_AT_A_TIME = 3;
    private static final int OPERATIONS_PER_KEY_SET = 120;

    /** One recorded operation, with the client that made it. */
    record Recorded(int client, Operation operation) {
    }

    private final String[] endpoints;
    private final int requestTimeoutMillis;
    private final AtomicLong time = new AtomicLong();
    private final AtomicLong operations = new AtomicLong();
    private final AtomicLong values = new AtomicLong();
    private final ConcurrentLinkedQueue<Recorded> history = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicLong answered = new AtomicLong();
    private final AtomicLong unknown = new AtomicLong();
    private final AtomicLong withoutEffect = new AtomicLong();
    private volatile boolean stop;

    Workload(String[] endpoints, int requestTimeoutMillis) {
        this.endpoints = endpoints;
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    static byte[] key(int register) {
        return ("r" + register).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] number(long value) {
        return Long.toString(value).getBytes(StandardCharsets.UTF_8);
    }

    private static long number(Optional<FreyjaClient.Versioned> stored) {
        return stored.map(v -> Long.parseLong(new String(v.value(), StandardCharsets.UTF_8))).orElse(0L);
    }

    /** Starts the clients. Each has a connection, a session, and a random sequence of its own. */
    void start(int clients, long seed) {
        for (int c = 0; c < clients; c++) {
            int client = c;
            Thread thread = Thread.ofPlatform().name("workload-" + c).daemon(true)
                    .start(() -> run(client, new Random(seed * 31 + client)));
            threads.add(thread);
        }
    }

    /** Tells the clients to end and waits until the operations on their way are over. */
    void stop() throws InterruptedException {
        stop = true;
        for (Thread thread : threads) {
            thread.join();
        }
    }

    long answered() {
        return answered.get();
    }

    String summary() {
        return answered.get() + " operations answered, " + unknown.get() + " with unknown outcome, "
                + withoutEffect.get() + " without effect";
    }

    /** What went wrong in a client beyond what a client may be told; empty if nothing. */
    List<Throwable> failures() {
        return List.copyOf(failures);
    }

    List<Recorded> history() {
        return List.copyOf(history);
    }

    /** Checks the history with the checker of the project: {@code null} if it is linearizable. */
    String check() {
        return LinearizabilityChecker.check(history.stream().map(Recorded::operation).toList());
    }

    /**
     * Writes the history for a checker that is not of this project, one operation per line:
     * client, key, kind ({@code r}, {@code w}, {@code c}), the two arguments, the times of
     * invocation and response (-1 for none), and the result.
     */
    void export(Path file) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("# client key kind a b invoke response result");
        for (Recorded recorded : history) {
            Operation o = recorded.operation();
            String kind = switch (o.kind()) {
                case READ -> "r";
                case WRITE -> "w";
                case COMPARE_AND_SET -> "c";
            };
            lines.add(recorded.client() + " r" + o.key() + " " + kind + " " + o.a() + " " + o.b() + " " + o.invoke()
                    + " " + (o.response() == LinearizabilityChecker.NO_RESPONSE ? -1 : o.response()) + " "
                    + o.result());
        }
        Files.write(file, lines, StandardCharsets.UTF_8);
    }

    private void run(int client, Random random) {
        Map<Integer, Long> seen = new HashMap<>(); // what this client last knew of a register
        FreyjaClient connection = null;
        try {
            while (!stop) {
                if (connection == null) {
                    // Every other client is impatient: it gives up while a fault still lasts,
                    // and so the history gets operations whose outcome nobody knows.
                    connection = FreyjaClient.open(FreyjaClient.Options.of(endpoints)
                            .withRequestTimeout(client % 2 == 0 ? requestTimeoutMillis : requestTimeoutMillis / 8)
                            .withKeepAlive(0));
                }
                int register = (int) (operations.getAndIncrement() / OPERATIONS_PER_KEY_SET) * KEYS_AT_A_TIME
                        + random.nextInt(KEYS_AT_A_TIME);
                byte[] key = key(register);
                int choice = random.nextInt(100);
                Kind kind = choice < 45 ? Kind.READ : choice < 80 ? Kind.WRITE : Kind.COMPARE_AND_SET;
                long a = 0;
                long b = 0;
                if (kind == Kind.WRITE) {
                    a = values.incrementAndGet();
                } else if (kind == Kind.COMPARE_AND_SET) {
                    a = seen.getOrDefault(register, 0L);
                    b = values.incrementAndGet();
                }
                long invoke = time.incrementAndGet();
                try {
                    long result = 0;
                    switch (kind) {
                        case READ -> {
                            result = number(connection.get(key));
                            seen.put(register, result);
                        }
                        case WRITE -> {
                            connection.put(key, number(a));
                            seen.put(register, a);
                        }
                        case COMPARE_AND_SET -> {
                            Condition expected = a == 0 ? Condition.notExists(key)
                                    : Condition.valueEquals(key, number(a));
                            boolean held = connection.conditional(List.of(expected),
                                    List.of(new Put(key, number(b))), List.of());
                            result = held ? 1 : 0;
                            if (held) {
                                seen.put(register, b);
                            } else {
                                seen.remove(register);
                            }
                        }
                    }
                    history.add(new Recorded(client,
                            new Operation(register, kind, a, b, invoke, time.incrementAndGet(), result)));
                    answered.incrementAndGet();
                } catch (FreyjaException.TimedOut | FreyjaException.NoSpace | FreyjaException.Refused e) {
                    withoutEffect.incrementAndGet(); // it never happened
                } catch (FreyjaException e) {
                    unknown.incrementAndGet();
                    seen.remove(register);
                    if (kind != Kind.READ) {
                        history.add(new Recorded(client, new Operation(register, kind, a, b, invoke,
                                LinearizabilityChecker.NO_RESPONSE, 0)));
                    }
                    if (!(e instanceof FreyjaException.UnknownOutcome)) {
                        // Whatever it was, this session is not relied on any further.
                        connection.close();
                        connection = null;
                    }
                }
            }
        } catch (RuntimeException | Error e) {
            failures.add(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (RuntimeException e) {
                    // the cluster may be gone
                }
            }
        }
    }
}
