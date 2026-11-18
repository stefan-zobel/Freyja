package io.github.sz.freyja.bench;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.client.FreyjaException;

/**
 * An open-loop load generator (docs/spec/verification.md, section 7).
 *
 * <p>Requests are scheduled by time: request number {@code i} is due at {@code i / rate}
 * seconds after the start, whatever became of the requests before it. The latency of a
 * request is measured from the moment it was due, not from the moment a client had time for
 * it. So a stall of the system shows in the latencies of everything that was due during the
 * stall, and is not hidden by a generator that politely waited.
 *
 * <p>A fixed number of clients carry the requests out, each in a virtual thread. If they
 * cannot keep up, requests wait, and their latencies grow; if more wait than a bound allows,
 * the rest is counted as shed and not sent, so that the generator stays bounded too.
 */
public final class LoadGenerator {

    /** What the requests of a run are. */
    public enum Workload {
        /** Puts of new keys in ascending order. */
        BULK_LOAD(0, 0),
        /** 95 reads to 5 writes over the keys. */
        READ_HEAVY(95, 0),
        /** As many reads as writes. */
        MIXED(50, 0),
        /** Writes only. */
        WRITE_ONLY(0, 0),
        /** 90 scans of up to 50 keys to 10 writes. */
        SCAN_HEAVY(0, 90),
        /** As {@link #MIXED}, with values a hundred times as long. */
        LARGE_VALUES(50, 0);

        final int readPercent;
        final int scanPercent;

        Workload(int readPercent, int scanPercent) {
            this.readPercent = readPercent;
            this.scanPercent = scanPercent;
        }
    }

    /**
     * @param endpoints  the addresses of nodes for clients, as {@code host:port}
     * @param workload   what to ask for
     * @param ratePerSecond how many requests are due per second
     * @param seconds    how long requests become due
     * @param clients    how many clients carry them out
     * @param keys       how many keys there are (except for the bulk load, which adds keys)
     * @param valueBytes the length of a value; the workload with large values takes a
     *                   hundred times that
     * @param seed       decides keys, values, and the kind of every request
     * @param requestTimeoutMillis how long one request may take
     */
    public record Config(List<String> endpoints, Workload workload, int ratePerSecond, int seconds, int clients,
                         int keys, int valueBytes, long seed, int requestTimeoutMillis) {
        public Config {
            endpoints = List.copyOf(endpoints);
            if (endpoints.isEmpty() || ratePerSecond < 1 || seconds < 1 || clients < 1 || keys < 1
                    || valueBytes < 1 || requestTimeoutMillis < 1) {
                throw new IllegalArgumentException("invalid load settings");
            }
        }
    }

    /**
     * What a run measured.
     *
     * @param due        requests that became due
     * @param completed  requests that were answered
     * @param failed     requests that were refused, timed out, or have an unknown outcome
     * @param shed       requests that were not sent because too many were waiting already
     * @param seconds    how long the run took, until the last request was answered
     * @param latencies  the latencies of the completed requests in microseconds, from the
     *                   moment each was due
     */
    public record Result(Config config, long due, long completed, long failed, long shed, double seconds,
                         Histogram latencies) {

        /** Completed requests per second of the run. */
        public double throughput() {
            return seconds <= 0 ? 0 : completed / seconds;
        }
    }

    private LoadGenerator() {
    }

    private static byte[] key(long number) {
        return String.format("key-%012d", number).getBytes(StandardCharsets.UTF_8);
    }

    /** Runs the load and returns when every request that was sent is answered or has failed. */
    public static Result run(Config config) throws InterruptedException {
        int valueBytes = config.workload() == Workload.LARGE_VALUES ? config.valueBytes() * 100 : config.valueBytes();
        long total = (long) config.ratePerSecond() * config.seconds();
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / config.ratePerSecond();
        // What waits for a client. Beyond a few seconds of load, requests are shed.
        ArrayBlockingQueue<long[]> waiting = new ArrayBlockingQueue<>(
                (int) Math.min(1_000_000, Math.max(1_000, (long) config.ratePerSecond() * 5)));
        AtomicBoolean done = new AtomicBoolean();
        Histogram[] histograms = new Histogram[config.clients()];
        long[] failed = new long[config.clients()];
        List<Thread> workers = new ArrayList<>();
        long start = System.nanoTime();

        for (int c = 0; c < config.clients(); c++) {
            int worker = c;
            histograms[c] = new Histogram();
            workers.add(Thread.ofVirtual().name("freyja-bench-" + c).start(() -> {
                Random random = new Random(config.seed() * 31 + worker);
                byte[] value = new byte[valueBytes];
                FreyjaClient.Options options = new FreyjaClient.Options(rotated(config.endpoints(), worker), 0,
                        config.requestTimeoutMillis(), Math.min(2_000, config.requestTimeoutMillis()), 10, 200,
                        1_000, 0);
                try (FreyjaClient client = FreyjaClient.open(options)) {
                    while (true) {
                        long[] request = waiting.poll(50, TimeUnit.MILLISECONDS);
                        if (request == null) {
                            if (done.get()) {
                                return;
                            }
                            continue;
                        }
                        try {
                            random.nextBytes(value);
                            one(client, config, request[0], random, value);
                            histograms[worker].record((System.nanoTime() - request[1]) / 1_000);
                        } catch (FreyjaException e) {
                            failed[worker]++;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        long shed = 0;
        for (long i = 0; i < total; i++) {
            long dueAt = start + i * intervalNanos;
            long wait = dueAt - System.nanoTime();
            if (wait > 0) {
                TimeUnit.NANOSECONDS.sleep(wait);
            }
            if (!waiting.offer(new long[] {i, dueAt})) {
                shed++;
            }
        }
        done.set(true);
        for (Thread worker : workers) {
            worker.join();
        }
        double seconds = (System.nanoTime() - start) / 1e9;

        Histogram all = new Histogram();
        long failures = 0;
        for (int c = 0; c < config.clients(); c++) {
            all.merge(histograms[c]);
            failures += failed[c];
        }
        return new Result(config, total, all.count(), failures, shed, seconds, all);
    }

    /** The list starting at another node for every client, so that they do not all ask the same first. */
    private static List<String> rotated(List<String> endpoints, int by) {
        List<String> list = new ArrayList<>(endpoints.size());
        for (int i = 0; i < endpoints.size(); i++) {
            list.add(endpoints.get((i + by) % endpoints.size()));
        }
        return list;
    }

    private static void one(FreyjaClient client, Config config, long number, Random random, byte[] value) {
        Workload workload = config.workload();
        if (workload == Workload.BULK_LOAD) {
            client.put(key(number), value);
            return;
        }
        long keyNumber = random.nextInt(config.keys());
        int choice = random.nextInt(100);
        if (choice < workload.scanPercent) {
            client.scan(key(keyNumber), new byte[0], 50);
        } else if (choice < workload.scanPercent + workload.readPercent) {
            client.get(key(keyNumber));
        } else {
            client.put(key(keyNumber), value);
        }
    }
}
