package io.github.sz.freyja.bench;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
 *
 * <p>The clock starts when every client has connected, found the leader, and opened its
 * session. What that costs is not part of the load: a run measures the cluster under its
 * workload, not a crowd of clients that arrive at once.
 *
 * <p>A run may begin with a warm-up: the same load at the same rate for a number of seconds,
 * which is carried out but not measured. Without it, a run also measures how a cluster that
 * was idle copes with a sudden full load, which includes the warm-up of its code.
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
     * @param retryWhenOverloaded whether a client sends a request that a node refused as
     *                   overloaded again; if not, the request counts as rejected
     * @param warmUpSeconds how long the load runs at the same rate before the measured part
     *                   begins; 0 for none
     */
    public record Config(List<String> endpoints, Workload workload, int ratePerSecond, int seconds, int clients,
                         int keys, int valueBytes, long seed, int requestTimeoutMillis, boolean retryWhenOverloaded,
                         int warmUpSeconds) {

        /** Settings with clients that send a request the cluster had no room for again. */
        public Config(List<String> endpoints, Workload workload, int ratePerSecond, int seconds, int clients,
                      int keys, int valueBytes, long seed, int requestTimeoutMillis) {
            this(endpoints, workload, ratePerSecond, seconds, clients, keys, valueBytes, seed, requestTimeoutMillis,
                    true, 0);
        }

        /** The same settings with another rate. */
        public Config withRate(int rate) {
            return new Config(endpoints, workload, rate, seconds, clients, keys, valueBytes, seed,
                    requestTimeoutMillis, retryWhenOverloaded, warmUpSeconds);
        }

        /** The same settings with another duration. */
        public Config withSeconds(int duration) {
            return new Config(endpoints, workload, ratePerSecond, duration, clients, keys, valueBytes, seed,
                    requestTimeoutMillis, retryWhenOverloaded, warmUpSeconds);
        }

        /** The same settings with clients that do or do not send a refused request again. */
        public Config withRetryWhenOverloaded(boolean retry) {
            return new Config(endpoints, workload, ratePerSecond, seconds, clients, keys, valueBytes, seed,
                    requestTimeoutMillis, retry, warmUpSeconds);
        }

        /** The same settings with a warm-up of the given length before the measured part. */
        public Config withWarmUp(int warmUp) {
            return new Config(endpoints, workload, ratePerSecond, seconds, clients, keys, valueBytes, seed,
                    requestTimeoutMillis, retryWhenOverloaded, warmUp);
        }

        public Config {
            endpoints = List.copyOf(endpoints);
            if (endpoints.isEmpty() || ratePerSecond < 1 || seconds < 1 || clients < 1 || keys < 1
                    || valueBytes < 1 || requestTimeoutMillis < 1 || warmUpSeconds < 0) {
                throw new IllegalArgumentException("invalid load settings");
            }
        }
    }

    /**
     * What a run measured.
     *
     * @param due        requests that became due in the measured part of the run
     * @param completed  requests that were answered
     * @param failed     requests that timed out, have an unknown outcome, or were refused
     *                   for another reason than overload
     * @param rejected   requests that a node refused as overloaded, with clients that do not
     *                   send them again
     * @param shed       requests that were not sent because too many were waiting already
     * @param seconds    how long the measured part of the run took, until the last request
     *                   was answered
     * @param latencies  the latencies of the completed requests in microseconds, from the
     *                   moment each was due
     * @param service    the latencies of the completed requests in microseconds, from the
     *                   moment a client sent each
     * @param rejections how long it took in microseconds, from the moment a client sent a
     *                   request, until it was rejected
     * @param writtenBytes the bytes of keys and values that completed writes put
     * @param warmUpSeconds how long the clients took to connect and open their sessions
     *                   before the clock started
     * @param warmUpFailures clients that could not open their session before the clock
     *                   started; they try again with their first command
     */
    public record Result(Config config, long due, long completed, long failed, long rejected, long shed,
                         double seconds, Histogram latencies, Histogram service, Histogram rejections,
                         long writtenBytes, double warmUpSeconds, long warmUpFailures) {

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
        long warmUp = (long) config.ratePerSecond() * config.warmUpSeconds();
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / config.ratePerSecond();
        // What waits for a client. Beyond a few seconds of load, requests are shed.
        ArrayBlockingQueue<long[]> waiting = new ArrayBlockingQueue<>(
                (int) Math.min(1_000_000, Math.max(1_000, (long) config.ratePerSecond() * 5)));
        AtomicBoolean done = new AtomicBoolean();
        Histogram[] histograms = new Histogram[config.clients()];
        Histogram[] services = new Histogram[config.clients()];
        Histogram[] rejections = new Histogram[config.clients()];
        long[] failed = new long[config.clients()];
        long[] rejected = new long[config.clients()];
        long[] written = new long[config.clients()];
        List<Thread> workers = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(config.clients());
        AtomicLong warmUpFailures = new AtomicLong();

        for (int c = 0; c < config.clients(); c++) {
            int worker = c;
            histograms[c] = new Histogram();
            services[c] = new Histogram();
            rejections[c] = new Histogram();
            workers.add(Thread.ofVirtual().name("freyja-bench-" + c).start(() -> {
                Random random = new Random(config.seed() * 31 + worker);
                byte[] value = new byte[valueBytes];
                FreyjaClient.Options options = new FreyjaClient.Options(rotated(config.endpoints(), worker), 0,
                        config.requestTimeoutMillis(), Math.min(2_000, config.requestTimeoutMillis()), 10, 200,
                        1_000, 0).withRetryWhenOverloaded(config.retryWhenOverloaded());
                try (FreyjaClient client = FreyjaClient.open(options)) {
                    try {
                        client.openSession();
                    } catch (FreyjaException e) {
                        warmUpFailures.incrementAndGet(); // it tries again with its first command
                    } finally {
                        ready.countDown();
                    }
                    while (true) {
                        long[] request = waiting.poll(50, TimeUnit.MILLISECONDS);
                        if (request == null) {
                            if (done.get()) {
                                return;
                            }
                            continue;
                        }
                        long sent = System.nanoTime();
                        boolean measured = request[0] >= warmUp;
                        try {
                            random.nextBytes(value);
                            long bytes = one(client, config, request[0], random, value);
                            if (!measured) {
                                continue;
                            }
                            written[worker] += bytes;
                            long now = System.nanoTime();
                            histograms[worker].record((now - request[1]) / 1_000);
                            services[worker].record((now - sent) / 1_000);
                        } catch (FreyjaException.Overloaded e) {
                            if (!measured) {
                                continue;
                            }
                            rejected[worker]++;
                            rejections[worker].record((System.nanoTime() - sent) / 1_000);
                        } catch (FreyjaException e) {
                            if (measured) {
                                failed[worker]++;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }

        long warmUpStarted = System.nanoTime();
        ready.await();
        long begin = System.nanoTime();
        long start = begin + warmUp * intervalNanos;
        long shed = 0;
        for (long i = 0; i < warmUp + total; i++) {
            long dueAt = begin + i * intervalNanos;
            long wait = dueAt - System.nanoTime();
            if (wait > 0) {
                TimeUnit.NANOSECONDS.sleep(wait);
            }
            if (!waiting.offer(new long[] {i, dueAt}) && i >= warmUp) {
                shed++;
            }
        }
        done.set(true);
        for (Thread worker : workers) {
            worker.join();
        }
        double seconds = (System.nanoTime() - start) / 1e9;

        Histogram all = new Histogram();
        Histogram service = new Histogram();
        Histogram rejection = new Histogram();
        long failures = 0;
        long refusals = 0;
        long bytes = 0;
        for (int c = 0; c < config.clients(); c++) {
            all.merge(histograms[c]);
            service.merge(services[c]);
            rejection.merge(rejections[c]);
            failures += failed[c];
            refusals += rejected[c];
            bytes += written[c];
        }
        return new Result(config, total, all.count(), failures, refusals, shed, seconds, all, service, rejection,
                bytes, (begin - warmUpStarted) / 1e9, warmUpFailures.get());
    }

    /** The list starting at another node for every client, so that they do not all ask the same first. */
    private static List<String> rotated(List<String> endpoints, int by) {
        List<String> list = new ArrayList<>(endpoints.size());
        for (int i = 0; i < endpoints.size(); i++) {
            list.add(endpoints.get((i + by) % endpoints.size()));
        }
        return list;
    }

    /** Carries out one request; returns the bytes of key and value it wrote, 0 for a read. */
    private static long one(FreyjaClient client, Config config, long number, Random random, byte[] value) {
        Workload workload = config.workload();
        byte[] key;
        if (workload == Workload.BULK_LOAD) {
            key = key(number);
        } else {
            long keyNumber = random.nextInt(config.keys());
            int choice = random.nextInt(100);
            if (choice < workload.scanPercent) {
                client.scan(key(keyNumber), new byte[0], 50);
                return 0;
            }
            if (choice < workload.scanPercent + workload.readPercent) {
                client.get(key(keyNumber));
                return 0;
            }
            key = key(keyNumber);
        }
        client.put(key, value);
        return key.length + value.length;
    }
}
