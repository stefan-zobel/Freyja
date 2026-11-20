package io.github.sz.freyja.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.github.sz.freyja.client.FreyjaClient;
import io.github.sz.freyja.client.FreyjaException;
import io.github.sz.freyja.common.admin.AdminRequest;
import io.github.sz.freyja.common.admin.AdminResponse;

/**
 * What goes on in the cluster while a workload is measured (docs/spec/verification.md,
 * section 7): compactions, the transfer of a snapshot to a node that joins, or a node that
 * catches up with the log. The benchmark brings the condition about a quarter into the run,
 * watches whether it really came about, and undoes what it changed when the run is over.
 *
 * <ul>
 *   <li>{@link During#COMPACTION}: a writer that is not measured puts large values under keys
 *       of their own, so that the nodes flush and compact. Reached if the nodes report
 *       compactions while the run lasts; that needs their figures.</li>
 *   <li>{@link During#SNAPSHOT_TRANSFER}: every node shortens its log, then an empty spare
 *       node joins as a learner. The leader can only send it a snapshot. Reached if the
 *       leader was seen sending it one, or the spare got past the start of the log of the
 *       leader.</li>
 *   <li>{@link During#CATCH_UP}: the empty spare joins as a learner and the leader brings it
 *       up to date from its log (after its latest snapshot, if the log no longer reaches back
 *       to the start). Reached if the spare was seen getting entries of the log.</li>
 * </ul>
 *
 * <p>The spare is removed from the cluster when the run is over. A spare is used once: a
 * further run needs an empty one.
 */
final class Background {

    /** The conditions. */
    enum During {
        COMPACTION, SNAPSHOT_TRANSFER, CATCH_UP
    }

    /** An empty node that waits to join: its id, and where it listens for peers and clients. */
    record Spare(int nodeId, String peerAddress, String clientAddress) {
        /** Reads {@code ID,PEER-HOST:PORT,CLIENT-HOST:PORT}. */
        static Spare parse(String text) {
            String[] parts = text.split(",");
            if (parts.length != 3) {
                throw new IllegalArgumentException("a spare is ID,PEER-HOST:PORT,CLIENT-HOST:PORT: " + text);
            }
            return new Spare(Integer.parseInt(parts[0].trim()), parts[1].trim(), parts[2].trim());
        }
    }

    /** How long a value of the writer that feeds compactions is. */
    static final int BACKGROUND_VALUE_BYTES = 4096;
    /** How many clients the writer that feeds compactions has. */
    private static final int BACKGROUND_CLIENTS = 8;
    /** How long the removal of the spare may take to take effect. */
    private static final int LEAVE_SECONDS = 10;

    private final During during;
    private final List<String> clientEndpoints;
    private final List<String> admins;
    private final List<String> statuses;
    private final Spare spare;
    private final int backgroundRate;
    private final int keys;

    private final AtomicBoolean stop = new AtomicBoolean();
    private final List<Thread> threads = new ArrayList<>();
    private final AtomicLong written = new AtomicLong();
    private final AtomicLong backgroundFailures = new AtomicLong();
    private long compactionsBefore;
    private long snapshotIndex = -1;
    private volatile boolean sawSnapshot;
    private final AtomicLong spareMatch = new AtomicLong(-1);
    private String trouble;
    private boolean started;

    /**
     * @param during          the condition
     * @param clientEndpoints where the nodes listen for clients
     * @param admins          where the nodes listen for administrators
     * @param statuses        where the nodes serve their figures; may be empty
     * @param spare           the empty node that joins; needed for a transfer and for catching up
     * @param backgroundRate  how many values per second the writer that feeds compactions puts
     * @param keys            over how many keys of its own that writer goes
     */
    Background(During during, List<String> clientEndpoints, List<String> admins, List<String> statuses,
               Spare spare, int backgroundRate, int keys) {
        this.during = during;
        this.clientEndpoints = List.copyOf(clientEndpoints);
        this.admins = List.copyOf(admins);
        this.statuses = List.copyOf(statuses);
        this.spare = spare;
        this.backgroundRate = backgroundRate;
        this.keys = keys;
        switch (during) {
            case COMPACTION -> {
                if (statuses.isEmpty()) {
                    throw new IllegalArgumentException("whether the nodes compacted is seen from their figures:"
                            + " --status is needed");
                }
                if (backgroundRate < 1) {
                    throw new IllegalArgumentException("--background-rate must be at least 1");
                }
            }
            case SNAPSHOT_TRANSFER, CATCH_UP -> {
                if (admins.isEmpty() || spare == null) {
                    throw new IllegalArgumentException("a node that joins is needed: --admin and --spare");
                }
            }
        }
    }

    /** The bytes of keys and values the writer that feeds compactions put. */
    long writtenBytes() {
        return written.get();
    }

    /** Brings the condition about. */
    void start() throws IOException {
        started = true;
        switch (during) {
            case COMPACTION -> {
                compactionsBefore = sum(statuses, "freyja_compaction_count_total");
                int perClient = Math.max(1, backgroundRate / BACKGROUND_CLIENTS);
                for (int c = 0; c < BACKGROUND_CLIENTS; c++) {
                    int writer = c;
                    threads.add(Thread.ofVirtual().name("freyja-bench-background-" + c)
                            .start(() -> feedCompactions(writer, perClient)));
                }
            }
            case SNAPSHOT_TRANSFER -> {
                for (String admin : admins) {
                    AdminResponse answer = NodeAccess.admin(admin, new AdminRequest.Snapshot());
                    if (answer instanceof AdminResponse.Refused refused) {
                        throw new IOException(admin + " did not shorten its log: " + refused.reason());
                    }
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                AdminResponse.Status leader = leader();
                while (leader.snapshotIndex() == 0) {
                    if (System.nanoTime() > deadline) {
                        throw new IOException("the leader did not shorten its log within 30 s");
                    }
                    pause(100);
                    leader = leader();
                }
                snapshotIndex = leader.snapshotIndex();
                join();
            }
            case CATCH_UP -> {
                snapshotIndex = leader().snapshotIndex();
                join();
            }
        }
    }

    /**
     * Ends the condition, undoes what it changed, and says whether it came about.
     *
     * @return the lines of the report, {@code during.*}
     */
    Map<String, String> finish() throws InterruptedException {
        stop.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
        Map<String, String> report = new LinkedHashMap<>();
        report.put("during", during.name());
        boolean reached = false;
        if (!started) {
            trouble = trouble == null ? "the run ended before the condition was brought about" : trouble;
        } else {
            switch (during) {
                case COMPACTION -> {
                    try {
                        long compactions = sum(statuses, "freyja_compaction_count_total") - compactionsBefore;
                        report.put("during.compactions", Long.toString(compactions));
                        report.put("during.background.failures", Long.toString(backgroundFailures.get()));
                        reached = compactions > 0;
                    } catch (IOException e) {
                        trouble = "the figures of the nodes could not be read: " + e.getMessage();
                    }
                }
                case SNAPSHOT_TRANSFER, CATCH_UP -> {
                    report.put("during.leader.snapshot.index", Long.toString(snapshotIndex));
                    report.put("during.spare.match.index", Long.toString(spareMatch.get()));
                    report.put("during.spare.snapshot.seen", Boolean.toString(sawSnapshot));
                    reached = during == During.SNAPSHOT_TRANSFER
                            ? snapshotIndex > 0 && (sawSnapshot || spareMatch.get() >= snapshotIndex)
                            : spareMatch.get() > Math.max(0, snapshotIndex);
                    leave();
                }
            }
        }
        report.put("during.reached", Boolean.toString(reached && trouble == null));
        if (trouble != null) {
            report.put("during.trouble", trouble);
        }
        return report;
    }

    /** Notes that the condition could not be brought about. */
    void failed(String why) {
        trouble = why;
    }

    // ------------------------------------------------------------------ compactions

    private void feedCompactions(int writer, int perSecond) {
        Random random = new Random(writer);
        byte[] value = new byte[BACKGROUND_VALUE_BYTES];
        long interval = TimeUnit.SECONDS.toNanos(1) / perSecond;
        long next = System.nanoTime();
        try (FreyjaClient client = FreyjaClient.open(FreyjaClient.Options.of(
                clientEndpoints.toArray(String[]::new)).withRequestTimeout(10_000).withKeepAlive(0))) {
            while (!stop.get()) {
                long wait = next - System.nanoTime();
                if (wait > 0) {
                    TimeUnit.NANOSECONDS.sleep(wait);
                }
                next += interval;
                byte[] key = String.format("background-%010d", random.nextInt(Math.max(1, keys)))
                        .getBytes(StandardCharsets.UTF_8);
                random.nextBytes(value);
                try {
                    client.put(key, value);
                    written.addAndGet(key.length + value.length);
                } catch (FreyjaException e) {
                    backgroundFailures.incrementAndGet();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static long sum(List<String> endpoints, String figure) throws IOException {
        long sum = 0;
        for (String endpoint : endpoints) {
            sum += NodeAccess.figures(endpoint).getOrDefault(figure, 0L);
        }
        return sum;
    }

    // ------------------------------------------------------------------ the spare

    /** The state of the leader, from whichever node knows it. */
    private AdminResponse.Status leader() throws IOException {
        IOException last = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            for (String admin : admins) {
                try {
                    AdminResponse.Status status = NodeAccess.status(admin);
                    if (status.role().equals("LEADER")) {
                        return status;
                    }
                } catch (IOException e) {
                    last = e;
                }
            }
            pause(100);
        }
        throw new IOException("no node of " + admins + " is the leader", last);
    }

    /** Where the leader listens for administrators. */
    private String leaderAdmin() throws IOException {
        int leader = leader().nodeId();
        for (String admin : admins) {
            if (NodeAccess.status(admin).nodeId() == leader) {
                return admin;
            }
        }
        throw new IOException("the leader, node " + leader + ", is not among " + admins);
    }

    private void join() throws IOException {
        AdminResponse answer = NodeAccess.admin(leaderAdmin(),
                new AdminRequest.AddLearner(spare.nodeId(), spare.peerAddress(), spare.clientAddress()));
        if (answer instanceof AdminResponse.Refused refused) {
            throw new IOException("node " + spare.nodeId() + " could not join: " + refused.reason());
        }
        threads.add(Thread.ofVirtual().name("freyja-bench-watch").start(this::watchSpare));
    }

    /** Looks at the spare through the eyes of the leader until the run is over. */
    private void watchSpare() {
        while (!stop.get()) {
            try {
                for (AdminResponse.Member member : leader().members()) {
                    if (member.nodeId() == spare.nodeId()) {
                        if (member.state().equals(AdminResponse.Member.SNAPSHOT)) {
                            sawSnapshot = true;
                        }
                        spareMatch.accumulateAndGet(member.matchIndex(), Math::max);
                    }
                }
            } catch (IOException e) {
                // the next look
            }
            pause(100);
        }
    }

    /**
     * Removes the spare and waits until the removal has taken effect: the leader answers when
     * the removal is in its log, and the run is over only when the spare is no member any more.
     */
    private void leave() {
        try {
            AdminResponse answer = NodeAccess.admin(leaderAdmin(), new AdminRequest.RemoveMember(spare.nodeId(), false));
            if (answer instanceof AdminResponse.Refused refused) {
                trouble = "node " + spare.nodeId() + " could not be removed again: " + refused.reason();
                return;
            }
        } catch (IOException e) {
            trouble = "node " + spare.nodeId() + " could not be removed again: " + e.getMessage();
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(LEAVE_SECONDS);
        while (true) {
            try {
                if (leader().members().stream().noneMatch(m -> m.nodeId() == spare.nodeId())) {
                    return;
                }
            } catch (IOException e) {
                // the next look
            }
            if (System.nanoTime() - deadline > 0) {
                trouble = "node " + spare.nodeId() + " was still a member " + LEAVE_SECONDS
                        + " seconds after its removal";
                return;
            }
            pause(50);
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
