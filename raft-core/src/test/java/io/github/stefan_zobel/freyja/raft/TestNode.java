package io.github.stefan_zobel.freyja.raft;

import java.util.ArrayList;
import java.util.List;

import io.github.stefan_zobel.freyja.raft.RaftEvent.MessageReceived;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Persisted;
import io.github.stefan_zobel.freyja.raft.RaftEvent.Tick;

/**
 * A node with a hand-operated runtime: the test decides when writes become durable and when
 * messages are delivered.
 */
final class TestNode {

    /** The durable log of a test node. */
    static final class MemoryLog implements LogView {
        final List<LogEntry> entries = new ArrayList<>();

        @Override
        public long firstIndex() {
            return 1;
        }

        @Override
        public long lastIndex() {
            return entries.size();
        }

        @Override
        public long termAt(long index) {
            return index == 0 ? 0 : entries.get((int) index - 1).term();
        }

        @Override
        public List<LogEntry> entries(long fromInclusive, long toExclusive, int maxBytes) {
            List<LogEntry> result = new ArrayList<>();
            long bytes = 0;
            for (long i = fromInclusive; i < toExclusive; i++) {
                LogEntry entry = entries.get((int) i - 1);
                if (!result.isEmpty() && bytes + entry.size() > maxBytes) {
                    break;
                }
                bytes += entry.size();
                result.add(entry);
            }
            return result;
        }

        void apply(Ready ready) {
            if (ready.truncateSuffixFrom() != -1 && ready.truncateSuffixFrom() <= entries.size()) {
                entries.subList((int) ready.truncateSuffixFrom() - 1, entries.size()).clear();
            }
            for (LogEntry entry : ready.entries()) {
                if (entry.index() != entries.size() + 1) {
                    throw new AssertionError("gap in the stored log before " + entry);
                }
                entries.add(entry);
            }
        }
    }

    final RaftConfig config;
    final ClusterConfig cluster;
    final MemoryLog log = new MemoryLog();
    HardState hardState = HardState.INITIAL;
    RaftNode raft;

    /** Writes handed out and not yet made durable. */
    final List<Ready> pending = new ArrayList<>();
    final List<Ready> readies = new ArrayList<>();
    final List<Outbound> outbox = new ArrayList<>();
    final List<Notification> notes = new ArrayList<>();
    final List<LogEntry> applied = new ArrayList<>();
    /** Persist every write at once. */
    boolean autoPersist;

    TestNode(RaftConfig config, ClusterConfig cluster) {
        this.config = config;
        this.cluster = cluster;
        this.raft = RaftNode.create(config, PersistedState.initial(cluster), log, 0);
    }

    /** Loses everything that is not durable and starts again from the stored state. */
    void restart() {
        pending.clear();
        outbox.clear();
        raft = RaftNode.create(config,
                new PersistedState(hardState, SnapshotMeta.none(cluster)), log, applied.size());
    }

    void step(RaftEvent event) {
        raft.step(event);
        collect();
    }

    void tick(int count) {
        for (int i = 0; i < count; i++) {
            step(new Tick());
        }
    }

    void receive(int from, RaftMessage message) {
        step(new MessageReceived(from, message));
    }

    private void collect() {
        while (raft.hasReady()) {
            Ready ready = raft.takeReady();
            readies.add(ready);
            outbox.addAll(ready.messages());
            notes.addAll(ready.notifications());
            for (long i = ready.applyFrom(); i <= ready.applyTo(); i++) {
                if (i != applied.size() + 1) {
                    throw new AssertionError("apply out of order: " + i + " after " + applied.size());
                }
                applied.add(log.entries.get((int) i - 1));
            }
            if (ready.hasPersistentWork()) {
                pending.add(ready);
                if (autoPersist) {
                    persistAll();
                }
            }
        }
    }

    /** Makes the oldest pending write durable and reports it. */
    void persistNext() {
        Ready ready = pending.removeFirst();
        store(ready);
        step(new Persisted(ready.sequence()));
    }

    /** Makes every pending write durable and reports them with one event. */
    void persistAll() {
        if (pending.isEmpty()) {
            return;
        }
        long sequence = pending.getLast().sequence();
        for (Ready ready : pending) {
            store(ready);
        }
        pending.clear();
        step(new Persisted(sequence));
    }

    private void store(Ready ready) {
        if (ready.hardState() != null) {
            hardState = ready.hardState();
        }
        log.apply(ready);
    }

    /** Removes and returns the messages waiting to be sent. */
    List<Outbound> drainOutbox() {
        List<Outbound> messages = new ArrayList<>(outbox);
        outbox.clear();
        return messages;
    }

    <T extends RaftMessage> List<T> sent(Class<T> type) {
        List<T> result = new ArrayList<>();
        for (Outbound outbound : outbox) {
            if (type.isInstance(outbound.message())) {
                result.add(type.cast(outbound.message()));
            }
        }
        return result;
    }

    <T extends Notification> List<T> notes(Class<T> type) {
        List<T> result = new ArrayList<>();
        for (Notification note : notes) {
            if (type.isInstance(note)) {
                result.add(type.cast(note));
            }
        }
        return result;
    }

    ProtocolState view() {
        return raft.view();
    }
}
