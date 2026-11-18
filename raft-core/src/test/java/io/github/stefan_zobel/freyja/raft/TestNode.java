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

    /** The durable log of a test node. Entries a snapshot covers are removed from its start. */
    static final class MemoryLog implements LogView {
        /** The entries with the indexes {@code offset + 1} and following. */
        final List<LogEntry> entries = new ArrayList<>();
        long offset;

        @Override
        public long firstIndex() {
            return offset + 1;
        }

        @Override
        public long lastIndex() {
            return offset + entries.size();
        }

        LogEntry get(long index) {
            return entries.get((int) (index - offset - 1));
        }

        @Override
        public long termAt(long index) {
            return get(index).term();
        }

        @Override
        public List<LogEntry> entries(long fromInclusive, long toExclusive, int maxBytes) {
            List<LogEntry> result = new ArrayList<>();
            long bytes = 0;
            for (long i = fromInclusive; i < toExclusive; i++) {
                LogEntry entry = get(i);
                if (!result.isEmpty() && bytes + entry.size() > maxBytes) {
                    break;
                }
                bytes += entry.size();
                result.add(entry);
            }
            return result;
        }

        void apply(Ready ready) {
            long truncate = ready.truncateSuffixFrom();
            if (truncate != -1 && truncate <= lastIndex()) {
                entries.subList((int) (truncate - offset - 1), entries.size()).clear();
            }
            for (LogEntry entry : ready.entries()) {
                if (entry.index() != lastIndex() + 1) {
                    throw new AssertionError("gap in the stored log before " + entry);
                }
                entries.add(entry);
            }
        }

        /** Removes the entries up to {@code index}; the rest too unless {@code keepSuffix}. */
        void startAfter(long index, boolean keepSuffix) {
            if (!keepSuffix || index >= lastIndex()) {
                entries.clear();
            } else if (index > offset) {
                entries.subList(0, (int) (index - offset)).clear();
            }
            offset = Math.max(offset, index);
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
    /** The index of the last entry the state machine holds; it may come from a snapshot. */
    long appliedIndex;
    final List<SnapshotDirective> directives = new ArrayList<>();
    final List<Long> prefixTruncations = new ArrayList<>();
    /** The snapshot the node starts from after a restart. */
    SnapshotMeta snapshot;
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
                new PersistedState(hardState, snapshot == null ? SnapshotMeta.none(cluster) : snapshot), log,
                appliedIndex);
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
            directives.addAll(ready.snapshots());
            if (ready.truncatePrefixThrough() != -1) {
                prefixTruncations.add(ready.truncatePrefixThrough());
            }
            for (long i = ready.applyFrom(); i <= ready.applyTo(); i++) {
                if (i != appliedIndex + 1) {
                    throw new AssertionError("apply out of order: " + i + " after " + appliedIndex);
                }
                applied.add(log.get(i));
                appliedIndex = i;
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
