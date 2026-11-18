package io.github.stefan_zobel.freyja.raft;

import java.util.List;

/**
 * The work the runtime must do next.
 *
 * <p>The runtime persists {@code hardState}, the suffix truncation, and {@code entries}, in
 * this order and in the order of the sequence numbers, and then feeds
 * {@link RaftEvent.Persisted}. One sync may cover several {@code Ready}s. Sending the
 * messages, applying entries, and persisting may run concurrently: every message in a
 * {@code Ready} may be sent at once.
 *
 * @param sequence           number of this {@code Ready}; ascending by one, starting at 1
 * @param hardState          the hard state to persist, or {@code null} if it did not change
 * @param truncateSuffixFrom the stored log is cut off from this index (inclusive) before
 *                           {@code entries} are stored; -1 if nothing is cut off
 * @param entries            entries to persist, with consecutive indexes
 * @param messages           messages to send
 * @param applyFrom          first committed and durable entry to hand to the state machine
 * @param applyTo            last such entry; the range is empty if it is below
 *                           {@code applyFrom}
 * @param notifications      answers to proposals, and role changes
 * @param truncatePrefixThrough stored entries up to this index (inclusive) are covered by a
 *                           snapshot and may be removed, now or later; -1 if none
 * @param snapshots          snapshots to send, install, or discard
 */
public record Ready(
        long sequence,
        HardState hardState,
        long truncateSuffixFrom,
        List<LogEntry> entries,
        List<Outbound> messages,
        long applyFrom,
        long applyTo,
        List<Notification> notifications,
        long truncatePrefixThrough,
        List<SnapshotDirective> snapshots) {

    /** Whether this {@code Ready} asks to persist anything. */
    public boolean hasPersistentWork() {
        return hardState != null || truncateSuffixFrom != -1 || !entries.isEmpty();
    }

    /** Whether this {@code Ready} hands entries to the state machine. */
    public boolean hasEntriesToApply() {
        return applyFrom <= applyTo;
    }
}
