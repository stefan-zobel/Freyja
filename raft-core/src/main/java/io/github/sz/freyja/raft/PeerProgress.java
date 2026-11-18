package io.github.sz.freyja.raft;

/**
 * What a leader knows of one other member. A view for people and for the checks of an
 * administrator; the protocol does not read it.
 *
 * @param nodeId       the member
 * @param matchIndex   the highest index known to be durable on it and equal to the log of the
 *                     leader
 * @param active       whether it was heard from since the last check for a quorum, or at
 *                     that check
 * @param recovering   whether it is in log-recovering mode, as far as its answers have told
 * @param snapshotting whether a snapshot is on its way to it
 */
public record PeerProgress(int nodeId, long matchIndex, boolean active, boolean recovering, boolean snapshotting) {
}
