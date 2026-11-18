package io.github.stefan_zobel.freyja.node.sim;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import io.github.stefan_zobel.freyja.common.kv.CommandResult;

/**
 * What the whole-system simulation has seen of a cluster: the history that the invariants of
 * {@link SystemInvariants} are stated over, and what the latest step added to it
 * (docs/spec/verification.md, sections 2.6 and 3).
 *
 * <p>The simulation records; the invariants only read.
 */
public final class SystemObservation {

    /**
     * A node applied an entry to its state machine.
     *
     * @param commandHash a hash of the bytes of the entry
     * @param sessionId   the session of the command; 0 if the entry carries no command of a
     *                    session
     * @param sequence    the sequence number of the command in its session
     * @param result      what applying gave; {@code null} if the entry carries no command
     */
    public record Applied(int node, long index, long term, long commandHash, long sessionId, long sequence,
                          CommandResult result) {
    }

    /** The state of a node right after it applied the entry {@code index}. */
    public record Digest(int node, long index, long digest) {
    }

    /**
     * A client was told the result of a command.
     *
     * @param index the log index at which the node that answered had applied the command
     */
    public record Acknowledged(int client, long sessionId, long sequence, long index, long commandHash,
                               CommandResult result) {
    }

    private final TreeMap<Long, Applied> firstApplied = new TreeMap<>();
    private final TreeMap<Long, Digest> firstDigest = new TreeMap<>();
    /** By session and sequence number: every application of that command, in the order seen. */
    private final TreeMap<Long, TreeMap<Long, List<Applied>>> bySession = new TreeMap<>();
    private final List<Acknowledged> acknowledged = new ArrayList<>();
    private final TreeMap<Long, List<Acknowledged>> acknowledgedByIndex = new TreeMap<>();
    private final List<Applied> appliedThisStep = new ArrayList<>();
    private final List<Digest> digestsThisStep = new ArrayList<>();
    private final List<Acknowledged> acknowledgedThisStep = new ArrayList<>();

    // ------------------------------------------------------------------ reading

    /** The first application of the entry {@code index} that any node made, or {@code null}. */
    public Applied firstApplied(long index) {
        return firstApplied.get(index);
    }

    /** The first digest that any node had after the entry {@code index}, or {@code null}. */
    public Digest firstDigest(long index) {
        return firstDigest.get(index);
    }

    /**
     * Every application of the command with this session and sequence number, on any node and
     * at any index, in the order the simulation saw them. Two nodes that apply the same entry
     * give two elements.
     */
    public List<Applied> applications(long sessionId, long sequence) {
        TreeMap<Long, List<Applied>> session = bySession.get(sessionId);
        List<Applied> list = session == null ? null : session.get(sequence);
        return list == null ? List.of() : list;
    }

    /** Everything clients were told so far. */
    public List<Acknowledged> acknowledged() {
        return acknowledged;
    }

    /** What clients were told about the entry {@code index}. */
    public List<Acknowledged> acknowledgedAt(long index) {
        List<Acknowledged> list = acknowledgedByIndex.get(index);
        return list == null ? List.of() : list;
    }

    public List<Applied> appliedThisStep() {
        return appliedThisStep;
    }

    public List<Digest> digestsThisStep() {
        return digestsThisStep;
    }

    public List<Acknowledged> acknowledgedThisStep() {
        return acknowledgedThisStep;
    }

    /** The highest index any node has applied. */
    public long highestApplied() {
        return firstApplied.isEmpty() ? 0 : firstApplied.lastKey();
    }

    // ------------------------------------------------------------------ recording

    /** Forgets what the previous step did. The history stays. */
    public void beginStep() {
        appliedThisStep.clear();
        digestsThisStep.clear();
        acknowledgedThisStep.clear();
    }

    public void applied(Applied applied) {
        firstApplied.putIfAbsent(applied.index(), applied);
        if (applied.sessionId() != 0) {
            bySession.computeIfAbsent(applied.sessionId(), _ -> new TreeMap<>())
                    .computeIfAbsent(applied.sequence(), _ -> new ArrayList<>()).add(applied);
        }
        appliedThisStep.add(applied);
    }

    public void digest(Digest digest) {
        firstDigest.putIfAbsent(digest.index(), digest);
        digestsThisStep.add(digest);
    }

    public void acknowledged(Acknowledged ack) {
        acknowledged.add(ack);
        acknowledgedByIndex.computeIfAbsent(ack.index(), _ -> new ArrayList<>()).add(ack);
        acknowledgedThisStep.add(ack);
    }
}
