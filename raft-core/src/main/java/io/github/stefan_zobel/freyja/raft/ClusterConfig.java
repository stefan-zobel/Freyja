package io.github.stefan_zobel.freyja.raft;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * The members of the cluster. Both sets iterate in ascending order of the node ids, so that
 * everything derived from them is deterministic.
 *
 * @param voters   nodes that vote and count toward quorums
 * @param learners nodes that receive the log but do not vote
 */
public record ClusterConfig(Set<Integer> voters, Set<Integer> learners) {

    public ClusterConfig {
        voters = sorted(voters);
        learners = sorted(learners);
        for (int learner : learners) {
            if (voters.contains(learner)) {
                throw new IllegalArgumentException("node " + learner + " is voter and learner");
            }
        }
    }

    /** A configuration of voters only. */
    public static ClusterConfig ofVoters(Integer... voters) {
        return new ClusterConfig(Set.of(voters), Set.of());
    }

    /** A configuration without members: what a node knows that has not been told anything. */
    public static final ClusterConfig EMPTY = new ClusterConfig(Set.of(), Set.of());

    /**
     * The configuration after {@code change}, or {@code null} if the change does not fit this
     * configuration. The last voter cannot be removed.
     */
    public ClusterConfig with(ConfigChange change) {
        TreeSet<Integer> newVoters = new TreeSet<>(voters);
        TreeSet<Integer> newLearners = new TreeSet<>(learners);
        boolean fits = switch (change) {
            case ConfigChange.AddLearner c -> !voters.contains(c.node()) && newLearners.add(c.node());
            case ConfigChange.PromoteLearner c -> newLearners.remove(c.node()) && newVoters.add(c.node());
            case ConfigChange.RemoveVoter c -> voters.size() > 1 && newVoters.remove(c.node());
            case ConfigChange.RemoveLearner c -> newLearners.remove(c.node());
        };
        return fits ? new ClusterConfig(newVoters, newLearners) : null;
    }

    /** The payload of a CONFIG entry: the voters and the learners, each as a count and ids. */
    public byte[] encode() {
        byte[] bytes = new byte[4 * (2 + voters.size() + learners.size())];
        int at = putInt(bytes, 0, voters.size());
        for (int id : voters) {
            at = putInt(bytes, at, id);
        }
        at = putInt(bytes, at, learners.size());
        for (int id : learners) {
            at = putInt(bytes, at, id);
        }
        return bytes;
    }

    /** Reads what {@link #encode} wrote. */
    public static ClusterConfig decode(byte[] bytes) {
        int[] at = {0};
        TreeSet<Integer> voters = readIds(bytes, at);
        TreeSet<Integer> learners = readIds(bytes, at);
        if (at[0] != bytes.length) {
            throw new IllegalArgumentException("trailing bytes in a configuration");
        }
        return new ClusterConfig(voters, learners);
    }

    private static TreeSet<Integer> readIds(byte[] bytes, int[] at) {
        int count = getInt(bytes, at);
        TreeSet<Integer> ids = new TreeSet<>();
        for (int i = 0; i < count; i++) {
            if (!ids.add(getInt(bytes, at))) {
                throw new IllegalArgumentException("a node id occurs twice in a configuration");
            }
        }
        return ids;
    }

    // Little-endian, written out here: the core has no dependencies.
    private static int putInt(byte[] bytes, int at, int value) {
        bytes[at] = (byte) value;
        bytes[at + 1] = (byte) (value >>> 8);
        bytes[at + 2] = (byte) (value >>> 16);
        bytes[at + 3] = (byte) (value >>> 24);
        return at + 4;
    }

    private static int getInt(byte[] bytes, int[] at) {
        int i = at[0];
        if (i < 0 || i + 4 > bytes.length) {
            throw new IllegalArgumentException("a configuration ends early");
        }
        at[0] = i + 4;
        return (bytes[i] & 0xFF) | (bytes[i + 1] & 0xFF) << 8 | (bytes[i + 2] & 0xFF) << 16
                | (bytes[i + 3] & 0xFF) << 24;
    }

    /** The number of voters that form a majority. */
    public int quorum() {
        return voters.size() / 2 + 1;
    }

    private static SortedSet<Integer> sorted(Set<Integer> ids) {
        TreeSet<Integer> copy = new TreeSet<>(ids);
        if (!copy.isEmpty() && copy.first() < 0) {
            throw new IllegalArgumentException("negative node id " + copy.first());
        }
        return Collections.unmodifiableSortedSet(copy);
    }
}
