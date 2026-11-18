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
