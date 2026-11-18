package io.github.stefan_zobel.freyja.raft;

/**
 * A membership change. Each changes the set of voters by at most one node, so that every
 * majority of the old configuration shares a node with every majority of the new one.
 */
public sealed interface ConfigChange {

    /** The node the change is about. */
    int node();

    /** A new node joins as a learner: it receives the log but does not vote. */
    record AddLearner(int node) implements ConfigChange {
    }

    /** A learner that has caught up becomes a voter. */
    record PromoteLearner(int node) implements ConfigChange {
    }

    /** A voter leaves the cluster. */
    record RemoveVoter(int node) implements ConfigChange {
    }

    /** A learner leaves the cluster. */
    record RemoveLearner(int node) implements ConfigChange {
    }
}
