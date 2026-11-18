package io.github.stefan_zobel.freyja.raft;

/** The role of a node. */
public enum Role {
    FOLLOWER,
    /** Asks whether an election could be won, without changing its term. */
    PRE_CANDIDATE,
    CANDIDATE,
    LEADER
}
