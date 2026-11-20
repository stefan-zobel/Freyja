package io.github.sz.freyja.common;

/** The version of this software, as nodes and clients name it to each other. */
public final class Version {

    /** The release this code belongs to. */
    public static final String SOFTWARE = "0.1.0";

    /**
     * The highest cluster version this release can run in. A node names it in every handshake,
     * and the leader raises the cluster version only to a version that every member supports
     * (docs/spec/operations.md, section 6).
     */
    public static final int CLUSTER = 1;

    private Version() {
    }
}
