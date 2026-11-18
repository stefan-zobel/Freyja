package io.github.sz.freyja.node;

/**
 * How a node reaches other nodes (docs/spec/node.md, section 1). Nodes exchange only bytes.
 * An implementation may lose, delay, duplicate, and reorder messages; it must not block.
 *
 * <p>Messages for a node arrive through {@link Node#offer}.
 */
@FunctionalInterface
public interface Transport {

    /** Sends one encoded message to a node. A message that cannot be sent is dropped. */
    void send(int toNode, byte[] message);
}
