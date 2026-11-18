package io.github.stefan_zobel.freyja.raft;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Several test nodes joined by a network that delivers when the test says so. */
final class TestCluster {

    final List<TestNode> nodes = new ArrayList<>();
    /** Nodes that neither receive nor get their messages delivered. */
    final Set<Integer> isolated = new HashSet<>();

    TestCluster(int size) {
        this(size, RaftConfig.defaults(0, 0));
    }

    TestCluster(int size, RaftConfig template) {
        Set<Integer> ids = new TreeSet<>();
        for (int i = 0; i < size; i++) {
            ids.add(i);
        }
        ClusterConfig cluster = new ClusterConfig(ids, Set.of());
        for (int i = 0; i < size; i++) {
            RaftConfig config = template.withNode(i, 1000 + i);
            TestNode node = new TestNode(config, cluster);
            node.autoPersist = true;
            nodes.add(node);
        }
    }

    /** Starts a node that knows of no cluster yet. Its id is the next free one. */
    TestNode addEmptyNode() {
        int id = nodes.size();
        TestNode node = new TestNode(nodes.getFirst().config.withNode(id, 1000 + id), ClusterConfig.EMPTY);
        node.autoPersist = true;
        nodes.add(node);
        return node;
    }

    TestNode node(int id) {
        return nodes.get(id);
    }

    /** Delivers messages until none is left. Returns the number delivered. */
    int deliverAll() {
        int delivered = 0;
        boolean progress = true;
        while (progress) {
            progress = false;
            for (TestNode node : nodes) {
                int from = node.config.nodeId();
                for (Outbound outbound : node.drainOutbox()) {
                    if (isolated.contains(from) || isolated.contains(outbound.toNode())) {
                        continue;
                    }
                    nodes.get(outbound.toNode()).receive(from, outbound.message());
                    delivered++;
                    progress = true;
                }
            }
        }
        return delivered;
    }

    /** One tick on every node, then all messages. */
    void tickAll() {
        for (TestNode node : nodes) {
            node.tick(1);
        }
        deliverAll();
    }

    TestNode leader() {
        TestNode leader = null;
        for (TestNode node : nodes) {
            if (node.view().role() == Role.LEADER && !isolated.contains(node.config.nodeId())) {
                if (leader != null && leader.view().term() == node.view().term()) {
                    throw new AssertionError("two leaders in term " + node.view().term());
                }
                if (leader == null || node.view().term() > leader.view().term()) {
                    leader = node;
                }
            }
        }
        return leader;
    }

    /** Ticks until a leader exists among the connected nodes. */
    TestNode electLeader() {
        for (int i = 0; i < 1000; i++) {
            TestNode leader = leader();
            if (leader != null) {
                return leader;
            }
            tickAll();
        }
        throw new AssertionError("no leader after 1000 ticks");
    }
}
