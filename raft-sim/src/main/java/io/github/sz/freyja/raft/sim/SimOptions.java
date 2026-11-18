package io.github.sz.freyja.raft.sim;

import java.util.Random;

/**
 * The settings of one simulation run. All times are in milliseconds of virtual time.
 *
 * @param nodes               number of nodes; their ids are {@code 0..nodes-1}
 * @param seed                seed of everything random in the run
 * @param tickMillis          average time between two ticks of a node
 * @param tickSpread          the tick interval of a node differs from the average by up to
 *                            this much; it is fixed per node
 * @param minDelay            minimum time a message travels
 * @param maxDelay            maximum time a message travels
 * @param maxPersistDelay     maximum time until a write is durable
 * @param electionTicks       minimum election timeout in ticks
 * @param heartbeatTicks      heartbeat interval in ticks
 * @param maxInflightAppends  flow control: append messages in flight per follower
 * @param maxAppendBytes      flow control: size of the entries of one append
 * @param maxUncommittedBytes flow control: uncommitted backlog of a leader
 * @param proposalInterval    average time between two proposals of the load client
 * @param maxPayload          maximum number of padding bytes of a command
 * @param checkedOperations   number of operations whose history is checked for
 *                            linearizability
 * @param maxPause            maximum length of a process pause; 0 for no pauses
 * @param dropPercent         share of messages that are lost
 * @param duplicatePercent    share of messages that arrive twice
 * @param partitions          whether the network is partitioned now and then
 * @param crashes             whether nodes crash and restart
 * @param spareNodes          number of nodes that may join the cluster during the run; 0
 *                            for a cluster whose membership never changes
 * @param snapshots           whether nodes write snapshots of their state machine and cut
 *                            their logs
 */
public record SimOptions(
        int nodes,
        long seed,
        int tickMillis,
        int tickSpread,
        int minDelay,
        int maxDelay,
        int maxPersistDelay,
        int electionTicks,
        int heartbeatTicks,
        int maxInflightAppends,
        int maxAppendBytes,
        long maxUncommittedBytes,
        int proposalInterval,
        int maxPayload,
        int checkedOperations,
        int maxPause,
        int dropPercent,
        int duplicatePercent,
        boolean partitions,
        boolean crashes,
        int spareNodes,
        boolean snapshots) {

    public SimOptions {
        if (nodes < 1 || tickMillis < 1 || tickSpread < 0 || tickSpread >= tickMillis
                || minDelay < 0 || maxDelay < minDelay || maxPersistDelay < 0
                || proposalInterval < 1 || maxPayload < 0 || checkedOperations < 0 || maxPause < 0
                || dropPercent < 0 || dropPercent > 100 || duplicatePercent < 0
                || duplicatePercent > 100 || spareNodes < 0) {
            throw new IllegalArgumentException(toString());
        }
    }

    /** A quiet cluster: fast network, fast disks, default flow control, no faults. */
    public static SimOptions calm(int nodes, long seed) {
        return new SimOptions(nodes, seed, 10, 0, 1, 3, 2, 10, 1, 256, 1 << 20, 64L << 20, 10, 16,
                100, 0, 0, 0, false, false, 0, false);
    }

    /**
     * A reliable network: every message arrives exactly once, no node crashes. Delays,
     * flow-control limits, load, and process pauses are drawn from the seed.
     */
    public static SimOptions reliable(long seed) {
        return draw(seed, false);
    }

    /**
     * The full fault model: in addition messages are lost and duplicated, the network is
     * partitioned, tick rates differ, and nodes crash and lose what was not durable.
     */
    public static SimOptions faulty(long seed) {
        return draw(seed, true, 2, 0);
    }

    /**
     * The full fault model, and the membership of the cluster changes: nodes join as
     * learners, learners are promoted, voters (also the leader) and learners are removed.
     */
    public static SimOptions reconfiguring(long seed) {
        return draw(seed, true, 3, 4);
    }

    /**
     * Everything of {@link #reconfiguring}, and nodes write snapshots and cut their logs, so
     * that nodes that are behind or new receive a snapshot instead of the log.
     */
    public static SimOptions compacting(long seed) {
        SimOptions o = draw(seed, true, 4, 4);
        return new SimOptions(o.nodes, o.seed, o.tickMillis, o.tickSpread, o.minDelay, o.maxDelay,
                o.maxPersistDelay, o.electionTicks, o.heartbeatTicks, o.maxInflightAppends,
                o.maxAppendBytes, o.maxUncommittedBytes, o.proposalInterval, o.maxPayload,
                o.checkedOperations, o.maxPause, o.dropPercent, o.duplicatePercent, o.partitions,
                o.crashes, o.spareNodes, true);
    }

    private static SimOptions draw(long seed, boolean faults) {
        return draw(seed, faults, 1, 0);
    }

    private static SimOptions draw(long seed, boolean faults, int salt, int spareNodes) {
        Random random = new Random(seed * 0x9E3779B97F4A7C15L + salt);
        int[] sizes = {1, 3, 3, 5, 5, 7};
        int[] delays = {3, 10, 40, 150};
        int[] persistDelays = {0, 2, 20, 80};
        int[] windows = {1, 2, 4, 256};
        int[] appendSizes = {32, 64, 1 << 20};
        long[] backlogs = {128, 512, 64L << 20};
        int[] intervals = {2, 5, 20};
        int[] pauses = {0, 300, 600};
        int[] drops = {0, 5, 10, 15};
        int[] duplicates = {0, 2, 5};
        int nodes = sizes[random.nextInt(sizes.length)];
        int maxDelay = delays[random.nextInt(delays.length)];
        int maxPersistDelay = persistDelays[random.nextInt(persistDelays.length)];
        int heartbeatTicks = 1 + random.nextInt(3);
        int window = windows[random.nextInt(windows.length)];
        int appendSize = appendSizes[random.nextInt(appendSizes.length)];
        long backlog = backlogs[random.nextInt(backlogs.length)];
        int interval = intervals[random.nextInt(intervals.length)];
        int payload = random.nextInt(40);
        int pause = pauses[random.nextInt(pauses.length)];
        int drop = drops[random.nextInt(drops.length)];
        int duplicate = duplicates[random.nextInt(duplicates.length)];
        boolean partitions = random.nextBoolean();
        boolean crashes = random.nextInt(3) != 0;
        int spread = random.nextBoolean() ? 3 : 0;
        return new SimOptions(nodes, seed, 10, faults ? spread : 0, 1, maxDelay, maxPersistDelay,
                10, heartbeatTicks, window, appendSize, backlog, interval, payload, 150, pause,
                faults ? drop : 0, faults ? duplicate : 0, faults && partitions, faults && crashes,
                spareNodes, false);
    }
}
