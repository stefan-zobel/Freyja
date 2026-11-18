package io.github.stefan_zobel.freyja.raft.sim;

import java.util.Random;

/**
 * The settings of one simulation run. All times are in milliseconds of virtual time.
 *
 * @param nodes               number of nodes; their ids are {@code 0..nodes-1}
 * @param seed                seed of everything random in the run
 * @param tickMillis          time between two ticks of a node
 * @param minDelay            minimum time a message travels
 * @param maxDelay            maximum time a message travels
 * @param maxPersistDelay     maximum time until a write is durable
 * @param electionTicks       minimum election timeout in ticks
 * @param heartbeatTicks      heartbeat interval in ticks
 * @param maxInflightAppends  flow control: append messages in flight per follower
 * @param maxAppendBytes      flow control: size of the entries of one append
 * @param maxUncommittedBytes flow control: uncommitted backlog of a leader
 * @param proposalInterval    average time between two client proposals
 * @param maxPayload          maximum number of padding bytes of a command
 * @param maxPause            maximum length of a process pause; 0 for no pauses
 */
public record SimOptions(
        int nodes,
        long seed,
        int tickMillis,
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
        int maxPause) {

    public SimOptions {
        if (nodes < 1 || tickMillis < 1 || minDelay < 0 || maxDelay < minDelay
                || maxPersistDelay < 0 || proposalInterval < 1 || maxPayload < 0 || maxPause < 0) {
            throw new IllegalArgumentException(toString());
        }
    }

    /** A quiet cluster: fast network, fast disks, default flow control. */
    public static SimOptions calm(int nodes, long seed) {
        return new SimOptions(nodes, seed, 10, 1, 3, 2, 10, 1, 256, 1 << 20, 64L << 20, 10, 16, 0);
    }

    /** Settings drawn from the seed, so that many seeds cover many shapes of a cluster. */
    public static SimOptions fromSeed(long seed) {
        Random random = new Random(seed * 0x9E3779B97F4A7C15L + 1);
        int[] sizes = {1, 3, 3, 5, 5, 7};
        int[] delays = {3, 10, 40, 150};
        int[] persistDelays = {0, 2, 20, 80};
        int[] windows = {1, 2, 4, 256};
        int[] appendSizes = {16, 64, 1 << 20};
        long[] backlogs = {64, 512, 64L << 20};
        int[] intervals = {2, 5, 20};
        int[] pauses = {0, 300, 600};
        return new SimOptions(
                sizes[random.nextInt(sizes.length)],
                seed,
                10,
                1,
                delays[random.nextInt(delays.length)],
                persistDelays[random.nextInt(persistDelays.length)],
                10,
                1 + random.nextInt(3),
                windows[random.nextInt(windows.length)],
                appendSizes[random.nextInt(appendSizes.length)],
                backlogs[random.nextInt(backlogs.length)],
                intervals[random.nextInt(intervals.length)],
                random.nextInt(40),
                pauses[random.nextInt(pauses.length)]);
    }
}
