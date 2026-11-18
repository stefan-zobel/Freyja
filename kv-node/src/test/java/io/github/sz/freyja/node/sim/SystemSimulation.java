package io.github.sz.freyja.node.sim;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import io.github.sz.freyja.common.Checksums;
import io.github.sz.freyja.common.DecodeException;
import io.github.sz.freyja.common.LittleEndian;
import io.github.sz.freyja.common.kv.ClientCodec;
import io.github.sz.freyja.common.kv.ClientRequest;
import io.github.sz.freyja.common.kv.ClientResponse;
import io.github.sz.freyja.common.kv.Command;
import io.github.sz.freyja.common.kv.Command.Condition;
import io.github.sz.freyja.common.kv.Command.Delete;
import io.github.sz.freyja.common.kv.Command.Mutation;
import io.github.sz.freyja.common.kv.Command.Put;
import io.github.sz.freyja.common.kv.CommandCodec;
import io.github.sz.freyja.common.kv.CommandResult;
import io.github.sz.freyja.common.kv.CommandResult.ConditionalApplied;
import io.github.sz.freyja.common.kv.CommandResult.Failed;
import io.github.sz.freyja.common.kv.CommandResult.Failure;
import io.github.sz.freyja.common.kv.CommandResult.SessionRegistered;
import io.github.sz.freyja.common.kv.CommandResult.Written;
import io.github.sz.freyja.lsm.CorruptionException;
import io.github.sz.freyja.lsm.EngineOptions;
import io.github.sz.freyja.lsm.fs.FaultyFileSystem;
import io.github.sz.freyja.lsm.fs.MemoryFileSystem;
import io.github.sz.freyja.node.Node;
import io.github.sz.freyja.node.NodeListener;
import io.github.sz.freyja.node.service.ClientService;
import io.github.sz.freyja.node.sm.StateMachineStore;
import io.github.sz.freyja.node.snapshot.SnapshotTransfers;
import io.github.sz.freyja.node.store.LostIdentityException;
import io.github.sz.freyja.node.store.NodeIdentity;
import io.github.sz.freyja.node.store.RepairMarker;
import io.github.sz.freyja.node.wire.MessageCodec;
import io.github.sz.freyja.raft.ClusterConfig;
import io.github.sz.freyja.raft.ConfigChange;
import io.github.sz.freyja.raft.EntryType;
import io.github.sz.freyja.raft.HardState;
import io.github.sz.freyja.raft.LogEntry;
import io.github.sz.freyja.raft.Notification;
import io.github.sz.freyja.raft.Outbound;
import io.github.sz.freyja.raft.ProtocolState;
import io.github.sz.freyja.raft.RaftConfig;
import io.github.sz.freyja.raft.RaftEvent;
import io.github.sz.freyja.raft.Ready;
import io.github.sz.freyja.raft.Role;
import io.github.sz.freyja.raft.SnapshotMeta;
import io.github.sz.freyja.raft.sim.LinearizabilityChecker;
import io.github.sz.freyja.raft.sim.Observation;
import io.github.sz.freyja.raft.sim.SafetyInvariants;

/**
 * The whole-system simulation (docs/spec/verification.md, section 3): complete nodes and
 * clients in one process, on a simulated clock, a simulated network, and in-memory file
 * systems. Everything that happens follows from the seed.
 *
 * <p>Nodes exchange only bytes. Clients talk to a node as the layer that will receive
 * requests does: they hand it proposals and requests for a read index, and take the answers
 * from what the node reports. A client keeps a session and sends a command again, with the
 * same sequence number, until it is answered.
 *
 * <p>After every step the invariants of {@link SystemInvariants} are checked. At the end the
 * network heals, every node is started, and the run must converge: all nodes in the same
 * state, nothing acknowledged missing, and the history of the clients linearizable.
 */
public final class SystemSimulation {

    /**
     * What a run does.
     *
     * @param nodes       the number of nodes; all are voters from the start
     * @param clients     the number of clients
     * @param keys        the number of keys the clients use
     * @param duration    how long faults are injected and clients start operations, in units
     *                    of simulated time; a tick of a node is {@value #TICK} units
     * @param maxDelay    a message is on its way for 1 to this many units
     * @param drop        the probability that a message is lost
     * @param duplicate   the probability that a message arrives twice
     * @param chaos       the probability, each {@value #CHAOS_PERIOD} units, that a node
     *                    crashes or the network is cut
     * @param storage     whether stored data is damaged, disks run full or slow, and leaders
     *                    are asked to hand over
     * @param spares      how many nodes stand by to take the place of members; with spares,
     *                    members are replaced and identities are lost
     */
    public record Options(int nodes, int clients, int keys, long duration, int maxDelay, double drop,
                          double duplicate, double chaos, boolean storage, int spares) {

        /** A reliable network and no crashes. */
        public static Options calm() {
            return new Options(3, 3, 4, 30_000, 3, 0, 0, 0, false, 0);
        }

        /** Lost and repeated messages, partitions, and crashes, also in the middle of a write. */
        public static Options faulty() {
            return new Options(3, 4, 4, 60_000, 12, 0.03, 0.03, 0.25, false, 0);
        }

        /** All of {@link #faulty}, and damaged files, full disks, slow disks, and hand-overs. */
        public static Options hostile() {
            return new Options(3, 4, 4, 80_000, 12, 0.02, 0.02, 0.3, true, 0);
        }

        /** All of {@link #hostile}, and members that are replaced, some after losing their identity. */
        public static Options changing() {
            return new Options(3, 4, 4, 100_000, 12, 0.02, 0.02, 0.25, true, 4);
        }
    }

    /** What happened in a run; a set of runs must reach every situation. */
    public static final class Stats {
        public long steps;
        public long acknowledged;
        public long reads;
        public long casHeld;
        public long casFailed;
        public long retries;
        public long givenUp;
        public long sessionsLost;
        public long sessionsLostInRetry;
        public long lateDuplicates;
        public long leaderChanges;
        public long crashes;
        public long crashesInAnOperation;
        public long partitions;
        public long dropped;
        public long duplicated;
        public long snapshotsCreated;
        public long snapshotsInstalled;
        public long installsAroundALeaderChange;
        public long highestApplied;
        public long damagedLogs;
        public long damagedTables;
        public long recoveringEntered;
        public long recoveringLeft;
        public long repairs;
        public long damageFoundByScrub;
        public long disksShort;
        public long diskFullStops;
        public long rejectedForSpace;
        public long alarmsCleared;
        public long slowDisks;
        public long slowDiskStepDowns;
        public long transfers;
        public long transfersAborted;
        public long replacements;
        public long identitiesLost;
        public long configurationsReverted;

        public void add(Stats other) {
            steps += other.steps;
            acknowledged += other.acknowledged;
            reads += other.reads;
            casHeld += other.casHeld;
            casFailed += other.casFailed;
            retries += other.retries;
            givenUp += other.givenUp;
            sessionsLost += other.sessionsLost;
            sessionsLostInRetry += other.sessionsLostInRetry;
            lateDuplicates += other.lateDuplicates;
            leaderChanges += other.leaderChanges;
            crashes += other.crashes;
            crashesInAnOperation += other.crashesInAnOperation;
            partitions += other.partitions;
            dropped += other.dropped;
            duplicated += other.duplicated;
            snapshotsCreated += other.snapshotsCreated;
            snapshotsInstalled += other.snapshotsInstalled;
            installsAroundALeaderChange += other.installsAroundALeaderChange;
            highestApplied += other.highestApplied;
            damagedLogs += other.damagedLogs;
            damagedTables += other.damagedTables;
            recoveringEntered += other.recoveringEntered;
            recoveringLeft += other.recoveringLeft;
            repairs += other.repairs;
            damageFoundByScrub += other.damageFoundByScrub;
            disksShort += other.disksShort;
            diskFullStops += other.diskFullStops;
            rejectedForSpace += other.rejectedForSpace;
            alarmsCleared += other.alarmsCleared;
            slowDisks += other.slowDisks;
            slowDiskStepDowns += other.slowDiskStepDowns;
            transfers += other.transfers;
            transfersAborted += other.transfersAborted;
            replacements += other.replacements;
            identitiesLost += other.identitiesLost;
            configurationsReverted += other.configurationsReverted;
        }

        @Override
        public String toString() {
            return "steps " + steps + ", acknowledged " + acknowledged + ", reads " + reads + ", cas held/failed "
                    + casHeld + "/" + casFailed + ", retries " + retries + ", given up " + givenUp
                    + ", sessions lost " + sessionsLost + " (" + sessionsLostInRetry
                    + " under retries), late duplicates " + lateDuplicates + ", leader changes "
                    + leaderChanges + ", crashes "
                    + crashes + " (" + crashesInAnOperation + " in an operation), partitions " + partitions
                    + ", dropped " + dropped + ", duplicated " + duplicated + ", snapshots created/installed "
                    + snapshotsCreated + "/" + snapshotsInstalled + " (" + installsAroundALeaderChange
                    + " around a leader change), applied " + highestApplied
                    + ", damaged logs/tables " + damagedLogs + "/" + damagedTables + ", recovering entered/left "
                    + recoveringEntered + "/" + recoveringLeft + ", repairs " + repairs + " (" + damageFoundByScrub
                    + " after a scrub), disks short " + disksShort + " (" + diskFullStops + " stops, "
                    + rejectedForSpace + " rejections, " + alarmsCleared + " alarms cleared), slow disks "
                    + slowDisks + " (" + slowDiskStepDowns + " step-downs), transfers " + transfers + " ("
                    + transfersAborted + " aborted), members replaced " + replacements + " (" + identitiesLost
                    + " after losing their identity), configurations reverted " + configurationsReverted;
        }
    }

    static final Path DATA = Path.of("data");
    static final int TICK = 10;
    static final int CHAOS_PERIOD = 100;
    /** How long a client waits for an answer before it sends its request again. */
    private static final int PATIENCE = 40 * TICK;
    /** After this many requests without an answer a client gives an operation up. */
    private static final int MAX_ATTEMPTS = 12;
    /** Clients stop when this many operations have an unknown outcome: the checker pays for each. */
    private static final int MAX_GIVEN_UP = 8;
    /**
     * Less than half the time after which a client gives up, so that the session of a client
     * that waits through a longer outage expires while its command is still on its way.
     */
    private static final long SESSION_TIMEOUT = 5L * PATIENCE;

    private static final int EVENT_TICK = 0;
    private static final int EVENT_DELIVER = 1;
    private static final int EVENT_CLIENT = 2;
    private static final int EVENT_CHAOS = 3;
    private static final int EVENT_RESTART = 4;
    private static final int EVENT_HEAL = 5;
    private static final int EVENT_REPLAY = 6;
    private static final int EVENT_DISK_HEAL = 7;
    /** What a node wants to have free; below it, it raises the alarm. */
    private static final long MIN_FREE = 96 * 1024;

    private record Event(long time, long order, int kind, int target, int from, byte[] frame)
            implements Comparable<Event> {
        @Override
        public int compareTo(Event other) {
            int byTime = Long.compare(time, other.time);
            return byTime != 0 ? byTime : Long.compare(order, other.order);
        }
    }

    /** One node with everything that survives a crash, and what does not. */
    private final class Host implements NodeListener {
        final int id;
        final MemoryFileSystem memory = new MemoryFileSystem();
        FaultyFileSystem fs;
        Node node;
        final List<byte[]> outbox = new ArrayList<>();
        long digestedAt = -1;
        boolean crashArmed;
        Role role = Role.FOLLOWER;
        /** Serves the requests of clients on this node. */
        ClientService service;
        /** A file of this node was damaged and the node has not been found whole since. */
        boolean damaged;
        boolean diskShort;
        boolean recovering;
        /** While simulated time is below this, every sync of this node takes long. */
        long slowUntil;
        /** What the clock of this node is ahead of simulated time. */
        long clockSkew;
        /** Whether the node has ever been started; a spare has not. */
        boolean started;
        /** Whether the node is out of the cluster for good. */
        boolean retired;
        boolean identityLost;
        long configIndex;

        /** Whether the node is one of those that are meant to run. */
        boolean active() {
            return started && !retired;
        }

        /** Takes the node out of the cluster for good. */
        void retire() {
            retired = true;
            if (node != null) {
                Node last = node;
                node = null;
                service.detach();
                stats.snapshotsCreated += last.snapshotsCreated();
                stats.slowDiskStepDowns += last.slowSyncStepDowns();
                outbox.clear();
                try {
                    last.close();
                } catch (IOException | RuntimeException | FaultyFileSystem.SimulatedCrash e) {
                    memory.crash(random); // it died while it was stopped
                }
            }
        }

        Host(int id, ClusterConfig config) throws IOException {
            this.id = id;
            Node.initialize(memory, DATA, new NodeIdentity(0x5EED, id), config, 1);
        }

        void start() throws IOException {
            started = true;
            fs = new FaultyFileSystem(memory);
            // A slow disk: every sync takes long by the clock of this node.
            fs.setFault((operation, path, index) -> {
                if (operation == FaultyFileSystem.Operation.SYNC && time < slowUntil) {
                    clockSkew += 20L * TICK;
                }
                return null;
            });
            crashArmed = false;
            role = Role.FOLLOWER;
            digestedAt = -1;
            Node.Options own = new Node.Options(nodeOptions.raft(), nodeOptions.segmentSize(),
                    nodeOptions.queueCapacity(), nodeOptions.snapshotLogBytes(), nodeOptions.snapshotRetainEntries(),
                    nodeOptions.transfers(), nodeOptions.limits(), () -> time + clockSkew);
            // Requests of clients go through the service of the node, as bytes; a tick of the
            // node is TICK units, so the service counts the time of a request in units.
            service = new ClientService(new ClientService.Options(TICK, 100, 1024), () -> time,
                    node -> "node-" + node, this);
            node = Node.open(fs, DATA, own, new StateMachineStore.Options(new EngineOptions()
                    .deterministic(true).reserveSize(0).minFreeSpace(0).memTableSize(32 * 1024)
                    .blockCacheCapacity(1024 * 1024), SESSION_TIMEOUT), (to, frame) -> outbox.add(frame), service);
            service.attach(node);
            if (node.repairedAtStart()) {
                stats.repairs++;
            }
            // What the node holds after its start is its log; what it held in memory is gone.
            protocol.resetLog(id, fullLog());
            log("node " + id + " started" + (node.repairedAtStart() ? ", state machine rebuilt" : "")
                    + (node.lostTailWitnessed() ? ", lost log tail witnessed" : "") + ": " + node.view()
                    + " applied " + node.store().appliedIndex() + " snapshot " + node.snapshot().lastIncludedIndex());
            observeRecovering();
        }

        /** Notes when the node enters or leaves log-recovering mode. */
        void observeRecovering() {
            boolean now = node.view().logRecovering();
            if (now && !recovering) {
                stats.recoveringEntered++;
                log("node " + id + " is in log-recovering mode: " + node.view());
            } else if (!now && recovering) {
                stats.recoveringLeft++;
                log("node " + id + " left log-recovering mode: " + node.view());
            }
            recovering = now;
        }

        boolean running() {
            return node != null;
        }

        /** The process is gone. What it held in memory is lost with it. */
        void down(boolean crashFirst) {
            Node dead = node;
            node = null;
            service.detach(); // whoever waits is told to ask again
            // The state machine of a node that died in an installation is not to be asked.
            log("node " + id + " down" + (crashFirst ? "" : " (crash in an operation)") + ": " + dead.view());
            stats.snapshotsCreated += dead.snapshotsCreated();
            stats.slowDiskStepDowns += dead.slowSyncStepDowns();
            outbox.clear();
            if (crashFirst) {
                memory.crash(random);
            }
            try {
                dead.close(); // only releases the memory of the dead instance
            } catch (IOException | RuntimeException | FaultyFileSystem.SimulatedCrash expected) {
                // its files are gone
            }
            if (!retired) {
                schedule(time + 5L * TICK + random.nextInt(60 * TICK), EVENT_RESTART, id, -1, null);
            }
        }

        @Override
        public void notification(Notification notification) {
            switch (notification) {
                case Notification.RoleChanged change -> {
                    if (change.role() == Role.LEADER && role != Role.LEADER) {
                        stats.leaderChanges++;
                        lastLeaderChange = time;
                    }
                    if (change.role() != role) {
                        log("node " + id + " is " + change.role() + " of term " + change.term() + ": "
                                + node.view());
                    }
                    role = change.role();
                }
                case Notification.TransferAborted _ -> stats.transfersAborted++;
                case Notification.ConfigChanged changed -> {
                    // A configuration of an entry that was cut off again gives way to an
                    // older one.
                    if (changed.index() < configIndex) {
                        stats.configurationsReverted++;
                    }
                    configIndex = changed.index();
                }
                default -> {
                }
            }
        }

        @Override
        public void applied(LogEntry entry, CommandResult result) {
            long session = 0;
            long sequence = 0;
            if (entry.type() == EntryType.COMMAND) {
                try {
                    Command command = CommandCodec.decode(entry.payload()).command();
                    if (command instanceof Command.Write write) {
                        session = write.sessionId();
                        sequence = write.sequence();
                    } else if (command instanceof Command.Conditional conditional) {
                        session = conditional.sessionId();
                        sequence = conditional.sequence();
                    }
                } catch (DecodeException e) {
                    throw new AssertionError("a command of the simulation does not decode", e);
                }
            }
            long hash = hash(entry);
            if (debug && entry.index() % 50 == 0) {
                log("node " + id + " applied " + entry.index() + "@" + entry.term());
            }
            protocol.applied(id, entry);
            observation.applied(new SystemObservation.Applied(id, entry.index(), entry.term(), hash, session,
                    sequence, result));
        }

        // ---- the protocol as the invariants of the Raft simulator see it

        @Override
        public boolean observesCore() {
            return true;
        }

        @Override
        public void stepped(RaftEvent event, ProtocolState state) {
            protocol.sawCommitted(state.commitIndex(), state.term());
            if (event instanceof RaftEvent.SnapshotInstalled && node != null) {
                // The stored log starts after the snapshot now, with or without what followed.
                protocol.resetLog(id, fullLog());
            }
        }

        @Override
        public void ready(Ready ready, HardState durableHardState, long durableLastIndex) {
            if (ready.truncateSuffixFrom() != -1) {
                protocol.truncated(id, ready.truncateSuffixFrom());
            }
            for (LogEntry entry : ready.entries()) {
                protocol.appended(id, entry);
            }
            // Roles first: the election of a leader is on record before its messages.
            for (Notification note : ready.notifications()) {
                if (note instanceof Notification.RoleChanged change && change.role() == Role.LEADER) {
                    protocol.elected(id, change.term());
                }
            }
            for (Outbound outbound : ready.messages()) {
                protocol.released(id, outbound, durableHardState, durableLastIndex);
            }
        }

        /** The log of the node from index 1: what its snapshot covers, then what it stores. */
        List<LogEntry> fullLog() {
            long covered = node.snapshot().lastIncludedIndex();
            List<LogEntry> applied = protocol.appliedLog();
            if (covered > applied.size()) {
                throw new AssertionError("node " + id + " has a snapshot at " + covered + ", applied anywhere are "
                        + applied.size());
            }
            List<LogEntry> log = new ArrayList<>(applied.subList(0, (int) covered));
            log.addAll(node.storedEntries(covered + 1, Integer.MAX_VALUE));
            return log;
        }

        @Override
        public void snapshotInstalled(SnapshotMeta meta) {
            stats.snapshotsInstalled++;
            if (time - lastLeaderChange < 40L * TICK) {
                // The leader changed while the snapshot was on its way or being installed.
                stats.installsAroundALeaderChange++;
            }
            log("node " + id + " installed the snapshot " + meta.lastIncludedIndex() + "@" + meta.lastIncludedTerm());
        }
    }

    private enum Kind {
        REGISTER, WRITE, CAS, READ
    }

    /** A client with one operation at a time. */
    private final class Client {
        final int id;
        long session;
        long sequence;
        Kind kind;
        int key;
        long a;
        long b;
        long invoked;
        boolean waiting;
        /** The request ids of the attempts of the operation in hand. */
        final Set<Long> attempts = new HashSet<>();
        /** How often the client waited for an answer in vain. */
        int timeouts;
        /** Whether a node has taken a request of this operation: it may be applied. */
        boolean taken;
        /** How often the command in hand was handed to a node. */
        int sends;
        /** Whether the last request was refused at once; the next turn sends it again. */
        boolean refused;
        long sentAt;
        int target = -1;
        boolean stopped;
        /** The encoded command of the operation in hand. */
        byte[] sent;

        Client(int id) {
            this.id = id;
        }

        void act() {
            if (stopped) {
                return;
            }
            if (!waiting) {
                if (!clientsMayStart) {
                    stopped = true;
                    return;
                }
                attempts.clear();
                timeouts = 0;
                taken = false;
                sends = 0;
                refused = false;
                if (session == 0) {
                    kind = Kind.REGISTER;
                } else {
                    int choice = random.nextInt(10);
                    kind = choice < 4 ? Kind.READ : choice < 8 ? Kind.WRITE : Kind.CAS;
                    key = random.nextInt(options.keys());
                    a = random.nextInt(4);
                    b = 1 + random.nextInt(3);
                    if (kind != Kind.READ) {
                        sequence++;
                    }
                }
                invoked = ++logicalTime;
                waiting = true;
                send();
            } else if (refused) {
                // Refused at once, for example by a node that is not the leader. That is
                // no sign of trouble and does not count toward giving up.
                refused = false;
                send();
            } else if (time - sentAt >= PATIENCE) {
                timeouts++;
                if (timeouts >= MAX_ATTEMPTS && kind != Kind.REGISTER) {
                    giveUp();
                } else {
                    stats.retries++;
                    target = -1;
                    send();
                }
            }
        }

        private void send() {
            sentAt = time;
            if (target < 0 || !hosts.get(target).running()) {
                target = randomActive().id;
            }
            Host host = hosts.get(target);
            if (!host.running()) {
                target = -1;
                refused = true; // connection refused
                return;
            }
            long requestId = ++requests;
            attempts.add(requestId);
            clientOf.put(requestId, this);
            ClientRequest request;
            if (kind == Kind.READ) {
                request = new ClientRequest.Get(key(key), false);
            } else {
                request = new ClientRequest.Execute(switch (kind) {
                    case REGISTER -> new Command.RegisterSession();
                    case WRITE -> new Command.Write(session, sequence, List.of(mutation(key, a)));
                    default -> new Command.Conditional(session, sequence,
                            List.of(a == 0 ? Condition.notExists(key(key)) : Condition.valueEquals(key(key), value(a))),
                            List.of(mutation(key, b)), List.of());
                });
                // From here on a node may have the command, whatever it answers: also a node
                // that says at once that it is not the leader any more, or that stops.
                taken = true;
                sends++;
            }
            // The request travels as bytes, and so does its answer. The node that takes a
            // command stamps it with its clock.
            sent = ClientCodec.encode(requestId, 3 * PATIENCE, request);
            try {
                host.service.request(sent, replies::add);
            } catch (IOException e) {
                stopped(host, e);
                return;
            }
            pump(host);
        }

        void rejected(long requestId, int leaderHint) {
            if (waiting && attempts.contains(requestId)) {
                target = leaderHint >= 0 && leaderHint < hosts.size() ? leaderHint : -1;
                refused = true;
            }
        }

        void answered(long requestId, CommandResult result, long index) {
            if (!waiting || !attempts.contains(requestId)) {
                return; // the answer to an operation that is over
            }
            if (kind != Kind.REGISTER) {
                // The answer names the entry at which the command was applied.
                SystemObservation.Applied entry = observation.firstApplied(index);
                if (entry == null || entry.sessionId() != session || entry.sequence() != sequence) {
                    throw new AssertionError("client " + id + " was answered for command " + sequence
                            + " of session " + session + " with entry " + index + ", which is " + entry);
                }
                stats.acknowledged++;
                observation.acknowledged(new SystemObservation.Acknowledged(id, session, sequence, index,
                        entry.commandHash(), result));
            }
            switch (result) {
                case SessionRegistered registered -> {
                    this.session = registered.sessionId();
                    this.sequence = 0;
                    waiting = false;
                }
                case Written _ -> complete(0);
                case ConditionalApplied applied -> {
                    if (applied.conditionsHeld()) {
                        stats.casHeld++;
                    } else {
                        stats.casFailed++;
                    }
                    complete(applied.conditionsHeld() ? 1 : 0);
                }
                case Failed failed when failed.failure() == Failure.NO_SUCH_SESSION -> {
                    // The session has expired. If the command was handed to a node before,
                    // that earlier request may have taken effect before the session ended:
                    // the outcome is not known. Otherwise this was the only request and it
                    // did nothing.
                    stats.sessionsLost++;
                    this.session = 0;
                    if (sends > 1) {
                        stats.sessionsLostInRetry++;
                        unknown();
                    } else {
                        waiting = false;
                    }
                }
                case Failed failed when failed.failure() == Failure.NO_SPACE -> {
                    // The alarm is set. The command did not use its sequence number up; the
                    // client sends it again later.
                    stats.rejectedForSpace++;
                    refused = true;
                }
                default -> throw new AssertionError("client " + id + " got " + result + " for " + kind
                        + " " + this.sequence + " of session " + this.session);
            }
        }

        void readAnswered(long requestId, long value) {
            if (waiting && kind == Kind.READ && attempts.contains(requestId)) {
                stats.reads++;
                complete(value);
            }
        }

        private void complete(long result) {
            history.add(new LinearizabilityChecker.Operation(key, switch (kind) {
                case WRITE -> LinearizabilityChecker.Kind.WRITE;
                case CAS -> LinearizabilityChecker.Kind.COMPARE_AND_SET;
                default -> LinearizabilityChecker.Kind.READ;
            }, a, b, invoked, ++logicalTime, result));
            waiting = false;
            if (kind != Kind.READ && faults && random.nextInt(4) == 0) {
                // A copy of the request that was held up somewhere arrives much later, when
                // the client has long moved on.
                stats.lateDuplicates++;
                schedule(time + random.nextInt(60 * TICK), EVENT_REPLAY, randomActive().id, -1, sent);
            }
        }

        private void giveUp() {
            if (kind == Kind.READ || !taken) {
                // A read that never returned says nothing, and neither does a command that
                // no node ever took.
                waiting = false;
                return;
            }
            unknown();
        }

        /** The operation may have taken effect or not, now or later. */
        private void unknown() {
            history.add(new LinearizabilityChecker.Operation(key,
                    kind == Kind.WRITE ? LinearizabilityChecker.Kind.WRITE : LinearizabilityChecker.Kind.COMPARE_AND_SET,
                    a, b, invoked, LinearizabilityChecker.NO_RESPONSE, 0));
            waiting = false;
            stats.givenUp++;
            if (stats.givenUp >= MAX_GIVEN_UP) {
                clientsMayStart = false;
            }
        }
    }

    private final long seed;
    private final Options options;
    private final Random random;
    private final Node.Options nodeOptions;
    private final List<Host> hosts = new ArrayList<>();
    private final List<Client> clients = new ArrayList<>();
    private final PriorityQueue<Event> events = new PriorityQueue<>();
    private final TreeMap<Long, Client> clientOf = new TreeMap<>();
    /** Answers of nodes to clients, as bytes, that the clients have not looked at yet. */
    private final java.util.ArrayDeque<byte[]> replies = new java.util.ArrayDeque<>();
    private final Set<Integer> isolated = new HashSet<>();
    private final SystemObservation observation = new SystemObservation();
    /** What the invariants of the Raft simulator are stated over, fed from complete nodes. */
    private Observation protocol;
    private final List<LinearizabilityChecker.Operation> history = new ArrayList<>();
    private final Stats stats = new Stats();
    private long time;
    private long order;
    private long lastLeaderChange = Long.MIN_VALUE / 2;
    private long logicalTime;
    private long requests;
    /**
     * Request ids of what the simulation itself proposes to a core, as administrator. The
     * service of a node numbers the requests of clients from 1 on; these stay clear of them.
     */
    private long ownRequests = 1L << 50;
    private boolean clientsMayStart = true;
    private boolean faults = true;
    /** Set the environment variable FREYJA_SIM_DEBUG to see what a run does. */
    private final boolean debug = System.getenv("FREYJA_SIM_DEBUG") != null;

    private void log(String what) {
        if (debug) {
            System.out.println("[sim " + time + "] " + what);
        }
    }

    public SystemSimulation(long seed, Options options) {
        this.seed = seed;
        this.options = options;
        this.random = new Random(seed);
        this.nodeOptions = new Node.Options(RaftConfig.defaults(0, seed), 8 * 1024, 4096, 32 * 1024, 20,
                new SnapshotTransfers.Options(2048, 5, 60, 2),
                new Node.Limits(10_000, 1L << 40, options.storage() ? MIN_FREE : 0, 10,
                        options.storage() ? 5L * TICK : Long.MAX_VALUE), () -> time);
    }

    // ------------------------------------------------------------------ helpers

    private static byte[] key(int key) {
        return ("k" + key).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * The stored form of a value: the number, and padding that follows from it, so that the
     * tables of the state machine grow beyond one block and damage can hide in them.
     */
    private static byte[] value(long value) {
        byte[] bytes = new byte[8 + 248];
        LittleEndian.putUint64(bytes, 0, value);
        Arrays.fill(bytes, 8, bytes.length, (byte) (value * 37 + 11));
        return bytes;
    }

    /** The value 0 stands for a key that is not there. */
    private static Mutation mutation(int key, long value) {
        return value == 0 ? new Delete(key(key)) : new Put(key(key), value(value));
    }

    private static long hash(LogEntry entry) {
        return Checksums.crc32c(entry.payload(), 0, entry.payload().length) * 31L + entry.type().ordinal();
    }

    private void schedule(long at, int kind, int target, int from, byte[] frame) {
        events.add(new Event(at, order++, kind, target, from, frame));
    }

    /**
     * Hands the answers of the nodes to the clients. An answer is looked at only after the
     * node has finished its step: by then the simulation has seen what the node applied.
     */
    private void deliverReplies() {
        while (!replies.isEmpty()) {
            ClientCodec.ResponseEnvelope envelope = ClientCodec.decodeResponse(replies.pollFirst());
            Client client = clientOf.remove(envelope.requestId());
            if (client == null) {
                continue; // nobody waits for it: a late duplicate
            }
            switch (envelope.response()) {
                case ClientResponse.Result result -> client.answered(envelope.requestId(), result.result(),
                        result.index());
                case ClientResponse.Value value -> client.readAnswered(envelope.requestId(),
                        value.found() ? LittleEndian.getUint64(value.value(), 0) : 0);
                case ClientResponse.NotLeader redirect -> client.rejected(envelope.requestId(), redirect.leaderId());
                case ClientResponse.Rejected rejected -> {
                    if (rejected.reason() == ClientResponse.Reason.INVALID
                            || rejected.reason() == ClientResponse.Reason.UNSUPPORTED) {
                        throw new AssertionError("client " + client.id + " was refused: " + rejected.reason());
                    }
                    client.rejected(envelope.requestId(), -1);
                }
                default -> throw new AssertionError("client " + client.id + " got " + envelope.response());
            }
        }
    }

    // ------------------------------------------------------------------ nodes and network

    /** Lets a node work until it is idle, and sends what it produced. */
    private void pump(Host host) {
        if (!host.running()) {
            return;
        }
        try {
            while (host.node.runOnce()) {
                // work the queue off
            }
            host.observeRecovering();
            long applied = host.node.store().appliedIndex();
            if (applied != host.digestedAt) {
                host.digestedAt = applied;
                observation.digest(new SystemObservation.Digest(host.id, applied,
                        host.node.store().machine().digest()));
            }
            host.service.afterStep();
            deliverReplies();
        } catch (FaultyFileSystem.SimulatedCrash crash) {
            stats.crashes++;
            stats.crashesInAnOperation++;
            host.down(false);
            return;
        } catch (IOException | java.io.UncheckedIOException e) {
            stopped(host, e);
            return;
        }
        List<byte[]> frames = new ArrayList<>(host.outbox);
        host.outbox.clear();
        for (byte[] frame : frames) {
            route(host.id, frame);
        }
    }

    /**
     * A node stopped itself or the simulation met a failure of its storage. That is expected
     * of a node whose files were damaged or whose disk is full, and of no other.
     */
    private void stopped(Host host, Exception cause) {
        Throwable root = cause instanceof java.io.UncheckedIOException ? cause.getCause() : cause;
        try {
            if (host.damaged && (root instanceof CorruptionException
                    || RepairMarker.exists(host.memory, DATA))) {
                if (root instanceof CorruptionException && host.running() && !host.node.stopped()) {
                    host.node.stateMachineDamaged(); // the simulation itself read the damage
                }
                host.down(true);
                return;
            }
        } catch (IOException e) {
            throw new AssertionError("node " + host.id + ": " + e, e);
        }
        if (host.diskShort) {
            stats.diskFullStops++;
            host.down(true);
            return;
        }
        throw new AssertionError("node " + host.id + " stopped: " + cause, cause);
    }

    private void route(int from, byte[] frame) {
        int to = MessageCodec.addressee(frame);
        if (to < 0 || to >= hosts.size()) {
            return;
        }
        if (isolated.contains(from) || isolated.contains(to) || (faults && random.nextDouble() < options.drop())) {
            stats.dropped++;
            return;
        }
        schedule(time + 1 + random.nextInt(options.maxDelay()), EVENT_DELIVER, to, from, frame);
        if (faults && random.nextDouble() < options.duplicate()) {
            stats.duplicated++;
            schedule(time + 1 + random.nextInt(options.maxDelay() * 3), EVENT_DELIVER, to, from, frame.clone());
        }
    }

    private void chaos() {
        if (random.nextDouble() >= options.chaos()) {
            return;
        }
        long down = active().stream().filter(h -> !h.running()).count();
        int choice = random.nextInt(options.spares() > 0 ? 17 : options.storage() ? 16 : 10);
        if (choice == 16) {
            loseIdentity();
        } else if (choice >= 10) {
            storageChaos(choice);
        } else if (choice < 5) {
            // Mostly a minority is down at one time, so that the cluster makes progress.
            Host victim = randomActive();
            if (victim.running() && (down < (active().size() - 1) / 2 || random.nextInt(8) == 0)) {
                if (random.nextBoolean()) {
                    stats.crashes++;
                    victim.down(true);
                } else if (!victim.crashArmed) {
                    // The crash comes in the middle of one of the next file operations.
                    victim.crashArmed = true;
                    victim.fs.crashAt(victim.fs.operationCount() + random.nextInt(60), random,
                            () -> victim.memory.crash(random));
                }
            }
        } else if (isolated.isEmpty()) {
            stats.partitions++;
            isolated.add(randomActive().id);
            schedule(time + 20L * TICK + random.nextInt(80 * TICK), EVENT_HEAL, -1, -1, null);
        }
    }

    /** The nodes that are meant to run: started and not retired. */
    private List<Host> active() {
        List<Host> list = new ArrayList<>();
        for (Host host : hosts) {
            if (host.active()) {
                list.add(host);
            }
        }
        return list;
    }

    private Host randomActive() {
        List<Host> list = active();
        return list.get(random.nextInt(list.size()));
    }

    /** Whether every node runs, none is repairing anything, and no member is being replaced. */
    private boolean allHealthy() {
        if (phase != Phase.IDLE) {
            return false;
        }
        for (Host host : active()) {
            if (!host.running() || host.damaged || host.recovering || host.node.view().logRecovering()) {
                return false;
            }
        }
        return true;
    }

    // ------------------------------------------------------------------ membership

    private enum Phase {
        IDLE, ADD, PROMOTE, REMOVE
    }

    private Phase phase = Phase.IDLE;
    private int joining = -1;
    private int leaving = -1;
    private int nextSpare;

    private Host leader() {
        for (Host host : active()) {
            if (host.running() && host.role == Role.LEADER) {
                return host;
            }
        }
        return null;
    }

    /**
     * The administrator, played by the simulation, replaces a member: a spare joins as a
     * learner, is promoted, and then the member is removed and retired. One step per turn;
     * a step that the cluster refuses is asked for again at the next turn.
     */
    private void administer() throws IOException {
        Host leader = leader();
        if (leader == null) {
            return;
        }
        ClusterConfig config = leader.node.store().machine().config();
        switch (phase) {
            case IDLE -> {
                if (!faults || nextSpare >= hosts.size() || random.nextInt(8) != 0 || !allHealthy()) {
                    return;
                }
                List<Integer> voters = new ArrayList<>(new java.util.TreeSet<>(config.voters()));
                begin(voters.get(random.nextInt(voters.size())));
            }
            case ADD -> {
                if (config.learners().contains(joining) || config.voters().contains(joining)) {
                    phase = Phase.PROMOTE;
                } else {
                    change(leader, new ConfigChange.AddLearner(joining));
                }
            }
            case PROMOTE -> {
                if (config.voters().contains(joining)) {
                    phase = Phase.REMOVE;
                } else {
                    change(leader, new ConfigChange.PromoteLearner(joining));
                }
            }
            case REMOVE -> {
                if (config.voters().contains(leaving)) {
                    change(leader, new ConfigChange.RemoveVoter(leaving));
                    return;
                }
                // The member goes when nobody who stays counts on it any more.
                for (Host host : active()) {
                    if (host.id != leaving && (!host.running()
                            || host.node.store().machine().config().voters().contains(leaving))) {
                        return;
                    }
                }
                hosts.get(leaving).retire();
                stats.replacements++;
                phase = Phase.IDLE;
            }
        }
    }

    private void begin(int member) throws IOException {
        joining = nextSpare++;
        leaving = member;
        hosts.get(joining).start();
        pump(hosts.get(joining));
        phase = Phase.ADD;
    }

    private void change(Host leader, ConfigChange change) {
        leader.node.submit(new RaftEvent.ProposeConfigChange(++ownRequests, change));
        pump(leader);
    }

    /**
     * A node loses the file that says who it is. It can never start again; the administrator
     * replaces it.
     */
    private void loseIdentity() {
        try {
            if (nextSpare >= hosts.size() || !allHealthy()) {
                return;
            }
            Host victim = randomActive();
            victim.identityLost = true;
            victim.down(true);
            stats.crashes++;
            stats.identitiesLost++;
            Path file = DATA.resolve(NodeIdentity.FILE_NAME);
            byte[] one = victim.memory.peek(file, 6, 1);
            one[0] ^= 0x10;
            victim.memory.overwrite(file, 6, one);
            begin(victim.id);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void storageChaos(int choice) {
        Host victim = randomActive();
        try {
            switch (choice) {
                case 10, 11 -> {
                    // One node at a time loses what it has stored: the others still hold it.
                    if (allHealthy()) {
                        damage(victim);
                    }
                }
                case 12 -> {
                    if (victim.running() && !victim.diskShort) {
                        log("node " + victim.id + ": disk short");
                        stats.disksShort++;
                        victim.diskShort = true;
                        long used = Long.MAX_VALUE - victim.memory.freeSpace(DATA);
                        victim.memory.setCapacity(used + MIN_FREE / 2);
                        schedule(time + 100L * TICK + random.nextInt(300 * TICK), EVENT_DISK_HEAL, victim.id, -1,
                                null);
                    }
                }
                case 13 -> {
                    if (victim.running() && victim.slowUntil < time) {
                        stats.slowDisks++;
                        victim.slowUntil = time + 50L * TICK + random.nextInt(100 * TICK);
                    }
                }
                case 14 -> {
                    Host host = leader();
                    if (host != null && victim != host) {
                        log("node " + host.id + " is asked to hand over to node " + victim.id);
                        stats.transfers++;
                        host.node.submit(new RaftEvent.TransferLeadership(++ownRequests, victim.id));
                        pump(host);
                    }
                }
                default -> scrub(victim);
            }
        } catch (IOException e) {
            throw new AssertionError("node " + victim.id + ": " + e, e);
        }
    }

    /** The node crashes, and one bit of a segment of its log or of a table is not what it was. */
    private void damage(Host victim) throws IOException {
        victim.down(true);
        stats.crashes++;
        boolean log = random.nextBoolean();
        List<Path> candidates = new ArrayList<>();
        if (log) {
            for (Path path : victim.memory.list(DATA.resolve(Node.RAFT_DIRECTORY))) {
                if (path.getFileName().toString().endsWith(".rlog")) {
                    candidates.add(path);
                }
            }
            // Only bytes that are proven to be synced are damaged (verification.md,
            // section 2.2): the newest segment has no successor that vouches for its
            // length, and damage at its end is the stated residual case of the format.
            if (!candidates.isEmpty()) {
                candidates.sort(null);
                candidates.removeLast();
            }
        }
        if (!log || candidates.isEmpty()) {
            log = false;
            for (Path generation : victim.memory.list(DATA.resolve(StateMachineStore.DIRECTORY))) {
                if (generation.getFileName().toString().matches("\\d{10}")) {
                    for (Path path : victim.memory.list(generation)) {
                        if (path.getFileName().toString().endsWith(".sst")) {
                            candidates.add(path);
                        }
                    }
                }
            }
        }
        candidates.removeIf(path -> {
            try {
                return victim.memory.size(path) == 0;
            } catch (IOException e) {
                return true;
            }
        });
        if (candidates.isEmpty()) {
            return;
        }
        Path file = candidates.get(random.nextInt(candidates.size()));
        long offset = (long) (random.nextDouble() * victim.memory.size(file));
        byte[] one = victim.memory.peek(file, offset, 1);
        one[0] ^= (byte) (1 << random.nextInt(8));
        victim.memory.overwrite(file, offset, one);
        victim.damaged = true;
        log("node " + victim.id + ": a bit of " + file.getFileName() + " flipped at " + offset);
        if (log) {
            stats.damagedLogs++;
        } else {
            stats.damagedTables++;
        }
    }

    /**
     * Reads every block of the state machine of a node. Damage that is found stops the node
     * at its next step; a node without damage counts as whole again.
     */
    private void scrub(Host host) throws IOException {
        if (!host.running() || !host.damaged || host.node.view().logRecovering()) {
            return;
        }
        if (host.node.store().machine().engine().scrub() > 0) {
            stats.damageFoundByScrub++;
            pump(host);
        } else {
            host.damaged = false;
        }
    }

    /** An administrator clears the alarm for lack of space once no disk is short any more. */
    private void clearAlarm() {
        for (Host host : hosts) {
            if (host.diskShort) {
                return;
            }
        }
        for (Host host : hosts) {
            if (host.running() && host.role == Role.LEADER
                    && host.node.store().machine().alarms().contains(Command.Alarm.NOSPACE)) {
                stats.alarmsCleared++;
                host.node.propose(++ownRequests, CommandCodec.encode(0, new Command.ClearAlarm(Command.Alarm.NOSPACE)),
                        host.node.now() + 3L * PATIENCE / TICK);
                pump(host);
                return;
            }
        }
    }

    private void handle(Event event) {
        switch (event.kind()) {
            case EVENT_TICK -> {
                Host host = hosts.get(event.target());
                if (host.running()) {
                    host.node.tick();
                    pump(host);
                }
                schedule(time + TICK, EVENT_TICK, host.id, -1, null);
            }
            case EVENT_DELIVER -> {
                Host host = hosts.get(event.target());
                if (host.running() && !isolated.contains(host.id) && !isolated.contains(event.from())) {
                    host.node.offer(event.frame());
                    pump(host);
                }
            }
            case EVENT_CLIENT -> {
                Client client = clients.get(event.target());
                client.act();
                if (!client.stopped) {
                    schedule(time + TICK + random.nextInt(3 * TICK), EVENT_CLIENT, client.id, -1, null);
                }
            }
            case EVENT_CHAOS -> {
                if (options.storage()) {
                    clearAlarm();
                }
                if (options.spares() > 0) {
                    try {
                        administer();
                    } catch (IOException e) {
                        throw new AssertionError("the administrator: " + e, e);
                    }
                }
                if (faults) {
                    chaos();
                }
                schedule(time + CHAOS_PERIOD, EVENT_CHAOS, -1, -1, null);
            }
            case EVENT_DISK_HEAL -> {
                Host host = hosts.get(event.target());
                host.memory.setCapacity(Long.MAX_VALUE);
                host.diskShort = false;
            }
            case EVENT_RESTART -> {
                Host host = hosts.get(event.target());
                if (!host.running() && !host.retired) {
                    try {
                        host.start();
                    } catch (LostIdentityException e) {
                        if (!host.identityLost) {
                            throw new AssertionError("node " + host.id + " does not start: " + e, e);
                        }
                        host.retired = true; // it refuses to start, for good
                        return;
                    } catch (IOException e) {
                        if (!host.diskShort) {
                            throw new AssertionError("node " + host.id + " does not start: " + e, e);
                        }
                        // No room to start. The failed start is one more crash.
                        host.memory.crash(random);
                        schedule(time + 20L * TICK + random.nextInt(60 * TICK), EVENT_RESTART, host.id, -1, null);
                        return;
                    }
                    pump(host);
                }
            }
            case EVENT_HEAL -> isolated.clear();
            case EVENT_REPLAY -> {
                Host host = hosts.get(event.target());
                if (host.running()) {
                    // Nobody waits for the answer.
                    try {
                        host.service.request(event.frame(), replies::add);
                    } catch (IOException e) {
                        stopped(host, e);
                        return;
                    }
                    pump(host);
                }
            }
            default -> throw new IllegalStateException("event " + event.kind());
        }
    }

    private void step() {
        Event event = events.poll();
        time = Math.max(time, event.time());
        observation.beginStep();
        protocol.beginStep();
        handle(event);
        deliverReplies(); // also what a node that went down told those who waited
        SystemInvariants.checkAll(observation);
        SafetyInvariants.checkAll(protocol);
        stats.steps++;
        if (stats.steps % 5_000 == 0) {
            checkCompleteLogs();
        }
    }

    /** Log matching over the complete logs of all nodes; too costly for every step. */
    private void checkCompleteLogs() {
        String violation = SafetyInvariants.logMatchingComplete(protocol);
        if (violation != null) {
            throw new AssertionError("LogMatching violated: " + violation);
        }
    }

    // ------------------------------------------------------------------ the run

    /**
     * Runs the simulation.
     *
     * @return what happened
     * @throws AssertionError if an invariant does not hold, the run does not converge, or the
     *                        history of the clients is not linearizable; the message names
     *                        the seed
     */
    public Stats run() {
        try {
            simulate();
            return stats;
        } catch (AssertionError | RuntimeException | IOException e) {
            throw new AssertionError("whole-system simulation, seed " + seed + ", time " + time + ": "
                    + e.getMessage() + " [" + stats + "]", e);
        } finally {
            for (Host host : hosts) {
                if (host.running()) {
                    try {
                        host.node.close();
                    } catch (IOException | RuntimeException | FaultyFileSystem.SimulatedCrash ignored) {
                        // the run is over
                    }
                    host.node = null;
                }
            }
        }
    }

    private void simulate() throws IOException {
        Integer[] ids = new Integer[options.nodes()];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = i;
        }
        ClusterConfig config = ClusterConfig.ofVoters(ids);
        protocol = new Observation(options.nodes() + options.spares(), config);
        for (int i = 0; i < options.nodes(); i++) {
            Host host = new Host(i, config);
            hosts.add(host);
            host.start();
            schedule(random.nextInt(TICK), EVENT_TICK, i, -1, null);
        }
        // Spares know nothing of the cluster; they run from the moment they are to join.
        for (int i = options.nodes(); i < options.nodes() + options.spares(); i++) {
            hosts.add(new Host(i, ClusterConfig.EMPTY));
            schedule(random.nextInt(TICK), EVENT_TICK, i, -1, null);
        }
        nextSpare = options.nodes();
        for (int i = 0; i < options.clients(); i++) {
            clients.add(new Client(i));
            schedule(30L * TICK + random.nextInt(10 * TICK), EVENT_CLIENT, i, -1, null);
        }
        schedule(CHAOS_PERIOD, EVENT_CHAOS, -1, -1, null);

        while (time < options.duration()) {
            step();
        }

        // Healing: no more faults, every node runs, clients finish what they have in hand.
        faults = false;
        clientsMayStart = false;
        isolated.clear();
        for (Host host : hosts) {
            if (host.running() && host.crashArmed) {
                host.fs.crashAt(-1, null, null);
                host.crashArmed = false;
            }
            host.slowUntil = 0;
            host.memory.setCapacity(Long.MAX_VALUE);
            host.diskShort = false;
        }
        long deadline = time + 4_000L * TICK;
        while (!converged()) {
            if (time > deadline) {
                throw new AssertionError("the cluster does not converge after healing: " + describe());
            }
            step();
        }
        finish();
    }

    private boolean converged() throws IOException {
        if (phase != Phase.IDLE) {
            return false; // a member is still being replaced
        }
        long applied = -1;
        for (Host host : active()) {
            if (!host.running() || host.node.view().logRecovering()) {
                return false;
            }
            if (host.damaged) {
                // Damage that nobody has read yet must not outlive the run.
                scrub(host);
                return false;
            }
            if (!host.node.store().machine().alarms().isEmpty()) {
                return false;
            }
            long own = host.node.store().appliedIndex();
            if (applied != -1 && own != applied) {
                return false;
            }
            applied = own;
        }
        if (applied < observation.highestApplied()) {
            return false;
        }
        for (Client client : clients) {
            if (client.waiting && !client.stopped) {
                return false;
            }
        }
        return true;
    }

    private String describe() {
        StringBuilder text = new StringBuilder();
        for (Host host : hosts) {
            text.append("node ").append(host.id).append(host.running()
                    ? " " + host.node.view() + " applied " + host.node.store().appliedIndex()
                    : host.retired ? " retired" : host.started ? " down" : " spare").append("; ");
        }
        text.append("replacement ").append(phase).append(" joining ").append(joining).append(" leaving ")
                .append(leaving).append("; ");
        for (Client client : clients) {
            text.append("client ").append(client.id).append(client.waiting ? " waits for " + client.kind : " idle")
                    .append("; ");
        }
        return text.toString();
    }

    private void finish() throws IOException {
        long digest = 0;
        long applied = 0;
        Host first = null;
        for (Host host : active()) {
            long own = host.node.store().machine().digest();
            if (first != null && own != digest) {
                throw new AssertionError("after healing node " + host.id + " is in another state than node "
                        + first.id);
            }
            first = first == null ? host : first;
            digest = own;
            applied = host.node.store().appliedIndex();
            stats.snapshotsCreated += host.node.snapshotsCreated();
            stats.slowDiskStepDowns += host.node.slowSyncStepDowns();
            if (host.node.store().machine().engine().scrub() != 0) {
                throw new AssertionError("after healing the state machine of node " + host.id + " is damaged");
            }
        }
        for (SystemObservation.Acknowledged ack : observation.acknowledged()) {
            if (ack.index() > applied) {
                throw new AssertionError("the acknowledged entry " + ack.index() + " is beyond what the nodes"
                        + " have applied after healing: " + applied);
            }
        }
        stats.highestApplied = applied;
        checkCompleteLogs();
        String violation = LinearizabilityChecker.check(history);
        if (violation != null) {
            throw new AssertionError("the history is not linearizable: " + violation);
        }
        for (Host host : hosts) {
            if (host.running()) {
                host.node.close();
                host.node = null;
            }
            if (host.memory.openHandleCount() != 0) {
                throw new AssertionError("node " + host.id + " left " + host.memory.openHandleCount()
                        + " file handles open");
            }
        }
    }

    /** For tests of the simulation itself: the clients' history. */
    List<LinearizabilityChecker.Operation> history() {
        return history;
    }

    @Override
    public String toString() {
        return "SystemSimulation(seed " + seed + ", " + options + ", " + Arrays.toString(new long[] {time}) + ")";
    }
}
