package io.github.stefan_zobel.freyja.raft.sim;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.github.stefan_zobel.freyja.raft.HardState;
import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Outbound;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.RaftMessage;
import io.github.stefan_zobel.freyja.raft.Ready;

/**
 * Turns a run into the TLA+ module {@code TraceData} that the trace specification
 * {@code docs/tla/RaftTrace.tla} validates against the model (docs/spec/verification.md,
 * section 2.7). It records the same steps as {@link TraceRecorder}, in the form TLC reads
 * without any library: a sequence of records.
 *
 * <p>How a step is mapped to the model is described in {@code docs/tla/README.md}. Two things
 * are prepared here, because the model has no sequence numbers:
 *
 * <ul>
 *   <li>For a {@code Persisted} event: how many of the writes handed out it covers.</li>
 *   <li>"No vote" is written as 99, the value the model uses for "no node".</li>
 * </ul>
 */
public final class TlaTraceWriter implements Simulation.StepListener {

    /** One step of the trace. Tests change steps to show that the validation is not vacuous. */
    public static final class Step {
        public int node;
        /** Tick, Recv, Persist, Propose, Read, or Restart. */
        public String event;
        /** For Persist: the number of writes that became durable. */
        public int persisted;
        /** For Recv: the message as a TLA+ record. */
        public String message = "[type |-> \"none\"]";
        public long term;
        public String role;
        public int votedFor;
        public long lastLogIndex;
        public long lastLogTerm;
        public long commitIndex;
        public long durableIndex;
        /** The released messages as TLA+ records. */
        public final Set<String> released = new LinkedHashSet<>();
    }

    private final int nodes;
    private final List<Step> steps = new ArrayList<>();
    /** Per node: the sequences of the writes handed out and not yet reported durable. */
    private final Map<Integer, ArrayDeque<Long>> pending = new HashMap<>();

    public TlaTraceWriter(int nodes) {
        this.nodes = nodes;
    }

    /** The recorded steps, in order. */
    public List<Step> steps() {
        return steps;
    }

    @Override
    public void stepped(long time, int node, RaftEvent event, ProtocolState state, List<Ready> readies) {
        Step step = new Step();
        step.node = node;
        ArrayDeque<Long> waiting = pending.computeIfAbsent(node, _ -> new ArrayDeque<>());
        switch (event) {
            case RaftEvent.Tick _ -> step.event = "Tick";
            case RaftEvent.MessageReceived m -> {
                step.event = "Recv";
                step.message = message(m.fromNode(), node, m.message());
            }
            case RaftEvent.Persisted p -> {
                step.event = "Persist";
                while (!waiting.isEmpty() && waiting.peekFirst() <= p.readySequence()) {
                    waiting.pollFirst();
                    step.persisted++;
                }
            }
            case RaftEvent.ClientPropose _ -> step.event = "Propose";
            case RaftEvent.ClientReadIndex _ -> step.event = "Read";
            case RaftEvent.ProposeConfigChange _ -> step.event = "Config";
        }
        for (Ready ready : readies) {
            if (ready.hasPersistentWork()) {
                waiting.addLast(ready.sequence());
            }
            for (Outbound outbound : ready.messages()) {
                step.released.add(message(node, outbound.toNode(), outbound.message()));
            }
        }
        state(step, state);
        steps.add(step);
    }

    @Override
    public void restarted(long time, int node, ProtocolState state) {
        Step step = new Step();
        step.node = node;
        step.event = "Restart";
        pending.remove(node);
        state(step, state);
        steps.add(step);
    }

    private static void state(Step step, ProtocolState state) {
        step.term = state.term();
        step.role = switch (state.role()) {
            case FOLLOWER -> "F";
            case PRE_CANDIDATE -> "P";
            case CANDIDATE -> "C";
            case LEADER -> "L";
        };
        step.votedFor = state.votedFor() == HardState.NO_VOTE ? 99 : state.votedFor();
        step.lastLogIndex = state.lastLogIndex();
        step.lastLogTerm = state.lastLogTerm();
        step.commitIndex = state.commitIndex();
        step.durableIndex = state.durableIndex();
    }

    private static String bool(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    /** A message in exactly the shape the model gives it. */
    static String message(int from, int to, RaftMessage message) {
        String head = "from |-> " + from + ", to |-> " + to + ", term |-> " + message.term();
        return switch (message) {
            case RaftMessage.RequestVote m -> "[type |-> \"RV\", " + head + ", lidx |-> " + m.lastLogIndex()
                    + ", lterm |-> " + m.lastLogTerm() + ", pre |-> " + bool(m.preVote()) + "]";
            case RaftMessage.VoteResponse m -> "[type |-> \"RVR\", " + head + ", pre |-> " + bool(m.preVote())
                    + ", granted |-> " + bool(m.granted()) + "]";
            case RaftMessage.AppendEntries m -> {
                StringBuilder terms = new StringBuilder("<<");
                for (LogEntry entry : m.entries()) {
                    if (terms.length() > 2) {
                        terms.append(", ");
                    }
                    terms.append(entry.term());
                }
                yield "[type |-> \"AE\", " + head + ", prev |-> " + m.prevLogIndex() + ", pterm |-> "
                        + m.prevLogTerm() + ", ents |-> " + terms + ">>, commit |-> " + m.leaderCommit() + "]";
            }
            case RaftMessage.AppendResponse m -> "[type |-> \"AER\", " + head + ", ok |-> " + bool(m.success())
                    + ", idx |-> " + m.index() + "]";
            case RaftMessage.Heartbeat m -> "[type |-> \"HB\", " + head + ", commit |-> " + m.commit() + "]";
            case RaftMessage.HeartbeatResponse _ -> "[type |-> \"HBR\", " + head + "]";
        };
    }

    /** One step as a TLA+ record. */
    public static String record(Step step) {
        return "[node |-> " + step.node
                + ", ev |-> \"" + step.event + '"'
                + ", k |-> " + step.persisted
                + ", msg |-> " + step.message
                + ", term |-> " + step.term
                + ", role |-> \"" + step.role + '"'
                + ", vote |-> " + step.votedFor
                + ", lidx |-> " + step.lastLogIndex
                + ", lterm |-> " + step.lastLogTerm
                + ", commit |-> " + step.commitIndex
                + ", durable |-> " + step.durableIndex
                + ", rel |-> {" + String.join(", ", step.released) + "}]";
    }

    /** The module {@code TraceData}. */
    public String module() {
        StringBuilder s = new StringBuilder(steps.size() * 200);
        s.append("----------------------------- MODULE TraceData -----------------------------\n");
        s.append("\\* Generated from a simulated run. Do not edit.\n");
        s.append("TraceNodes == {");
        for (int i = 0; i < nodes; i++) {
            s.append(i == 0 ? "" : ", ").append(i);
        }
        s.append("}\n\nTrace == <<\n");
        for (int i = 0; i < steps.size(); i++) {
            s.append("  ").append(record(steps.get(i))).append(i + 1 < steps.size() ? ",\n" : "\n");
        }
        s.append(">>\n=============================================================================\n");
        return s.toString();
    }
}
