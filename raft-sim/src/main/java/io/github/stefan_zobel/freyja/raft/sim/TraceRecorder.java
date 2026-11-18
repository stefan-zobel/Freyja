package io.github.stefan_zobel.freyja.raft.sim;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import io.github.stefan_zobel.freyja.raft.LogEntry;
import io.github.stefan_zobel.freyja.raft.Outbound;
import io.github.stefan_zobel.freyja.raft.ProtocolState;
import io.github.stefan_zobel.freyja.raft.RaftEvent;
import io.github.stefan_zobel.freyja.raft.RaftMessage;
import io.github.stefan_zobel.freyja.raft.Ready;

/**
 * Records a run as a trace (docs/spec/verification.md, section 2.7): one line of JSON per
 * step of every node. A line holds the node, the event, the protocol state of the node after
 * the step, and the messages the step released. A restart of a node is a line of its own,
 * because it changes the state of the node without a step.
 *
 * <p>The format is written by hand; it needs no library:
 *
 * <pre>
 * {"time":120,"node":1,"event":{"type":"MessageReceived","from":0,"message":{...}},
 *  "state":{"term":2,"role":"FOLLOWER","votedFor":0,"leader":0,"lastLogIndex":5,
 *           "lastLogTerm":2,"durableIndex":4,"commitIndex":3,"voters":[0,1,2],"learners":[],
 *           "logRecovering":false},
 *  "persisted":[{"term":2,"votedFor":0,"truncateFrom":-1,"entries":[{"index":5,"term":2,
 *                "type":"COMMAND"}]}],
 *  "released":[{"to":0,"message":{"type":"AppendResponse","term":2,...}}]}
 * </pre>
 *
 * <p>{@code persisted} lists what the step handed out to be made durable, one object per
 * {@code Ready}; {@code term} and {@code votedFor} are absent if the hard state did not
 * change. Payloads are left out: the protocol does not depend on them.
 */
public final class TraceRecorder implements Simulation.StepListener {

    private final Appendable out;
    private long lines;

    public TraceRecorder(Appendable out) {
        this.out = out;
    }

    /** Number of lines written so far. */
    public long lines() {
        return lines;
    }

    @Override
    public void stepped(long time, int node, RaftEvent event, ProtocolState state, List<Ready> readies) {
        StringBuilder line = new StringBuilder(256);
        line.append("{\"time\":").append(time).append(",\"node\":").append(node).append(",\"event\":");
        appendEvent(line, event);
        line.append(",\"state\":");
        appendState(line, state);
        line.append(",\"persisted\":[");
        boolean first = true;
        for (Ready ready : readies) {
            if (ready.hasPersistentWork()) {
                if (!first) {
                    line.append(',');
                }
                first = false;
                appendPersistentWork(line, ready);
            }
        }
        line.append("],\"released\":[");
        first = true;
        for (Ready ready : readies) {
            for (Outbound outbound : ready.messages()) {
                if (!first) {
                    line.append(',');
                }
                first = false;
                line.append("{\"to\":").append(outbound.toNode()).append(",\"message\":");
                appendMessage(line, outbound.message());
                line.append('}');
            }
        }
        line.append("]}\n");
        write(line);
    }

    @Override
    public void restarted(long time, int node, ProtocolState state) {
        StringBuilder line = new StringBuilder(200);
        line.append("{\"time\":").append(time).append(",\"node\":").append(node)
                .append(",\"event\":{\"type\":\"Restart\"},\"state\":");
        appendState(line, state);
        line.append(",\"persisted\":[],\"released\":[]}\n");
        write(line);
    }

    private void write(StringBuilder line) {
        try {
            out.append(line);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        lines++;
    }

    private static void appendEvent(StringBuilder s, RaftEvent event) {
        switch (event) {
            case RaftEvent.Tick _ -> s.append("{\"type\":\"Tick\"}");
            case RaftEvent.MessageReceived m -> {
                s.append("{\"type\":\"MessageReceived\",\"from\":").append(m.fromNode()).append(",\"message\":");
                appendMessage(s, m.message());
                s.append('}');
            }
            case RaftEvent.Persisted p ->
                    s.append("{\"type\":\"Persisted\",\"sequence\":").append(p.readySequence()).append('}');
            case RaftEvent.ClientPropose p ->
                    s.append("{\"type\":\"ClientPropose\",\"requestId\":").append(p.requestId()).append('}');
            case RaftEvent.ClientReadIndex r ->
                    s.append("{\"type\":\"ClientReadIndex\",\"requestId\":").append(r.requestId()).append('}');
        }
    }

    private static void appendState(StringBuilder s, ProtocolState state) {
        s.append("{\"term\":").append(state.term())
                .append(",\"role\":\"").append(state.role()).append('"')
                .append(",\"votedFor\":").append(state.votedFor())
                .append(",\"leader\":").append(state.leaderId())
                .append(",\"lastLogIndex\":").append(state.lastLogIndex())
                .append(",\"lastLogTerm\":").append(state.lastLogTerm())
                .append(",\"durableIndex\":").append(state.durableIndex())
                .append(",\"commitIndex\":").append(state.commitIndex())
                .append(",\"voters\":").append(ids(state.config().voters()))
                .append(",\"learners\":").append(ids(state.config().learners()))
                .append(",\"logRecovering\":").append(state.logRecovering())
                .append('}');
    }

    private static String ids(Iterable<Integer> ids) {
        StringBuilder s = new StringBuilder("[");
        for (int id : ids) {
            if (s.length() > 1) {
                s.append(',');
            }
            s.append(id);
        }
        return s.append(']').toString();
    }

    private static void appendPersistentWork(StringBuilder s, Ready ready) {
        s.append("{\"sequence\":").append(ready.sequence());
        if (ready.hardState() != null) {
            s.append(",\"term\":").append(ready.hardState().term())
                    .append(",\"votedFor\":").append(ready.hardState().votedFor());
        }
        s.append(",\"truncateFrom\":").append(ready.truncateSuffixFrom()).append(",\"entries\":");
        appendEntries(s, ready.entries());
        s.append('}');
    }

    private static void appendEntries(StringBuilder s, List<LogEntry> entries) {
        s.append('[');
        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            if (i > 0) {
                s.append(',');
            }
            s.append("{\"index\":").append(entry.index()).append(",\"term\":").append(entry.term())
                    .append(",\"type\":\"").append(entry.type()).append("\"}");
        }
        s.append(']');
    }

    private static void appendMessage(StringBuilder s, RaftMessage message) {
        switch (message) {
            case RaftMessage.RequestVote m -> s.append("{\"type\":\"RequestVote\",\"term\":").append(m.term())
                    .append(",\"lastLogIndex\":").append(m.lastLogIndex())
                    .append(",\"lastLogTerm\":").append(m.lastLogTerm())
                    .append(",\"preVote\":").append(m.preVote()).append('}');
            case RaftMessage.VoteResponse m -> s.append("{\"type\":\"VoteResponse\",\"term\":").append(m.term())
                    .append(",\"preVote\":").append(m.preVote())
                    .append(",\"granted\":").append(m.granted()).append('}');
            case RaftMessage.AppendEntries m -> {
                s.append("{\"type\":\"AppendEntries\",\"term\":").append(m.term())
                        .append(",\"prevLogIndex\":").append(m.prevLogIndex())
                        .append(",\"prevLogTerm\":").append(m.prevLogTerm())
                        .append(",\"leaderCommit\":").append(m.leaderCommit())
                        .append(",\"entries\":");
                appendEntries(s, m.entries());
                s.append('}');
            }
            case RaftMessage.AppendResponse m -> s.append("{\"type\":\"AppendResponse\",\"term\":").append(m.term())
                    .append(",\"success\":").append(m.success())
                    .append(",\"index\":").append(m.index())
                    .append(",\"hintIndex\":").append(m.hintIndex())
                    .append(",\"hintTerm\":").append(m.hintTerm()).append('}');
            case RaftMessage.Heartbeat m -> s.append("{\"type\":\"Heartbeat\",\"term\":").append(m.term())
                    .append(",\"commit\":").append(m.commit())
                    .append(",\"round\":").append(m.round()).append('}');
            case RaftMessage.HeartbeatResponse m -> s.append("{\"type\":\"HeartbeatResponse\",\"term\":")
                    .append(m.term()).append(",\"round\":").append(m.round()).append('}');
        }
    }
}
