# TLA+ Model of the Raft Core

| File | Content |
|---|---|
| `Raft.tla` | Elections with PreVote, log replication with asynchronous persistence, membership change with learners, leadership transfer, log-recovering mode |
| `Raft.cfg` | Exhaustive check of three voters within small bounds, without membership change and log damage |
| `RaftMembership.cfg` | One voter and two nodes that may join, with one log damage; a bounded search, not exhaustive |
| `RaftTrace.tla`, `RaftTrace.cfg` | Trace validation: constrains `Raft` to one recorded run |

ReadIndex and snapshots are not in the model. A run with snapshots cannot be validated
against it; trace validation uses the seed sets without them.

**State of the checks (B3):**

- `Raft.cfg`: exhaustive, no error, with the partly durable restart (section 1): 1,015,386
  distinct states, depth 27, two minutes (2026-11-19).
- `RaftMembership.cfg`: a breadth-first search of nine minutes (14.7 million distinct states,
  depth 14) found no error. It did not finish. A run with smaller bounds that was meant to
  be exhaustive was stopped by the machine for lack of memory and has to be repeated.
- Trace validation: the runs under the full fault model and all mutated runs behave as
  required with the extended model, also with the partly durable restart (2026-11-19).
- Trace validation of the runs with membership changes, transfers, and log damage
  (`trace-validation-membership`): TLC accepted all ten seeds on 2026-11-19, runs cut off
  at 3,000 steps, in six minutes; no TLC process needed more than 1.6 GB. Seed 1 was
  rejected at first: a leader that removed itself sends the new commit index in the step in
  which it steps down, which the mapping did not allow (section 3.2).

TLC is an external development tool and not a dependency of the build. The commands below
expect `tla2tools.jar` in `tools/` of the repository directory (which is not under version
control).

## 1. What the Model Says

- One action of the model is one step of a node of the implementation.
- A node has volatile state, durable state, and a queue of snapshots handed out to be made
  durable. A step that changes the term, the vote, or the log hands out one snapshot.
- A message that promises something may be sent only if the durable state backs it:
  - a request for a real vote: the candidacy (term and own vote) is durable;
  - a granted vote: the vote is durable;
  - a successful append acknowledgment: the entries are durable, and they are known to
    equal the log of the leader of that term.
- A restart loses everything but the durable state. Of the snapshots on their way, any prefix
  may have become durable, and of the rest a part (`PartlyDurable`): a node writes the term
  and the vote of a step before its log, so they may be durable while the log is the old one
  or the new one cut anywhere after what the two have in common. The log-recovering state
  stays as it was until the log is durable (`docs/spec/raft.md`, section 3).
- What the implementation decides by timers and by flow control is left open: a node may
  time out, step down, or send any valid message at any time. The model therefore allows
  more behaviors than the implementation has, which is the safe direction.
- A log is a sequence of terms. Payloads do not matter to the protocol.

- A log entry is a term and, for a `CONFIG` entry, the complete new configuration. The
  configuration in force is the newest one in the log.
- A node in log-recovering mode votes only for a candidate that covers the log it has lost.
  The case that the node does not know what it lost (it then grants no vote) is not modeled.
- Log damage cuts the log of a node, also its durable log and the snapshots on their way.
  The damage marks itself (it is found again at every start), so the log-recovering state
  counts as durable from the moment of the damage. Damage is allowed only while a majority
  of the voters holds an intact log.

Checked invariants: `ElectionSafety`, `LogMatching`, `StateMachineSafety` (no two nodes ever
see different entries as committed at an index), `LeaderCompleteness` (an entry seen as
committed is in the log of every leader of a later term), `OneChangeAtATime`,
`CommittedIsDurable` (without damage and membership change, a committed entry is durable on
a majority), `NodeSanity`.

## 2. Model Checking

```
java -cp ../../tools/tla2tools.jar tlc2.TLC -workers auto -config Raft.cfg Raft.tla
```

- `Raft.cfg` checks `SpecMC` for three nodes, exhaustively within its bounds: terms up to
  2, logs up to 2 entries, one pending snapshot per node, 4 messages. The file states the
  size and the duration of the run.
- These bounds are small because every message ever sent stays deliverable, in any order
  and any number of times. Larger bounds are searched breadth-first for a limited time: such
  a search finds short counterexamples but proves nothing by ending without one.
- `SpecMC` differs from the full specification in two ways that keep the state space small
  without losing behaviors that matter for safety:
  - The pre-vote is replaced by the action `Campaign`: a node may become candidate at any
    time. The pre-vote only restricts when that happens.
  - Only messages that change the state of their receiver beyond its term are sent: requests
    for votes, granted votes, appends, acknowledgments.
- Within the bounds of `Raft.cfg` a commit and a second election are reachable, and with 6
  messages a commit of two entries (checked by letting TLC refute "no commit" and "fewer
  than two elections").
- The check is not vacuous: variants of the model with a deliberate error are refuted by
  TLC. The variants and their outcomes are listed in `docs/spec/verification.md`, section 2.7.

## 3. Trace Validation

```
./mvnw test -pl raft-sim -am -Ptrace-validation
```

With `-pl kv-node -am` the same profile also runs `SystemTraceValidationTest`: traces of
whole-system runs, recorded from complete nodes on real log stores. With the partly durable
restart, TLC accepted all nine seeds on 2026-11-19: the six of `system-trace-validation` and
the three chosen because they crash between the write of the hard state and the write of
the log of one step (`SystemTraceTest.CRASH_BETWEEN_WRITES`). Complete runs of 3,967 to
5,820 steps, 42,410 steps in all, in 21 minutes.

The profile runs `TraceValidationTest`. For each seed it simulates a cluster under the full
fault model, writes the run as the module `TraceData` (`TlaTraceWriter`), and lets TLC check
`RaftTrace`. A run is accepted if TLC gets past its last step. The same test changes recorded
runs (a second leader in a term, an acknowledgment or a vote before persistence, a changed
committed entry, a commit without a majority) and demands that TLC rejects each.

### 3.1 The Trace

`Trace[p]` is one step of one node:

| Field | Meaning |
|---|---|
| `node` | the node that took the step |
| `ev` | `Tick`, `Recv`, `Persist`, `Propose`, `Read`, or `Restart` |
| `k` | for `Persist`: how many of the writes handed out became durable |
| `msg` | for `Recv`: the message |
| `term`, `role`, `vote`, `lidx`, `lterm`, `commit`, `durable` | the protocol state of the node after the step: term, role (`F`, `P`, `C`, `L`), vote (99 for none), last log index and term, commit index, durable index |
| `rel` | the messages the step released |

A step is accepted if all of the following hold:

1. An action that belongs to the event (table below) leads to a state of the node in which
   the seven recorded fields have the recorded values.
2. A received message was released by an earlier step.
3. Every released message is one the node may send in its new state (`ValidSend`). For a
   `Persist` step that covers several snapshots, a promise may be backed by any of them.
4. No invariant of the model is violated.

### 3.2 Mapping of Events to Actions

Every step of the implementation corresponds to one action of the model, or to none (the
state of the node does not change). An event that fits no row fails validation.

| Event of the implementation | Action of the model |
|---|---|
| `Tick` that starts a pre-vote (it releases pre-vote requests) | `Timeout` |
| `Tick` of a single node that starts an election | `Timeout` |
| `Tick` on which a leader finds no majority (CheckQuorum) | `StepDown` |
| any other `Tick` (heartbeats, resent appends) | none |
| `MessageReceived` with a request for a real vote | `RecvRequestVote`, or none if the node ignores it because it heard from a leader recently |
| `MessageReceived` with a pre-vote request | none (the answer promises nothing) |
| `MessageReceived` with an answer to a vote or pre-vote request | `RecvVoteResponse` (of several answers of one node in a round only the first counts; a node that a majority turned down gives up); none if the answer is out of date |
| `MessageReceived` with an append | `RecvAppendEntries`; none if it is of an earlier term |
| `MessageReceived` with an append answer | `RecvAppendResponse`; none if it is a rejection or out of date |
| `MessageReceived` with a heartbeat | `RecvHeartbeat`; none if it is of an earlier term |
| `MessageReceived` with a heartbeat answer | `RecvLaterTerm` if it is of a later term, else none |
| `Persisted` | `PersistK` with the number of writes covered |
| `ClientPropose` | `Propose` if accepted, else none |
| `ClientReadIndex` | none (ReadIndex is not in the model yet) |
| `ProposeConfigChange` | `ProposeCfg` if accepted, else none |
| `TransferLeadership` | none (the order to the target is a message) |
| `MessageReceived` with the order to start an election (`TimeoutNow`) | `RecvTimeoutNow`, or none if the node ignores it |
| `LogDamaged` | the damage of the model (`DamageNode`) at the reported index; none if the index is beyond the log |
| restart of a node | `Restart` |
| restart of a node that finds its damage again | `Restart` (one step: for the model the damage was durable from the moment it happened; the write of the repeated report is not counted) |

Where the table allows "none", `RaftTrace` allows it only if the implementation does not act
on the message in the state of the receiver (`ActsOn`). Otherwise a step that changes only
what the trace does not show, such as the votes a candidate has counted, could pass as a step
in which nothing happened.

The actions that follow from a step inside the implementation are part of the action of the
model: a candidate that has a majority becomes leader within `RecvVoteResponse` or `PersistK`,
and a leader advances its commit index within `RecvAppendResponse` or `PersistK`.

A leader that removed itself steps down within the action that commits the change, but in
the same step it still sends the appends that carry the new commit index. The messages of
such a step are checked against the state of the leader at the end of the step, before it
became a follower (`ValidRelease` in `RaftTrace`). The model checks of `Raft.tla` do not
send these appends; they are appends the leader could have sent as well if it had stayed.

### 3.3 Limits

- The trace shows the last index and term of a log, not its content. The content is followed
  by the model through the actions; a wrong entry shows as soon as it changes one of the
  recorded values or makes a released message invalid.
- At a restart the trace does not say how many writes on their way had reached the disk, nor
  how much of the write that was cut off. TLC tries every choice that gives the recorded
  state.
- A node that joins reports the empty configuration until the first `CONFIG` entry reaches
  it; the model has the initial configuration there. The comparison allows for that.
- Whether a majority holds an intact log when damage is injected is not checked in trace
  validation; it is a condition on whoever injects the damage, not on a node.
- Heartbeat answers and ReadIndex are outside the model. Their messages are accepted as they
  are.
