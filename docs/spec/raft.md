# Raft Core (`raft-core`)

## 1. Purity

Inside `raft-core`, these APIs are forbidden:

- `System.currentTimeMillis()`, `System.nanoTime()`, `Instant.now()`
- `java.util.Random`, `ThreadLocalRandom`, `Math.random()`
- `java.lang.Thread`, `Executors`, `CompletableFuture`
- `java.io.*`, `java.nio.*`

A test scans the compiled classes of `raft-core` with the `java.lang.classfile` API and fails
on any reference to these.

- **Time** exists only as `Tick` events. All timeouts are counted in ticks.
- **Randomness** (election timeout jitter) comes from a small PRNG (SplitMix64) whose state is
  part of the node state. The seed is part of the node configuration.
- `byte[]` payloads are treated as immutable once handed to or returned from the core.
- The core does not interpret command payloads.

## 2. API Shape

The names are a sketch and may change. The semantics in §3 may not. The sketch shows the
complete API. Events, `Ready` fields, and messages that belong to a later milestone
(snapshots, membership change, leadership transfer, read index, log damage) enter the code
with that milestone.

```java
public final class RaftNode {
    public static RaftNode create(RaftConfig config, PersistedState state, LogView log,
                                  long appliedIndex);   // what the state machine has applied
    public void step(RaftEvent event);   // changes state only; performs nothing
    public boolean hasReady();
    public Ready takeReady();            // the work the runtime must do next
    public ProtocolState view();         // read-only protocol state, see §9
}

public interface LogView {               // read-only access to durably stored entries
    long firstIndex();                   // first entry still stored (snapshot index + 1)
    long lastIndex();
    long termAt(long index);             // valid for firstIndex() - 1 through lastIndex()
    List<LogEntry> entries(long fromInclusive, long toExclusive, int maxBytes);
}

public record LogEntry(long index, long term, EntryType type, byte[] payload) {}
public enum EntryType { COMMAND, NOOP, CONFIG }
public record HardState(long term, int votedFor, boolean logRecovering) {}   // -1 = no vote
public record ClusterConfig(Set<Integer> voters, Set<Integer> learners) {}
public record SnapshotMeta(long lastIncludedIndex, long lastIncludedTerm, ClusterConfig config) {}
public record PersistedState(HardState hardState, SnapshotMeta snapshot, ClusterConfig config) {}

public sealed interface RaftEvent {
    record Tick() implements RaftEvent {}
    record MessageReceived(int fromNode, RaftMessage message) implements RaftEvent {}
    record Persisted(long readySequence) implements RaftEvent {}
    record ClientPropose(long requestId, byte[] command) implements RaftEvent {}
    record ClientReadIndex(long requestId) implements RaftEvent {}
    record ProposeConfigChange(long requestId, ConfigChange change) implements RaftEvent {}
    record TransferLeadership(long requestId, int toNode) implements RaftEvent {}
    record SnapshotCreated(SnapshotMeta meta) implements RaftEvent {}
    record SnapshotOffered(int fromNode, long leaderTerm, SnapshotMeta meta) implements RaftEvent {}
    record SnapshotInstalled(SnapshotMeta meta) implements RaftEvent {}
    record SnapshotSendFinished(int toNode, boolean success) implements RaftEvent {}
    record LogDamaged(long firstDamagedIndex) implements RaftEvent {}
}

public record Ready(
    long sequence,
    HardState hardState,              // null if unchanged; persist
    long truncateSuffixFrom,          // -1 if none; applies before `entries`
    List<LogEntry> entries,           // persist
    long truncatePrefixThrough,       // -1 if none; may be done lazily
    List<Outbound> messages,          // may be sent immediately
    long applyFrom, long applyTo,     // committed entries for the state machine; empty if from > to
    List<Notification> notifications, // proposal accepted/rejected, read index ready, role and config changes
    SnapshotDirective snapshot        // send to a node, install, or reject; null if none
) {}
```

## 3. Semantics

1. **Determinism:** the state after `step` and the content of every `Ready` are a deterministic
   function of the previous node state, the content of the `LogView`, and the event.
2. **The core enforces durability before visibility.** The core keeps entries and hard-state
   changes that are not yet confirmed durable in memory. It releases a message that promises
   something (a vote, an append acknowledgment) only in a `Ready` that follows the `Persisted`
   event for the state the message depends on. Consequently every message in a `Ready` may be
   sent at once, and the runtime needs no ordering logic of its own.
   - **Rule:** a promising message waits until the *complete* state the node had when it
     created the message is durable, not only the part the message mentions. The node then
     behaves exactly like one that persists instantly and whose messages are delayed on the
     way, so the usual safety argument for Raft applies unchanged.
   - **Messages that wait:** a granted vote, a successful append acknowledgment, and a
     request for votes. The last one waits because a candidate that forgot its candidacy in
     a crash could otherwise collect votes granted for a log it no longer has. A candidate
     also counts its own vote only once that vote is durable.
   - **Messages that leave at once:** pre-vote requests and answers, rejections, heartbeats
     and their answers, and the appends of a leader. They state what the node thinks at that
     moment and promise nothing about the future.
3. **Runtime contract:**
   - Persist `hardState`, the suffix truncation, and `entries` of each `Ready` in sequence
     order. Several `Ready`s may be covered by one sync.
   - Feed `Persisted(sequence)` when everything up to and including that `Ready` is durable.
   - Sending messages, applying entries, and persisting may run concurrently.
   - Within one `Ready` the order is: hard state, suffix truncation, entries. A truncation
     removes the stored entries from the given index on. The entries of a `Ready` have
     consecutive indexes and follow the stored log directly after the truncation.
   - A write that is on its way when the core truncates its log in memory does not bring
     the removed entries back: when that write is reported, the core counts as durable only
     the part that is still in its log.
4. **Leader replication runs in parallel with the leader's own disk write.** A leader may send
   new entries to followers before they are durable locally. It counts itself toward the
   commit quorum only up to its last persisted index.
5. **Apply:** the core offers an entry for application only when it is committed and durable
   locally. After a restart it continues after the `appliedIndex` given to `create`.
6. **Reading the log:** the core reads the `LogView` only for indexes at or below its last
   persisted index; newer entries come from its own memory.
7. **Proposals:** a leader answers `ClientPropose` with a notification carrying
   `(requestId, index, term)`; any other role rejects with a leader hint. The runtime
   completes the request when the entry at `index` is applied and still has that `term`;
   otherwise the proposal failed.
8. **Snapshots carry no bytes through the core.**
   - Sending: a directive asks the runtime to stream the snapshot out of band; the runtime
     reports the outcome with `SnapshotSendFinished`.
   - Receiving: once the runtime has staged the complete snapshot locally, it feeds
     `SnapshotOffered`. The core answers with an install or reject directive. After
     installing, the runtime feeds `SnapshotInstalled`, and the core acknowledges to the
     leader with a normal message.
   - Compaction: after the state machine has written a snapshot, the runtime feeds
     `SnapshotCreated`; the core answers with `truncatePrefixThrough`.

## 4. Flow Control

- **Per-follower state:** probing (one message at a time until the match point is known),
  replicating (pipelined), or snapshotting.
- **In-flight window:** at most `maxInflightAppends` unacknowledged append messages per
  follower (default 256).
- **Message size:** at most `maxAppendBytes` of entries per message (default 1 MiB). The
  size of an entry is the length of its payload. A message always carries at least one entry,
  even if that entry alone exceeds the limit.
- **Uncommitted cap:** when the uncommitted part of the leader's log exceeds
  `maxUncommittedBytes` (default 64 MiB), new proposals are rejected as overloaded. A
  proposal is always accepted when nothing is uncommitted, whatever its size.
- **Finding the match point:** a follower that rejects an append names the last index at or
  below the rejected one whose term is not above the term the leader offered. The leader
  continues at the last index at or below that hint whose term in its own log is not above
  the term the follower named. Whole terms are skipped this way.
- **Heartbeats** are separate from appends, so that flow control never holds them back. A
  heartbeat carries the commit index, limited to the match index of its receiver.
- **Recovery from lost appends:** when a heartbeat answer arrives and the follower is behind,
  a probing leader sends its probe again. A replicating leader that has appends in flight but
  saw no progress since the previous heartbeat answer falls back to probing.

## 5. Mandatory Protocol Details

- **PreVote:** a node starts a real election only after a majority indicated it would grant
  the vote. A pre-vote changes no state on either side. The answer to a pre-vote names the
  election it is for, so a grant for an earlier election is not counted.
- **CheckQuorum:** a leader steps down when it has not heard from a majority within one
  election timeout. A node that has heard from a leader within the minimum election timeout
  does not grant votes, except for a leadership transfer. It does not answer such a request
  at all, so that a node that lost contact cannot raise the term of the others.
- **Old terms:** an append or heartbeat of an older term is answered with a rejection that
  carries the current term, so that a deposed leader steps down. A node can still win the
  election of a term the cluster has left behind (its votes arrive late). Such a leader can
  commit nothing and steps down on the first answer.
- **Leader no-op:** a new leader appends a no-op entry in its term immediately.
- **ReadIndex:** a leader serves `ClientReadIndex` only after it has committed an entry in its
  current term. It records the commit index, confirms leadership with a heartbeat round to a
  majority that started after the request arrived, then reports the read index. Pending
  requests share heartbeat rounds. The runtime serves the read once the state machine has
  applied through that index.

## 6. Membership Change

- Changes go through `CONFIG` entries. One change at a time: add learner, promote learner to
  voter, remove voter, remove learner. Each changes the voter set by at most one node.
- A node uses the newest configuration in its log, committed or not. If that entry is
  truncated, the node reverts to the previous configuration.
- A leader rejects a change while an earlier `CONFIG` entry is uncommitted, and before it has
  committed an entry of its own term.
- **Learners** receive the log and snapshots but do not vote and do not count toward quorums.
  A learner is promoted only when its match index is within `promotionLag` entries of the
  leader's last index (default 1,000).
- A leader that removes itself steps down once the change is committed.
- Node ids are never reused. A node that was removed must join again under a new id.
- The configuration is part of `SnapshotMeta` and of the persisted state.

## 7. Leadership Transfer

The leader stops accepting proposals, brings the target up to date, and tells it to start an
election at once. That election skips PreVote and is marked so that other nodes grant the vote
despite having a live leader. If the target has not taken over within one election timeout,
the transfer is aborted and the leader resumes.

## 8. Log-Recovering Mode

A node whose runtime reports `LogDamaged` (see `node.md`) has lost entries it may have
acknowledged. The core then:

1. Sets `logRecovering` in the hard state and persists it before anything else happens.
2. Discards its log from the first damaged index.
3. Grants no votes and starts no elections, but accepts entries and snapshots from a leader of
   its current or a later term and acknowledges what it has durably stored.
4. Clears `logRecovering` once its log matches a leader's log through a committed entry of
   that leader's own term.

Reasoning: while the node abstains, any leader is elected by a majority that excludes it. That
majority intersects every earlier commit quorum in a node other than the recovering one, so
the leader holds every committed entry, including those the recovering node had acknowledged.

The term and the vote are not covered by this mode. They must never be lost; `node.md`
specifies how they are stored and what happens if they are lost anyway.

## 9. Formal Model

A TLA+ specification covers elections with PreVote, replication, membership change with
learners, ReadIndex, and the log-recovering mode. It is model-checked for clusters of up to
five nodes with bounded terms and log lengths. The specification is kept in `docs/tla/` and is
updated together with the protocol rules above.

The model is tied to the implementation by trace validation (`verification.md` §2.7). For
that purpose the core exposes a read-only view of its protocol state (term, role, vote, known
leader, last log index and term, durable index, commit index, configuration, log-recovering
flag). The view performs no
I/O and does not affect determinism.
