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
public record HardState(long term, int votedFor, boolean logRecovering, long logEpoch,
                        long lostLogIndex, long lostLogTerm) {}              // -1 = no vote
public record ClusterConfig(Set<Integer> voters, Set<Integer> learners) {}
public record SnapshotMeta(long lastIncludedIndex, long lastIncludedTerm, ClusterConfig config) {}
public record PersistedState(HardState hardState, SnapshotMeta snapshot) {}

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
   - One exception: if the hard state of a `Ready` ends log-recovering mode (§8) and the
     `Ready` also changes the log, the runtime first stores the hard state with the mode
     still set and the end of the lost log as before, and the hard state as handed out only
     after the log is durable. A crash in between must not leave a node out of the mode
     without the entries that end it.
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

   Details:

   - **Meta data:** the runtime asks the core for the meta data of a snapshot at an applied
     index (`snapshotMeta`): the term of that entry and the configuration as of it. The
     state machine stores them with the snapshot.
   - **The log starts after the snapshot** from the moment `SnapshotCreated` is processed.
     The runtime may remove the covered entries later; the core never reads them.
   - **Sending:** a leader sends a snapshot to a follower whose next entry is no longer in
     its log. Until the runtime reports the end of the stream, the follower gets no appends.
     After a successful stream the leader continues after the snapshot; after a failed one
     it probes again, which leads to another snapshot if the follower still needs one.
   - **Offer:** the core rejects a staged snapshot if it comes from an earlier term, if
     another one is being installed, or if the log already holds everything it includes
     (its last index is at or below the commit index).
   - **Install directive:** says whether the stored entries after the snapshot stay (the
     log holds the last entry of the snapshot with the same term) or the stored log is
     emptied, and whether the state machine is replaced. The state machine never goes back:
     a node whose log was damaged can have applied more than the snapshot includes, and
     then only its log starts anew after the snapshot.
   - **Order:** the installation takes its place in the order of the `Ready`s: after
     everything that `Ready` and earlier ones ask to persist, before anything later.
   - **While a snapshot is being installed** the node accepts no entries and does not
     advance its commit index, so that the state machine does not move under the snapshot.
   - **After the installation** the node acknowledges the last index of the snapshot to the
     leader with a normal append answer.
   - A `Ready` can carry several snapshot directives.

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
- **Recovery from lost appends:** a lost append is normally noticed through the rejection
  of the one after it. For the case that nothing follows, the leader checks once per election
  timeout: a probing leader sends its probe again, and a replicating leader that has appends
  in flight but saw no acknowledgment since the previous check falls back to probing.

## 5. Mandatory Protocol Details

- **PreVote:** a node starts a real election only after a majority indicated it would grant
  the vote. A pre-vote changes no state on either side. The answer to a pre-vote names the
  election it is for, so a grant for an earlier election is not counted.
- **CheckQuorum:** a leader steps down when it has not heard from a majority within one
  election timeout. A node that has heard from a leader within the minimum election timeout
  does not grant votes, except for a leadership transfer. It does not answer such a request
  at all, so that a node that lost contact cannot raise the term of the others.
- **What the leader knows of the others** is readable as a view (`peers()`): per member the
  match index, whether it was heard from since the last check for a quorum or at it, whether
  it is in log-recovering mode, and whether a snapshot is on its way to it. The view is for
  people and for the checks of an administrator; the protocol does not read it.
- **Stepping down on request:** the runtime may ask a leader to give its leadership up
  (`StepDown`), for example because its disk has become slow (`node.md` §7). The leader
  becomes a follower in its term and knows no leader. Nothing else changes: what it has
  stored stays, and it may be elected again. Any other node ignores the request.
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
  - A request to a node that is not the leader is rejected with a leader hint. A leader that
    has not committed an entry of its term rejects with "not ready"; the client tries again.
  - A request that finds no round on its way starts one at once. One that arrives while a
    round is on its way waits for the next round, which starts when the current one is
    answered by a majority, or with the next regular heartbeat.
  - A leader that steps down rejects the requests it has not answered.
  - A single node is its own majority and answers at once.
  - Why this is safe although heartbeat answers do not wait for persistence: a node that
    answers a heartbeat of term T has not granted a vote in a later term, because its term
    never goes back, not even across a crash (a vote leaves only after it is durable). If a
    leader of a later term existed when the request arrived, a majority had voted for it
    before, and no majority could answer a later round of term T.

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

Details:

- **A `CONFIG` entry carries the complete new configuration** (voters and learners), not the
  change. A node that receives the log from its start therefore learns the configuration
  from the first `CONFIG` entry it sees.
- **The configuration in force** on a node is the one of its snapshot, replaced by every
  `CONFIG` entry of its log in order. The node derives it at start from the snapshot and the
  stored log; it is not stored separately. Without a snapshot, the snapshot meta data holds
  the configuration the cluster started with.
- **A node that is to join** starts with the empty configuration. It is no voter, so it
  starts no election, and it accepts the log from whichever leader contacts it.
- **Who counts:** votes and pre-votes count only if they come from voters of the
  configuration in force. A leader counts toward commit and toward CheckQuorum only the
  voters, and itself only if it is one. A leader that is no longer a voter (it removed
  itself) keeps replicating until the change is committed and then steps down. In the step
  that commits the change it still sends the appends that carry the new commit index, so
  that the others learn of the commit without waiting for a new leader.
- **Messages are accepted from any node**, member or not: a new node does not know its
  leader, and a node may have to learn from a non-member that its term is old.
- **A removed node** receives nothing from the moment its removal is in the log of the
  leader, so it may never see that entry. It will try to start elections; PreVote and the
  rule that a node in contact with a leader grants no vote keep it from disturbing the
  cluster. It is shut down by the operator.
- **Rejections** of a requested change: not the leader; no entry of the own term committed
  yet; an earlier change uncommitted; the change does not fit the configuration (the node is
  already a member, is no learner or no voter, or is the last voter); the learner to promote
  is too far behind; a leadership transfer is under way; after the change a majority would
  need a voter that the leader knows to be in log-recovering mode.
- **Why the last rule:** a node in log-recovering mode votes only for candidates that cover
  the log it has lost, and it leaves the mode only with the help of a leader. A
  configuration whose majority depends on such a node can end without a leader for good.
  The leader learns of the mode from heartbeat answers, so the rule cannot rule out a
  change that races with the damage; that case is a lost majority (see §8).

## 7. Leadership Transfer

The leader stops accepting proposals, brings the target up to date, and tells it to start an
election at once. That election skips PreVote and is marked so that other nodes grant the vote
despite having a live leader. If the target has not taken over within one election timeout,
the transfer is aborted and the leader resumes.

- The target must be another voter. While a transfer is under way the leader rejects
  proposals, membership changes, and further transfers.
- The order to start the election (`TimeoutNow`) is sent when the match index of the target
  equals the last index of the leader, at once or after the missing entries were
  acknowledged. Only a voter that follows the sender in the term of the order acts on it.
- The mark on the requests for votes only lifts the rule that a node in contact with a
  leader grants no vote. Everything else about the election is unchanged: the term and the
  vote of the target are durable before the requests leave, and the logs are compared.
- An aborted transfer is reported to the runtime. A successful one shows as a change of role.

## 8. Log-Recovering Mode

A node whose runtime reports `LogDamaged` (see `node.md`) has lost entries it may have
acknowledged. The core then:

1. Sets `logRecovering` in the hard state, raises the log epoch (see below), and persists
   both before anything else happens. A leader or candidate becomes a follower.
2. Discards its log from the first damaged index. Its commit index falls back with the log;
   what its state machine has applied stays applied and is not applied again.
3. Starts no elections. It grants a vote or a pre-vote only to a candidate whose log is at
   least as up to date as the log the node has lost: it votes as it would have before the
   damage. For that the hard state holds the last index and term of the log before the
   damage. If they are not known (the runtime found the damage when it opened the log, so
   the node never saw the lost entries), the node grants no vote at all. It accepts
   entries and snapshots from a leader of its current or a later term and acknowledges what
   it has durably stored.
4. Clears `logRecovering` on an append of a leader that (a) names the present log epoch of
   the node, (b) leaves the log of the node equal to the log of the leader through the commit
   index the append carries, and (c) carries a commit index whose entry is of the leader's
   own term.

**The log epoch** counts the times a node has lost log entries. It is part of the hard state.

- Every append answer carries the epoch of its sender. A leader remembers the highest epoch
  it has seen from each follower. When a higher one arrives, the leader forgets the match
  index of that follower; answers of an earlier epoch are ignored from then on.
- Every append carries the epoch of its receiver as the leader knows it.
- An acknowledgment that still waits for its entries to become durable when the damage is
  reported is dropped: it would promise entries that are gone.
- A node in the mode says so in its heartbeat answers, and the leader then sends an append
  even if it has nothing new; otherwise an idle cluster would never end the mode.
- A node in the mode takes no commit index from a heartbeat. Its commit index moves only
  through appends, which are checked against its log entry by entry.

Why heartbeats are left out: the commit index of a heartbeat is bounded by what the leader
believes the follower holds, and the follower takes it up to the end of its own log without
any check. That is sound as long as the belief is true. After damage it is not: until the
leader has seen the new epoch it still believes in the log the node had, and the log the
node has now need not be a prefix of it. The store of a node vouches for what its undamaged
files hold, and those may hold entries that the node had cut off again later, in a record
that is lost (`node.md` §3.3). A heartbeat would make the node apply such entries although
no leader ever committed them.

Why rule 4 needs the epoch: without it, the node could leave the mode on an append that was
sent before the leader knew of the damage. Such an append may carry an old commit index, and
the leader may still count an acknowledgment of the lost log toward a later commit. An
append that names the present epoch was sent after the leader had dropped everything it knew
about the lost log. Its commit index covers every entry that was committed with the help of
an acknowledgment of the lost log, at this leader or at an earlier one (condition c: a
leader that has committed an entry of its own term knows every earlier commit).

Reasoning: the election rule of Raft only asks that a voter does not vote for a candidate
whose log is behind its own. A node that compares the candidate with the log it has lost
gives exactly the votes it would have given with that log, so every leader holds every
committed entry, including those the recovering node had acknowledged. A node that does not
know what it has lost abstains: then any leader is elected by a majority that excludes it,
and that majority intersects every earlier commit quorum in another node.

A report of damage always starts the mode, also if the damage lies beyond the log the node
holds: entries the node may have acknowledged are gone either way.

**Lost majority.** The protocol repairs a minority. If so many voters are in the mode
without knowing what they lost that no candidate can get a majority, the cluster has no
leader, and without a leader no node leaves the mode. That is a case for the procedure for a
lost majority (`operations.md`), not for the protocol.

The term and the vote are not covered by this mode. They must never be lost; `node.md`
specifies how they are stored and what happens if they are lost anyway.

## 9. Formal Model

A TLA+ specification covers elections with PreVote, replication, membership change with
learners, ReadIndex, and the log-recovering mode. It is model-checked for clusters of up to
five nodes with bounded terms and log lengths. The specification is kept in `docs/tla/` and is
updated together with the protocol rules above.

- The specification grows with the milestones. `docs/tla/README.md` says what it covers and
  within which bounds it has been checked. So far: elections with PreVote, replication, and
  asynchronous persistence, exhaustively for three nodes within small bounds. Five nodes
  are out of reach of an exhaustive check with the present model of the network.
- It models durable and volatile state separately: a promise may be sent only if the
  durable state backs it, and a restart loses everything else.
- Vote counting: of several answers of one node to a request for a vote or pre-vote, only
  the first counts in a round. A node that a majority has turned down gives up the round.

The model is tied to the implementation by trace validation (`verification.md` §2.7). For
that purpose the core exposes a read-only view of its protocol state (term, role, vote, known
leader, last log index and term, durable index, commit index, configuration, log-recovering
flag). The view performs no
I/O and does not affect determinism.
