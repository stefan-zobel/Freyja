# Node (`kv-node`) and Client (`kv-client`)

## 1. Runtime Structure

The node logic never touches the outside world directly. It reaches it only through three
interfaces, each with a real and a simulated implementation:

- **Clock:** the source of ticks, timeouts, and timestamps. No direct calls to
  `System.nanoTime()` or similar outside the real implementation.
- **Transport:** sending and receiving messages and snapshot streams between nodes and clients.
- **File system:** the `StorageFileSystem` from `lsm-core`.

The node logic is driven by an event loop that a test can step manually. Several complete
nodes can therefore run inside one deterministic simulation (`verification.md`).

These three interfaces must stay stable across releases; the multi-version simulation loads
two releases against one implementation of them.

Timing defaults: tick 100 ms, heartbeat every tick, election timeout 10–20 ticks.

The event loop:

- The node has no thread and no clock of its own. Whoever runs it reports that a tick has
  passed, hands over the messages that arrive, and tells it to process the next event. The
  real runtime does that from one thread driven by the clock; a simulation does it in any
  order it likes.
- The queue of waiting events is bounded (default 4,096). An event that does not fit is
  rejected at once.
- The work of a `Ready` is done in the order of the contract of the core: hard state,
  suffix truncation, entries, then one sync of the log. Only then the core is told that the
  work is durable; messages are sent and entries applied after that. Persisting is
  synchronous in the loop for now; the contract of the core allows to move it to its own
  thread later without changing the core.
- A failure of storage stops the node: it rejects every further event. It is started again
  from what is on disk.
- A message that fails its checksum, has an unknown format, or is meant for another node is
  dropped and counted.
- Each node derives the seed of its election jitter from its node id, so that the election
  timers of different nodes do not fire together.
- Start: identity, hard state, log. If the log is damaged, the core is started in
  log-recovering mode and told of the damage; its hard state is stored; then the damaged
  segments are set aside (§3.3).
- A data directory is prepared once, with the configuration and the cluster version the
  cluster starts with; a node that is to join a cluster is prepared with an empty
  configuration. After that a start needs nothing but the directory.
- Start of a complete node: identity, hard state, an interrupted installation (§5.3), the
  state machine, `SNAPSHOT`, the log.
- If damage of the log begins at or before the index of `SNAPSHOT`, the core is told that
  the entries after that index are lost: what the snapshot covers is not missed. If what is
  left of the log ends before that index, the log starts anew after it.

## 2. Data Directory

```
NODE              identity: cluster id, node id, format version (written once)
HARDSTATE-A/-B    term, vote, state of the log-recovering mode (§3.2)
BOOTSTRAP         what the node was told when the directory was prepared (written once)
SNAPSHOT          what the stored log starts after: index, term, configuration (§5)
INSTALL           present only while a snapshot is being installed (§5)
REPAIR            present only while a damaged state machine is given up (§6)
raft/             Raft log segments
sm/CURRENT        the active state-machine generation and what it started from
sm/<generation>/  an lsm-core engine directory; a snapshot is received into a new one
snap/out-<n>/     a checkpoint being sent
```

The small files (`NODE`, `HARDSTATE-*`, `BOOTSTRAP`, `SNAPSHOT`, `INSTALL`, `REPAIR`,
`sm/CURRENT`) share one layout:
magic (`uint32`), format version (`uint8`), body, CRC32C over everything before it, bound to
a number that stands for the file. Each is replaced as a whole: written to a temporary file,
synced, renamed, and the directory is synced.

| File | Magic | Body |
|---|---|---|
| `BOOTSTRAP` | `FRYB` | cluster version (`uint32`), encoded configuration |
| `SNAPSHOT` | `FRYS` | index, term (`uint64` each), encoded configuration |
| `REPAIR` | `FRYR` | empty; only the presence of the file counts |
| `INSTALL` | `FRYI` | flags (`uint8`: bit 0 keep the log suffix, bit 1 replace the state machine), generation (`uint64`), cluster version (`uint32`), then as `SNAPSHOT` |
| `sm/CURRENT` | `FRYC` | generation (`uint64`), cluster version (`uint32`), then as `SNAPSHOT`: the origin of the generation |

`BOOTSTRAP` holds the configuration and the cluster version the cluster started with; for a
node that joined a cluster, an empty configuration. An empty state machine that is to apply
the log from its first entry starts from exactly this.

The origin of a generation is what it held when it became the state machine: nothing for
the first generation of a node, a snapshot of another node later. Its configuration and its
cluster version count until the generation applies an entry that changes them.

## 3. Raft Log Store

### 3.1 Segments

- Segment files use the WAL record format with sync points (`storage-format.md` §3).
  Target segment size 64 MiB.
- Logical records: `ENTRIES` (a batch of `index`, `term`, `type`, `payload`) and
  `TRUNCATE_SUFFIX` (discard all entries from an index). The log is append-only; a suffix
  truncation is a record, not a file truncation.
- Each segment header repeats the first index of the segment and the cluster configuration
  in effect at its start.
- Prefix truncation deletes whole segments that lie entirely at or below the snapshot index.
- An in-memory index maps entry indexes to segment offsets.
- Sealed segments are included in background scrubbing.

Details:

- **Files:** `raft/NNNNNNNNNN.rlog`, numbered upward from 1. The number is the file number
  of the record checksums. A number is never used twice.
- **Logical records** (the first byte is the type):
  - Header (type 1), the first record of every segment: format version (`uint8`, 1), index
    of the next entry at the time the segment was started (`uint64`), number of the
    preceding segment (`uint64`, 0 for none), valid length of the preceding segment
    (`uint64`), length and bytes of the configuration at the end of the log.
  - Entries (type 2): index of the first entry (`uint64`), count (`uint32`), then per entry
    term (`uint64`), type (`uint8`), payload length (`uint32`), payload.
  - Truncation (type 3): the index from which all entries are discarded (`uint64`).
- **A segment is used only after its header is synced** and the directory is synced. A
  last segment without a complete header was never used; it is removed when the log is
  opened.
- **Rolling:** when a segment has reached the target size, it is closed at the next sync
  and a new one is started.
- **A torn tail is not cut off.** The header of the next segment records how much of its
  predecessor counts, and a sealed segment is read up to that length only. Every open
  starts a new segment, so nothing is ever appended after a torn tail.
- **Chain:** each header names its predecessor. A segment that does not follow the one
  before it (a file in the middle is missing) is damage. A missing last segment cannot be
  told from a log that ends earlier; the loss of whole files is outside the fault model.
- **Reading:** the entries of the segment being written are kept in memory (bounded by the
  segment size); entries of sealed segments are read from their file, one record at a
  time, and checked against the index.
- **Prefix truncation** deletes the oldest segment as long as all entries it still
  contributes to the log are at or below the given index. The store may therefore hold
  entries a snapshot covers. A truncation record that reaches below the first stored index
  (its entries were in deleted segments) restarts the index there.
- A failed write or sync is fatal for the store.

### 3.2 Hard State

Term, vote, and the state of the log-recovering mode (flag, log epoch, and the end of the
lost log; `raft.md` §8) are stored in two files, `HARDSTATE-A` and `HARDSTATE-B`, each
holding one checksummed record with a version counter.

- Update: write and sync A, then write and sync B. The node acts on the new state only after
  both are durable.
  - Each file is replaced as a whole: the record is written to a temporary file and synced,
    the file is renamed over the old one, and the directory is synced. A crash leaves each
    file with its old or its new content.
  - A failed update is fatal for the node (a failed sync is never retried). A restart decides
    from what is on disk.
- Recovery:
  - Both valid: use the one with the higher counter.
  - Exactly one valid: use it. If A is unreadable, the update to A never completed or A was
    damaged later; in both cases B holds a state the node had fully committed to. If B is
    unreadable, A is at least as new.
  - Both invalid: the identity is lost (§3.4).
  - If the two files do not hold the same record, the state that counts is written to both
    again before the node acts on it. Temporary files of an update that did not complete
    are removed.

Format of `NODE` and of the hard-state files (all integers little-endian):

```
magic      uint32   "FRYN" for NODE, "FRYH" for a hard-state file
version    uint8    1
body       ...      see below
checksum   uint32   CRC32C over everything before it, bound to the file and to offset 0
```

- Body of `NODE`: cluster id (`uint64`), node id (`uint32`).
- Body of a hard-state file: version counter (`uint64`), term (`uint64`), vote (`int32`, -1
  for none), flags (`uint8`; bit 0: log-recovering), log epoch (`uint64`), last index of the
  lost log (`uint64`), term of that entry (`uint64`; the maximum value if unknown).
- The checksum covers a number that stands for the file (`NODE`, A, or B), so the record of
  one file is not valid in another.
- A record with a valid checksum and another version is rejected as an unsupported format.
  It is not treated as damage: the other file must not be taken for the truth then.

### 3.3 Log Damage

On open, damage in a segment (`storage-format.md` §3.2) is never truncated away. The store
determines the first entry index it can no longer vouch for and feeds `LogDamaged` to the
core, which enters log-recovering mode (`raft.md` §8). The node stays in the cluster, votes
only as that mode allows, and is refilled by the leader.

- The store vouches for what the segments before the damaged one hold. Nothing of the
  damaged segment is kept, and nothing of the segments after it: the index it cannot vouch
  for is the one after the last entry of the segments before.
- What it vouches for is a state the log once had, not necessarily a prefix of the log the
  node had last. A cut of the log is a record; if that record is in a lost segment, the
  entries it removed are in the log again, though the node had replaced them. They were
  never committed. The core therefore treats nothing in the log of a node in log-recovering
  mode as agreed with the leader until an append has checked it (`raft.md` §8).
- If the first stored entry lies beyond the entry after the snapshot (the first segments
  are gone), the store vouches for nothing.
- The store accepts no writes while damaged segments are in place. The node first makes its
  hard state say that it is in log-recovering mode; only then the damaged segments are moved
  into `raft/damaged/` and a new segment is started. A crash in between finds the damage
  again. Nothing is deleted: the files stay as evidence until an operator removes them.

A torn tail is not damage: those entries were never acknowledged.

**The state machine as a witness.** Damage that hits only the data of the last completed
sync round of the newest segment cannot be told from a torn tail (`storage-format.md`
§3.2), and the store cuts it away. The node narrows that gap with what it knows besides the
log: an entry is applied only after it is durable in the log. If, at a start, the state
machine has applied more than an undamaged log holds and the node is not in log-recovering
mode, the log has lost entries that were synced. The node then tells the core that the log
is lost from the entry after its end, exactly as for damage that the store reports; nothing
is set aside, because nothing damaged is left. What remains outside this check are entries
that were stored and acknowledged but not yet applied.

### 3.4 Lost Identity

If `NODE` or both hard-state files are unreadable, the node cannot know what it promised. It
refuses to start. The operator removes the member and adds a new one with a new id on an empty
data directory; it then joins as a learner. This is the only safe recovery.

### 3.5 Correlated Loss

If a majority of voters is in log-recovering mode or has lost its identity at the same time,
no leader can be elected. Recovery then is a manual, explicitly unsafe procedure
(`operations.md`): start a new cluster from the node with the most advanced log.

## 4. State Machine

- The state machine is an `lsm-core` engine. It writes with `sync = false`: the Raft log is
  the durable record, and after a crash the engine recovers a prefix of what was applied.
- Key namespaces: every key is prefixed with one byte. `0x00` is system state, `0x01` is user
  data.
- **Apply:** each batch of committed entries is applied in one `WriteBatch` that also updates
  the applied index and the session table. On restart, the node replays the Raft log from the
  applied index found in the engine.
- **Stored values** are prefixed with `ModIndex` (`uint64`): the Raft index of the command
  that wrote them.
- **System state:** applied index and term, cluster configuration, cluster version
  (`operations.md`), session table, alarms.
- System keys (all integers little-endian):
  - `0x00 'm'`: applied index, applied term, state-machine time, time of the last session
    sweep (`uint64` each).
  - `0x00 'c'`: index of the CONFIG entry (`uint64`), then the encoded configuration. Absent
    until the first CONFIG entry is applied; until then the configuration the cluster
    started with counts.
  - `0x00 's'` + session id (`uint64`): last activity, last sequence number (`uint64` each),
    then the encoded result of the command with that sequence number.
  - `0x00 'a'`: the alarms that are set, one bit per alarm (`uint64`; bit 0 `NOSPACE`).
    Absent until an alarm is first set.
  - `0x00 'v'`: the cluster version (`uint32`). Absent until it is first raised; until then
    the version the cluster was created with counts.
- The state is a function of the applied entries alone. No clock, no random number, and no
  local circumstance of a node enters it. In particular, the results of a batch do not depend
  on how the committed entries were cut into batches.
- An entry whose bytes are not a valid command changes nothing but the applied index and is
  answered with `INVALID`. Every node decides that alike.
- The Raft log is truncated only through the index of a completed checkpoint, never through
  an index that is merely applied.

### 4.1 Commands

- `put`, `delete`, atomic batch of both.
- Conditional batch: a list of conditions, a batch applied if all hold, and optionally a
  batch applied otherwise. Conditions per key: exists, does not exist, value equals,
  `ModIndex` equals.
- Session commands: register, keep alive, close.
- Limits: key 64 KiB, value 1 MiB, one request 4 MiB. A key is not empty. A command that
  breaks a limit is applied as a whole or not at all: it fails with `INVALID`.
- Commands inside one batch of entries see the effects of the commands before them.
- Encoding of a command in the log: version (`uint8`, 1), time stamp of the leader in
  milliseconds (`uint64`), type (`uint8`: 1 write, 2 conditional, 3 register session,
  4 keep alive, 5 close session, 6 set alarm, 7 clear alarm, 8 set cluster version), body.
  - Write: session id, sequence number (`uint64` each), mutations.
  - Conditional: session id, sequence number, conditions, mutations if all hold, mutations
    otherwise.
  - Keep alive, close session: session id.
  - Set alarm, clear alarm: alarm (`uint8`: 0 `NOSPACE`). Set cluster version: version
    (`uint32`).
  - A list is a count (`uint32`) and its elements; a byte string is a length (`uint32`) and
    its bytes. Mutation: kind (`uint8`: 1 put, 2 delete), key, and for a put the value.
    Condition: kind (`uint8`: 0 exists, 1 does not exist, 2 value equals, 3 `ModIndex`
    equals), key, value, `ModIndex` (`uint64`).
- Encoding of a result: kind (`uint8`: 1 written, 2 conditional applied, 3 session
  registered, 4 session ok, 5 failed, 6 admin applied), then for kind 2 whether the
  conditions held (`uint8`), for kind 3 the session id (`uint64`), for kind 5 the failure
  (`uint8`: 0 no such session, 1 sequence too old, 2 no space, 3 invalid, 4 version too
  old).
- Administrative commands (alarms, cluster version) belong to no session. They are
  idempotent by their meaning.
- The command types and their codec live in `kv-common`, because the client needs them too.

### 4.2 Sessions and Exactly-Once

- A client registers a session through the log. The session id is the index of that entry.
- Every command carries `(sessionId, sequenceNumber)`. The state machine stores the highest
  applied sequence number and its result per session and answers a repeated command from
  that record without applying it again.
- The leader stamps every command with its clock reading. State-machine time is the maximum
  stamp applied so far, so it never goes backward and is identical on all nodes.
- A session expires when state-machine time exceeds its last activity by the session timeout
  (default 60 s). Commands for an expired session fail with a dedicated error; the client
  must not retry them blindly.
- A command whose sequence number is below the highest applied one of its session fails
  with `SEQUENCE_TOO_OLD`: its answer is no longer kept. A client therefore has at most one
  command outstanding per session.
- An expired session is closed when a command names it, and by a sweep over all sessions
  that runs when state-machine time has advanced by a quarter of the session timeout since
  the last sweep. Both depend only on applied entries.
- The session timeout is part of the configuration of the cluster and must be the same on
  all nodes.

### 4.3 Reads

- **Linearizable (default):** ReadIndex, then read from the engine once the applied index has
  reached the read index. `get` and `scan` (range, limit, forward or reverse).
- **Stale:** served by any node from its local state, only when the request asks for it. The
  response carries the applied index it was served at.

### 4.4 Quota and Alarms

- `quotaBytes` (default 16 GiB) limits the size of the state machine. When a node exceeds it,
  it proposes a `NOSPACE` alarm. While the alarm is set in the replicated state, all nodes
  reject writes other than deletes. An administrator clears it.
- A node whose engine reports low free space raises the same alarm.
- The size is the bytes the engine holds: its tables and what waits in memory. Low free
  space is less than `minFreeBytes` (default 1 GiB) in the file system of the data directory.
- A node looks at both every few ticks (default 10) while the alarm is not set in its state
  machine. A leader proposes the command that sets the alarm; any other node asks the leader
  to, with a message of its own (§8). Nothing is remembered: as long as the condition lasts
  and the alarm is not set, the node asks again. An alarm that an administrator clears while
  the condition lasts therefore comes back.
- Commands that a node proposes itself carry the time stamp 0, so they do not move the time
  of the state machine, and request ids below 0, which no client uses.
- While `NOSPACE` is set, a batch that contains a put is rejected with `NO_SPACE`; for a
  conditional batch a put in either of its two batches is enough, whatever the conditions
  say. A batch of deletes only, and every session command, is applied. A rejected command
  does not use up its sequence number: the client may send it again once the alarm is
  cleared.
- The cluster version only rises. A command that asks for a lower one fails with
  `VERSION_TOO_OLD`; asking for the present one is accepted.

## 5. Snapshots

Two things are called a snapshot, and they are separate:

- **Shortening the log.** The node makes its state machine durable (a flush of the engine)
  and lets the log start after an applied entry. Nothing is copied.
- **Sending the state machine.** A leader sends an engine checkpoint
  (`storage-engine.md` §9) to a node whose next entry its log no longer holds.

### 5.1 Shortening the Log

- Triggers: the log takes more than a threshold on disk (default 1 GiB), or on request.
- The log keeps a number of applied entries (default 10,000) for followers that are a little
  behind; on request it keeps none.
- Order: flush the engine; replace `SNAPSHOT`; tell the core; delete the segments that hold
  only entries up to that index. The log gives up only what the state machine holds
  durably, and `SNAPSHOT` never names an index before the first one the log needs.
- Segments are deleted as a whole, so the stored log may reach back before the index in
  `SNAPSHOT`.
- At a start, the core is given the content of `SNAPSHOT` as its snapshot. If the file is
  missing or damaged, the state machine itself stands in: its applied index, the term of
  that entry, and its configuration. That is a valid start of the log for the same reason.

### 5.2 Transfer

- The sender takes a checkpoint when the transfer starts, in `snap/out-<n>/`. The snapshot
  therefore covers what the sender has applied at that moment, which may be more than the
  core asked for. The checkpoint is removed when the transfer ends or times out. The number
  of outgoing transfers is bounded (default 2); beyond it a request fails at once.
- The receiver drives the transfer. Messages:
  - `Offer`: transfer id, term of the leader, `SnapshotMeta`, cluster version, and the files
    of the checkpoint with their sizes.
  - `Fetch`: the receiver asks for the bytes of one file from an offset on.
  - `Chunk`: at most 1 MiB of a file. The frame around every message carries a CRC32C.
  - `Done`: the receiver has every file and found the snapshot intact, or damaged.
  - `Abort`: one side gives the transfer up.
- The receiver writes into the directory of a new generation. It asks for the next bytes
  only after it has stored the ones before, so a lost or repeated message costs a repeated
  request and nothing else: a chunk that is not the one asked for is ignored.
- A request without an answer is repeated (default after 5 ticks). A transfer without
  progress is given up (default after 100 ticks), and the other side is told.
- A receiver accepts file names that are plain names of files only. It refuses an offer it
  has no use for: of a term below its own, or not beyond its commit index.
- One snapshot is received at a time. An offer of the same sender replaces the one being
  received; an offer of another sender does so only with a higher term.
- When every file is there, the generation is verified by its own checksums: it must open as
  a state machine, hold exactly the index and term of the offer, and every block of every
  table must be intact. Only then the core is told. A damaged snapshot is discarded and the
  sender is told; nothing of it is ever installed.
- Partial files do not survive a restart of the receiver: a generation that `sm/CURRENT`
  does not name is removed at the start.

### 5.3 Installation

The core decides: install or reject, whether the stored entries after the snapshot stay,
and whether the state machine is replaced (`raft.md` §9). The steps:

1. Write `INSTALL`.
2. If the state machine is replaced: close it, replace `sm/CURRENT`, open the new generation,
   delete the old one. Otherwise delete the received generation.
3. Replace `SNAPSHOT`.
4. If the entries after the snapshot stay, delete the segments before it; otherwise delete
   every segment and start the log after the snapshot.
5. Delete `INSTALL`, then tell the core.

A node that finds `INSTALL` at its start repeats steps 2 to 5 before it opens anything.
Each step has no effect if it was done already. A damaged `INSTALL` stops the start: the
node cannot know how far the installation went.

## 6. Replica Repair

Local storage never guesses; the cluster repairs.

| Finding | Reaction |
|---|---|
| Damage in the state-machine engine (read path, scrubber, failed open) | Give the generation up and rebuild it: from the own log if that reaches back to its first entry, otherwise from the leader (below). The identity is unaffected. |
| Damage in the Raft log | Log-recovering mode (§3.3). |
| Lost identity | Refuse to start; replace the member (§3.4). |
| Sync failure | Stop the process. On restart, normal recovery decides. |

A node that is repairing reports that state through the admin API and the metrics.

**A damaged state machine** is never used again once the damage is known:

1. Whoever finds the damage (applying an entry, a read, a scrub through the damage listener
   of the engine, or the start when the engine does not open) writes `REPAIR`. A running
   node stops; the repair is part of the next start.
2. If `SNAPSHOT` names index 0, the log reaches back to its first entry and stays. Otherwise
   the log is of no use without the state it continues, and it goes too:
   1. The core is told that the log is lost from the entry after `SNAPSHOT` on. It enters
      log-recovering mode and notes in the hard state what the lost log held, so the node
      can still vote as it would have voted with its log (`raft.md` §8). This is durable
      before the log is touched. If `SNAPSHOT` cannot be read either, the hard state says
      that the lost log is unknown, and the node gives no vote until it is refilled.
   2. `SNAPSHOT` is set to index 0 with the configuration of `BOOTSTRAP`, and every segment
      of the log is deleted.
3. `sm/CURRENT` is set to a new, empty generation whose origin is `BOOTSTRAP`.
4. `REPAIR` is deleted.

After that the node starts as usual. With its log it applies everything again. Without it,
the leader sends its state machine and the entries after it, as to any node in
log-recovering mode, and the mode ends as specified there.

Every step can be repeated; a node that finds `REPAIR` at its start begins again at step 2.
Without a readable `BOOTSTRAP` the state machine cannot be rebuilt: the node refuses to
start and the member is replaced.

Giving up an intact log costs a transfer that a cleverer procedure could avoid. It keeps the
repair on the one path that is verified for a node that has lost what it acknowledged.

## 7. Overload and Slow Disks

- Every queue is bounded. When a queue is full, the request is rejected at once with an
  overload error; it never waits unboundedly.
- Every client request carries a deadline. It is checked at admission, before proposing, and
  before answering. Expired requests are dropped.
  - Inside a node a deadline is a point in the time of the node, which is the number of
    ticks it has processed. The layer that receives a request turns the time the client
    allows into that.
  - A proposal whose deadline has passed is refused at admission. One that waits in the
    queue beyond its deadline is dropped when its turn comes, and the listener is told.
- A leader whose log sync takes longer than `slowSyncThreshold` (default: one election
  timeout) steps down, so that a node with a healthy disk can lead.
  - Measured is the time to make the work of one `Ready` durable: hard state, truncation,
    entries, sync. The node takes the time from the clock it is given.
  - The core is told to step down after it was told that the work is durable: the entries
    are stored, and their acknowledgments count.
  - Followers are not judged. A slow follower holds nobody up.
- Proposals are rejected when the uncommitted cap of `raft.md` §4 is reached, or when the
  state machine falls more than a configured number of entries (default 10,000) behind the
  commit index.
- A burst can overrun the queue of a follower as well. What a follower drops is sent again
  by the rule for lost appends (`raft.md` §4).

## 8. Wire Protocol

- Length-prefixed binary frames over TCP, one virtual thread per connection.
- A connection starts with a handshake: protocol version, cluster id, node id (peers), and
  the software version. A node rejects peers of another cluster.
- Every encoded message starts with a message-format version byte and ends with a checksum.
  - A message between Raft cores: version (`uint8`, 1), type (`uint8`), sender (`uint32`),
    addressee (`uint32`), term (`uint64`), the remaining fields of the message in a fixed
    order, CRC32C over everything before it (`uint32`). Integers are little-endian, a
    boolean is one byte.
  - A receiver rejects what it does not know: another version, an unknown type, a length
    that does not fit the type, a wrong checksum.
  - A request to the leader to set an alarm uses the same frame with type 40, 0 in the
    place of the term, and one byte for the alarm (0 `NOSPACE`).
  - The messages of a snapshot transfer use the same frame with the types from 32 on
    (32 offer, 33 fetch, 34 chunk, 35 done, 36 abort) and the transfer id in the place of
    the term.
    - Offer: term of the leader, last included index, last included term (`uint64` each),
      cluster version (`uint32`), length of the encoded configuration (`uint32`) and the
      configuration, number of files (`uint32`), and per file the length of its name
      (`uint32`), the name in UTF-8, and its size (`uint64`).
    - Fetch: file number (`uint32`), offset (`uint64`). Chunk: file number, offset, number
      of bytes (`uint32`), the bytes. Done: accepted (one byte). Abort: nothing.
- Separate listeners for peers, clients, and administration.
- Not-leader responses carry the leader's client address if known.

## 9. Client Library (`kv-client`)

- Discovers the leader from any reachable node and follows redirects.
- Registers a session, keeps it alive, numbers commands, and retries on timeouts, redirects,
  and overload with backoff until the request deadline. Retries are exactly-once through the
  session.
- A request whose outcome is unknown at the deadline is reported as unknown, not as failed.
- Depends only on `kv-common`.
