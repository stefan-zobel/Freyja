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

## 2. Data Directory

```
NODE              identity: cluster id, node id, format version (written once)
HARDSTATE-A/-B    term, vote, log-recovering flag (§3.2)
raft/             Raft log segments
sm/CURRENT        name of the active state-machine generation
sm/<generation>/  an lsm-core engine directory
snap/             checkpoints being sent, snapshots being received
```

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

### 3.2 Hard State

Term, vote, and the log-recovering flag are stored in two files, `HARDSTATE-A` and
`HARDSTATE-B`, each holding one checksummed record with a version counter.

- Update: write and sync A, then write and sync B. The node acts on the new state only after
  both are durable.
- Recovery:
  - Both valid: use the one with the higher counter.
  - Exactly one valid: use it. If A is unreadable, the update to A never completed or A was
    damaged later; in both cases B holds a state the node had fully committed to. If B is
    unreadable, A is at least as new.
  - Both invalid: the identity is lost (§3.4).

### 3.3 Log Damage

On open, damage in a segment (`storage-format.md` §3.2) is never truncated away. The store
determines the first entry index it can no longer vouch for and feeds `LogDamaged` to the
core, which enters log-recovering mode (`raft.md` §8). The node stays in the cluster, does not
vote, and is refilled by the leader.

A torn tail is not damage: those entries were never acknowledged.

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
- The Raft log is truncated only through the index of a completed checkpoint, never through
  an index that is merely applied.

### 4.1 Commands

- `put`, `delete`, atomic batch of both.
- Conditional batch: a list of conditions, a batch applied if all hold, and optionally a
  batch applied otherwise. Conditions per key: exists, does not exist, value equals,
  `ModIndex` equals.
- Session commands: register, keep alive, close.
- Limits: key 64 KiB, value 1 MiB, one request 4 MiB.

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

## 5. Snapshots

- A Raft snapshot is an engine checkpoint (`storage-engine.md` §9) plus a metadata file with
  `SnapshotMeta`. Creating one does not block writes.
- Triggers: log size above a threshold (default 1 GiB), or on request.
- **Transfer:** file by file in chunks of 1 MiB, each chunk with a checksum. The receiver
  keeps partial files and resumes at `(file, offset)` after a connection loss. Every received
  file is verified through its own checksums before the snapshot is offered to the core.
- **Install:** the received directory becomes a new state-machine generation; `sm/CURRENT`
  is switched by atomic replace; the old generation is deleted afterward.
- A checkpoint being sent is kept until the transfer ends or times out. The number of
  concurrent outgoing transfers is bounded.

## 6. Replica Repair

Local storage never guesses; the cluster repairs.

| Finding | Reaction |
|---|---|
| Damage in the state-machine engine (read path, scrubber, failed open) | Discard the generation, request a snapshot from the leader, install it. Raft log and identity are unaffected. |
| Damage in the Raft log | Log-recovering mode (§3.3). |
| Lost identity | Refuse to start; replace the member (§3.4). |
| Sync failure | Stop the process. On restart, normal recovery decides. |

A node that is repairing reports that state through the admin API and the metrics.

## 7. Overload and Slow Disks

- Every queue is bounded. When a queue is full, the request is rejected at once with an
  overload error; it never waits unboundedly.
- Every client request carries a deadline. It is checked at admission, before proposing, and
  before answering. Expired requests are dropped.
- A leader whose log sync takes longer than `slowSyncThreshold` (default: one election
  timeout) steps down, so that a node with a healthy disk can lead.
- Proposals are rejected when the uncommitted cap of `raft.md` §4 is reached, or when the
  state machine falls more than a configured number of entries behind the commit index.

## 8. Wire Protocol

- Length-prefixed binary frames over TCP, one virtual thread per connection.
- A connection starts with a handshake: protocol version, cluster id, node id (peers), and
  the software version. A node rejects peers of another cluster.
- Every encoded message starts with a message-format version byte and ends with a checksum.
- Separate listeners for peers, clients, and administration.
- Not-leader responses carry the leader's client address if known.

## 9. Client Library (`kv-client`)

- Discovers the leader from any reachable node and follows redirects.
- Registers a session, keeps it alive, numbers commands, and retries on timeouts, redirects,
  and overload with backoff until the request deadline. Retries are exactly-once through the
  session.
- A request whose outcome is unknown at the deadline is reported as unknown, not as failed.
- Depends only on `kv-common`.
