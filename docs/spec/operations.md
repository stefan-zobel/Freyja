# Operations

## 1. Admin API

Served on the administration listener and used by the command-line tool in `kv-tools`.

- Membership: add learner, promote learner, remove member, list members with role, match
  index, and repair state.
- Leadership: transfer to a given node.
- Maintenance: trigger snapshot, trigger compaction, start a scrub pass, create a backup.
- State: node status, cluster version, alarms; clear an alarm; finalize an upgrade.

Membership changes are refused when they would leave the cluster without a healthy majority,
unless forced.

**Protocol.** The administration listener speaks frames with the handshake role 3
(`node.md` §8). A message: version (`uint8`, 1), type (`uint8`), request id (`uint64`), body,
CRC32C (`uint32`); a string is a length (`uint32`) and UTF-8. The types and their codec live
in `kv-common`.

- Requests: 1 status; 2 add learner (node id `uint32`, address for peers, address for
  clients); 3 promote learner, 4 remove member (node id, force as one byte); 5 transfer
  leadership (node id); 6 snapshot; 7 scrub; 8 compact; 9 clear the space alarm; 10 set
  cluster version (`uint32`); 11 set address (as 2).
- Responses: 65 done (a detail for a person); 66 refused (the reason, and the id of the
  leader if the reason is that the node is not the leader); 67 status (node id, cluster id,
  software, role, term, leader id, last log index, commit index, applied index, the index the
  log starts after, log-recovering, cluster version, space alarm, restarts, snapshots
  installed, and per member its id, whether it votes, its two addresses, its match index
  (`uint64`, all bits set for unknown), and its state as a string).

**What an answer means.**

- Status, snapshot, scrub, and compact concern the node that is asked and are done at once.
- A change of the membership goes to the leader and is answered when the core has taken or
  refused it. Taken means: it is in the log of the leader at the index the answer names. The
  status shows when it has taken effect.
- A command for the state machine (clear the alarm, set the cluster version, set an
  address) goes to the leader and is answered when that node has applied it, with what it
  did: a cluster version below the present one is refused there. If another entry takes its
  place, the answer says that it was lost with a change of the leader.
- Add learner is two entries: where the node listens, then the node as a learner.
- A node that stops while a change waits says so; the change may be in the log.
- **Members in the status.** The leader says of every member how far its log is known to
  agree with its own (the match index) and how it is: `ok` (heard from since the last check
  for a quorum or at it, and neither of the following), `silent`, `recovering` (refilling a
  log it lost), `snapshot` (receiving the state machine). A node that is not the leader
  does not know and says so with an empty state.
- **The check for a healthy majority.** Promoting a learner and removing a member are
  refused if, of the voters there would be afterward, fewer than a majority are `ok`,
  counting the leader itself. Such a cluster could commit nothing, not even the way back.
  With `force` the check is skipped.

**The tool.** `FreyjaAdmin HOST:PORT COMMAND [ARGUMENT]...` in `kv-tools` sends one request
to one node and prints the answer: `status`, `add-learner N PEER CLIENT`, `promote N [--force]`,
`remove N [--force]`, `transfer N`, `snapshot`, `scrub`, `compact`, `clear-alarm`, `set-version V`,
`set-address N PEER CLIENT`. Exit code 0: done; 1: refused or not reachable; 2: wrong usage.

**The node program.** `FreyjaNode init --data DIR --cluster HEX --node N [--voters N,...]
[--cluster-version V]` prepares a data directory; without voters, for a node that is to
join. `FreyjaNode run --data DIR --peer HOST:PORT --client HOST:PORT --admin HOST:PORT
[--seed N=HOST:PORT]... [--tick-ms MILLIS]` runs the node and prints
`READY node=N peer=PORT client=PORT admin=PORT` when it listens.

Not there yet: the backup (milestone E3).

## 2. Observability

No third-party libraries.

- **Metrics:** an internal registry of counters, gauges, and histograms, exposed as plain
  text over HTTP using the JDK's built-in HTTP server. Covered at least: everything listed in
  `storage-engine.md` §10; Raft term, role, commit and applied index, per-follower lag,
  proposal and read latency histograms, rejected requests by reason, elections, snapshot
  transfers; queue depths; repair state; alarms.
- **Logging:** through `System.Logger`. Every state change of a node (role, configuration,
  repair, alarm, damage) is logged once with its cause.
- **Health:** a liveness endpoint (process is running) and a readiness endpoint (node has a
  leader and its applied index is within a configured distance of the commit index).

**The endpoint.** `FreyjaNode run ... --status HOST:PORT` serves, over HTTP and for `GET`
only:

- `/metrics`: every metric as one line `name value`, sorted by name. A name is `freyja_` and
  the reported name with every character that is not a letter, a digit, or an underscore
  replaced by an underscore. A counter ends in `_total`. A duration gives `_count`, and in
  microseconds `_p50_micros`, `_p99_micros`, `_p999_micros`, `_max_micros`, from a histogram
  with a relative error of about one percent.
- `/health`: 200 while the thread of the node runs; 503 with the reason once the node has
  ended for good (it has lost its identity).
- `/ready`: 200 if the node knows a leader, is not refilling its log, and has applied to
  within 10,000 entries of what is committed; otherwise 503 with the reason.

**What a node publishes**, besides the figures of its storage engine
(`storage-engine.md` §10), once per tick:

| Kind | Names |
|---|---|
| Gauges of the protocol | `raft.term`, `raft.role` (0 follower, 1 pre-candidate, 2 candidate, 3 leader), `raft.leader`, `raft.last.index`, `raft.commit.index`, `raft.applied.index`, `raft.snapshot.index`, `raft.log.recovering`, `raft.log.segments`; on the leader per follower `raft.follower.N.lag` and `raft.follower.N.active` |
| Gauges of the node | `node.queue.events`, `node.queue.inbound`, `node.events.rejected`, `node.messages.invalid`, `node.inbound.refused`, `node.restarts`, `node.snapshots.created`, `node.snapshots.installed`, `node.proposals.expired`, `node.slow.sync.step.downs`, `alarm.nospace`, `cluster.version`, `sessions` |
| Gauges of transfers and network | `transfers.outgoing`, `transfers.sent`, `transfers.received`, `transfers.failed`, `transport.sent`, `transport.received`, `transport.dropped`, `transport.refused`, `listener.clients.connections`, `listener.clients.refused` |
| Clients | durations `client.command` and `client.read`, from the arrival of a request to its answer; counters `client.redirected` and `client.rejected.REASON`; gauges `client.waiting`, `client.dropped` |
| Counters | `raft.elections.won` |

**What a node logs**, through `System.Logger` under the name `freyja.node`, once per change:
its start (with a rebuilt state machine or a witnessed loss of its log end, if so), its role
with term and leader, the configuration in force, entering and leaving log-recovering mode,
the alarm set and cleared, a snapshot taken from the leader, a stop with its cause before
the node is started again, and the loss of its identity.

## 3. Backup and Restore

- **Backup:** an online checkpoint of the state machine plus its `SnapshotMeta`, written to a
  target directory, with a manifest of files and checksums. It does not block writes.
- **Verify:** the offline tool checks a backup completely before it is relied on.
- **Restore:** creates the data directory of a new single-node cluster with a new cluster id
  from a backup. Further members are added as learners.
- **Lost majority:** a documented, explicitly unsafe procedure that starts a new cluster from
  the data directory of one surviving node. Acknowledged writes may be lost; the tool says so
  and requires confirmation.

## 4. Offline Tools (`kv-tools`)

Each format gets its dump tool in the milestone that introduces the format.

- Dump an SSTable, a WAL, a manifest, a Raft log segment, the hard state.
- Verify a data directory or a backup: every checksum, manifest consistency, log continuity.
- Salvage: open a damaged standalone engine directory with stated data loss. Never used
  automatically.

## 5. Security

Only the JDK's TLS implementation is used.

- **Peers:** mutual TLS. A node accepts a peer only if its certificate is signed by the
  cluster's certificate authority.
- **Clients:** TLS, plus authentication by client certificate or by a token from a local
  token file.
- **Administration:** mutual TLS with a separate certificate role.
- Certificates and the token file are reloaded without a restart.
- An insecure mode exists for development and tests. It must be enabled explicitly and is
  reported in the log and the metrics.
- Data at rest is not encrypted by the system.

## 6. Upgrades

- Every release has a version. A cluster may contain nodes of at most two adjacent releases.
- The **cluster version** is part of the replicated state. It starts at the version the
  cluster was created with.
- **Rolling upgrade:** replace the software node by node; move leadership away first. During
  this phase every node behaves as the old release: no new message types, no new command
  types, no new file-format versions.
- **Finalize:** an explicit admin action that raises the cluster version through the log. It
  is refused unless every member runs the new release. Only afterward are new formats written
  and new messages sent.
- **Downgrade** is possible until finalization and impossible after it.
- A node refuses to join a cluster whose cluster version it does not support.

## 7. Deployment Requirements

- Production platform: Linux. Windows and macOS are supported for development and pass all
  functional and simulation tests, without performance or fault-test guarantees.
- Drives must honor flush commands.
- Each node's data directory is on a file system that supports hard links.
- Node clocks need not be synchronized for correctness. Session expiry follows the leader's
  clock.
- Recommended cluster sizes: 3 or 5 voters.

## 8. Runbook

Each procedure is written as numbered steps with the exact commands and the expected output,
and each is executed at least once against a real cluster before release:

1. Bootstrap a cluster.
2. Replace a dead node.
3. Replace a node with lost identity or a damaged disk.
4. Recover from a full disk and clear the alarm.
5. Rolling upgrade, finalization, and downgrade before finalization.
6. Backup, verify, and restore.
7. Recover from a lost majority.
8. Rotate certificates.
