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
  cluster version (`uint32`); 11 set address (as 2); 12 backup (the target directory).
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

The addresses of a node are part of the replicated state, and a node that is not the leader
sends clients to the address the cluster knows the leader by. A node therefore listens at
the same addresses after every restart; if it has to move, its new addresses are set with
`set-address` before clients are expected to find it.

**The node program.** `FreyjaNode init --data DIR --cluster HEX --node N [--voters N,...]
[--cluster-version V]` prepares a data directory; without voters, for a node that is to
join. `FreyjaNode run --data DIR --peer HOST:PORT --client HOST:PORT --admin HOST:PORT
[--seed N=HOST:PORT]... [--tick-ms MILLIS]` runs the node and prints
`READY node=N peer=PORT client=PORT admin=PORT` when it listens. It needs either
`--insecure yes` or the options for TLS (§5). Further options of `run`,
each with a number of bytes or of entries: `--segment-bytes` (a segment of the Raft log,
64 MiB), `--snapshot-log-bytes` (the size of the log at which the node takes a snapshot and
shortens it, 1 GiB), `--snapshot-retain` (the applied entries it keeps then, 10,000),
`--quota-bytes` (the size of the state machine at which the alarm for lack of space is
raised, 16 GiB), `--min-free-bytes` (the free space of the file system below which it is
raised, 1 GiB).


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
| Gauges of space | `node.stored.bytes`: what the node holds, its Raft log and what its state machine holds (tables, and memtables at their size in memory, which stands in for the log of the engine they are written to); `disk.written.bytes`: the bytes the node has appended to files since it started, of every kind |
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

**A backup directory:**

```
BACKUP     the manifest, written last; a directory without it is not a backup
engine/    a checkpoint of the storage engine of the state machine
```

- The manifest: magic `FRYK` (`uint32`), format version (`uint8`, 1), cluster id (`uint64`),
  node id (`uint32`), cluster version (`uint32`), last included index and term (`uint64`
  each), length of the encoded configuration (`uint32`) and the configuration, number of
  files (`uint32`), and per file the length of its name (`uint32`), the name in UTF-8, its
  length (`uint64`), and the CRC32C of its content (`uint32`); then the CRC32C of everything
  before (`uint32`). Integers are little-endian.
- **Taking it** (`FreyjaAdmin HOST:PORT backup DIRECTORY`, any node): the node takes a
  checkpoint in `snap/backup-<n>/`, which is quick and holds nothing up. A thread beside the
  node copies it into the target, with a checksum of every file, syncs, writes the manifest,
  and removes the checkpoint. The target must not exist and lies on the machine of the node.
- **Verifying it** (`BackupVerify DIRECTORY`): the manifest; every file against its length
  and its checksum, and no file besides; then the engine by its own checksums.
- **Restoring it** (`FreyjaNode restore --backup DIR --data DIR --cluster HEX --node N`):
  the backup is verified first; nothing is made of one that is not whole. The new data
  directory holds the files of the backup as its state machine, `SNAPSHOT` with what the
  backup covers, and a log of one entry after that: a configuration entry that makes the
  node the only voter. The node elects itself and commits it, and so the state machine
  learns the new membership too. The hard state starts in the term of that entry.
  - The cluster id must be new. Nodes of the old cluster that still exist somewhere must
    not take the restored node for one of their own; the handshake keeps them apart.
  - Sessions, alarms, and the addresses of the old members are part of the restored state.
    Sessions expire; addresses are set anew as members are added.
- **A lost majority** (`FreyjaNode recover-majority --data DIR --cluster HEX
  --i-accept-data-loss yes`), on the data directory of the one node that is left, while it
  is not running: its log gets one more entry, a configuration that makes it the only voter,
  in a term above everything it has seen; its identity is replaced by one with the new
  cluster id. Whatever the lost nodes had acknowledged and this node never received is
  gone; entries this node holds without knowing them committed become part of the new
  cluster. The tool says so and does nothing without the confirmation. It refuses a node
  whose log is damaged or that was refilling its log.

## 4. Offline Tools (`kv-tools`)

Each format gets its dump tool in the milestone that introduces the format.

- Dump an SSTable, a WAL, a manifest, a Raft log segment, the hard state.
- Verify a data directory or a backup: every checksum, manifest consistency, log continuity.
  - The tools, all in `kv-tools`: `SstDump`, `WalDump`, `ManifestDump`, `RaftLogDump`,
    `NodeStateDump` (the small files of a node), `DbVerify` (an engine directory),
    `BackupVerify` (a backup), and
    `NodeVerify` (the whole data directory of a node that is not running: the small files,
    every segment of the Raft log, the engine of the generation that counts). None of them
    changes anything. Each ends with 0 if all is well, 1 on damage, 2 on wrong usage, and
    says what the node will do about damage at its next start.
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

**How a role is expressed.** A node has three listeners, and says for each of them whose
certificates it accepts there:

- by a **trust store** of the listener: the certificate authorities whose certificates are
  accepted at this listener and at no other;
- and, optionally, by a **required name**: an attribute that the subject of the certificate
  must have, written `ATTRIBUTE=VALUE`, for example `OU=freyja-admin`. The attribute is found
  whatever its case; the value must be the same, character for character. An attribute that
  shares its part of the subject with another one (a multi-valued part) does not count.

So the roles are kept apart either by an authority per role, or by one authority and a name
per role. **With one authority for everything and no names, every certificate is good for
every listener:** a client could then administer the cluster and pose as a node. An
installation with one authority sets all three names.

**Files.** Every store is a PKCS12 file. One password file holds the password of all stores
of a process in its first line; without it the password is empty.

- The **key store** of a node holds its private key and its certificate chain. The node
  shows this certificate at all three listeners and to the peers it connects to.
- A **trust store** holds certificates of authorities and nothing else that matters. A node
  never trusts the authorities that come with the JDK.
- The **token file** holds one token per line; empty lines and lines that start with `#` do
  not count. A token has 16 to 255 bytes. A file with a token outside of that, or with no
  token, is not used.

**The options of a node** (`FreyjaNode run`):

| Option | Meaning |
|---|---|
| `--insecure yes` | No TLS and no authentication at any listener. Excludes every option below. |
| `--tls-key FILE` | The key store of the node. |
| `--tls-password-file FILE` | The password file. |
| `--peer-trust FILE`, `--peer-name A=V` | Whose certificates are those of nodes. |
| `--client-trust FILE`, `--client-name A=V` | Whose certificates are those of clients. |
| `--client-tokens FILE` | The token file. |
| `--admin-trust FILE`, `--admin-name A=V` | Whose certificates are those of administrators. |

A node that is given neither `--insecure yes` nor `--tls-key` does not start. With TLS, the
trust stores for peers and for administrators are required, and for clients a trust store, a
token file, or both.

**What is checked on a connection.** Only TLS 1.3 is spoken.

- The side that **connects** (a node to a peer, a client, the tool of an administrator)
  checks the certificate of the node it reaches: it must come from an authority of its trust
  store, it must be made out for the name or the address that was dialed (a subject
  alternative name), and it must carry the required name if one is set. The addresses that
  the cluster holds of its members (`set-address`, `add-learner`) are therefore names or
  addresses for which the certificates of those members are made out.
- A node that **accepts a peer or an administrator** demands a certificate from an authority
  of the trust store of that listener, with the required name of that listener.
- A node that **accepts a client:**
  - with a trust store and no token file, it demands a certificate as above;
  - with a token file and no trust store, it asks for no certificate, and the client sends a
    token in its handshake (`node.md` §8);
  - with both, a client shows a certificate or sends a token. A certificate that is shown
    must be good for this listener, whatever the token.
- A token is compared with every token of the file, each in a time that does not depend on
  where they differ. A client sends a token over TLS only.
- A connection that fails a check is closed without an answer and counted
  (`transport.refused`, `listener.clients.refused`, `listener.administrators.refused`).
- The node id that a peer names in its handshake is not bound to its certificate: a
  certificate of a node is trusted to be used by a node of the cluster.

**Reloading.** When a connection is made, and at most once a second, a process compares the
content of its stores, its password file, and its token file with what it has loaded. Changed
files are loaded and used for this and every later connection. Files that cannot be used
leave the last good ones in force; the log says so. Connections that exist go on as they
are; a node whose certificate is replaced is not restarted. The metrics count both
(`security.reloads`, `security.reload_failures`).

**Rotating certificates** follows from that:

- A new certificate of the same authority: replace the key store of each node, of each
  client, of each administrator, in any order.
- A new authority: first add its certificate to every trust store, beside the old one; then
  replace the key stores; then remove the old authority from the trust stores.
- A token: add the new one to the token files, move the clients over, remove the old one.

**The insecure mode** is said to the node with `--insecure yes`. The node writes a warning
to its log when it starts, and its metrics show `security.insecure` as 1.

**Outside of this:** the HTTP endpoint for metrics and health (`--status`) speaks plain HTTP,
serves nothing but reads of the metrics and the health, and is bound to an address that only
the monitoring reaches. A backup is written with the rights of the node on its machine.

**The client library** takes a trust store, optionally a key store, optionally the name the
certificates of the nodes must carry, and optionally a token
(`FreyjaClient.Options.withTls`). **The tool of an administrator** takes the same before the
address of the node: `--tls-key FILE --tls-trust FILE [--tls-password-file FILE]
[--tls-name A=V]`.

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
and each is executed at least once against a real cluster before release. The procedures are
in `docs/runbook.md`:

1. Bootstrap a cluster.
2. Replace a dead node.
3. Replace a node with lost identity or a damaged disk.
4. Recover from a full disk and clear the alarm.
5. Rolling upgrade, finalization, and downgrade before finalization.
6. Backup, verify, and restore.
7. Recover from a lost majority.
8. Rotate certificates.

What the procedures rest on and the spec does not say elsewhere:

- The members of a cluster that was just prepared with `init --voters` have no addresses in
  the replicated state; they are set with `set-address`, or nodes cannot send clients to the
  leader by its address. A restored or recovered node carries the addresses of the old
  cluster and gets its own set the same way.
- A node id is used once. A member that is lost is replaced by a node with a new id, which
  is added before the lost one is removed.
- The guard of the finalization (§6) is not implemented yet: `set-version` takes any higher
  cluster version.
