# Roadmap

Rules:

- Work in vertical slices. Run `./mvnw test` after every slice.
- Anything that shapes an API or a file format is built in the milestone where that API or
  format first appears. Nothing structural is retrofitted in a later phase.
- A milestone is complete when the oracles named for it pass (`verification.md`).
- For one person this is a multi-year plan. Phases A to C produce a working system; phases D
  and E are at least as much work again.

## Phase A — Storage Engine

- **A1.** Multi-module build. `kv-common`. `StorageFileSystem` (real and in-memory). Windows
  behavior test. SSTable writer and reader with context checksums. Bloom filter. Format unit
  tests. Exhaustive checks of the pure kernels (`verification.md` §2.8). `kv-tools` with the
  SSTable dump.
- **A2.** WAL with sync points and the recovery classification. Exhaustive check of WAL
  fragmenting. MemTable. Group commit.
  Differential fuzz over MemTable and WAL with `reopen`. WAL dump.
- **A3.** Flush to L0. Block cache. Read path across MemTables and SSTables. Manifest.
  `LOCK` file. Orphan cleanup of logs and tables. Differential fuzz with `flush`. Manifest
  dump.
- **A4.** MVCC snapshots and `ScanIterator`. Differential fuzz with snapshots and scans.
- **A5.** Leveled compaction with snapshot-aware retention. Reference-counted file cleanup
  with pinned versions. Background threads for flush and compaction, and deterministic mode
  without them. Backpressure. Table-metadata budget. Manifest roll-over when it grows large.
  I/O rate limiting. Checkpoints. Differential fuzz with `compact` and `checkpoint`.
- **A6.** Faulty file system with the full storage fault model. Crash tests. Damage tests.
  Disk-full handling. Scrubbing. Damage listener. Metrics hooks. Handle-leak test. CI matrix
  for Windows, Linux, macOS. Offline verify tool.

## Phase B — Raft Core

- **B1.** `raft-core` with the batched `Ready` contract, asynchronous persistence, PreVote,
  CheckQuorum, replication, and flow control. Purity test. `raft-sim` with a reliable network,
  delayed persistence, and process pauses. The safety checks of the simulator are ordinary
  code in this milestone; they move into the protected `SafetyInvariants` file in B2.
- **B2.** Full fault injection. Safety invariants in the protected `SafetyInvariants` file
  with checksum test and negative tests. ReadIndex. Linearizability checker. TLA+ model for
  elections and replication. Trace recording and trace validation with mutation tests.
- **B3.** Membership change with learners. Leadership transfer. Log-recovering mode. TLA+
  model and trace mapping extended to these parts.
  - The traces of runs with these parts are validated (2026-11-19).
  - Log damage is checked exhaustively for three voters, up to the repair.
  - Open: an exhaustive check of membership change and of the repair
    (`docs/tla/README.md`).
- **B4.** Log compaction and snapshot directives, verified in the simulator with an in-memory
  state machine. Snapshots are not part of the TLA+ model.

## Phase C — Node

- **C1.** Node skeleton on the clock, transport, and file-system interfaces with a steppable
  event loop. Raft log store with segments. Double hard-state files. Damage and lost-identity
  handling. Log store tested with the faulty file system. Log dump.
  - The skeleton has the transport interface and the event loop; it is stepped by its
    caller and needs no clock yet. The clock enters with the runtime thread and with the
    time stamps of commands (C2, C4).
- **C2.** State machine on `lsm-core`: namespaces, applied index, sessions, conditional
  batches, quota and alarms, cluster version. Checkpoint snapshots with chunked, resumable
  transfer. Replica repair. Overload rules.
  - Done so far: commands and results with their codec (`kv-common`), the state machine
    with namespaces, applied index, configuration, sessions, and conditional batches.
    Alarms and the cluster version in the replicated state. Generations of the state
    machine, shortening of the log, snapshot transfer and crash-safe installation. Repair
    of a damaged state machine. The alarm from the quota and from low free space. Deadlines
    of proposals and the limit on the apply lag.
  - Reverse scans: the engine iterates backward too (`storage-engine.md`), and node and
    client serve them.
- **C3.** Whole-system simulation with all faults and the reachability probes. Protected
  `SystemInvariants` file. Trace validation of whole-system traces.
  - Done so far: the step-down of a leader with a slow disk; the simulation with clients,
    network faults, and crashes; `SystemInvariants` with its negative tests and its
    checksum; the mutation checks of `verification.md` §3; damaged logs and tables with
    their repairs, short and slow disks, hand-overs; replaced members and lost identities;
    the Raft invariants over complete nodes.
  - The probes below the node are counted (`verification.md` §3.1).
  - Whole-system runs are recorded as traces in the form of the Raft simulator
    (`verification.md` §2.7).
  - Those traces validated with TLC: six complete runs, 26,447 steps (2026-11-19).
  - A crash between the write of the hard state and the write of the log of one step:
    the node leaves log-recovering mode only once the entries that end it are durable; the
    simulation counts and aims at the crash; the model has a restart in which a step is
    durable in part (`verification.md` §3).
  - With that model: `Raft.cfg` checked exhaustively, and the traces of the Raft simulator
    and of nine whole-system runs (42,410 steps) validated (2026-11-19).
  - The Raft simulator lets the log of a node go back to an earlier state, with entries in
    it again that were cut off, as the log store of a node does after damage
    (`verification.md` §2.4).
- **C4.** Real transport with virtual threads. Wire protocol with handshake. `kv-client`.
  Admin API and command-line tool. End-to-end test with real processes.
  - Done so far: the client protocol with its codec (`kv-common`); the service that serves
    requests on a node, stepped like the node and tested with complete nodes; the runtime
    thread with the clock; frames and the handshake; the TCP transport between nodes; the
    listener for clients; `kv-client`; the addresses of the nodes in the replicated state;
    the admin protocol, its service, its listener, and the tool `FreyjaAdmin`; the program
    `FreyjaNode`. Tested with three nodes in one process over loopback sockets, with the
    client library against a scripted node and against that cluster, and end to end with
    real processes on real files: clients are served, the leader is killed and comes back, a
    fourth node joins and is promoted.
  - The status names the match index and the state of every member, and membership changes
    that would leave no majority of voters that are well are refused unless forced.

## Phase D — Hardening

- **D1.** Continuous simulation runner, seed corpus, probe thresholds. Multi-version
  simulation harness.
  - Done so far: the runner with its report, the filing of failing seeds, and the
    thresholds for the probes that the simulation counts.
  - Open: a scheduled job that runs it; the multi-version harness, which needs a second
    release to load.
- **D2.** External fault suite on Linux with the independent checker cross-check.
  - Done so far: the suite with its proxy, its clients, and its scenarios
    (`verification.md` §4); kills, partitions, and the slow network pass on the development
    machine.
  - Open: pauses, the full disk, and the cross-check, which need Linux; the count of
    consecutive green runs of the exit criteria; the privileged stage.
- **D3.** `kv-bench`: workloads, open-loop load generator, baselines. Tuning. Overload tests.
  - Done so far: the module with the open-loop load generator, the six workloads, the
    latency histogram, stored results, and the comparison with a baseline.
  - Write and space amplification from the figures of the nodes; every workload during
    compaction, snapshot transfer, and follower catch-up; the overload test at twice the
    sustainable load (`verification.md` §7).
  - Open: tuning and baselines on Linux.
- **D4.** Soak tests with monitoring.

## Phase E — Operations

- **E1.** Metrics endpoint, logging, health endpoints. Offline tools complete.
  - Done so far: the metrics registry, the HTTP endpoint with `/metrics`, `/health`, and
    `/ready`, what a node publishes and logs (`operations.md` §2).
  - `NodeVerify` checks the whole data directory of a node that is not running: the small
    files, every segment of the Raft log, and the engine of the state machine.
- **E2.** TLS for peers, clients, and administration. Authentication. Certificate reload.
  - Done: TLS 1.3 on the three listeners, a trust store and an optional required name per
    listener, tokens for clients, reloading of all files, the insecure mode as an explicit
    choice (`operations.md` §5). Tested with a cluster of processes (`SecureClusterTest`).
- **E3.** Backup and restore. Upgrade procedure with finalization. Compatibility suite.
  Lost-majority tool. Runbook, each procedure executed. Review against the exit criteria.
  - Done so far: backup of a running node, its verification, restore as a new cluster, and
    the recovery from a lost majority (`operations.md` §3).
  - The runbook (`docs/runbook.md`): every procedure but the upgrade carried out with node
    processes on one machine.
  - The guard of the finalization: nodes name the highest cluster version they support in
    their handshakes, the leader raises the cluster version only to one every member
    supports, and a node ends in a cluster whose version its release does not support
    (`operations.md` §6).
  - Open: the upgrade procedure and the compatibility suite, which need a second release;
    the execution of the runbook on a real cluster.

## Open Items

Everything that is not done, in one place. The milestones above say what is done. An item
leaves this list in the change that completes it.

**Work that can be done on the development machine:** none.

**Work that needs Linux:**

| Item | Milestone | Notes |
|---|---|---|
| External fault suite: pauses, the full disk, the cross-check by an independent checker, 50 green runs per scenario | D2 | The suite is written; the device-level stage is optional and needs privileges |
| Performance baselines on the production platform; tuning | D3 | |
| Soak tests of several days with monitoring | D4 | Not started; needs a machine that can be left alone for a week |

**Work that needs something that does not exist yet:**

| Item | Milestone | Needs |
|---|---|---|
| Multi-version simulation harness | D1 | A second release |
| Upgrade procedure with finalization, verified in simulation and with real processes | E3 | A second release |
| Compatibility suite | E3 | A first release to capture, a second to read it |
| A scheduled job for the continuous simulation | D1 | A decision of the project lead: the build workflow runs by hand and once a week (Fridays, 04:00 UTC), with the tests of every build only |
| Every procedure of the runbook executed once | E3 | A real cluster |
| Review against the exit criteria | E3 | Everything above |
| Exhaustive check of membership change and of the repair of a damaged log | B3 | A machine that can run TLC for days, with a large disk; the development machine cannot (`docs/tla/README.md`) |

**For the project lead:**

- Review of `SystemInvariants` (`kv-node`). It was written with milestone C3 and is protected
  from then on (`verification.md` §2.6).
- The numbers of the exit criteria (`verification.md` §8) are proposals.
