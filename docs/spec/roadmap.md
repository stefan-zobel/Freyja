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
  - Open: the trace validation of runs with these parts, and an exhaustive check of a
    configuration with them (`docs/tla/README.md`).
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
  - Moved: the step-down of a leader with a slow disk needs the clock and comes with the
    whole-system simulation (C3).
  - Open decision: `node.md` §4.3 asks for reverse scans; the engine iterates forward only.
    Either the engine gets backward iteration or the client API drops reverse scans. To be
    decided before the client API of C4.
- **C3.** Whole-system simulation with all faults and the reachability probes. Protected
  `SystemInvariants` file. Trace validation of whole-system traces.
- **C4.** Real transport with virtual threads. Wire protocol with handshake. `kv-client`.
  Admin API and command-line tool. End-to-end test with real processes.

## Phase D — Hardening

- **D1.** Continuous simulation runner, seed corpus, probe thresholds. Multi-version
  simulation harness.
- **D2.** External fault suite on Linux with the independent checker cross-check.
- **D3.** `kv-bench`: workloads, open-loop load generator, baselines. Tuning. Overload tests.
- **D4.** Soak tests with monitoring.

## Phase E — Operations

- **E1.** Metrics endpoint, logging, health endpoints. Offline tools complete.
- **E2.** TLS for peers, clients, and administration. Authentication. Certificate reload.
- **E3.** Backup and restore. Upgrade procedure with finalization. Compatibility suite.
  Lost-majority tool. Runbook, each procedure executed. Review against the exit criteria.
