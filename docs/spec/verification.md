# Verification

No milestone is complete without its oracles passing.

## 1. General Rules

- Every randomized test prints its seed on failure and can be rerun with that single seed
  through the system property `freyja.seed`. `freyja.seeds` overrides the number of seeds.
- Every seed that ever failed is added to a corpus file in the repository
  (`src/test/resources/seeds/<test name>.txt` of the module) and is rerun on every build.
- Both rules are implemented once, in `Seeds` in `kv-common`; randomized tests use it.
- The default `./mvnw test` runs a bounded number of seeds and finishes in a few minutes.
  Separate profiles run the large counts.
- Test dependencies: JUnit. The benchmark module may use JMH. Nothing else.

## 2. Component Oracles (fast, first line)

### 2.1 LSM Differential Fuzzing

- 100,000+ operations per run: `put`, `delete`, `get`, `scan`, `snapshot`, `release snapshot`,
  `flush`, `compact`, `checkpoint`, `reopen`.
- Reference model: `TreeMap` with unsigned byte order; a snapshot is a copy of the model.
- Deterministic mode with tiny thresholds (MemTable of a few KiB, small level targets) so
  that flushes and compactions happen constantly.
- Every `get` and `scan`, on the live state, on every open snapshot, and on every opened
  checkpoint, must match the model.

### 2.2 Storage Fault Model and Crash Tests

Fault model:

- At a crash, data written but not synced may be lost entirely, partially written, or
  garbled, in any order across blocks.
- Synced data survives a crash.
- Renames, links, and deletes not followed by a directory sync may be undone.
- Synced data may later be damaged: bit flips, zeroed ranges, a block written to the wrong
  offset or the wrong file.
- Appends may fail for lack of space. Syncs may fail. Any operation may be slow.

Checks:

- Crash at every file-system operation of a workload in turn, reopen, and verify: every
  acknowledged write is present, no partial batch is visible, the engine accepts new writes.
  - The workload mixes synced and unsynced writes, flushes, compactions through several
    levels, checkpoints, manifest replacements, and reopens.
  - Each crash has a random outcome within the fault model for everything not synced.
  - The recovered state must be one of: the state after the last write known to be durable,
    any later state, or the state a write in flight would have produced.
  - For a part of the crash points the recovery crashes as well, once or twice in a row.
- Damage synced files: the engine must detect it and fail the affected read or the open. It
  must never return wrong data. The classification of `storage-format.md` §3.2 is tested with
  both torn tails and damage, including the stated residual case.
  - Damage is a flipped bit, a zeroed range, a garbled range, or bytes of another file (a
    misdirected write), at a random place in a table, a log, the manifest, or `CURRENT`.
  - In a log and in the manifest only bytes that a later sync point proves to be synced are
    damaged. Damage in the tail after the last sync point is the stated residual case and
    is covered by the format tests.
  - Every such damage must be noticed: by the open, by a point read, or by a full scan.
- Disk full: writes are rejected, reads keep working, compaction frees space, writes resume.

### 2.3 File-Handle Leaks

Run many flush, compaction, and checkpoint cycles on the real file system, close the engine,
and verify that only files referenced by the manifest remain and that the directory can be
deleted. This test must pass on Windows.

### 2.4 Raft Simulation (`raft-sim`)

- 3-, 5-, and 7-node clusters, virtual time driven by a priority queue.
- Faults: message drops (5–15%), delays (1–100 ms), duplicates, reordering, symmetric and
  asymmetric partitions, differing tick rates per node, crashes and restarts, delayed and
  lost persistence (a crash discards everything not yet reported as persisted), log damage,
  membership changes, leadership transfers, snapshots.
- **Safety invariants, checked after every step:**
  - Election safety: at most one leader per term.
  - Log matching: equal `(index, term)` implies identical prefixes.
  - Leader completeness: a committed entry is present in the log of every later leader.
  - State machine safety: no two nodes apply different entries at the same index.
  - No message that promises a state is sent before that state was reported persisted.
  - At most one uncommitted configuration change; any two quorums of consecutive
    configurations intersect.
- **Liveness, checked only after the network has been healed for a bounded time:** a leader
  exists and new proposals commit.

### 2.5 Linearizability

- Clients issue concurrent writes, conditional writes, and reads, and retry through sessions.
- An operation without a response is treated as possibly taking effect at any later time.
- The checker is Wing & Gong with memoization of visited states, applied per key. Histories
  are kept small enough per run for the search to finish; many seeds replace long histories.
- Additional check: no command is applied twice (exactly-once through sessions).

### 2.6 Protected Invariants

The safety invariants are the project's laws: a human states them, and no code change may
break them.

- The invariants of §2.4 live in one source file, `SafetyInvariants` in `raft-sim`. The
  invariants of §3 (state-machine equality, no acknowledged write lost, exactly-once) live in
  one source file, `SystemInvariants`, next to the whole-system simulation in `kv-node`.
- Each invariant is a named, side-effect-free predicate over the observed state of all nodes.
  The simulators call every predicate after every step.
- These two files are **protected**. They are changed only on the explicit instruction of the
  project owner, never as part of making a failing test pass. Weakening or removing an
  invariant is never an acceptable fix.
- `docs/spec/invariants.sha256` records the checksum of both files. A test compares them, so
  every change to an invariant needs a second, deliberate edit and is visible on its own.
- Every invariant has a negative test: a deliberately broken history that the predicate must
  reject. An invariant that cannot fail proves nothing.

### 2.7 Trace Validation Against the TLA+ Model

Model checking proves the TLA+ specification, not the Java code. Trace validation connects
the two.

- `raft-sim` can record a run as a trace: one line per `step` of every node, in a line-based
  JSON format written by a small hand-written encoder. Each line holds the node id, the event,
  and the node's resulting protocol state: term, role, vote, last log index and term, commit
  index, configuration, log-recovering flag, and the messages released.
- `raft-core` stays pure. It exposes a read-only view of its protocol state; the simulator
  writes the trace.
- A trace specification in `docs/tla/` constrains the model to the recorded sequence. TLC
  checks that the trace is a behavior the specification allows. A trace that the
  specification does not allow is a bug in the code or in the specification, and one of them
  is fixed.
- **Mapping:** every `step` corresponds to one action of the specification or to a fixed,
  documented sequence of actions. The mapping is part of `docs/tla/`. An event without a
  mapping fails validation.
- **Not vacuous:** the validation must reject mutated traces (a second leader in a term, an
  acknowledgment before persistence, a changed committed entry). These mutations are tests.
- TLC is an external development tool, not a dependency. Trace validation runs in its own
  profile, not in the default `./mvnw test`, on a fixed set of seeds plus the failing-seed
  corpus.
- The whole-system simulation (§3) records the same trace format for the Raft part of every
  node, so traces from complete nodes are validated as well.

### 2.8 Pure Kernels: Exhaustive Checks

Small pure functions are not sampled; they are checked over their complete input space, or
over a space built to contain every boundary. Each check compares against a reference that is
written independently and as simply as possible. These tests run in the default build and
must finish in seconds.

- **Varints:**
  - `decode(encode(x)) == x` for every value below 2^21, for 2^(7k) - 1, 2^(7k), and
    2^(7k) + 1 for every k, and for the maximum value.
  - The encoding has the minimal length.
  - For every byte string of up to 3 bytes, the decoder either rejects it or returns a value
    whose encoding is exactly that string. Truncated and overlong encodings are rejected.
- **Fixed-width codecs:** round trip at every boundary value of every width; the encoded
  bytes are compared with a hand-written little-endian reference.
- **Unsigned byte comparator:** over all byte strings of up to 3 bytes from the alphabet
  `{0x00, 0x01, 0x7F, 0x80, 0xFF}`: agreement with the reference for every pair, a shorter
  prefix sorts first, antisymmetry for every pair, transitivity for every triple.
- **Internal-key comparator:** over the same user keys, combined with the sequence numbers 0,
  1, 255, 256, and the two largest valid values, and with every value type: the order is user
  key ascending, then sequence number descending, and it equals the reference for every
  pair. A second check shows that plain bytewise comparison of the encoded keys gives a
  different order for at least one pair, so the test cannot pass by accident.
- **Checksums:** published test vectors for CRC32C; the context checksum changes when the
  file number or the offset changes, for every single-bit change of either.
- **WAL fragmenting:** for every start offset inside a block, combined with payload lengths
  around every fragment boundary (one byte below, at, and above), writing and reading back
  returns the payload, and no fragment header crosses a block boundary.
- **Block prefix compression:** round trip for every sorted key sequence of up to 3 keys over
  the strings of up to 3 bytes from the alphabet above, and for every sorted sequence of 4
  keys over its strings of up to 2 bytes, each with every restart interval from 1 to 4.
  (Sequences of 4 keys over all 156 strings would be 95 million blocks, too many for the
  default build.) A seek to every string is checked against a linear search.
- **Table files:** one small table is compared byte for byte with a file assembled by hand
  from the format specification. Every single-bit flip and every truncation of a table that
  spans several blocks must be reported as damage.
- **Bloom filter:** no false negative for any inserted key, checked for every filter size
  from 1 key to 1,000 keys.
- **Compaction retention:** for every history of up to 5 versions of a key (values and
  tombstones), every set of snapshots on and between them, with and without data in deeper
  levels: the retained versions show every snapshot and the present exactly what the full
  history shows, and no version is retained that no reader can see.

## 3. Whole-System Simulation (primary oracle)

Complete nodes and clients run in one deterministic process on the simulated clock,
transport, and file system. Nodes exchange only bytes through these interfaces.

Injected, all driven by the seed:

- every network fault of §2.4 and every storage fault of §2.2
- process crashes at arbitrary points, slow disks, full disks
- membership changes, leadership transfers, snapshots, backups and restores
- lost identity of a node followed by replacement
- rolling upgrades and downgrades between two releases (each node's code is loaded by its
  own class loader; the previous release's artifacts are stored in the repository)

Checked: all invariants of §2.4, linearizability of the client history (§2.5), state-machine
equality across nodes at equal applied index, no acknowledged write lost, convergence after
healing, and that every repair path ends in a healthy node.

### 3.1 Reachability Checks

Fault injection only proves something for situations that actually occur. The code contains
named probes (no-ops outside tests). The simulation reports how often each was hit. A probe
that is never hit across the seed set fails the run. Required probes include at least:

- snapshot installed while the leader changed
- log-recovering mode entered and left
- state machine reinstalled after damage
- torn tail truncated; damage detected through a sync point
- configuration entry truncated and reverted
- leadership transfer aborted
- session expired while a retry was in flight
- write stalled by backpressure; write rejected by the quota alarm
- upgrade finalized; downgrade before finalization

### 3.2 Continuous Simulation

A runner works through seeds indefinitely on dedicated machines or scheduled CI jobs, records
the simulated time and the number of seeds per commit, and files every failing seed into the
corpus.

## 4. External Fault Tests (Linux)

Real processes on real file systems and sockets.

- Faults: kill, pause and resume, restart; network partitions, delays, and drops through a
  proxy controlled by the test; disk full through a size-limited file system.
- Optional privileged stage: device-level fault injection (dropped and delayed writes, power
  loss) through the kernel's device-mapper fault targets.
- Histories are checked with the project's checker and also exported in the history format
  of an independent linearizability checker for a cross-check.

## 5. Compatibility Suite

- For every release: a data directory, a backup, and captures of every message type are
  stored in the repository.
- The current code must read all of them from the previous release and must reject older or
  newer unknown versions with the documented error.

## 6. Soak Tests

Multi-day runs of 3- and 5-node clusters on Linux under a repeating fault schedule and
continuous load. Monitored: invariant violations, heap and off-heap memory, open file handles,
disk usage relative to live data, latency drift.

## 7. Performance

- Workloads (module `kv-bench`): bulk load, read-heavy (95/5), mixed (50/50), write-only,
  scan-heavy, large values, and each of them during compaction, snapshot transfer, and
  follower catch-up.
- The load generator is open-loop (requests are scheduled by time, not by the previous
  response), so stalls show up in the latencies.
- Reported: throughput, latency at p50, p99, p99.9, write amplification, space amplification.
- Results are stored with the hardware description. A change that worsens a stored baseline
  by more than 10% fails the benchmark job.
- Overload tests: at twice the sustainable load the system must reject requests quickly and
  keep the latency of admitted requests bounded.

## 8. Exit Criteria

"Production-grade" is defined for this project by the following measurable conditions on one
release candidate. The numbers are proposals and may be adjusted deliberately, in this file.

| Criterion | Threshold |
|---|---|
| Whole-system simulation without violation | 1,000 CPU-hours, all reachability probes hit |
| Component oracles | 10,000 seeds each without failure |
| TLA+ model | checked for 3 and 5 nodes within the stated bounds |
| Trace validation | 1,000 simulator traces and 100 whole-system traces accepted; every mutation rejected |
| Pure kernels | every exhaustive check of §2.8 green in the default build |
| Protected invariants | checksum test green; every invariant has a passing negative test |
| External fault suite | 50 consecutive green runs per scenario |
| Soak | 7 days, 3 and 5 nodes, no violation, no unbounded growth |
| Compatibility | previous release readable; rolling upgrade and downgrade verified in simulation and with real processes |
| Performance | baselines recorded; overload behavior verified |
| CI | functional and simulation tests green on Windows, Linux, macOS |
| Runbook | every procedure executed once on a real cluster |
