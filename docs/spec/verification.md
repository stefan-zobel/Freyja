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
  asymmetric partitions, differing tick rates per node, process pauses, crashes and
  restarts, delayed and lost persistence (a crash discards everything not yet reported as
  persisted), log damage, membership changes, leadership transfers, snapshots.
  - A paused node processes nothing for a while and then everything that arrived in the
    meantime at once. Even on a reliable network this produces leader changes, leaders of
    terms the cluster has left behind, and truncated logs.
  - Writes become durable in order after a random delay; one completion may cover several.
  - A crash discards what was not durable. Of the writes on their way any prefix may have
    reached the disk without having been reported. The state machine of a restarted node
    either survived or is rebuilt from the log.
  - Compound faults make rare situations frequent enough to be reached: a disk that stops
    completing writes followed by a crash of that node (everything the node did in between
    was done on state that is lost), and a leader that freezes while everything sent to it
    is lost (it wakes up believing that it still leads).
  - There is one seed set for a reliable network (delays, pauses, flow control), one for
    the full fault model, one for the full fault model with membership changes, leadership
    transfers, and log damage, and one that adds snapshots to all of that.
  - Snapshots: every node writes a snapshot of its state machine after a number of applied
    entries that differs per node, and its log is cut. A leader streams the snapshot to a
    follower that needs entries the log no longer holds; the stream can fail. A restarted
    node whose state machine did not survive starts from its snapshot. The seed set must
    reach created and installed snapshots.
  - Membership changes: spare nodes join as learners, learners are promoted or removed,
    voters are removed, now and then the leader itself. A node that has left keeps running
    for a while (it tries to start elections) and is then shut down for good, but only when
    no other node has it in its configuration any more. Liveness is demanded of the members
    of the final configuration.
  - Leadership transfers are requested in the same seed set, also to targets that are down
    or cut off. The seed set must reach both changes of leader and aborted transfers.
  - Log damage in the same seed set: the stored log of a node becomes unreadable from some
    index on, also where the node has acknowledged or applied it. If the node crashes before
    the truncation has reached its disk, the damage is found again at the restart. Damage
    is injected only while a majority of the voters holds an intact log: the protocol
    repairs a minority, not a lost majority. The seed set must reach nodes that enter the
    log-recovering mode and nodes that leave it.
  - Mutations of the mode that the simulator catches: a recovering node that votes with
    the log it holds now or starts an election, one that leaves the mode at once, a leader
    that keeps the match index of a follower of a new epoch, an acknowledgment that outlives
    its entries. Leaving the mode on an append that does not name the present epoch is
    caught only by a unit test of the core; the simulator has not reached that interleaving.
  - Mutations of snapshots that the simulator catches: a commit index that moves during an
    installation, a snapshot installed behind the commit index, a state machine that goes
    back, entries accepted during an installation. Keeping a suffix that disagrees with
    the snapshot is caught only by a unit test.
  - Two liveness problems the simulator found are now rules of the protocol: a node whose
    state machine is ahead of its damaged log takes a snapshot for its log only, and a
    leader refuses a membership change that would make a majority depend on a node in
    log-recovering mode.
  - The settings of a run (cluster size, delays, flow-control limits, load) are drawn from
    its seed, so that the seed set covers many shapes of a cluster.
- **Safety invariants, checked after every step:**
  - Election safety: at most one leader per term.
  - Log matching: equal `(index, term)` implies identical prefixes.
  - Leader completeness: a committed entry is present in the log of every leader of a later
    term than the one in which it was committed.
  - State machine safety: no two nodes apply different entries at the same index.
  - No message that promises a state is sent before that state was reported persisted.
  - At most one uncommitted configuration change; any two quorums of consecutive
    configurations intersect. Examined for every `CONFIG` entry a node appends, against the
    configuration before it in the same log: the voters differ by at most one node, the
    majorities intersect, and, if the entry is new to the cluster, the entry that carries
    the configuration before it is committed.
- **Liveness, checked only after the network has been healed for a bounded time:** a leader
  exists and new proposals commit. Once the load stops, all nodes hold the same log, fully
  committed and applied, their state machines are equal, and every accepted proposal is
  either applied or known to be lost. A narrow flow-control window can leave a long backlog;
  the bound is therefore generous, but the backlog must be worked off.
- **The seed set must reach the interesting situations:** leader changes, log truncations,
  overload rejections, lost proposals, crashes, partitions, lost and duplicated messages,
  served reads. A run of the full set that reaches none of one kind fails.
- **The oracles are themselves tested by mutation:** a deliberately broken core (a promise
  sent before it is durable, a second vote in a term, an acknowledgment for entries a pending
  write no longer covers, a leader that counts its own entries before they are durable, a
  read answered without a confirmed leadership) must be caught by the simulator or by a unit
  test of the core. The mutations are applied by hand when the simulator or the core changes
  in a way that could blind an oracle; they are not part of the build.

### 2.5 Linearizability

- Clients issue concurrent writes, conditional writes, and reads, and retry through sessions.
  In `raft-sim` there are no sessions yet, so its clients do not retry: an operation without
  a response stays open.
- An operation without a response is treated as possibly taking effect at any later time.
- Operations that are known to have had no effect are left out of the history: rejected
  requests, proposals that never reached a log, and, once the cluster has converged,
  proposals whose place in the log was taken by another entry.
- The search grows with the number of operations that overlap and doubles with every
  operation that never returns. The checked client therefore waits for at most a few answers
  at a time and stops when it has given up on a few operations.
- Right after a change of leader, the checked client reads back every completed write from
  the leader of before. A node that was deposed without noticing must not answer from what
  it knows.
- The checker is Wing & Gong with memoization of visited states, applied per key. Histories
  are kept small enough per run for the search to finish; many seeds replace long histories.
- Additional check: no command is applied twice (exactly-once through sessions).

### 2.6 Protected Invariants

The safety invariants are the project's laws: a human states them, and no code change may
break them.

- The invariants of §2.4 live in one source file, `SafetyInvariants` in `raft-sim`. They
  are predicates over an `Observation`: the logs of all nodes, the history of elections and
  of applied entries, and what the latest step did. A predicate examines the latest step
  against the history and relies on having held after every earlier step. Log matching also
  exists in a form over the complete logs, called at restarts, at intervals, and at the end
  of a run. The
  invariants of §3 (state-machine equality, no acknowledged write lost, exactly-once) live in
  one source file, `SystemInvariants`, next to the whole-system simulation in `kv-node`.
- Each invariant is a named, side-effect-free predicate over the observed state of all nodes.
  The simulators call every predicate after every step.
- These two files are **protected**. They are changed only on the explicit instruction of the
  project owner, never as part of making a failing test pass. Weakening or removing an
  invariant is never an acceptable fix.
- `docs/spec/invariants.sha256` records the checksum of both files. A test compares them, so
  every change to an invariant needs a second, deliberate edit and is visible on its own.
  The checksum is taken over the file with LF line ends, so that it is the same on every
  platform.
- Every invariant has a negative test: a deliberately broken history that the predicate must
  reject. An invariant that cannot fail proves nothing.

### 2.7 Trace Validation Against the TLA+ Model

Model checking proves the TLA+ specification, not the Java code. Trace validation connects
the two.

- `raft-sim` can record a run as a trace: one line per `step` of every node, in a line-based
  JSON format written by a small hand-written encoder. Each line holds the node id, the event,
  and the node's resulting protocol state: term, role, vote, last log index and term, commit
  index, configuration, log-recovering flag, and the messages released.
  - A line also lists what the step handed out to be made durable (hard state, truncation,
    entries without payloads), because the model distinguishes durable from volatile state.
  - A restart of a node after a crash is a line of its own: it changes the state of the node
    without a step.
  - Recording does not change a run, and the trace is a function of the seed.
- `raft-core` stays pure. It exposes a read-only view of its protocol state; the simulator
  writes the trace.
- A trace specification in `docs/tla/` constrains the model to the recorded sequence. TLC
  checks that the trace is a behavior the specification allows. A trace that the
  specification does not allow is a bug in the code or in the specification, and one of them
  is fixed.
  - TLC reads the run as a generated module (`TraceData`, a sequence of records), written
    from the same steps as the JSON lines. This needs no library on either side.
  - A step is accepted if an action that belongs to its event leads to the recorded state
    of the node, a received message was released before, and every released message is one
    the node may send in its new state. For a promise that means: the durable state backs it.
  - The trace shows the last index and term of a log, not its content. The model follows the
    content through the actions.
- **Mapping:** every `step` corresponds to one action of the specification or to a fixed,
  documented sequence of actions. The mapping is part of `docs/tla/`. An event without a
  mapping fails validation.
- **Not vacuous:** the validation must reject mutated traces (a second leader in a term, an
  acknowledgment before persistence, a vote before persistence, a changed committed entry, a
  commit without a majority). These mutations are tests.
- **The model itself is checked for not being vacuous:**
  - Within the bounds of `Raft.cfg` (exhaustive: three nodes, 4 messages) TLC must be able
    to reach a commit and a second election.
  - TLC must refute variants of the model with a deliberate error. Refuted by a
    breadth-first search with 6 messages, each within two minutes: an acknowledgment backed
    by volatile state, a vote backed by volatile state, a leader that counts its own entries
    before they are durable, a second vote in a term.
  - Not refuted by that search within nine minutes, because the shortest counterexample
    needs more steps: a commit of an entry of an earlier term without one of the current
    term, and a vote granted without comparing logs. These two are covered by unit tests of
    the core and by the simulator.
  - The variants are produced by hand when the model changes.
- **State of the model for membership change, transfer, and log-recovering mode:** see
  `docs/tla/README.md`. The exhaustive check covers three voters without these features; the
  configuration with them has only been searched for a limited time, and the traces of runs
  with them have not been validated yet. Both are open items of milestone B3.
- TLC is an external development tool, not a dependency. Trace validation runs in its own
  profile (`-Ptrace-validation`), not in the default `./mvnw test`, on a fixed set of seeds
  plus the failing-seed corpus. The profile expects `tla2tools.jar` in `tools/` of the
  repository directory, or its path in the system property `freyja.tlc`.
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

### 2.9 Node Components

Fast tests of the parts of a node, with complete nodes on in-memory file systems and a
network that the test steps. They come before the whole-system simulation and do not
replace it.

- **State machine replay:** a random log (sessions, repeated commands, conditional batches,
  alarms, other entries) is applied once without any fault; that gives the expected results
  and the expected digest of the state. The same log is then applied in batches of random
  size with crashes in between, each time continuing at the applied index the engine still
  has. Every result and the final digest must be the same.
- **Replicated store:** three nodes with random crashes. Every node gives the same result
  for every entry, every time it applies it, and all end with the same digest.
- **Snapshots:** logs are shortened and the cluster restarts from what is left; a node that
  is too far behind and a node that joins receive the state machine; a transfer with lost
  and repeated messages completes without starting again; a transfer without answers is
  given up on both sides and started again; a snapshot that arrives damaged in a block that
  opening does not read is discarded.
- **Interrupted installation:** the installation of a snapshot is failed at its first file
  operation, then at its second, and so on until one runs through. After each failure the
  node crashes and starts again; it must complete the installation, leave neither the
  intent file nor a second generation behind, and converge with the others.
- **Repair:** tables of a stopped node are damaged so that the engine does not open; with a
  log that reaches back to its first entry the node applies it again, with a shortened log it
  enters log-recovering mode with the lost log noted in its hard state and gets the state
  machine from the leader. Damage in a block that only a scrub reads stops the running node
  and is repaired at its next start. The repair is failed at every file operation in turn,
  as the installation is.
- **Limits:** a state machine over its quota and a follower short of disk space set the
  alarm for the whole cluster, puts are rejected and deletes applied, and the alarm comes
  back when it is cleared too early; a proposal past its deadline is refused or dropped; a
  full queue rejects at once and the cluster recovers from the burst.
- Mutations that these tests catch: no flush before the log is shortened; `SNAPSHOT` written
  after the log is shortened; an interrupted installation not completed; the log not reset
  by a completed installation; the intent not written; a received snapshot not scrubbed; a
  repeated chunk appended again; a request that is never repeated.

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

**How it is built.** The simulation is a queue of events ordered by simulated time: ticks
of nodes, deliveries of messages, turns of clients, faults, restarts. One event is one step;
after every step the invariants of `SystemInvariants` are checked.

- **Clients** talk to a node as the layer that receives requests will: they hand it
  proposals with a deadline and requests for a read index, and take the answers from what
  the node reports. A client has a session and one operation at a time. It sends a command
  again, with the same sequence number, until it is answered, and gives up after waiting in
  vain many times. An answer that a node refuses at once (not the leader, queue full) does
  not count as waiting in vain.
- **Operations:** write, compare-and-set (a conditional batch), and linearizable read, on a
  few keys with a few values, so that conditions hold and fail. The value 0 stands for a
  key that is not there: writing it is a delete.
- **History:** every answered operation enters the history with its invocation and response
  time. An operation with an unknown outcome (given up, or the session expired after the
  client had waited in vain) stays open: it may have taken effect or not, now or later. An
  operation that no node ever took is left out. Clients stop when too many operations are
  open, because the checker pays for each.
- **Late duplicates:** a copy of an answered command arrives again much later, when its
  client has moved on. Nobody waits for that answer.
- **Observation:** every applied entry of every node (index, term, hash of the command,
  session and sequence number, result), the digest of the state machine after every batch,
  and every answer given to a client.
- **End of a run:** faults stop, every node is started, clients finish what they have in
  hand. The run must converge within a bound: every node running and out of log-recovering
  mode, all at the same applied index with the same digest, nothing acknowledged beyond it,
  no file handle left open, and the history linearizable.

**The invariants** (`SystemInvariants`):

- *State-machine equality:* whatever node applies the entry with a given index applies the
  same entry and gets the same result; two state machines that have applied everything up to
  the same index have the same digest.
- *No acknowledged write lost:* an answer names an entry that was applied, with the result
  the client was told; no node ever applies another entry at that index.
- *Exactly-once:* along the log, the applications of one command of a session give first
  any number of `NO_SPACE`, then at most one result of taking effect, after it only that
  result again or `SEQUENCE_TOO_OLD`; once the session is gone, only `NO_SUCH_SESSION`. A
  client is told the result of the one time the command took effect.

**State of the work.** Injected so far: lost, delayed, and repeated messages; partitions;
crashes between operations and in the middle of a file operation; restarts. Still to come
in this milestone: damage to stored data and the repairs it triggers, full and slow disks,
membership changes, leadership transfers, lost identity, the invariants of §2.4 over
complete nodes, the probes of §3.1, and traces for the model.

Mutations that the simulation catches: a repeated command applied again; an older command
of a session applied; the log shortened without a flush before; the applied index not
stored with its batch; a delete of a batch not written.

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
