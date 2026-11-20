# Freyja: Educational replicated LSM-Tree Key-Value Store in Java

Freyja is a linearizable key-value store written in pure Java 25, with no runtime dependencies
besides the JDK. Every node keeps a complete copy of the data: one Raft group replicates all
writes, as in etcd. Each node stores its data in its own LSM-tree engine with a write-ahead log,
SSTables, Bloom filters, and leveled compaction.

Clients get puts, deletes, reads, scans, and atomic and conditional batches, with exactly-once
semantics through sessions. Operators get membership changes, snapshots, replica repair, backup
and restore, TLS, metrics, and a command-line admin tool. The whole node is tested in a
deterministic simulation with injected faults.

Status: version 0.1.0, not released yet. Build and test with `./mvnw test` (JDK 25 with
`--enable-preview`). The specification is in `docs/spec/`, the operator procedures in
`docs/runbook.md`.
