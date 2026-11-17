# Storage Engine (`lsm-core`)

Behavior rules for the LSM engine. On-disk layouts are in `storage-format.md`.

## 1. File-System Abstraction

Every file operation goes through `StorageFileSystem` (create, open, positional read, append,
sync, truncate, rename, hard link, delete, list, directory sync, free space). Files are
append-only: an existing file is never reopened for writing. `truncate` cuts a closed file
and makes its remaining contents and its length durable. It exists from the first
milestone with three implementations:

- **Real:** `FileChannel`-based.
- **In-memory:** deterministic, used by most tests and by the simulations. It models the
  difference between written and synced data:
  - File contents are durable up to the length at the last sync of the file.
  - The names in a directory (create, rename, link, delete) are durable as of the last sync
    of that directory.
  - `crash()` replaces the current state by the durable state and invalidates every open
    handle.
  - It enforces platform rule 2 on every platform: a name through which a file is open can
    be neither deleted, nor renamed, nor replaced by a rename, nor truncated. A hard link to
    an open file is allowed.
  - An optional capacity makes appends fail when the file system is full.
- **Faulty:** wraps another implementation and injects the faults in `verification.md`.

SSTables are read with positional reads into the block cache. Memory-mapped files are not used:
they cannot be fault-injected, bypass the memory budget, and block file deletion on Windows.

The interface is shared with `kv-node` and must stay stable across releases, because the
multi-version simulation loads two releases against one implementation of it.

## 2. Platform Rules

1. **Paths:** always `java.nio.file.Path`. Never hardcode `/` or `\`.
2. **File lifetime:** a file is deleted or replaced only after every handle to it is closed.
   `SSTableReader` uses atomic reference counting (`retain()` / `release()`); iterators,
   compactions, checkpoints in progress, and pinned versions each hold a reference. An
   obsolete SSTable is deleted when its count reaches zero.
3. **Atomic replace:** write to a temporary file, sync it, close it, then
   `Files.move(tmp, target, ATOMIC_MOVE, REPLACE_EXISTING)`, then sync the directory.
4. **Directory sync:** required after create, rename, hard link, and delete on platforms that
   support it. It is a no-op on Windows.
5. **Delete retries:** deletion retries a bounded number of times with a short pause, because
   virus scanners and indexers hold handles briefly on Windows. A file that still cannot be
   deleted is left for the orphan cleanup.
6. **Orphan cleanup:** on open, delete every `.sst`, `.wal`, and temporary file that the
   manifest does not reference.
7. **Single owner:** a `LOCK` file with a file lock prevents two engine instances from opening
   the same directory, in one process or in two. The lock is an operation of
   `StorageFileSystem`.

**Windows behavior is to be measured, not assumed.** The first task of milestone A1 is a small
test that records what actually happens on Windows for: deleting a file with an open
`FileChannel`, renaming over a file with an open channel, reusing a file name while an old
handle is still open, and creating a hard link to an open file. Record the results here.
Rule 2 holds regardless.

Measured by `PlatformFileBehaviorTest` (it prints its findings on every run and on every
platform). Windows 10 Pro 10.0.19045, NTFS, JDK 25.0.5, files opened with `FileChannel`:

| Operation while a read channel to the file is open | Result |
|---|---|
| Delete the file | succeeds; the name disappears at once; the open channel still reads the data |
| Create a new file under the deleted name | succeeds |
| Rename another file over it (`ATOMIC_MOVE`, `REPLACE_EXISTING`) | fails with `AccessDeniedException`; the target is unchanged |
| Rename the open file itself | succeeds; the open channel still reads the data |
| Create a hard link to it | succeeds |
| Delete that hard link | succeeds |

Consequence: on this system only the replace-by-rename needs rule 2. The rule stays
unconditional, because older Windows versions and other file systems refuse the delete as
well.

### 2.1 Durability per Platform

| Platform | Expected system call behind a file sync | Directory sync |
|---|---|---|
| Linux | `fdatasync` / `fsync` | `fsync` on the directory |
| macOS | `fcntl(F_FULLFSYNC)` | `fsync` on the directory |
| Windows | `FlushFileBuffers` | not available; no-op |

- Confirm the first column against the source of the JDK in use and record the JDK version here.
- Durability additionally requires that the drive honors flush commands. Volatile write
  caches that ignore flushes are outside the fault model; the operations documentation says so.

## 3. Fault Handling

The engine never returns data it cannot verify and never hides a fault.

1. **Sync failure is fatal.** The engine instance enters a failed state and rejects all
   further writes. It never retries the sync.
2. **Checksum failure on read:** the read fails with a corruption error that names the file
   and offset. The file is marked damaged and reported through the damage listener.
3. **Damage at open** (WAL, manifest, footer, index, or filter block): opening fails with a
   corruption error. An explicit salvage operation in the offline tools can open such a
   directory with data loss; the engine never does that on its own.
4. **Disk full:**
   - `minFreeSpace` (default 1 GiB): below it, the engine rejects user writes with a
     dedicated error and stays readable. Flushes and compactions continue, because they are
     the only way to free space.
   - `RESERVE` is a preallocated file (default 256 MiB). If a flush, compaction, or manifest
     write fails for lack of space, the engine deletes it to finish that operation and
     recreates it once space is available.
   - A failed append for lack of space fails only that write. A failed sync stays fatal.
   - Until disk-full handling is built (milestone A6), every failed log append is treated
     like a failed sync: a partly written record leaves the log in an unknown state.
5. **Damage listener:** a callback through which the engine reports damaged files. The
   standalone default logs it. The node uses it to start a replica repair (`node.md`).

### 3.1 Scrubbing

A low-priority background task reads every block of every live SSTable and verifies its
checksum. A full pass completes within `scrubPeriod` (default 7 days). Findings go to the
damage listener and to the metrics.

### 3.2 I/O Rate Limiting

Compaction and scrubbing draw from a shared token bucket (bytes per second, configurable).
Flushes are not limited. The limit is raised automatically while writes are slowed down or
stalled by backpressure (§7), so compaction can catch up.

## 4. Memory Budget

All limits are fields of `EngineOptions` and are enforced, not advisory.

| Budget | Default |
|---|---|
| Active MemTable size | 32 MiB |
| Immutable MemTables waiting for flush | at most 2 |
| Block cache capacity | 256 MiB |
| Table metadata (filters and index blocks of open SSTables) | 128 MiB |
| Maximum key size | 64 KiB (keys are non-empty) |
| Maximum value size | 1 MiB |
| Maximum write batch size | 4 MiB |

- The MemTable is an on-heap concurrent skip list with approximate byte accounting.
- The block cache stores blocks in off-heap `MemorySegment`s carved from a pool that is
  allocated once at open and freed at close. Do not create an `Arena` per block. Cached blocks
  are pinned while in use and evicted only when unpinned.
  - The pool is divided into slabs of 64 KiB. A slab serves one size class (4, 8, 16, 32, or
    64 KiB) and is cut into slots of that size; a block takes one slot of the smallest class
    that holds it. Blocks larger than 64 KiB are not cached.
  - Eviction is least recently used within a size class. If every slot of a class is pinned
    and no free slab is left, the block is not cached and the reader uses its own copy.
  - Known limitation: a slab stays with its size class, so memory does not move between
    classes when the mix of block sizes changes.
  - Only blocks whose checksum has been verified enter the cache. The key is file number and
    block offset.
  - An iterator pins the one data block it is positioned in.
- When the table-metadata budget is exceeded, the least recently used SSTable readers are
  closed and reopened on demand. (Not built yet: every live table keeps its reader open; see
  `roadmap.md`, A5.)

## 5. Concurrency Model

- One writer at a time appends to the WAL and the MemTable. Concurrent writers queue up and
  are committed as a group with a single sync.
  - The writer at the head of the queue commits for the group: one log record per batch,
    then one sync, then the MemTable inserts, then the new last sequence number is published.
  - The group syncs if its first writer asked for a sync. A writer that asked for a sync
    never joins a group that does not sync.
  - A group takes at most `maxGroupCommitSize` bytes of batches (default 8 MiB).
- A read uses the published last sequence number as its snapshot, so it never sees part of a
  batch.
- Readers never block writers. A read pins the current version (the set of live SSTables plus
  MemTables) for its duration.
- Flush, compaction, and scrubbing run on background threads in normal mode. In
  **deterministic mode** there are no background threads; these tasks run only when the caller
  invokes them. All randomized tests and both simulations use deterministic mode.
- The engine takes its time source through an interface, never from the system clock directly.

## 6. Write Path

- **One WAL per MemTable.** A WAL is synced and closed (sealed) before its successor is
  created. It is deleted after its MemTable has been flushed and the manifest records that.
- **Sync policy:** by default a write is acknowledged only after the WAL has been synced.
  `sync = false` is available per write and means the write can be lost in a crash. After a
  crash the engine always recovers a prefix of the write history, never a state with gaps.
- Recovery follows `storage-format.md` §3.2. The logs are replayed in file-number order; all
  but the newest are sealed. The batches must continue the sequence numbers without a gap;
  a gap is damage.
- After replaying the newest log, the engine truncates it at its valid length (which also
  makes its contents durable) and only then creates a new log. It never appends to a log of
  an earlier run.
- The directory is synced after a log is created and before any write to it is acknowledged.
- `close` syncs the current log.

### 6.1 Flush

A flush moves one MemTable into a table file in level 0. Steps, in this order:

1. Sync and close the log of the MemTable. Create the next log and sync the directory.
2. Replace the MemTable by an empty one. Reads look at both until step 5.
3. Write the table file, sync it, sync the directory.
4. Append one manifest edit (file added, next file number, last sequence number, the new
   log as the oldest live log) and sync the manifest.
5. Publish the new version. Delete the sealed log.

A crash before the manifest sync leaves the table file as an orphan and both logs in place;
they are replayed at the next open. A crash after it leaves at most an obsolete log.

Until background threads are built (milestone A5), the writer whose turn it is flushes inline
when the MemTable has reached its size, and `flush()` does the same on request.

### 6.2 Open

1. Take the `LOCK`.
2. Without `CURRENT`: the directory must hold no log and no table file; the engine starts
   empty. Otherwise read `CURRENT` and replay the manifest it names.
3. Open every table file the manifest lists and compare its size with the manifest.
4. Replay the logs that are not obsolete, in order (see above).
5. If the logs held entries, write them to a table file in level 0.
6. Write a new manifest whose first edit describes the complete state, sync it, sync the
   directory, switch `CURRENT`.
7. Create the new log and sync the directory. Only now delete what is obsolete: the old
   manifest, all older logs, table files the manifest does not list, temporary files.

File numbers continue above the largest number found in the directory, so a leftover file
never collides with a new one. A crash at any point before `CURRENT` is switched leaves the
directory as the old manifest describes it.

## 7. Write Backpressure

- Slow down writes when L0 has 8 or more files.
- Stall writes when L0 has 12 or more files, or when the immutable-MemTable limit is reached.
- A stalled write waits at most until its deadline, then fails with a dedicated error.
- Thresholds are configurable; tests use much smaller values.

## 8. Levels and Compaction

- L0 files may overlap each other. Files in L1 and deeper never overlap within a level.
- Triggers: L0 file count (default 4); for deeper levels, total level size against a target
  that grows by a factor of 10 per level. The number of levels is configurable.
- **Retention:** for each user key, a compaction keeps the newest version, plus every version
  that is the newest one visible to some live snapshot. Everything else is dropped.
- **Tombstones** are dropped only when no deeper level can contain the key and no live
  snapshot needs them.
- The output files become visible through a single manifest edit. A crash before that edit
  leaves only orphans.
- A compaction verifies the checksum of every block it reads. A damaged input aborts the
  compaction and is reported; damage is never copied into new files.

## 9. Snapshots and Checkpoints

- **Snapshot:** an in-process read view at a sequence number. Long-lived snapshots block
  space reclamation; the age of the oldest live snapshot is exposed as a metric.
  - A snapshot is the last published sequence number at its creation, so it never shows part
    of a batch. The engine keeps a registry of live snapshots; its smallest sequence number
    is the bound below which compaction may drop shadowed versions.
  - A snapshot ends when it is closed or when the engine is closed. It does not survive a
    reopen.
- **Scan:** a forward iterator over a key range `[start, end)` at one sequence number, the
  current one or a snapshot's. Either bound may be absent.
  - It merges the MemTable, a MemTable that is being flushed, and every table whose key
    range intersects the scan range, in internal-key order.
  - Per user key it returns the newest value at or below its sequence number and skips keys
    whose newest such entry is a tombstone.
  - A scan keeps the sources it started with. Later writes and flushes do not change what it
    returns.
  - A scan holds table readers and one pinned block per table. It must be closed, and closed
    before the engine.
- **Checkpoint:** a consistent on-disk copy of the engine in another directory on the same
  file system. Procedure: flush the MemTable, pin the current version, hard-link every live
  SSTable into the target directory under its original name, write a fresh manifest and
  `CURRENT` there, sync the directory. A checkpoint is a complete engine directory and can be
  opened on its own. Checkpoints are the basis for Raft snapshots and backups.

## 10. Observability Hooks

The engine exposes counters and gauges through a small metrics interface from `kv-common`
(no dependency on any metrics library): bytes written and read, flush and compaction counts
and durations, level sizes and file counts, stall time, cache hits and misses, sync latency
histogram, scrub progress, damage count, oldest snapshot age, free space.
