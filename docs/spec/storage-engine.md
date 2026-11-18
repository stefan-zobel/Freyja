# Storage Engine (`lsm-core`)

Behavior rules for the LSM engine. On-disk layouts are in `storage-format.md`.

## 1. File-System Abstraction

Every file operation goes through `StorageFileSystem` (create, open, positional read, append,
sync, truncate, rename, hard link, delete, remove an empty directory, list, directory sync,
free space). Files are
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
  - It counts every operation that changes something. A fault source can make any of them
    fail with an I/O error.
  - A crash can be scheduled at any counted operation. The operation then happens not at
    all, in part (an append), or completely; the wrapped file system crashes; and from then
    on every call fails, so that code which keeps running cannot change what the crash left.
- The in-memory crash has two forms. The plain one drops everything that is not durable.
  The randomized one follows the storage fault model: of the unsynced name changes of a
  directory a prefix survives, and of the unsynced bytes of a file a prefix of any length
  survives, in which any stretch may be zeroed or garbled.
- An append that runs out of space fails with a dedicated error (`NoSpaceException`); the
  part that still fit has been written.

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
   - Every path that reads a table reports: point reads, scans, compaction, scrubbing.
   - A table that is marked damaged is never chosen for compaction again, because the
     compaction would fail every time. Reads of its intact blocks keep working.
   - The mark lasts until the engine is closed. Nothing is repaired locally.
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
   - Writes rejected for lack of space fail with `DiskFullException`. The engine is intact.
   - **A group that does not fit into the log:** the log is closed and cut back to where
     the group began, so that no record of it can ever be replayed. Then a new log is
     started (a log is never appended to after a failed write), and the MemTable of the old
     log is frozen with it. If not even the new log can be created, the engine stops.
   - **A flush or compaction that runs out of space** removes its unfinished output, gives
     up `RESERVE`, and tries once more. If it fails again, it fails with
     `DiskFullException` and leaves the engine as it was; the data stays in its log.
   - **A manifest edit that runs out of space:** `RESERVE` is given up and the manifest is
     replaced by a new one that already contains the edit.
   - `RESERVE` is created again after a flush or compaction when there is room for it and
     for `minFreeSpace`. A `RESERVE` of the wrong size (left by a crash) is recreated at open.
5. **Damage listener:** a callback through which the engine reports damaged files. The
   standalone default logs it. The node uses it to start a replica repair (`node.md`).

### 3.1 Scrubbing

A low-priority background task reads every block of every live SSTable and verifies its
checksum. A full pass completes within `scrubPeriod` (default 7 days). Findings go to the
damage listener and to the metrics.

- A pass verifies, for every table: the footer, the index block, the filter block, the
  checksum and structure of every data block, the ascending order of all keys, and that the
  index names the last key of every block. It reads from the file, never from the cache.
- The background thread starts a pass when the period has elapsed since the last one.
  `scrub()` runs a pass on request, also in deterministic mode. A period of 0 disables the
  periodic pass.
- A pass continues after a finding and reports how many tables were damaged.

### 3.2 I/O Rate Limiting

Compaction and scrubbing draw from a shared token bucket (bytes per second, configurable).
Flushes are not limited. The limit is raised automatically while writes are slowed down or
stalled by backpressure (§7), so compaction can catch up.

- `compactionBytesPerSecond` sets the rate; the default 0 means no limit.
- A caller announces the bytes it is about to write and waits until the bytes announced
  before are paid for at the rate. Idle time is not saved up.
- While at least one write is delayed or stalled, nobody waits, and no debt builds up.
- Deterministic mode has no limit, because nothing there may wait for time to pass.

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
- When the table-metadata budget is exceeded, the tables that were used least recently
  drop their index and filter blocks. A table loads them again, and verifies their checksums
  again, when it is next used.
  - The file handle of a table stays open; only the two blocks are dropped.
  - The table that has just loaded its blocks is never asked to drop them, so a budget
    smaller than the blocks of one table still lets every read finish.
  - An iterator keeps the index block it started with.

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
  - The read state (MemTable, waiting MemTables, version) is one immutable object that is
    replaced as a whole. It holds a reference to the reader of each of its table files.
  - A read pins the state it starts with and unpins it when it ends; a scan keeps its pin
    until it is closed.
  - A read without a snapshot takes its sequence number after it has pinned its state, never
    before. No snapshot protects that number: between the two steps a compaction could drop
    the version the number sees, because a newer one shadows it, and publish a state that
    holds only the newer one. The read would find an older version in a deeper level, or
    nothing. A pinned state keeps the newest version of every key it held when it was
    pinned, and all of those are at or below a number taken afterward.
- Flush, compaction, and scrubbing run on background threads in normal mode. In
  **deterministic mode** there are no background threads; these tasks run only when the caller
  invokes them. All randomized tests and both simulations use deterministic mode.
  - Normal mode has one background thread. It flushes waiting MemTables first, oldest first,
    and compacts when nothing waits.
  - In deterministic mode a flush runs inside the write that fills the MemTable. Compaction
    runs when the caller invokes `compact()` or `runBackgroundWork()`, and inside a write
    that would otherwise stall (§7).
- At most one flush and one compaction run at a time. The manifest and the read state are
  changed under one lock, by whichever of them finishes.
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

A flush moves one MemTable into a table file in level 0. It has two parts.

**Freeze**, done by the writer that finds the MemTable full:

1. Sync and close the log of the MemTable. Create the next log and sync the directory.
2. Replace the MemTable by an empty one and put the full one, together with its log number
   and its last sequence number, at the end of the list of waiting MemTables. Reads look at
   all of them.

**Write out**, done by the background thread (or inline in deterministic mode), always for
the oldest waiting MemTable:

3. Write the table file, sync it, sync the directory.
4. Append one manifest edit (file added, next file number, the MemTable's last sequence
   number, the log of the next waiting MemTable or else of the active one as the oldest live
   log) and sync the manifest.
5. Publish the new version without that MemTable. Delete its log.

Level-0 files are created in the order of their MemTables, so a larger file number means
newer data.

A crash before the manifest sync leaves the table file as an orphan and the logs in place;
they are replayed at the next open. A crash after it leaves at most an obsolete log.

`flush()` writes out everything that is in memory and returns when that is done. `close`
does not flush: MemTables that still wait stay in their logs.

The manifest is replaced by a new one that holds only the current state when it exceeds
`maxManifestSize` (default 64 MiB), by the same steps as at open. If that fails, it is
unknown which manifest `CURRENT` names, and the engine stops.

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
- Slowing down means one delay of 1 ms per write group.
- The deadline is `writeStallTimeoutMillis` after the write began to wait (default 60 s).
  The error is `WriteStalledException`; the write stored nothing, and the engine is intact.
- A write that waits wakes up when a flush or a compaction has finished. A compaction that
  fails is not progress: writes that wait for it wait until their deadline.
- In deterministic mode nothing waits. A write that would stall on waiting MemTables flushes
  the oldest one itself. A write that would stall on level 0 runs one compaction itself and,
  if nothing can be compacted, goes on.
- The stall limit for level 0 must not be below the compaction trigger.

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
- **Choice:** the level whose score is highest and at least 1. The score of L0 is its file
  count divided by the trigger; the score of a deeper level is its size divided by its
  target (64 MiB for L1 by default). The last level is never a source.
  - From L0 all files are taken, because they overlap each other. From a deeper level the
    oldest file is taken. In both cases every file of the next level whose key range
    intersects is taken too.
- **Output:** files in the next level. A new file is started when the current one has
  reached `targetFileSize` (default 8 MiB), and only between two user keys, so that no user
  key occurs in two files of a level. A compaction that keeps nothing writes no file.
- **Retention in detail,** for one user key, from the newest version to the oldest:
  - a version is kept if it is the newest, or if a live snapshot lies at or after it and
    before the next newer version;
  - a tombstone that would be kept is dropped if no live snapshot is older than it and no
    level below the output level has a file whose key range includes the key.
  The snapshots are those that are live when the compaction starts. A snapshot taken later
  is newer than every input and sees the newest version, which is kept.
- **File lifetime:** the inputs leave the version with the manifest edit. Each input file is
  closed and deleted when the last read state and the last iterator that refer to it are
  gone; until then readers that started earlier keep using it.
- A failure before the manifest edit removes the output files and leaves the engine as it
  was. A failure of the manifest edit itself stops the engine.

## 9. Snapshots and Checkpoints

- **Snapshot:** an in-process read view at a sequence number. Long-lived snapshots block
  space reclamation; the age of the oldest live snapshot is exposed as a metric.
  - A snapshot is the last published sequence number at its creation, so it never shows part
    of a batch. The engine keeps a registry of live snapshots; its smallest sequence number
    is the bound below which compaction may drop shadowed versions.
  - A snapshot ends when it is closed or when the engine is closed. It does not survive a
    reopen.
- **Scan:** an iterator over a key range `[start, end)` at one sequence number, the current
  one or a snapshot's. Either bound may be absent. A scan is opened forward (ascending keys)
  or backward (descending keys, the same range) and keeps its direction.
  - It merges the MemTable, a MemTable that is being flushed, and every table whose key
    range intersects the scan range, in internal-key order.
  - Per user key it returns the newest value at or below its sequence number and skips keys
    whose newest such entry is a tombstone.
  - Backward, the versions of a user key come oldest first; the scan passes all of them and
    takes the last one at or below its sequence number. The keys of a block are stored as
    differences to their predecessors, so a step back within a block starts again at the
    restart point before the entry.
  - A scan keeps the sources it started with. Later writes and flushes do not change what it
    returns.
  - A scan holds table readers and one pinned block per table. It must be closed, and closed
    before the engine.
- **Checkpoint:** a consistent on-disk copy of the engine in another directory on the same
  file system. Procedure: flush the MemTable, pin the current version, hard-link every live
  SSTable into the target directory under its original name, write a fresh manifest and
  `CURRENT` there, sync the directory. A checkpoint is a complete engine directory and can be
  opened on its own. Checkpoints are the basis for Raft snapshots and backups.
  - The target directory must not exist. It holds table files, one manifest, and `CURRENT`;
    no log and no `LOCK`.
  - The copy holds every write that returned before the call, and possibly later ones; it
    is always a prefix of the write history.
  - The pin keeps the linked files in place while the copy is made. Afterwards the source
    may delete its names; the copy keeps the data.
  - A failed checkpoint may leave a directory without `CURRENT`. It cannot be opened and is
    to be removed.

**Errors in the middle of a change.** An `Error` that ends a write or a flush (an
allocation that fails, a simulated crash in a test) puts the engine into the failed state
like any other failure, and the request gives its turn in the write queue back before the
error goes on. Nobody waits forever behind a request that will never finish: later writes
are rejected, and `close` returns.

## 10. Observability Hooks

The engine exposes counters and gauges through a small metrics interface from `kv-common`
(no dependency on any metrics library): bytes written and read, flush and compaction counts
and durations, level sizes and file counts, stall time, cache hits and misses, sync latency
histogram, scrub progress, damage count, oldest snapshot age, free space.

The interface has three calls: add to a counter, set a gauge, record one duration.

| Kind | Names |
|---|---|
| Counters | `write.groups`, `write.batches`, `write.bytes`, `write.stall.timeouts`, `write.rejected.disk.full`, `flush.count`, `flush.bytes`, `compaction.count`, `compaction.bytes`, `scrub.passes`, `scrub.bytes`, `damage.files`, `reserve.released`, `manifest.replaced` |
| Durations | `wal.sync`, `write.stall`, `flush.time`, `compaction.time` |
| Gauges | `level.N.files`, `level.N.bytes`, `memtables.waiting`, `memtable.bytes`, `cache.hits`, `cache.misses`, `cache.evictions`, `table.metadata.bytes`, `snapshots.live`, `snapshots.oldest.age.millis`, `damage.files.current`, `disk.free.bytes` |

Gauges are published after every flush, compaction, and scrub, and when the owner asks for
them before it exports its metrics.
