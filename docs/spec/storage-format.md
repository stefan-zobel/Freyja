# Storage Formats

Binding for `lsm-core`, and for every other component that writes files in the WAL record
format (manifest, Raft log segments).

Files in an engine directory: `NNNNNN.wal`, `NNNNNN.sst`, `MANIFEST-NNNNNN`, `CURRENT`, `LOCK`,
`RESERVE`. All file numbers come from one monotonically increasing counter. A file keeps its
number for its whole life, including inside checkpoints (see `storage-engine.md`).

## 1. General Rules

- Every fixed-width integer is little-endian. The single exception is the internal-key trailer
  (§4), which is big-endian on purpose. Varints are LEB128.
- All checksums are CRC32C.
- **Context checksums:** every checksum is computed over the stored bytes and then extended
  with two values that are not stored: the file number (`uint64`) and the file offset at which
  the checksummed unit starts (`uint64`). A block or record that ends up in the wrong file or
  at the wrong offset therefore fails its checksum.

## 2. Format Versioning

Every persistent format and every wire format carries a version, and every reader checks it.

- **Files in the WAL record format:** the first logical record is a header record with the
  payload `Magic` (`uint32`) + `FileKind` (`uint8`) + `FormatVersion` (`uint16`).
  - `Magic` is `0x4A595246`, stored as the bytes `FRYJ`.
  - `FileKind`: `1 = WAL`, `2 = MANIFEST`, `3 = RAFT_LOG`. A file of another kind than the
    reader expects is damage (it is in the wrong place). An unknown kind is an unsupported
    format.
  - `FormatVersion` is `1`.
  - A file of length zero is a valid log without records. A writer that crashes before its
    header record is durable leaves such a file behind.
- **SSTables:** the format version is the low 16 bits of the footer magic number.
- **Other files:** begin with a magic number and a format version.
- **Network:** see `node.md`.
- A reader that sees an unknown magic number or a version newer than it supports fails with a
  clear error. It never guesses.
- Changing the meaning of any existing field requires a new version number.
- When a new format version is introduced, readers keep supporting the previous one. Writers
  switch to the new one only after the upgrade has been finalized (`operations.md`).

## 3. WAL Record Format

Used by `.wal` files, the manifest, and Raft log segments.

- The file is a sequence of fixed **32 KiB (32,768 byte)** blocks.
- A logical record is split into one or more fragments. Each fragment has a **7-byte header**:
  - `CRC32C` (`uint32`): covers the 3 header bytes that follow it plus the fragment payload,
    in on-disk order, plus the context (§1).
  - `Length` (`uint16`): fragment payload length (max `32761`).
  - `Type` (`uint8`): `0x01 = FULL`, `0x02 = FIRST`, `0x03 = MIDDLE`, `0x04 = LAST`,
    `0x05 = SYNC_POINT`. `0x00` is invalid.
- The context of a fragment checksum is the file number and the offset of the fragment
  header.
- **Padding:** if fewer than 7 bytes remain in a block, fill them with `0x00` and continue in
  the next block. If exactly 7 remain, write a zero-length `FIRST` fragment. A reader treats
  padding that is not zero as an invalid record.
- A logical record may be empty (one `FULL` fragment of length 0).

### 3.1 Sync Points

A `SYNC_POINT` record is never fragmented. Its payload is `SyncedLength` (`uint64`): every byte
of this file below that offset had been synced when the record was written. It occupies 15
bytes.

- After each completed sync, the writer emits one sync point together with the next data it
  writes. No extra sync is needed for it. Syncs happen only between logical records, so
  `SyncedLength` is always the end of a logical record.
- **Placement:** the sync point is written immediately before the next fragment of the next
  record, in the first block that still has 15 bytes left. If the current block has fewer
  left, the record's first fragment fills the block and the sync point follows at the start
  of the next block, between two fragments of that record. If the record ends in the current
  block, the sync point stays pending for the following record. A reader accepts a sync
  point between any two fragments; it does not interrupt a fragment sequence.
- A sync point whose checksum is valid is true, whether or not it was itself synced. A sync
  point whose `SyncedLength` is greater than its own offset is invalid.

### 3.2 Recovery

The reader replays records until the first invalid one (bad checksum, invalid type, length past
the end of the file, incomplete fragment sequence) at offset `P`. It then decides:

1. **Sealed file** (a successor of the same kind exists): sealed files were fully synced.
   The invalid record is **damage**.
2. **Last file:** let `T` be the truncation point, the end of the last complete logical
   record before `P` (a sync point that follows it outside a fragment sequence, and block
   padding, count as part of it). The reader searches the rest of the file, at every byte
   offset from `P` on, for a valid `SYNC_POINT` record.
   - A sync point with `SyncedLength > T` exists, or one read before `P` already had
     `SyncedLength > T`: truncating at `T` would remove synced bytes. This is **damage**.
     (In an intact file every `SyncedLength` is at most `T`, because syncs happen only
     between logical records.)
   - Otherwise it is a **torn tail**. The file is truncated at `T`. Everything after that
     was never acknowledged.

The reader only classifies. The owner of the file truncates a torn tail and makes the
truncation durable before it creates a successor, because the file is sealed from then on.

What happens on damage is decided by the owner of the file: the standalone engine fails to
open; the node starts a repair (`node.md`). Damage is never truncated away silently.

Residual risk, stated precisely: damage that hits only data of the final completed sync round,
with no later sync point in the file, is classified as a torn tail.

### 3.3 WAL Payload

`StartSequenceNumber` (`uint64`) + `EntryCount` (`uint32`) + entries of
`[OpType (1B: 0x01 = PUT, 0x02 = DELETE), KeyLen (uint32), Key, (ValueLen (uint32), Value if PUT)]`.
Entry `i` of a batch has sequence number `StartSequenceNumber + i`.

## 4. Internal Keys

**Internal key:** `UserKey` + 9-byte trailer. The trailer is `~SequenceNumber` as big-endian
`uint64`, followed by `ValueType` (`0x01 = VALUE`, `0x00 = TOMBSTONE`).

Sequence numbers are in the range `0` to `2^63 - 1`. User keys are not empty.

**Ordering:** compare the user keys as unsigned bytes first; only if they are equal, compare
the trailers as unsigned bytes. Newer versions of a key therefore sort first; for an equal
sequence number a tombstone sorts before a value. Never compare the concatenated bytes
directly: user keys have variable length, so that order is wrong.

To read key `K` at snapshot sequence `S`, seek to `(K, ~S, 0x00)` and take the first entry
whose user key equals `K`.

## 5. Sorted String Table (`.sst`)

**Block framing:** every block (data, filter, index) is followed by a 5-byte trailer:
`CompressionType` (`1B`, only `0x00 = none` is defined) + `CRC32C` (`uint32`) over the block
contents, the compression-type byte, and the context (§1). A `BlockHandle` is `offset`
(`uint64`) + `size` (`uint64`), where `size` excludes the trailer. It is always stored as
these 16 bytes.

A table holds at least one entry; empty tables are never written. A reader checks the
checksum of a unit first and only then interprets it: a block with a valid checksum and an
unknown compression type, or a footer with a valid checksum and an unknown format version, is
an unsupported format, not damage.

File layout, written sequentially:

1. **Data blocks (target 4 KiB, always at least one entry):**
   - A block is closed before an entry that would make it larger than 4 KiB. Only a block
     with a single larger entry exceeds the target.
   - Entries sorted by internal key, each encoded as `SharedKeyLen` (varint) +
     `UnsharedKeyLen` (varint) + `ValueLen` (varint) + key suffix + value.
   - Restart interval 16: every 16th entry stores its full key (`SharedKeyLen = 0`).
   - Block tail: `RestartOffsets` (`uint32[]`) + `NumRestarts` (`uint32`).
2. **Filter block:** one blocked Bloom filter over all user keys of the file.
   - 512-bit buckets, 10 bits per key, sized at file close from the collected key hashes.
   - Layout: bucket bytes + `NumBuckets` (`uint32`) + `NumProbes` (`uint8`).
   - `NumBuckets = max(1, ceil(keys * 10 / 512))`, where `keys` counts distinct user keys.
     `NumProbes` is 6. A reader uses the probe count stored in the block.
   - **Hash:** `h` = XXH64 of the user key with seed 0. The function lives in `kv-common`
     (`KeyHash`) and is part of the format; changing it requires a new format version.
   - **Bucket:** index `((h >>> 32) * NumBuckets) >>> 32`, computed in 64-bit unsigned
     arithmetic.
   - **Bits:** `x` = low 32 bits of `h`; `delta` = `x` rotated left by 15 bits. Probe `i`
     uses bit `x mod 512` of the bucket, then `x = x + delta` (mod 2^32).
   - Bit `b` of a bucket is bit `b mod 8` (least significant first) of its byte `b / 8`.
3. **Index block:** same entry format and restart interval as a data block. One entry per
   data block: key = last internal key of that block, value = its `BlockHandle`.
4. **Footer (exactly 48 bytes at the end of the file):**
   - `FilterBlockHandle` (16 bytes)
   - `IndexBlockHandle` (16 bytes)
   - `Padding` (4 bytes of `0x00`)
   - `FooterCRC32C` (`uint32`) over the 36 bytes before it, the magic number, and the context
   - `MagicNumber` (`uint64`: `0x4C534D4A32350001L`; the low 16 bits are the format version)

## 6. Manifest

- `MANIFEST-NNNNNN` is a log of version edits in the WAL record format (§3), including sync
  points and the recovery rule of §3.2.
- A version edit contains any of: files added (level, file number, file size, smallest and
  largest internal key, smallest and largest sequence number), files removed (level, file
  number), next file number, last sequence number, and the WAL number below which all WALs are
  obsolete. Fields are tagged, so unknown tags can be detected.
- **Encoding of an edit:** a sequence of fields, each starting with a tag (varint). All
  numbers are varints below 2^63; a key is a varint length followed by the bytes of an
  internal key.

  | Tag | Field | Content |
  |---|---|---|
  | 1 | next file number | number |
  | 2 | last sequence number | number |
  | 3 | WAL number below which all WALs are obsolete | number |
  | 4 | file added | level, file number, file size, smallest key, largest key, smallest sequence, largest sequence |
  | 5 | file removed | level, file number |

  - Tags 1 to 3 occur at most once per edit. A writer emits the fields in the order 1, 2, 3,
    5, 4; a reader accepts any order and applies removals before additions.
  - An unknown tag cannot be skipped and makes the manifest an unsupported format.
  - An edit that contradicts the state before it (a file added twice, a removed file that is
    not there, a level that does not exist, a counter that moves backward) is damage.
- An edit takes effect only after the manifest has been synced.
- The first edit of a manifest describes the complete state. A manifest is never appended to
  by a later run: every open writes a new manifest and then switches `CURRENT`.
- **`CURRENT`** names the active manifest. It is ASCII text of three lines, each ended by a
  line feed: the magic text and format version `FREYJA CURRENT 1`, the manifest name, and
  `crc32c=` followed by the CRC32C of the first two lines (line feeds included) as eight
  lowercase hexadecimal digits. It is replaced by the atomic-replace procedure
  (`storage-engine.md`). A valid checksum with another version is an unsupported format.
- A torn tail in the manifest holds only edits that never took effect. It is dropped when the
  manifest is replaced at the next open; files that are then unreferenced are removed by the
  orphan cleanup. Damage in the manifest is damage of the engine. So is a manifest named by
  `CURRENT` that is missing or holds no edit.
