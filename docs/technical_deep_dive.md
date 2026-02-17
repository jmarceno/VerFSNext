# VerFSNext Technical Deep Dive

## Current State

This repository now contains a Phase 2 implementation focused on full async-fusex operation coverage and POSIX/rsync operational correctness:

- FUSE runtime wired through `vendor/async-fusex`
- Metadata runtime wired through `vendor/surrealkv`
- Mount lifecycle + SIGINT graceful shutdown
- Baseline metadata schema and storage
- Baseline hash-addressed pack storage backend
- Write batcher (`>= 3000` blocks or `500ms`)
- Background sync service + shutdown full-sync barrier
- Full `VirtualFs` operation implementation, including symlink/xattr/link/access/locking/bmap paths
- rsync-oriented rename/replace/truncate correctness hardening

## Runtime Layout

- `src/main.rs`
  - Loads `config.toml`
  - Initializes `VerFs`
  - Mounts via `async_fusex::session::new_session`
  - Installs SIGINT handler (`tokio::signal::ctrl_c`)
  - Runs session until cancellation

- `src/fs/mod.rs`
  - Implements `async_fusex::VirtualFs`
  - Owns namespace + file I/O behavior
  - Integrates metadata (`MetaStore`), packs (`PackStore`), batching, and syncing
  - Implements Phase 2 operation semantics:
    - overwrite-capable rename
    - hard links and link-count accounting
    - symlink create/readlink
    - xattr CRUD with proper flag/errno handling
    - access checks and lock table behavior
    - file truncate correctness on `setattr(size)` and `O_TRUNC`

- `src/meta/mod.rs`
  - Opens/bootstraps SurrealKV tree
  - Provides read/write transaction wrappers
  - Initializes root inode and system keys

- `src/types/mod.rs`
  - `rkyv` metadata records (`InodeRecord`, `DirentRecord`, `ExtentRecord`, `ChunkRecord`)
  - Key encoding helpers for keyspace prefixes
  - Added key helpers for xattrs and symlink targets

- `src/data/pack.rs`
  - Append-only pack format
  - Hash-addressed chunk writes/reads
  - Per-record magic/hash/length validation

- `src/write/batcher.rs`
  - Async worker queue with two flush triggers:
    - block threshold (`batch_max_blocks`)
    - time threshold (`batch_flush_interval_ms`)
  - Synchronous caller contract via per-write ack channel

- `src/sync/mod.rs`
  - Periodic sync task
  - Coordinated shutdown + final full sync

## Config

`config.toml` fields in current use:

- `mount_point`
- `data_dir`
- `sync_interval_ms`
- `batch_max_blocks` (default `3000`)
- `batch_flush_interval_ms` (default `500`)

Derived directories:

- metadata: `<data_dir>/metadata`
- packs: `<data_dir>/packs`

## Metadata Model

Hot metadata is binary (`rkyv`) and prefix-keyed:

- `I:<inode_id_be>` -> inode record
- `D:<parent_inode_be>:<name_bytes>` -> dirent record
- `E:<inode_id_be><logical_block_be>` -> extent record
- `C:<chunk_hash128>` -> chunk record
- `X:<inode_id_be>:<xattr_name>` -> xattr payload
- `Y:<inode_id_be>` -> symlink target payload
- `SYS:*` -> system counters/config (`next_inode`, `active_pack_id`)

`InodeRecord` stores inode identity, ownership, permissions, link count, file size, timestamps, and generation.

## Data Plane

### Chunk identity

- Block-sized payloads (`4096` bytes) are hashed with `XXH3-128`
- Hash bytes are authoritative chunk identifiers

### Pack format

Each appended chunk record in a pack file is:

- magic: `"VPK1"` (4 bytes)
- chunk hash (16 bytes)
- payload length (u32 little-endian)
- payload bytes

`ChunkRecord` stores `pack_id`, `offset`, `len`, and `refcount`.

### Dedup behavior

- Writes probe `C:<hash>` first
- Existing chunk: refcount increment
- Missing chunk: append to pack + create metadata record

## Write and Truncate Semantics

### Write path

1. FUSE `write` enqueues `WriteOp` into batcher.
2. Batcher flushes on block threshold or interval.
3. Apply path updates extents/chunk refs/inode metadata transactionally.

### Truncate hardening

`setattr(size)` and `open(O_TRUNC)` now execute real truncate logic:

- Shrink deletes fully truncated extents and decrements chunk refcounts.
- Partial tail block is rewritten with zeroed bytes beyond new EOF.
- Grow is sparse (holes read as zero).

This prevents stale post-truncate data visibility and keeps chunk refcounts consistent.

## Namespace and POSIX Semantics

### Rename

Rename now supports:

- overwrite of existing targets
- `RENAME_NOREPLACE`
- `RENAME_EXCHANGE`
- directory/non-directory compatibility checks
- non-empty directory replacement rejection
- parent link-count/timestamp updates where required

### Link and unlink

- Hard links implemented for non-directory inodes.
- `unlink` decrements `nlink` and removes inode payload only at final unlink.

### Symlinks

- `symlink` creates `INODE_KIND_SYMLINK` + target payload record.
- `readlink` returns stored target bytes.

### Xattrs

Implemented operations:

- `setxattr` with `XATTR_CREATE` / `XATTR_REPLACE` semantics
- `getxattr` with `ERANGE` behavior
- `listxattr` with NUL-delimited names and `ERANGE` behavior
- `removexattr` with `ENODATA` on missing key

### Access and locking

- `access` checks inode mode/uid/gid against requested mask.
- `getlk`/`setlk` implemented with in-memory per-inode range lock tracking.
- `setlkw` behavior is provided via retry loop in the async path.

## FUSE Surface Coverage

`VirtualFs` implementation now covers:

- `init`, `destroy`, `lookup`, `forget`, `getattr`, `setattr`
- `readlink`, `mknod`, `mkdir`, `unlink`, `rmdir`, `symlink`, `rename`, `link`
- `open`, `read`, `write`, `flush`, `release`, `fsync`
- `opendir`, `readdir`, `releasedir`, `fsyncdir`
- `statfs`, `setxattr`, `getxattr`, `listxattr`, `removexattr`, `access`, `create`
- `getlk`, `setlk`, `bmap`

Vendor updates in `async-fusex` for this phase:

- `bmap` reply now returns a real block mapping response instead of hardcoded `ENOSYS`.
- Session fallbacks for unsupported ioctl/poll/fallocate/lseek now return deterministic non-`ENOSYS` errors.

## Integration Testing

Added integration test:

- `tests/rsync_integration.rs`

Behavior:

- mounts VerFSNext
- runs Linux `cp`, `mv`, and `rsync -a` through the mounted filesystem
- validates integrity with Linux `sha256sum`
- validates metadata with Linux `stat`
- unmounts/remounts and re-validates data persistence
- deletes copied tree, validates deletion via `ls` and `stat`
- unmounts/remounts again and validates deletion persistence
- runs all external commands via Linux `timeout`

Execution guard:

- test runs only when `VERFSNEXT_RUN_MOUNT_TESTS=1` is set
- otherwise it exits successfully without mount execution

Run example:

```bash
VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test --test rsync_integration -- --nocapture
```
