# VerFSNext Technical Deep Dive

## Current State

This repository now contains a working Phase 1 vertical slice implementation of VerFSNext:

- FUSE runtime wired through `vendor/async-fusex`
- Metadata runtime wired through `vendor/surrealkv`
- Mount lifecycle + SIGINT graceful shutdown
- Baseline metadata schema and storage
- Baseline hash-addressed pack storage backend
- Write batcher skeleton (`>= 3000` blocks or `500ms`)
- Background sync service skeleton + shutdown full-sync barrier
- Core filesystem operations for Phase 1 shell workflows

## Runtime Layout

- `src/main.rs`
  - Loads `config.toml`
  - Initializes `VerFs`
  - Mounts via `async_fusex::session::new_session`
  - Installs SIGINT handler (`tokio::signal::ctrl_c`)
  - Runs session until cancellation

- `src/fs/mod.rs`
  - Implements `async_fusex::VirtualFs`
  - Owns the operational logic for namespace + file I/O
  - Integrates metadata (`MetaStore`), packs (`PackStore`), batching, and syncing

- `src/meta/mod.rs`
  - Opens/bootstraps SurrealKV tree
  - Provides read/write transaction wrappers
  - Initializes root inode and system keys

- `src/types/mod.rs`
  - `rkyv` metadata records (`InodeRecord`, `DirentRecord`, `ExtentRecord`, `ChunkRecord`)
  - Key encoding helpers for keyspace prefixes

- `src/data/pack.rs`
  - Append-only pack format
  - Hash-addressed chunk writes/reads
  - Integrity checks via per-record magic/hash/length validation

- `src/write/batcher.rs`
  - Async worker queue with two flush triggers:
    - block threshold (`batch_max_blocks`)
    - time threshold (`batch_flush_interval_ms`)
  - Synchronous caller contract via per-write ack channel

- `src/sync/mod.rs`
  - Periodic sync task
  - Coordinated shutdown + final full sync

## Config

`config.toml` fields used now:

- `mount_point`
- `data_dir`
- `sync_interval_ms`
- `batch_max_blocks` (default `3000`)
- `batch_flush_interval_ms` (default `500`)

Derived directories:

- metadata: `<data_dir>/metadata`
- packs: `<data_dir>/packs`

## Metadata Model (Phase 1)

All hot metadata records are binary (`rkyv`) and keyed by compact prefixes:

- `I:<inode_id_be>` -> inode record
- `D:<parent_inode_be>:<name_bytes>` -> dirent record
- `E:<inode_id_be><logical_block_be>` -> extent record (hash pointer)
- `C:<chunk_hash128>` -> chunk record (pack location + refcount)
- `SYS:*` -> system counters/config (`next_inode`, `active_pack_id`)

### Inode fields

`InodeRecord` stores:

- inode id, parent inode
- kind (file/dir/symlink)
- uid/gid, perm, nlink
- logical file size
- atime/mtime/ctime
- generation

## Data Plane (Phase 1)

### Chunk identity

- Block-sized payloads (`4096` bytes) are hashed with `XXH3-128`
- Hash bytes are authoritative chunk identifiers

### Pack format

Each appended chunk record in a pack file is:

- magic: `"VPK1"` (4 bytes)
- chunk hash (16 bytes)
- payload length (u32 little-endian)
- payload bytes

`ChunkRecord` in metadata stores:

- `pack_id`
- `offset`
- `len`
- `refcount`

### Dedup behavior

- Writes check `C:<hash>` first
- Existing chunk: bump refcount, reuse
- Missing chunk: append to pack, create metadata record

## Write Path

1. FUSE `write` submits a `WriteOp` to the batcher.
2. Batcher flushes on threshold/time and waits for sink completion.
3. Apply step updates block-level extents:
   - load old block bytes if needed
   - patch byte range
   - hash new block
   - resolve chunk metadata/create chunk
   - update extent and inode metadata in one metadata transaction
4. Caller receives completion only after apply step finishes.

## Read Path

1. Resolve inode + file size.
2. Map logical block range to extents.
3. Resolve chunk locations from metadata.
4. Read chunk payload from pack and copy requested slices into reply buffer.

Reads are independent from the write batch queue and operate against committed metadata snapshots.

## Implemented Phase 1 Operation Surface

Implemented and wired in `VirtualFs`:

- `lookup`
- `getattr`
- `setattr`
- `mknod`
- `mkdir`
- `create`
- `open`
- `read`
- `write`
- `readdir`
- `unlink`
- `rmdir`
- `rename`
- `flush`
- `release`
- `fsync`
- `opendir`
- `releasedir`
- `fsyncdir`
- `statfs`

Deferred in this phase (returns `ENOSYS`):

- `readlink`
- `symlink`

## Sync + Shutdown

- Background sync periodically flushes:
  - active pack file (`sync_data`)
  - SurrealKV WAL (`flush_wal`)
- SIGINT path:
  - stop intake path
  - drain batcher
  - perform final full sync (`sync_all` + metadata WAL sync)
  - close metadata engine

## Notes

This phase is intentionally a baseline vertical slice focused on mountability, real file I/O, persistence across remount, and clean shutdown behavior. Advanced correctness/performance hardening (full POSIX edge behavior, full async-fusex surface, richer dedup/compression/indexing, snapshots/GC, vault) remains for later phases.
