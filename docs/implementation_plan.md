# VerFSNext Linux FUSE Filesystem — Detailed Implementation Plan

## 1. Scope, Constraints, and Non-Negotiables

This document defines the implementation plan for a Linux userspace filesystem with these hard constraints:

1. FUSE binding must be from `vendor/async-fusex`.
2. Metadata KV must be from `vendor/surrealkv`.
3. Phase 1 features: COW, snapshots, inline dedup (UltraCDC + XXH3-128), inline compression (zstd + rayon), caching with moka.
4. Phase 2 feature: `.vault` data encryption only using XChaCha20-Poly1305 + Argon2id + envelope encryption workflow.
5. All FUSE operations supported by vendored async-fusex must be implemented and usable in first prototype (no exceptions).
6. POSIX-compliant behavior and rsync compatibility are required.
7. Reads must never be blocked by writers (hard rule).
8. Writers are synchronous from caller perspective, but write pipeline batches at least 3000 blocks or 500ms flush interval.
9. No full-file buffering in memory.
10. fsync is sparse and background-driven; shutdown performs final sync and waits for completion.
11. SIGINT/CTRL+C triggers graceful shutdown.
12. Mountpoint and data directory are loaded from `config.toml`.
13. Metadata model must leverage SurrealKV capabilities: vlog support, range scans/deletes, merge operators, put_if_not_exists.
14. Metadata must be compact and fast to traverse/recover.
15. `rkyv` zero-copy is first-class priority.
16. Strings are only for user-facing edges; hot-path metadata fields must be raw bytes/fixed-width numeric types.
17. `.vault` does not exist by default and is created explicitly by user command.
18. `.vault` resides only at filesystem root and cannot be renamed to any other final name.
19. `.vault` visibility is lock-state driven: fully hidden when locked; standard hidden folder when unlocked.
20. XXH3-128 hash collisions are ignored by design; hash is treated as authoritative content identity across metadata, indexing, and GC.

## 2. High-Level System Architecture

### 2.1 Runtime Components

- **FUSE Frontend (`async-fusex`)**
  - Receives kernel requests and dispatches to filesystem handlers.
  - Must implement complete operation surface exposed by `async-fusex` traits/APIs.

- **Metadata Engine (`surrealkv`)**
  - Stores inode graph, dirent indexes, extent/chunk refs, snapshots, reference counts, and system config.
  - Uses range scans and range deletes for traversal/GC/snapshot operations.
  - Uses merge operators for counters/refcounts/aggregates.
  - Uses put-if-not-exists for object creation race safety.

- **Data Plane**
  - Chunking via `cdc-chunkers = 0.1.3` UltraCDC.
  - Hashing via `xxhash-rust = 0.8` XXH3-128.
  - Compression via `zstd = 0.13.3`.
  - Parallel compression workers via `rayon = 1.11`.
  - Pack writer appends compressed chunks into pack files in data directory.

- **Write Batcher**
  - Aggregates write blocks from synchronous writer calls.
  - Flush trigger: `>= 3000 blocks` OR every `500ms`.
  - Flushes to OS buffers (no per-op fsync).

- **Read Path**
  - Lock-free or minimally contended snapshots over metadata references.
  - Never blocked by active write commits.

- **Cache Layer (`moka`)**
  - Metadata object cache.
  - Chunk index cache.
  - Negative lookup cache (ENOENT hot paths).

- **Background Services**
  - Periodic sync service (`fsync`/fdatasync policy from config).
  - GC/reclaimer (deferred to safe epochs).
  - Optional compaction/maintenance jobs.

- **Shutdown Coordinator**
  - Catches SIGINT.
  - Stops intake, drains in-flight operations, forces final batch flush and final sync, then unmount/shutdown.

### 2.2 Design Priorities Mapping

- **Never stall reads**
  - MVCC-like metadata epochs + immutable extent lists for readers.
  - Read APIs run on stable snapshot pointers.
- **Never lose data**
  - Ordered commit protocol across data + metadata.
  - Final sync barrier at shutdown.
- **Maximize performance**
  - Inline chunking/compress dedup with parallel workers.
  - Compact binary metadata and zero-copy decode.

## 3. Repository Layout Plan

Create or extend these modules:

- `src/main.rs` (bootstrap + config load + signal handling + mount lifecycle)
- `src/config.rs` (`config.toml` schema + defaults + validation)
- `src/fs/mod.rs` (FUSE trait wiring)
- `src/fs/ops/*.rs` (group operations by opcode families)
- `src/meta/mod.rs` (SurrealKV abstraction layer)
- `src/meta/schema.rs` (binary key/value layouts)
- `src/meta/txn.rs` (transaction helpers + conflict rules)
- `src/data/chunker.rs` (UltraCDC integration)
- `src/data/hash.rs` (XXH3-128 wrappers)
- `src/data/compress.rs` (zstd/rayon pipeline)
- `src/data/pack.rs` (pack writer/reader + index)
- `src/write/batcher.rs` (3000-or-500ms batching)
- `src/read/path.rs` (non-blocking read implementation)
- `src/cache/mod.rs` (moka caches)
- `src/snapshot/mod.rs` (snapshot lifecycle)
- `src/sync/mod.rs` (periodic sync scheduler)
- `src/shutdown/mod.rs` (graceful stop orchestration)
- `src/vault/mod.rs` (phase 2 encryption path)
- `src/types/*.rs` (inode IDs, chunk IDs, packed structs)

## 4. Config Plan (`config.toml`)

### 4.1 Mandatory Fields

- `mount_point` (path)
- `data_dir` (path)
- `sync_interval_ms` (u64)

### 4.2 Recommended Operational Fields

- `batch_max_blocks = 3000`
- `batch_flush_interval_ms = 500`
- `metadata_cache_capacity`
- `chunk_cache_capacity`
- `negative_cache_ttl_ms`
- `zstd_level`
- `pack_target_size_mb`
- `gc_idle_min_ms`
- `gc_pack_rewrite_min_reclaim_bytes`
- `gc_pack_rewrite_min_reclaim_percent`
- `gc_discard_filename = .DISCARD`
- `read_worker_threads`
- `write_worker_threads`

### 4.3 Phase 2 Vault Fields

- `vault_enabled = true|false`
- `vault_argon2_mem_kib`
- `vault_argon2_iters`
- `vault_argon2_parallelism`

Config loading behavior:
- Read once on startup.
- Validate all paths and numeric limits.
- Fail-fast on invalid config.

## 5. Metadata Model (Compact Binary / Hot-Path Safe)

## 5.1 Keyspace Design (prefix-oriented)

Use compact byte prefixes and fixed-width IDs:

- `I:{inode_id}` -> inode core record (mode, uid, gid, nlink, size, times, flags, generation, active_version)
- `D:{parent_inode}:{name_bytes}` -> dirent target inode
- `DX:{parent_inode}` -> sorted dirent index range for listing
- `E:{inode_id}:{file_version}:{logical_block}` -> extent/chunk reference
- `C:{chunk_hash128}` -> chunk metadata (refcount, pack_id, clen, ulen, codec)
- `P:{pack_id}` -> pack metadata
- `S:{snapshot_id}` -> snapshot root/version map
- `R:{inode_id}` -> refcount and lineage metadata
- `SYS:{key}` -> system config (including wrapped vault folder key in phase 2)
- `L:{lock_or_lease_key}` -> optional transient coordination records

Two-index rule:

- Metadata index stores only `pack_id` for each chunk hash (`C:{chunk_hash128}`).
- Physical byte offsets are resolved from pack-local index mapping `chunk_hash128 -> offset`.
- Metadata must not depend on physical offsets, enabling pack-internal relocation without metadata rewrites.
- Hash collisions are intentionally ignored in this design.

All hot path fields are bytes/u64/u32/u16/u8; avoid strings in value payloads.

### 5.2 Serialization Strategy

- Primary: `rkyv` archived structs for values.
- Zero-copy reads where validity guarantees are satisfied.
- Use byte-order stable packed structs for indexes.
- Human-facing names are raw bytes in dirent keys; UTF-8 conversion only at interface boundaries when needed.

### 5.3 SurrealKV Feature Utilization Map

- **Range scans**: dir listing, subtree traversal, snapshot discovery, GC walks.
- **Range deletions**: subtree remove, snapshot prune, stale index cleanup.
- **Merge operators**: chunk refcount increments/decrements, inode accounting counters.
- **put_if_not_exists**: mkdir/create/link race-safe insertion, snapshot ID reservation.
- **Vlog**: large metadata blobs or packed auxiliary data where beneficial.

## 6. Data Plane Plan (Chunk/Dedup/Compression/Packs)

### 6.1 Write Stream to Chunks

1. Accept write slices synchronously.
2. Feed rolling stream into UltraCDC boundaries (no full-file buffering).
3. For each produced chunk:
   - Compute XXH3-128.
   - Probe chunk index cache, then metadata key `C:{hash}`.
   - If exists: increment refcount via merge op and create extent mapping.
  - If missing: compress chunk (zstd), append to pack, write pack-local index entry (`chunk_hash128 -> offset`), create `C` record with refcount=1 and only `pack_id` for location.
4. Update inode versioned extent map (`E:*`) for COW consistency.

### 6.2 Compression

- zstd encoder level from config.
- Parallelization model:
  - Chunk compression jobs fan out to rayon pool.
  - Pack append ordering preserved by sequencer stage.
- Store both compressed and uncompressed lengths per chunk.

### 6.3 Pack Format

- Append-only pack files under `data_dir/packs/`.
- Per-pack index (embedded or sidecar) maps `chunk_hash128 -> byte offset` and stores block size/integrity fields.
- Rotation by target size.
- Durable metadata link only after pack append success.

### 6.4 Two-Index Resolution Path

- Metadata lookup returns `pack_id` only (from `C:{chunk_hash128}`).
- Reader opens pack index for `pack_id`, resolves `chunk_hash128` to offset, then reads block data.
- Index cache is keyed by `(pack_id, chunk_hash128)` and can be rebuilt from pack index files on recovery.
- Any pack rewrite that changes offsets updates only pack-local index; metadata records remain unchanged as long as `pack_id` mapping is preserved by rewrite policy.

## 7. COW and Snapshot Implementation Plan

### 7.1 Copy-on-Write Semantics

- Inode has active file version pointer.
- Mutating write allocates new version metadata records; old versions remain immutable for snapshots/readers.
- Extent mapping is version-addressed, not overwritten in place.

### 7.2 Snapshot Model

- `.snapshots` virtual/control directory at FS root.
- Snapshot creation stores root/version map in `S:{snapshot_id}`.
- Snapshot reads resolve inode versions frozen at snapshot creation epoch.
- Snapshot deletion decrements reachable refs through deferred GC walk.

### 7.3 GC/Reclaim

- GC runs only when idle thresholds met.
- Uses a two-stage process:
  1. **Metadata stage**: identify zero-refcount blocks, remove metadata references, and append discard records to `data_dir/packs/.DISCARD`.
  2. **Pack stage**: select packs with reclaimable space above configured threshold, rewrite only valid blocks into a new pack, and atomically rename to replace old pack.
- Stage 1 must consume SurrealKV GC API output for zero-ref entries (do not re-derive if API already provides block set).
- `.DISCARD` entries contain `pack_id`, `chunk_hash128`, and block size.
- Pack rewrite phase uses `.DISCARD` as input plus pack index scan to compute survivors.
- Atomic replacement guarantees readers observe either old or new complete pack file.

### 7.4 `.DISCARD` Binary Record Specification (Deterministic)

File location and lifecycle:

- Path: `data_dir/packs/.DISCARD`.
- Writer: metadata-stage GC appends records after successful zero-ref metadata removal.
- Reader: pack-stage GC consumes records and compacts selected packs.
- Rotation rule: after successful pack-stage consumption + checkpoint, truncate file to zero or atomically rotate to `.DISCARD.done.<epoch>` per config policy.

Encoding rules:

- Endianness: little-endian for all numeric fields.
- Strings: forbidden in records; binary-only fields.
- Integrity: each record includes CRC32C over payload fields (excluding header magic/version and excluding CRC field itself).

Header (written once per file creation/rotation):

- `magic: [u8; 8] = "VFDISC01"`
- `version: u16 = 1`
- `flags: u16`
- `header_crc32c: u32`

Record layout (fixed-size v1):

- `record_len: u16` (bytes, must equal fixed v1 size)
- `record_version: u8` (1)
- `record_flags: u8` (reserved)
- `epoch_id: u64` (GC metadata-stage run identifier)
- `pack_id: u64`
- `chunk_hash128: [u8; 16]`
- `block_size_bytes: u32`
- `reserved: u32`
- `record_crc32c: u32`

Record semantics:

- One record represents one dead block candidate proven zero-ref by metadata stage.
- Duplicate records for the same `(pack_id, chunk_hash128)` are allowed; consumer de-duplicates in memory by latest `epoch_id`.
- Records with invalid CRC are skipped and counted in corruption metrics; GC continues.

Durability and atomicity:

- Metadata-stage GC writes records in append mode and flushes file data before advancing stage checkpoint.
- Stage checkpoint in metadata (`SYS:gc.discard_checkpoint`) stores last durable byte offset.
- Pack-stage GC reads up to checkpointed offset only; partial tail writes are ignored until checkpoint advances.

Consumption protocol:

1. Load `.DISCARD` up to checkpoint offset.
2. Group by `pack_id`, compute reclaimable bytes and reclaim ratio.
3. Select packs meeting `gc_pack_rewrite_min_reclaim_bytes` OR `gc_pack_rewrite_min_reclaim_percent`.
4. Rewrite survivors to new pack, rebuild pack-local index, fsync new pack/index as policy requires.
5. Atomically rename new pack over old pack.
6. Mark consumed discard ranges and advance/rotate `.DISCARD`.

## 8. Full FUSE Operation Coverage Plan

Implement all operations supported by vendored async-fusex. Group by behavior:

### 8.1 Namespace/Metadata Ops

- `lookup`, `getattr`, `setattr`, `access`, `readlink`, `mknod`, `mkdir`, `unlink`, `rmdir`, `symlink`, `rename`, `link`, `create`

Plan notes:
- All namespace mutations are transactional in metadata.
- Name handling as raw bytes internally.
- Correct POSIX error codes and link count updates.

### 8.2 File I/O Ops

- `open`, `read`, `write`, `flush`, `release`, `fsync`, `fallocate`, `lseek` (if provided)

Plan notes:
- `read`: resolve latest visible version for handle/snapshot; no writer blocking.
- `write`: synchronous acceptance + batch pipeline enqueuing with bounded buffering.
- `fsync`: maps to policy engine (not per-op heavy sync unless requested semantics demand it).

### 8.3 Directory Ops

- `opendir`, `readdir`, `readdirplus`, `releasedir`, `fsyncdir`

Plan notes:
- Stable iteration cookies.
- Efficient range scans over `DX` prefix.
- rsync-compatible expectations for metadata and listing consistency.

### 8.4 XAttr and Locking Ops

- `setxattr`, `getxattr`, `listxattr`, `removexattr`
- `getlk`, `setlk`, `setlkw` (if exposed by async-fusex)

Plan notes:
- xattrs stored compactly under dedicated key prefixes.
- Locking semantics mapped to kernel expectations; avoid reader stalls.

### 8.5 Filesystem-Level Ops

- `statfs`, `init`, `destroy`, and any poll/ioctl/copy_file_range op that async-fusex surface exposes.

Plan notes:
- Feature negotiation in `init` must explicitly enable capabilities required for rsync/POSIX correctness.
- `destroy` triggers coordinated shutdown path.

## 9. Read-Nonblocking Concurrency Strategy (Hard Requirement)

### 9.1 Core Mechanism

- Readers get immutable metadata epoch handles.
- Writers commit new versions and atomically advance visibility pointer.
- No global write lock in read path.

### 9.2 Locking Rules

- Fine-grained key-range locks for conflicting metadata writes only.
- Reader code path avoids waiting on writer mutexes.
- Chunk data reads use stable pack offsets once committed.

### 9.3 Consistency Level

- Read operations observe a coherent snapshot (current or handle-bound) with monotonic visibility.
- Namespace and inode attribute reads are internally consistent per operation.

## 10. Write Batching and Sync Strategy

### 10.1 Synchronous Writer Contract

- System call returns only after operation is accepted into durable-intent pipeline and metadata intent persisted sufficiently for crash recovery policy.
- Does not imply immediate global fsync.

### 10.2 Batcher Contract

- Trigger flush when block count reaches 3000.
- Also flush every 500ms regardless of fill level.
- Never accumulate whole files; stream blocks/chunks.

### 10.3 Sync Service

- Background periodic sync interval from `config.toml`.
- Applies to active pack descriptors and metadata handles.
- `fsync` syscall path can request stronger synchronization but must avoid blanket heavy sync behavior by default.

### 10.4 Shutdown Sync Barrier

- On SIGINT: stop ingress, flush batcher, force sync all dirty handles, wait completion, then unmount/exit.
- Shutdown is successful only after sync barrier passes.

## 11. POSIX and rsync Compatibility Plan

### 11.1 POSIX Correctness Areas

- Accurate `st_mode`, `st_nlink`, `st_uid`, `st_gid`, `st_size`, timestamps.
- Correct atomic rename semantics.
- Hard-link behavior and nlink accounting.
- Sparse file handling (`fallocate`/hole punching if supported).
- Permission and access checks.

### 11.2 rsync Compatibility Focus

- Reliable handling of temp-file + rename workflows.
- Correct partial writes and file truncation interplay.
- Stable `readdir` and attrs during sync phases.
- Correct mtimes/ctimes update semantics.
- Support for xattrs/perms ownership operations rsync may invoke.

### 11.3 Validation Matrix

- rsync normal copy, `--inplace`, `--append`, `--delete`, metadata-preserving modes.
- Large trees with concurrent reads.

## 12. Caching Plan with moka

### 12.1 Cache Types

- Inode metadata cache keyed by inode ID.
- Dirent lookup cache keyed by `(parent_inode, name_bytes_hash)`.
- Chunk index cache keyed by XXH3-128 hash.
- Pack index cache keyed by `(pack_id, chunk_hash128)` for offset resolution.
- Negative lookup cache for ENOENT short TTL.

### 12.2 Invalidation

- Write commits publish invalidation events.
- Epoch tagging avoids stale data visibility for readers.

### 12.3 Memory Guardrails

- Configurable max capacities.
- Weighted eviction for larger values.

## 13. Crash Consistency and Recovery Plan

### 13.1 Ordering Rules

1. Write chunk data to pack (OS buffered).
2. Persist/update pack-local index entry (`chunk_hash128 -> offset`).
3. Persist/update chunk metadata references (`pack_id` only, never offset).
4. Persist extent/inode version pointers.
5. Publish visibility atomically.

### 13.2 Recovery Procedure

- Replay metadata journal/checkpoint mechanisms from SurrealKV.
- Reconcile incomplete pack appends via checksum/length guards.
- Rebuild or verify pack-local indexes before serving reads.
- Replay `.DISCARD` intents and complete/rollback interrupted pack rewrite transactions.
- Respect `SYS:gc.discard_checkpoint` to avoid consuming partial `.DISCARD` tails.
- Repair dangling metadata intents if needed.

### 13.3 Data-Loss Prevention

- No acknowledged write without durable-intent record.
- Final shutdown sync barrier mandatory.

## 14. Phase 2: `.vault` Data Encryption Plan

## 14.1 Scope

- Encrypt only file DATA under root `.vault` subtree.
- Metadata remains unencrypted.
- `.vault` is a special root-only folder and is not auto-created at mount/startup.
- `.vault` is created only via crypt create command workflow.

### 14.1.1 Folder Behavior Contract

- Folder name is fixed to `.vault` and location is always `/`.
- **Locked state**:
  - `/.vault` and all descendants are invisible in lookups and directory listings.
  - Any direct access to `/.vault` paths returns not found/inaccessible semantics as defined by policy.
- **Unlocked state**:
  - `/.vault` behaves as a regular hidden POSIX directory.
  - Standard POSIX operations apply to files/dirs beneath it.
- Rename persistence rule:
  - Renaming `/.vault` is blocked (`EPERM`/`EBUSY` policy decision finalized in implementation).
  - If rename-revert mode is chosen, final visible name still resolves to `.vault` only.

### 14.1.2 Control CLI Contract

Implement these commands in `verfs` control CLI:

- Unlock: `verfs crypt -u -p [password] -k [keyfile_path]`
- Lock: `verfs crypt -l`
- Create: `verfs crypt -c -p [password] -path [key_creation_path]`

Command semantics:

- `-c` creates `/.vault`, generates initial folder key, wraps it, and writes initial key material file as defined by implementation.
- `-u` authenticates/unlocks by recovering active folder key from wrapped metadata + keyfile/password inputs.
- `-l` immediately transitions vault state to locked and purges in-memory sensitive state.
- Return explicit non-zero exit codes for wrong password, missing keyfile, vault not initialized, and already locked/unlocked states.

### 14.2 Envelope Encryption Workflow

1. Generate random 256-bit folder key.
2. Derive key-encryption-key from user password using Argon2id.
3. Wrap folder key with derived key.
4. Store wrapped folder key in metadata system config (`SYS:*`, Bf-Tree system config area).
5. For each `.vault` data chunk, encrypt payload with folder key using XChaCha20-Poly1305 (192-bit nonce).
6. Store nonce + ciphertext + auth tag in pack payload record.

### 14.3 Password Change Flow

- Unwrap folder key with old password-derived key.
- Re-wrap same folder key with new password-derived key.
- Update wrapped key metadata only (no chunk re-encrypt).

### 14.4 Read/Write Integration

- Storage interception point: update `ChunkBackend` to detect and intercept requests where canonical path starts with `/.vault`.
- `.vault` write path: chunk -> compress (or encrypt-first policy decision; see milestone) -> encrypt -> pack append.
- `.vault` read path: fetch -> decrypt -> decompress (matching chosen order).
- Keep non-`.vault` path unchanged.

### 14.4.1 FUSE Integration Rules for Lock State

- `lookup`, `readdir`, `readdirplus`, `opendir` must exclude `/.vault` while locked.
- Direct ops targeting `/.vault` (create/open/read/write/setattr/xattr/etc.) must fail consistently while locked.
- Unlock transition makes `/.vault` immediately discoverable and accessible without remount.
- Lock transition revokes visibility and future access immediately.
- Active open handles under `/.vault` at lock time are force-closed/invalidated by policy and return deterministic errors on subsequent IO.

### 14.5 Security Details

- Nonce uniqueness enforced per encrypted chunk record.
- Argon2id parameters configurable with secure defaults.
- Key material zeroized on drop where possible.
- No cleartext password, derived key, folder key, or unwrapped key material is ever written to persistent metadata/data logs (including LogStore/vlog).
- Sensitive buffers use explicit memory hygiene patterns and shortest possible lifetime.

### 14.6 Cache and Session Invalidation on Lock

- Lock operation must invalidate/delete all cache entries associated with `/.vault`:
  - inode metadata cache entries
  - dirent cache entries
  - chunk index/data caches
  - negative cache entries involving `/.vault` paths
- Any in-memory decrypted data buffers and key schedules are wiped and dropped.
- Lock operation is serialized with vault-path I/O admission gate to prevent stale post-lock reads.

### 14.7 Metadata Additions for Vault State

- `SYS:vault.state` (locked/unlocked marker, boot default locked).
- `SYS:vault.wrap` (wrapped folder key + KDF params + version).
- `SYS:vault.policy` (optional flags: rename behavior, lock-handle policy).
- All vault state records remain metadata-only and unencrypted by explicit requirement.

### 14.8 POSIX Compliance Boundary

- When unlocked, `/.vault` follows normal POSIX behavior for permission checks, attrs, links, rename of children, xattrs, and I/O.
- Special behavior applies only to the top-level reserved path `/.vault` identity and lock visibility policy.

## 15. `rkyv`-First and String-Minimization Enforcement

### 15.1 Coding Rules

- New hot-path metadata structs must be `#[derive(Archive, Serialize, Deserialize)]` (or equivalent) and stored as archived bytes.
- IDs/names/hashes in keys and records are bytes and fixed-width integers.
- `String` forbidden in hot metadata records and indexes.

### 15.2 Review Gates

- CI lint/check script greps for `String` in designated hot-path modules.
- Exceptions explicitly documented and justified (user-facing only).

## 16. Milestone Plan and Deliverables

### Milestone 0 — Foundation

- Project scaffolding, config loader, mount lifecycle, clean shutdown framework.
- SurrealKV abstraction and binary schema baseline.
- Acceptance: mount/unmount + empty fs ops functional.

### Milestone 1 — Full FUSE Surface + POSIX Core

- Implement every async-fusex-supported operation (no stubs).
- Inode/dirent/xattr/link semantics complete.
- Acceptance: operation coverage checklist 100% complete.

### Milestone 2 — Data Plane (Dedup + Compression + Packs)

- UltraCDC chunking, XXH3 hash index, zstd/rayon compression, pack append/read.
- Two-index location model active (metadata stores `pack_id`; pack-local index maps `chunk_hash128 -> offsets`).
- Batcher 3000-or-500ms behavior live.
- Acceptance: dedup hit/miss correctness + streaming write constraints satisfied.

### Milestone 3 — COW + Snapshots + Read Non-Blocking Guarantees

- Versioned extents, snapshot namespace `.snapshots`, immutable reader epochs.
- Acceptance: concurrent stress proves reads never wait on writers.

### Milestone 4 — Sync/Recovery Hardening + rsync Validation

- Background sync daemon, shutdown barrier, crash recovery workflows.
- Two-stage GC implemented with SurrealKV GC output ingestion, `.DISCARD` generation, pack rewrite selection by reclaim threshold, and atomic pack replacement.
- rsync compatibility matrix pass.
- Acceptance: reliability/perf baseline achieved.

### Milestone 5 (Phase 2) — `.vault` Encryption

- Envelope key management in metadata.
- XChaCha20-Poly1305 data encryption for `.vault` only.
- Password rewrap workflow.
- `verfs crypt` CLI (`-c`, `-u`, `-l`) complete and tested.
- Lock/unlock visibility switching without remount.
- Cache invalidation and sensitive-memory purge on lock.
- `ChunkBackend` vault-path interception merged.
- Acceptance: encryption correctness + unchanged non-vault performance path.

## 17. Test Strategy and Coverage

### 17.1 Unit Tests

- Schema encode/decode (`rkyv`) and key layout tests.
- Chunk boundary/hash/compression correctness.
- Batch flush triggers and timing behavior.

### 17.2 Integration Tests

- Full FUSE op tests against mounted FS.
- Snapshot visibility and COW invariants.
- Crash/restart recovery tests.
- Concurrent reader/writer tests for non-blocking guarantee.
- Two-stage GC tests:
  - metadata stage removes zero-ref blocks and writes correct `.DISCARD` entries
  - pack stage rewrites only live blocks and atomically swaps packs
  - metadata remains valid without offset updates after pack rewrite
  - hash-collision behavior follows design policy (collisions ignored)
- Vault lock-state tests:
  - locked hides `/.vault` from `readdir`/`lookup`
  - unlocked exposes `/.vault` as hidden dir
  - lock transition invalidates open handles/cache entries as specified
  - rename of `/.vault` is blocked or reverted per chosen policy
  - CLI create/unlock/lock flows behave with correct exit codes

### 17.3 Compatibility Tests

- rsync behavior suite with representative flags.
- POSIX conformance-oriented tests (attrs, links, rename atomicity, xattrs).
- POSIX validation for `/.vault` subtree in unlocked mode.

### 17.4 Performance Tests

- Read latency under sustained write load.
- Dedup ratio and throughput.
- Pack index lookup overhead and cache hit ratio.
- GC rewrite throughput and reclaimed-space efficiency.
- Metadata traversal speed and mount recovery time.

## 18. Observability and Operations

- Structured logging for FUSE ops, write batch flushes, sync cycles, GC actions.
- Metrics:
  - read latency p50/p95/p99
  - write batch sizes and flush intervals
  - dedup hit ratio
  - compression ratio
  - cache hit/miss
  - sync duration/frequency
  - recovery duration
- Diagnostic command hooks (optional) for inspecting snapshots, packs, chunk refs.

## 19. Risk Register and Mitigations

- **Read stalls due to accidental lock coupling**
  - Mitigation: enforce lock hierarchy + reader lock-free invariants + contention tests.
- **Metadata bloat**
  - Mitigation: compact binary schema + prefix/range discipline + periodic compaction.
- **Crash windows around mixed data/meta commit**
  - Mitigation: strict ordering + replay reconciler + fault injection tests.
- **Rsync edge incompatibilities**
  - Mitigation: dedicated rsync scenario matrix in CI.
- **Encryption mode ordering mistakes (.vault)**
  - Mitigation: explicit selected order documented and tested with known vectors.

## 20. Definition of Done (Prototype and Beyond)

Prototype is done when all are true:

1. Filesystem mounts on Linux and performs as normal FS.
2. Every operation exposed by vendored async-fusex is implemented and functional.
3. POSIX behavior is correct for core metadata and I/O semantics.
4. rsync compatibility test matrix passes.
5. Reads demonstrably never block behind writers.
6. Writer batching satisfies 3000-block or 500ms trigger rules.
7. fsync policy is background-driven with configurable interval and shutdown full-sync barrier.
8. COW + snapshots + dedup + compression work inline and correctly.
9. Two-index storage model is active: metadata stores only `pack_id`; pack-local indexes own physical offsets via `chunk_hash128 -> offset`.
10. Two-stage GC works end-to-end: metadata discard generation (`.DISCARD`) and atomic pack rewrite replacement by reclaim threshold.
11. Metadata is compact, zero-copy-oriented with `rkyv`, and avoids strings on hot paths.
12. Phase 2 adds `.vault` data-only envelope encryption with password rewrap support.
13. `.vault` is root-only, not auto-created, created via `verfs crypt -c`, and hidden/inaccessible when locked.
14. `verfs crypt -u/-l/-c` control commands work end-to-end.
15. Lock operation purges vault-related caches and sensitive in-memory material.

## 21. Immediate Next Actions (Execution Order)

1. Create source tree skeleton and config loader.
2. Implement metadata schema + SurrealKV abstraction with binary keys/values.
3. Implement complete FUSE op surface with metadata-only behavior first.
4. Integrate streaming write pipeline (chunk/hash/compress/pack).
5. Add versioned extents for COW and snapshots.
6. Introduce read epoch model and contention tests.
7. Implement two-stage GC (`.DISCARD` + pack rewrite + atomic rename) and recovery hooks.
8. Add sync daemon and graceful SIGINT shutdown barrier.
9. Run rsync/POSIX/correctness/perf suites.
10. Implement `.vault` encryption phase.
11. Harden via crash injection and long-run stress tests.
