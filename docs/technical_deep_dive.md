# VerFSNext Technical Deep Dive

## Current State

The repository now includes a Phase 4 implementation on top of the existing full FUSE surface and Phase 3 data plane:

- FUSE runtime via `vendor/async-fusex`
- Metadata runtime via `vendor/surrealkv`
  - WAL batches, SSTable table metadata, and partitioned top-level index payloads are archived with `rkyv` and validated at decode boundaries
- Write batching (`>= 3000` blocks or `500ms`) with synchronous syscall completion
- Background sync + shutdown full-sync barrier
- Streaming UltraCDC chunking telemetry in write ingress path
- XXH3-128 hash-authoritative dedup decisions
- zstd compression with rayon parallel compression workers
- Hash-only two-index lookup model:
  - Metadata chunk record stores `pack_id` and chunk properties, not physical offset
  - Pack-local index stores `chunk_hash128 -> offset`
- Snapshot namespace and CLI control path:
  - `verfsnext snapshot create <name>`
  - `verfsnext snapshot list`
  - `verfsnext snapshot delete <name>`
- Root `/.snapshots` bootstrap directory is read-only from normal POSIX mutation paths
- Snapshot trees are materialized as read-only inode clones with chunk-ref accounting
- Two-stage GC with `.DISCARD` checkpointed metadata handoff:
  - Metadata stage: retains zero-ref chunks until GC stage, emits `.DISCARD` records, then deletes chunk metadata
  - Pack stage: rewrites eligible non-active packs by reclaim threshold and atomically replaces pack + index files

## Runtime Layout

- `src/fs/mod.rs`
  - `VirtualFs` implementation
  - write path stages unique missing chunks, compresses them in parallel, appends to pack, then commits metadata
  - read path resolves chunk location through pack-local hash index
  - bounded chunk metadata and chunk payload caches
  - dedup hit/miss counters emitted in write-path debug logs
  - read-only inode flag enforcement across mutating FUSE operations
  - background GC trigger integrated into periodic sync cycles with idle gating

- `src/snapshot/mod.rs`
  - Snapshot create/list/delete workflows over metadata
  - recursive tree cloning excluding `/.snapshots` subtree
  - chunk refcount delta handling on create/delete

- `src/gc/mod.rs`
  - `.DISCARD` binary header/record encode-decode and CRC32C validation
  - checkpoint-bounded record reads
  - atomic discard-file rewrite/rotation primitive used by GC pack stage

- `src/data/pack.rs`
  - Pack record format `VPK2`
  - Sidecar index file per pack (`.idx`) with fixed-size entries
  - Hash lookup path reads index entry first, then seeks pack payload
  - Index is rebuilt from pack data when missing
  - GC pack rewrite support for non-active packs with atomic replacement and index-cache invalidation

## Config

`config.toml` now includes Phase 4 tuning:

- `mount_point`
- `data_dir`
- `sync_interval_ms`
- `batch_max_blocks`
- `batch_flush_interval_ms`
- `metadata_cache_capacity_entries`
- `chunk_cache_capacity_entries`
- `pack_index_cache_capacity_entries`
- `zstd_compression_level`
- `ultracdc_min_size_bytes`
- `ultracdc_avg_size_bytes`
- `ultracdc_max_size_bytes`
- `fuse_max_write_bytes`
- `gc_idle_min_ms`
- `gc_pack_rewrite_min_reclaim_bytes`
- `gc_pack_rewrite_min_reclaim_percent`
- `gc_discard_filename`

## Metadata and Snapshot/GC Additions

- `ChunkRecord` keeps zero-ref entries at `refcount = 0` until metadata-stage GC consumes them.
- Snapshot metadata records are keyed under `S:<name>` and point to snapshot root inode.
- System keys now include:
  - `SYS:gc.discard_checkpoint`
  - `SYS:gc.epoch`
- Inodes now carry `flags` with a read-only bit used for snapshot immutability.

## `.DISCARD` Flow

1. Metadata-stage GC scans for zero-ref chunks.
2. For each candidate chunk, GC emits a CRC-protected discard record (`pack_id`, `chunk_hash128`, `block_size_bytes`, `epoch_id`).
3. `SYS:gc.discard_checkpoint` is advanced only after `.DISCARD` append + sync.
4. Pack-stage GC reads records up to checkpoint, chooses packs by reclaim byte/percent thresholds, rewrites live chunks only, and atomically swaps rewritten pack/index files.
5. Consumed discard entries are removed via atomic discard-file rewrite and checkpoint reset to the new file length.

## Validation Run

Executed after Phase 4 changes:

- `cargo fmt`
- `cargo build`
- `cargo test`

All completed successfully in this repository state.
