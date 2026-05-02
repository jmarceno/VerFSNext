# Autoresearch: VerFSNext Overall Read/Write Performance

## Objective
Optimize the end-to-end throughput of VerFSNext's ComfyUI-profile benchmark. The workload mimics a real ComfyUI filesystem interaction with small Python modules (3000 files, 1KB–1MB each with *unique* content to defeat dedup), large model files (~1.3GB total, unique content), random-order cold reads, reverse-order reads, and an overwrite phase. Every byte is unique — no dedup savings — so the benchmark measures raw chunking/compression/packing/readback throughput.

## Metrics
- **Primary**: `summary_total` (ms, lower is better) — total benchmark wall-clock time
- **Secondary**: `sm_write_dura` (ms), `sm_read_seq` (ms), `lg_write_dura` (ms), `sm_read_rnd` (ms), `lg_read_seq` (ms), `lg_read_rev` (ms), `mod_write_dura` (ms), `mod_read` (ms), `sync_barrier` (ms) — individual phase timings to watch tradeoffs

## How to Run
```bash
VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test bench_comfyui_profile --test rsync_integration -- --nocapture 2>&1 | tee /tmp/bench.out
# Or use autoresearch.sh which wraps this and outputs METRIC lines
./autoresearch.sh
```

## Files in Scope

### Primary data path
- `src/data/chunker.rs` — UltraCDC streaming chunker (64KB feed buffer, min/avg/max size configurable)
- `src/data/compress.rs` — zstd compression + decompression, parallel compress via rayon
- `src/data/hash.rs` — XXH3-128 hashing (plain + domain-prefixed)
- `src/data/pack.rs` — PackStore: append-only pack writes, rkyv index records, moka-cached file handles

### Write path
- `src/fs/write.rs` — `prepare_write_plan` (read-modify-write at block level), `apply_single_write_in_txn`, `apply_batch` (coalesces writes per inode, shares one SurrealKV txn)
- `src/fs/chunk.rs` — `load_chunk_record`, `load_extent_payload`, `materialize_pending_chunks`, `stage_chunk_if_missing`
- `src/write/batcher.rs` — WriteBatcher: async ingest → batching (max_size_bytes + flush_interval) → apply worker

### Read path
- `src/fs/fuse.rs` — FUSE `read` implementation (find at `async fn read()`) — builds `SmallFileReadPlan`, reads per-block
- `src/fs/mod.rs` — `load_extent_payload`, file read plan caching

### Metadata
- `src/meta/mod.rs` — MetaStore wrapping SurrealKV, read_txn/write_txn, lightweight `get_value` point-reads
- `src/types/mod.rs` — key encoding helpers (inode_key, chunk_key, extent_key, etc.), rkyv encode/decode

### Vendor (hard-forked, we own all code)
- `vendor/surrealkv/src/` — LSM-tree KV store with WAL, memtables (skip list), compaction, bloom filters
- `vendor/async-fusex/src/` — async FUSE protocol implementation, VirtualFs trait

### Config
- `src/config.rs` — all tunable parameters with defaults

## Off Limits
- Do NOT change chunk cache size (`chunk_cache_capacity_mb`) — we want hard performance gains, not memory delegation
- Do NOT sacrifice data correctness: CRC32 validation, xxh3-128 hashing must remain strict
- Do NOT sacrifice space savings: compression (zstd), deduplication must remain fully active
- Do NOT increase on-disk data size (compressed payloads, index records, metadata)

## Constraints
- All data correctness guarantees must be preserved (CRC32 checksums, XXH3-128 hashing, rkyv structural validation)
- Compression (zstd) must remain active — no changes that increase stored data size
- Deduplication must remain fully functional
- No increase in memory pressure beyond minimal amounts (a few MB, not hundreds)
- New crate dependencies must be lightweight and stable — avoid heavy/unstable crates
- Tests must pass: file counts (3020 small files, 3 large), file sizes > 0 for large files

## What's Been Tried

### 1. Fast-path for full-block new writes (KEPT)
**Change**: In `prepare_write_plan`, when a write covers an entire block with no old extent, hash the write data slice directly instead of zero-filling a 1MB buffer and copying into it.
**Result**: **-29%** total time (130,744 → 92,543 ms). lg_write_dura dropped 64% (54s → 20s) because large model files (`dd bs=1M`) write exactly one full block per FUSE message, all new extents.
**Files**: `src/fs/write.rs` — restructured block processing loop

### 2. Remove redundant chunk counting (KEPT as part of #1)
**Change**: Removed `ultracdc_chunk_count` call from `prepare_write_plan` and the `cdc_chunk_count` field from `PreparedWritePlan`. The chunk count was only used in a `debug!()` log message.
**Files**: `src/fs/write.rs`, `src/fs/chunk.rs`, `src/fs/mod.rs`

### 3-6. Discarded experiments (all within noise)
- Skip SurrealKV range scan when inode.size==0 — no improvement
- Point reads for single-block extent scans — no improvement
- Arc<Vec<u8>> sharing between cache and pending_chunks — no improvement
- Vec<u8> ownership in stage_chunk_if_missing — no improvement

### Key insight
After micro-optimizations, sm_write_dura remains at ~45s. Based on analysis:
- ~13s from FUSE message round-trip overhead (267,000+ messages for 3000 files via `dd bs=1024`)
- ~9s from `dd` process creation (fork+exec per file)
- ~6s from fsync overhead (conv=fsync triggers sync_cycle per file)
- ~17s from data processing + SurrealKV operations
Only the last category is within VerFSNext's control. The first three are inherent to the benchmark's structure.

## Ideas Backlog
See `autoresearch.ideas.md` for deferred/promising ideas.
