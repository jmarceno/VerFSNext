# Autoresearch: Improved Read Speeds

## Objective
Optimize the FUSE read path in VerFSNext to reduce the time spent reading files back from the deduplicated/compressed chunk store. The benchmark writes a mix of small files (ComfyUI env profile) and large files (ML model checkpoints) using random data, then reads them all back. We want to minimize the total benchmark wall-clock time, primarily by reducing the read phase latencies.

Read workflow:
- Small files (≤4 blocks / 4MB): uses `SmallFileReadPlan` — cached extents + chunks
- Large files (>4 blocks): does a range query on metadata store for extents, then iterates blocks sequentially
- Each 1MB block: metadata lookup → pack index lookup → pack header read (41B) + payload read → CRC32 validation → zstd decompression
- Benchmark data = random noise (CODEC_RAW — decompression is essentially a memcpy)

## Metrics
- **Primary**: summary_total (ms, lower is better) — total benchmark wall-clock time (ns → ms)
- **Secondary**: sm_read (ms) — small file read phase time
- **Secondary**: lg_read (ms) — large file read phase time
- **Secondary**: sm_write_dura (ms) — small file write phase time
- **Secondary**: lg_write_dura (ms) — large file write phase time

## How to Run
`cargo build --release && ./autoresearch.sh` — outputs `METRIC name=value` lines.

## Files in Scope
- `src/fs/fuse.rs` — FUSE read implementation (small & large file paths)
- `src/fs/chunk.rs` — `load_extent_payload`, `load_chunk_record`, `read_extent_bytes`
- `src/data/pack.rs` — `read_chunk_payload_with_index` (pack reads from disk)
- `src/fs/mod.rs` — `FsCore` struct, cache configs, `batch_max_blocks`
- `src/data/compress.rs` — `decompress_chunk` (zstd bulk decompress)

## Off Limits
- DO NOT touch anything in `vendor/` unless absolutely necessary
- DO NOT reduce correctness: CRC32 validation must remain, all error handling intact
- DO NOT change chunking parameters (UltraCDC min/avg/max sizes)
- DO NOT change the benchmark itself (tests/rsync_integration.rs)
- DO NOT add new dependencies
- DO NOT reduce BLOCK_SIZE

## Constraints
- No new crate dependencies (rust-only standard lib solutions preferred)
- Must compile with `cargo build --release`
- All existing tests must pass
- Must not degrade write performance (secondary metrics monitored)
- Must not lose data or reduce correctness guarantees

## What's Been Tried

### ✅ KEPT: Warm chunk_data_cache during writes
**Change**: In `stage_chunk_if_missing` (chunk.rs), insert the raw block data into `chunk_data_cache` via `Arc<Vec<u8>>` alongside the existing `pending_chunks` insertion.
**Result**: sm_read 35.1ms → 18.9ms (-46%), lg_read 24.3ms → 17.9ms (-26%). Write phases unchanged (within noise).
**Why it works**: The benchmark reads files immediately after writing them. With the data cache pre-warmed, `load_extent_payload` returns the `Arc<Vec<u8>>` directly from the cache, avoiding pack file reads, CRC32 validation, and zstd decompression. The overhead is one extra 1MB Vec clone + moka cache insertion per block during writes.

### ❌ DISCARDED: Parallel block reads for large files
**Change**: Used rayon `par_iter()` to load all block payloads concurrently in the large file read path (fuse.rs).
**Result**: No improvement. lg_read within noise (±1ms).
**Why it failed**: Benchmark data is random noise (CODEC_RAW), so decompression is a memcpy. Pack file I/O is the bottleneck and is serialized (single disk). Parallelism adds scheduling overhead without benefit.

### ❌ DISCARDED: Combined header+payload read in pack.rs
**Change**: Read pack record header + payload in a single `read_exact_at` call instead of two in `read_chunk_payload_with_index`.
**Result**: No measurable change.
**Why it failed**: Syscall overhead (~1µs) is dwarfed by the 1MB data transfer time (~100µs). Two sequential `pread` calls vs one makes no difference at this scale.
