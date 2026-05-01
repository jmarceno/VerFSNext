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
*(To be updated as experiments progress)*
- Baseline: initial fast benchmark run establishes baseline times.
