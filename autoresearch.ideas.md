# Autoresearch Ideas — Write Speed Optimization

## Current Active Optimizations (all kept, verified by 9/9 tests)

| # | Optimization | File | Impact |
|---|-------------|------|--------|
| 1 | `sync_data()` instead of `sync_all()` on pack/index fsync | `src/fs/write.rs` | ~3% — halves fsync metadata flushes |
| 2 | Removed redundant `seek(SeekFrom::End(0))` in pack append | `src/data/pack.rs` | ~2% — saves one seek syscall per chunk |
| 3 | Avoid `WriteOp.data` clone in `apply_queued_batch` | `src/write/batcher.rs` | Minor — saves one data copy per write |
| 4 | Pre-compute `SystemTime::now()` once per batch attempt | `src/fs/write.rs` | Minor — saves N clock_gettime syscalls per batch |
| 5 | Buffer index entries, flush in one `write_all` during sync | `src/data/pack.rs` | Minor — batches 44-byte index writes into one |
| 6 | Skip KV extent scan when `inode.size == 0` | `src/fs/write.rs` | ~2% — avoids read txn for new/truncated files |
| 7 | Unbounded channel instead of bounded for batcher queue | `src/write/batcher.rs` | Robustness — eliminates B005 channel ordering issue |

**Total improvement**: ~9% on `bench_comfyui_profile_fast` (17,662ms → 16,079ms avg)

## Deferred / Not Effective
- **BufWriter for index file**: Regressed — extra memcpy + flush overhead exceeded syscall savings.
- **Combined header+payload write**: Allocation overhead offset syscall savings.
- **write_all_vectored**: Not yet stable in the Rust version used (1.95.0).

## Ideas Not Yet Explored
- **Fire-and-forget writes with unbounded channel**: The unbounded channel now eliminates the FIFO ordering issue. But fire-and-forget writes (`enqueue` instead of `enqueue_and_wait`) still have the B005 issue: the kernel considers FUSE_WRITE durable when it returns, and with FUSE_WRITEBACK_CACHE it may evict the page before the batcher commits. This would require a different approach where the batcher owns the data and the kernel can't evict it.
- **Pre-allocate pack files with fallocate**: Pre-allocate pack file space to avoid kernel metadata updates from file extension. Requires changing from append-only to positional writes. Significant refactor.
- **Parallel pack appends across multiple active packs**: Use multiple active packs and distribute chunks across them. Would allow parallel pack I/O (round-robin or hash-based). Large refactor but could significantly improve large file write throughput.
- **Zero-fill avoidance for partial blocks**: For partial-block writes to new extents, the current code allocates and zero-fills 4KB then copies write data in. Could use a Vec initialized with write data and only fill the gaps.

## Profiling Notes
- `sm_write_dura` is ~48% of benchmark time — small file writes are the primary target
- Large file writes (`lg_write_dura`) improved from ~3600ms to ~2600ms after optimizations
- System noise is ±15% on the fast benchmark — changes need multiple runs to confirm
- Full benchmark (`bench_comfyui_profile`) is more consistent but takes ~140s to run
