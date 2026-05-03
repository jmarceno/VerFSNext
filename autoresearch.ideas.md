# Autoresearch Ideas — Write Speed Optimization

## Successful Optimizations (✓)
- **sync_data instead of sync_all**: Changed `packs.sync(true)` → `packs.sync(false)` in write.rs. sync_data() guarantees data + size metadata on disk, which is sufficient for crash ordering. sync_all's metadata flush (timestamps, sizes) is unnecessary since both pack and index files can be rebuilt from pack records. ~3% improvement.
- **Removed redundant seek in pack append**: `file.seek(SeekFrom::End(0))` in `append_chunk_with_crc32` was redundant because `active.size_bytes` (tracked under the active pack lock) already equals the current file position for O_APPEND files. Saves one syscall per chunk. ~2% additional improvement.
- **Avoid WriteOp data clone**: Changed `apply_queued_batch` to take ownership of `WriteOp.data` via `std::mem::take` instead of cloning. Saves one full data copy per write. Minor but measurable improvement in memory bandwidth.

## Deferred / Not Tested
- **BufWriter for index file**: Wrapping the index file in BufWriter regressed performance due to extra memcpy + flush overhead. Index entries are 44 bytes each, written under the active pack lock. The extra copy outweighed the syscall savings.
- **Combined header+payload write**: Pre-allocating a Vec and writing both at once added allocation overhead that offset the syscall savings for our write patterns.
- **write_all_vectored**: Not yet stable in the Rust version used by this project.

## Ideas for Future Exploration
- **Separate write/drain channels**: Use separate mpsc channels for Write and Drain messages to eliminate the channel ordering issue (B005 root cause). This would allow fire-and-forget writes with reliable drain ordering, enabling write batching.
- **Batch small file commits**: Instead of committing each file write individually (required by `dd conv=fsync` + FUSE writeback cache), accumulate FUSE_WRITE_CACHE writes in the batcher and only commit on FUSE fsync/flush. Requires solving the channel ordering issue first.
- **Index write batching**: Buffer index entries in memory (Vec of 44-byte entries) and flush the batch during sync instead of writing each entry individually. Unlike BufWriter, this avoids the per-entry copy by batching into a single write.
- **Parallel chunk appends across packs**: Use multiple active packs and distribute chunks across them with round-robin or hash-based distribution. Would allow parallel pack I/O.
- **Pre-allocate pack files with fallocate**: Pre-allocate space to avoid kernel metadata updates from file extension. Requires changing from append-only writes to positional writes.
- **Avoid zero-fill allocation for partial blocks**: For partial-block writes, the current code allocates and zero-fills a 4KB block, copies the write data in, then hashes it. A zero-fill-free approach (using Vec::resize would still zero) could save memory bandwidth.
- **Profile: sm_write_dura is the bottleneck**: ~48% of total benchmark time. Each small file write (~1-64KB) triggers a full commit cycle (sync + KV commit). Any optimization that reduces per-commit overhead or batches small writes would have outsized impact.
- **Avoid SystemTime::now() per write**: Each write in a batch calls `SystemTime::now()` (clock_gettime syscall). Pre-computing the timestamp once per batch and reusing it for all inode updates would save syscalls.
