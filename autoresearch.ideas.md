# Autoresearch Ideas (VerFSNext)

## Tried / Dead Ends
- **Skip SurrealKV range scan for inode.size==0**: No improvement. Range scan overhead for empty extents is negligible.
- **Point reads for single-block extent scans (meta.get_value)**: No improvement vs range iterator.
- **Share Arc<Vec<u8>> between chunk_data_cache and pending_chunks**: Saves 1 copy per chunk but within noise (~0.3%).
- **Vec<u8> ownership in stage_chunk_if_missing**: Avoids one copy per new chunk but within noise (~1%).

## Remaining Bottleneck Analysis
The benchmark's sm_write_dura (~45s) is dominated by:
1. **FUSE message round-trips**: ~267,000 writes at ~50µs each = ~13s
2. **dd process creation**: 3000 fork+exec at ~3ms each = ~9s
3. **fsync amplification**: conv=fsync per file triggers sync_cycle
4. **VerFSNext data processing**: ~17s (hash, compress, pack write, SurrealKV)

Only item 4 is within VerFSNext's control. The other three are benchmark overhead.

## Deferred / Promising Ideas

- **Batch SurrealKV commits across writes**: Currently each file's write + fsync forces a commit. If we could defer metadata for bulk operations...
- **Reduce pack I/O during reads**: For cold reads, decompression dominates. Could pre-decompress or use faster decompression (zstd level 1 for speed, but increases size).
- **Parallelize block hash loop**: For large writes, hash blocks in parallel with rayon. Currently hash is serial (fast, ~1µs/MB).
- **Zero-copy FUSE writes**: Avoid `data.to_vec()` in FUSE write handler by using the kernel buffer directly. Requires async-fusex changes.
- **Lighter write coalescing**: Currently coalesces by inode then sequential. Could also coalesce across inodes at the same block-level for metadata sharing.
- **SurrealKV tuning**: Increase memtable size, reduce WAL overhead for batch writes.
- **Remove unnecessary header validation in pack reads**: `read_chunk_payload_with_index` reads + validates the pack header on every read, but the index entry already contains the same info. This adds an extra pread per read. Savings: ~3000 * (header read + decode + validate) for cold reads.
