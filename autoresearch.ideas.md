# Autoresearch Ideas: Improved Read Speeds

## Implemented
- ✅ **Warm chunk_data_cache during writes** — Insert raw block data into the data cache in `stage_chunk_if_missing`. sm_read -46%, lg_read -26%.

## Deferred / Future Ideas

. **Lazy data cache insertion (no clone)** — Currently `data.to_vec()` clones the block data for the cache AND `data_vec.clone()` creates another copy for the Arc. Both are 1MB. Could share allocation by refactoring `pending_chunks` to hold `Arc<[u8]>` or `Bytes` (from bytes crate) instead of `Vec<u8>`, then `compress_parallel` would extract the bytes for compression while the Arc lives in the cache. This would halve the write-time memory overhead of cache warming.

. **Populate chunk_data_cache for dedup hits** — When `stage_chunk_if_missing` finds a chunk already exists (dedup hit), the block data is still available. Could also insert into the data cache here to warm it for existing data.

. **Avoid batcher.drain() on every read** — Add a flag/timestamp to skip the drain channel round-trip when the batcher is known-empty. The fsync handler already drains before returning, so subsequent reads don't need another drain. Requires careful correctness reasoning.

. **File read plan caching by (ino, data_version)** — Currently plans are cached by file handle (fh). If the file is closed and reopened, the plan is rebuilt. Keying by (ino, data_version) would avoid this. Small benefit for reopen-heavy workloads.

. **Smaller logical blocks for tiny files** — The 1MB block size causes massive read amplification for tiny files (e.g., 128B file stored as 1MB block, 8000x amplification). A future redesign could store sub-block files inline in the metadata or use variable-size extents. Architectural change, not a quick optimization.

. **Reduce read transaction overhead for small files** — Each small file read creates a SurrealKV read transaction to build the SmallFileReadPlan. Adding a point-read method to MetaStore (without full transaction) could reduce this overhead.
