# Autoresearch Ideas (VerFSNext)

## Deferred / Promising Ideas

- **Pack format optimization**: The current PackRecordHeader has 3 reserved bytes + 4 magic bytes. Could we combine magic + reserved into fewer bytes to reduce per-record overhead?
- **Index cache warming during pack rotation**: When rotating packs, the index cache is primed linearly. Could parallel warm with rayon speed this up?
- **Reduce rkyv encoding/decoding overhead**: rkyv's access/check pattern has overhead. For hot paths (extent lookups), hand-rolled packed struct reads might be faster.
- **Batch decompression**: When reading many chunks for a large file read, decompress them in parallel with rayon instead of sequentially.
- **Async pack I/O**: Use `tokio::fs` for pack reads to overlap I/O with decompression.
- **Tune write coalescing**: The write batcher coalesces adjacent writes. Could larger coalescing windows reduce metadata commits?
- **UltraCDC chunker tuning**: The 64KB feed buffer in `ultracdc_chunk_count` is hardcoded. Making it larger could reduce chunker overhead for the (pointless) chunk counting call.
- **Remove redundant chunk counting**: `ultracdc_chunk_count` is called on write data but the result is only used for logging (debug!). Could skip it.
- **Pluggable hash function**: XXH3-128 is fast but maybe XXH3-64 is enough if we accept slightly higher collision risk? (probably not worth the risk per correctness requirements)
- **Lazy decompression in cache**: Store zstd-compressed data in the chunk data cache instead of decompressed data, decompress on use. Saves memory at cost of CPU.
- **Skip zero-fill for new blocks**: When writing to a region past EOF with no old extents, the block is zero-filled then immediately overwritten. The zero-fill is wasted work.
