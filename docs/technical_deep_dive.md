# VerFSNext Technical Deep Dive

## Current State

The repository now includes a Phase 3 inline data-plane implementation on top of the existing full FUSE operation surface:

- FUSE runtime via `vendor/async-fusex`
- Metadata runtime via `vendor/surrealkv`
- Write batching (`>= 3000` blocks or `500ms`) with synchronous syscall completion
- Background sync + shutdown full-sync barrier
- Streaming UltraCDC chunking telemetry in write ingress path
- XXH3-128 hash-authoritative dedup decisions
- zstd compression with rayon parallel compression workers
- Hash-only two-index lookup model:
  - Metadata chunk record stores `pack_id` and chunk properties, not physical offset
  - Pack-local index stores `chunk_hash128 -> offset`
- Bounded moka caches for metadata/chunk/pack-index hot paths

## Runtime Layout

- `src/fs/mod.rs`
  - `VirtualFs` implementation
  - write path stages unique missing chunks, compresses them in parallel, appends to pack, then commits metadata
  - read path resolves chunk location through pack-local hash index
  - bounded chunk metadata and chunk payload caches
  - dedup hit/miss counters emitted in write-path debug logs

- `src/data/pack.rs`
  - Pack record format `VPK2`
  - Sidecar index file per pack (`.idx`) with fixed-size entries
  - Hash lookup path reads index entry first, then seeks pack payload
  - Index is rebuilt from pack data when missing

- `src/data/compress.rs`
  - Codec constants (`raw`, `zstd`)
  - Single-chunk compression/decompression helpers
  - Rayon-backed parallel compression fanout

- `src/data/chunker.rs`
  - Streaming wrapper over UltraCDC (`cdc-chunkers`)
  - Incremental feed/finish API for non-whole-file chunk boundary generation

- `src/data/hash.rs`
  - XXH3-128 helper returning little-endian 128-bit hash bytes

## Config

`config.toml` fields now include Phase 3 tuning:

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

## Metadata and Two-Index Model

`ChunkRecord` (`C:<chunk_hash128>`) now stores:

- `refcount`
- `pack_id`
- `codec`
- `uncompressed_len`
- `compressed_len`

Physical offsets are moved out of metadata and into pack-local index records.

## Pack Format

Each `VPK2` pack record is:

- `magic` (`"VPK2"`)
- `chunk_hash128` (16 bytes)
- `codec` (u8)
- reserved bytes
- `uncompressed_len` (u32 LE)
- `compressed_len` (u32 LE)
- payload bytes

Each index entry is fixed-size and stores:

- `chunk_hash128`
- `offset` (u64 LE)
- `codec`
- `uncompressed_len` (u32 LE)
- `compressed_len` (u32 LE)

## Read/Write Data Flow

### Write

1. FUSE write enqueues into batcher.
2. Apply path rewrites touched logical blocks.
3. For changed block hashes:
   - if metadata already has hash: dedup hit
   - else stage new chunk payload
4. New payloads are compressed in parallel (rayon).
5. Compressed records append to pack + pack-local index.
6. Metadata transaction commits extents/refcounts/chunk records.

### Read

1. Resolve extent `E:*` to `chunk_hash128`.
2. Resolve chunk metadata `C:*` to `pack_id` + lengths/codec.
3. Resolve `(pack_id, chunk_hash128)` via pack-local index to offset.
4. Read/decode payload and return bytes.

## Validation Run

Executed after Phase 3 changes:

- `cargo test -p verfsnext`
- `cargo build --release`

Both completed successfully.
