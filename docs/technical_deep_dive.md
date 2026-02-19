# VerFSNext Technical Deep Dive

## Current State

The repository now includes a Phase 5 implementation on top of the existing full FUSE surface and prior data-plane/snapshot/GC work:

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
- Vault encryption control path:
  - `verfsnext crypt -c -p <password> [-path <directory_or_file_path>]`
  - `verfsnext crypt -u -p <password> -k <key_file_path>`
  - `verfsnext crypt -l`
- Runtime stats control path:
  - `verfsnext stats`
- Mounted control-plane socket at `<data_dir>/verfsnext.sock` accepts snapshot/crypt/stats commands from CLI control mode.
- Snapshot CLI first tries socket RPC; if no socket listener is available, it falls back to offline metadata mode.
- Crypt CLI first tries socket RPC; create/lock can fall back to metadata-only mode if no daemon is mounted.
- Stats CLI uses socket RPC and requires a mounted daemon.
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
  - runtime counters for chunk cache hit/miss and read/write byte totals
  - `collect_stats` computes live-scope logical/chunk/dedup/cache/memory/throughput metrics
  - live scope is traversed from root while excluding `/.snapshots`; snapshot and all-namespace logical totals are also exposed separately
  - stats output includes full `data_dir` size and disk delta `data_dir_size - live_logical_size`
  - stats are rendered as an aligned table for terminal readability
  - read-only inode flag enforcement across mutating FUSE operations
  - exposes snapshot create/list/delete methods used by control socket server in mounted mode
  - enforces `.vault` lock-state visibility and access policy
  - blocks rename of top-level `/.vault` and blocks cross-boundary rename/link between vault and non-vault trees
  - marks vault inodes using inode flags and applies data encryption/decryption only for those inodes
  - provides `create_vault`, `unlock_vault`, and `lock_vault` runtime operations
  - background GC trigger integrated into periodic sync cycles with strict idle gating (recent activity checks plus write-lock contention checks before pack rewrite work)

- `src/vault/mod.rs`
  - Envelope wrapping metadata type (`VaultWrapRecord`) encoded with `rkyv`
  - Random 256-bit folder key generation
  - Argon2id KEK derivation
  - XChaCha20-Poly1305 wrapping for folder key and per-chunk data encryption/decryption helpers
  - Key-file generation/read helpers (`verfsnext.vault.key`)

- `src/snapshot/mod.rs`
  - Snapshot create/list/delete workflows over metadata
  - recursive tree cloning excluding `/.snapshots` subtree
  - chunk refcount delta handling on create/delete

- `src/gc/mod.rs`
  - `.DISCARD` binary header/record encode-decode and CRC32C validation
  - checkpoint-bounded record reads
  - atomic discard-file rewrite/rotation primitive used by GC pack stage

- `src/data/pack.rs`
  - Pack record format `VPK2` archived with `rkyv`
  - Sidecar index file per pack (`.idx`) uses fixed-size archived `rkyv` records
  - Pack header and index record decode paths use checked `rkyv::access` for zero-copy validation
  - Automatic active-pack rollover on append when configured pack size target is reached
  - Existing packs are discovered on startup; highest pack id becomes active if metadata lags
  - Hash lookup path reads index entry first, then seeks pack payload
  - Supports encrypted-payload reads (`read_chunk_payload`) for vault decrypt-then-decompress flow
  - Index is rebuilt from pack data when missing (including non-active packs loaded at startup)
  - GC pack rewrite support for non-active packs with atomic replacement and index-cache invalidation

## Config

`config.toml` now includes Phase 5 tuning:

- `mount_point`
- `data_dir`
- `sync_interval_ms`
- `batch_max_blocks`
- `batch_flush_interval_ms`
- `metadata_cache_capacity_entries`
- `chunk_cache_capacity_entries`
- `pack_index_cache_capacity_entries`
- `pack_max_size_mb`
- `zstd_compression_level`
- `ultracdc_min_size_bytes`
- `ultracdc_avg_size_bytes`
- `ultracdc_max_size_bytes`
- `fuse_max_write_bytes`
- `fuse_direct_io`
- `fuse_fsname`
- `fuse_subtype`
- `gc_idle_min_ms`
- `gc_pack_rewrite_min_reclaim_bytes`
- `gc_pack_rewrite_min_reclaim_percent`
- `gc_discard_filename`
- `vault_enabled`
- `vault_argon2_mem_kib`
- `vault_argon2_iters`
- `vault_argon2_parallelism`

## Metadata and Snapshot/GC Additions

- `ChunkRecord` keeps zero-ref entries at `refcount = 0` until metadata-stage GC consumes them.
- `ChunkRecord` now also stores vault encryption state (`flags`, `nonce`) for encrypted chunks.
- Snapshot metadata records are keyed under `S:<name>` and point to snapshot root inode.
- System keys now include:
  - `SYS:active_pack_id`
  - `SYS:gc.discard_checkpoint`
  - `SYS:gc.epoch`
  - `SYS:vault.state`
  - `SYS:vault.wrap`
  - `SYS:vault.policy`
- Inodes now carry `flags` with a read-only bit used for snapshot immutability.
  - Additional inode flags mark vault namespace and descendants.

## Vault Data Path

1. `verfsnext crypt -c` generates:
   - key file material (`verfsnext.vault.key`)
   - random 256-bit vault folder key
   - wrapped folder key metadata persisted under `SYS:vault.wrap`
   - top-level `/.vault` inode+dirent (root-only reserved path)
2. While vault is locked:
   - `/.vault` is hidden from root `readdir`
   - `lookup` and direct inode operations against vault entries return inaccessible/not-found semantics
3. `verfsnext crypt -u` unwraps the folder key into process memory and flips runtime state to unlocked.
4. Vault writes:
   - block payload is compressed
   - compressed bytes are encrypted with XChaCha20-Poly1305 and random 192-bit nonce
   - encrypted payload is appended to pack
   - nonce + encryption flag are persisted in chunk metadata
5. Vault reads:
   - encrypted payload is read from pack
   - payload is decrypted using in-memory folder key
   - decrypted compressed bytes are decompressed and returned
6. `verfsnext crypt -l` clears in-memory key material and invalidates vault-related caches.

## `.DISCARD` Flow

1. Metadata-stage GC scans for zero-ref chunks.
2. For each candidate chunk, GC emits a CRC-protected discard record (`pack_id`, `chunk_hash128`, `block_size_bytes`, `epoch_id`).
3. `SYS:gc.discard_checkpoint` is advanced only after `.DISCARD` append + sync.
4. Pack-stage GC reads records up to checkpoint, chooses packs by reclaim byte/percent thresholds, rewrites live chunks only, and atomically swaps rewritten pack/index files.
5. Consumed discard entries are removed via atomic discard-file rewrite and checkpoint reset to the new file length.

## Validation Run

Executed after Phase 5 changes:

- `cargo build --release`

Build completed successfully in this repository state.
