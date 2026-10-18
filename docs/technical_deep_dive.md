# VerFSNext Technical Deep Dive

## Current State

The repository now includes a Phase 5 implementation on top of the existing full FUSE surface and prior data-plane/snapshot/GC work:

- FUSE runtime via `crates/verfsnext-async-fusex`
- Metadata runtime via `crates/verfsnext-surrealkv`
  - WAL batches, SSTable table metadata, and partitioned top-level index payloads are archived with `rkyv` and validated at decode boundaries
- Two-stage write batching pipeline:
  - ingest stage batches by byte threshold or flush interval
  - apply stage drains ordered batches and reports completion to waiting syscalls
- Background sync + shutdown full-sync barrier
- Content-defined chunking (UltraCDC) of all file data, independent of FUSE request sizes (see "Content-Defined Chunking")
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
- Records check control path:
  - `verfsnext check` names the files behind every consistency counter (see "Records Check and Damaged-File Cleanup")
- Offline pack-size migration control path:
  - `verfsnext pack-size-migrate`
- Global config-file CLI option:
  - `verfsnext --config <path> ...`
  - `verfsnext -c <path> ...`
- Entry points (see "Desktop App"):
  - bare `verfsnext` opens the desktop app (builds with the default `gui` feature); a headless build (`--profile headless --no-default-features`, output in `target/headless/`) mounts instead
  - `verfsnext mount` mounts with config discovery; `verfsnext --config <path>` (no command) mounts with that file
- The crate is a library (`src/lib.rs`, `verfsnext::run`) plus a thin `src/main.rs`. cxx-qt links its generated C++ objects into every target of the package, so the Qt bridge must live in a library that all targets (including `tests/`) link.
- Mounted control-plane socket at `<data_dir>/verfsnext.sock` accepts snapshot/crypt/stats commands from CLI control mode.
  - Protocol types and the client live in `src/control.rs`, shared by the CLI and the desktop app.
  - `stats` responses carry both the rendered table (`message`) and the structured `VerFsStats` (`stats`, serde-optional so old clients/daemons interoperate). `Stats { allow_cached }` returns the last completed scan right away and lets the daemon refresh it in the background, reporting `stats_scanned_at_ms`, `stats_refreshing` and `stats_refresh_error`; without it the request waits for a scan taken after it arrived, which is what the CLI uses.
  - `status` is a cheap request for frequent polling (`DaemonStatus`: vault enabled/initialized/locked, GC running, uptime, I/O totals, RSS, cache hit rate and approximate cache memory, plus the last activity). It never scans metadata, unlike `stats`.
- The daemon shuts down gracefully (kernel writeback flush, FUSE request drain, final sync, unmount) on SIGINT and on SIGTERM.
- Auto config discovery order when `--config/-c` is not provided:
  - `./config.toml`
  - `~/.config/verfsnext/config.toml`
  - `/etc/verfsnext/config.toml`
- Auto-discovered config path is shown before command execution and requires confirmation with 5-second auto-accept.
- Snapshot CLI first tries socket RPC; if no socket listener is available, it falls back to offline metadata mode.
- Crypt CLI first tries socket RPC; create/lock can fall back to metadata-only mode if no daemon is mounted.
- Stats CLI uses socket RPC and requires a mounted daemon.
- Control socket file mode is forced to `0660` at bind time so `verfs` group members can run control commands. The systemd installer no longer widens it (it used to `chmod 666` the socket, letting every local user run control commands).
- Startup now normalizes `<data_dir>` tree permissions to group-writable POSIX modes:
  - directories: `0770`
  - regular runtime files (packs/indices/metadata/discard): `0660`
  - control socket: `0660`
  - normalization is best-effort when mode changes are not permitted by ownership/capabilities
- Startup enforces persisted pack-size compatibility:
  - `SYS:pack_max_size_mb` is initialized automatically on first startup after upgrade.
  - Daemon startup fails if `config.pack_max_size_mb` differs from `SYS:pack_max_size_mb`.
  - Pack-size changes require explicit offline migration via `pack-size-migrate`.
- Startup runs a one-time pack-index CRC32 migration:
  - rewrites all `.idx` files to include a payload CRC32 field
  - stores completion marker in `SYS:pack_index_crc32_migration_v1`
  - initializes `SYS:pack_crc32_read_errors`
- Snapshot trees are materialized as read-only inode clones with chunk-ref accounting
- Two-stage GC with `.DISCARD` checkpointed metadata handoff:
  - Metadata stage: retains zero-ref chunks until GC stage, emits `.DISCARD` records, then deletes chunk metadata
  - Pack stage: rewrites eligible packs by reclaim threshold and atomically replaces pack + index files. An eligible active pack is compacted in place, so idle cleanup reclaims it without rotating it: the active pack keeps its id and still grows to its size limit. The copy runs before the short swap, so reads of that pack are not stalled for the whole rewrite.
  - Offline recovery path: `verfsnext gc offline [--run]` rebuilds `.DISCARD` by scanning pack indexes against current chunk metadata (including orphaned/stale pack entries) and can optionally run only the pack-stage rewrite loop afterward

## Content-Defined Chunking

File data is stored as variable-length, content-defined chunks. Chunk boundaries depend only on the file's bytes, never on how the kernel split them into FUSE requests: `fuse_max_write_bytes` only sizes requests, while `ultracdc_min_size_bytes`, `ultracdc_avg_size_bytes` and `ultracdc_max_size_bytes` control the stored chunks. Until this was introduced (B018) every file was stored as fixed, zero-padded 1 MiB blocks and the UltraCDC settings had no effect.

- `src/data/chunker.rs` implements UltraCDC (Zhou et al., 2022): an 8-byte window is compared against the pattern `0xAA..AA`; its Hamming distance is updated byte by byte and a cut is taken where `distance & mask == 0`, with mask `0x2F` before the normal (average) size and `0x2C` after it. No cut is taken before the minimum size, a cut is forced at the maximum size, and a run of 64 identical 8-byte windows (low-entropy data such as zeros) is cut immediately. The chunker needs either `max_size` bytes or all remaining bytes of a data run, so a cut never depends on how much data happened to be buffered. With 16/32/64 KiB the mean chunk on random data is about 32 KiB.
- Config limits: `ultracdc_min_size_bytes >= 64`, `min <= avg <= max`, `ultracdc_max_size_bytes <= 4 MiB`.

### Extent layout (storage format 2)

- Extent key: `E` + inode (u64 BE) + first byte offset (u64 BE). Value: `ExtentRecord { chunk_hash, len }`; an extent always maps a whole chunk and `len` equals the chunk's `uncompressed_len` (a mismatch is an `EIO`, logged with both lengths).
- Extents of a file never overlap and lie inside `[0, size)`. Gaps between extents, and the space between the last extent and `size`, are holes that read as zeros; writes beyond end of file and `truncate` up never store zeros.
- `SYS:max_extent_len` is the largest maximum chunk size any mount of the data directory has used. At startup it is raised to the configured `ultracdc_max_size_bytes` if lower and is then fixed for the session (every extent written is at most the configured maximum). The extent covering an offset is found with a forward range scan that starts `max_extent_len - 1` bytes before it, so lookups need no reverse iteration.
- Snapshots copy extent records verbatim; GC, pack format and chunk records are unchanged, since the pack store was already chunk-size agnostic (one record per chunk hash).

### Chunk identity and domains

A chunk is identified by `ChunkId { vault, hash }` (`src/types/mod.rs`), and its
metadata key encodes the domain: plain records use `C` + hash, vault records
`C` + `0xA7` + hash; inline payloads use the same rule under `T`. The vault
content hash is `xxh3_128(0xA7 || plaintext)`, which equals the plain hash of
`0xA7 || plaintext`; the hash alone therefore does not separate the domains, so
everything keyed by a chunk — record, inline payload, metadata and data caches,
`corrupt_chunks`, `young_inline_chunks`, refcount deltas and the records check —
uses the pair. A vault write can never deduplicate onto a plain chunk (or the
reverse), and readers verify that a record's `CHUNK_FLAG_ENCRYPTED` agrees with
the domain of its key. The pack index remains keyed by hash alone (copies are
validated by CRC, AEAD nonce, codec/length and content hash before use), so a
hash that is live in either domain keeps every indexed copy during GC.


### Write planning

`prepare_write_plan` processes the sorted, disjoint segments of a write group as regions:

1. A region starts at the start of the extent that holds the byte just before its first write (or at the write itself after a hole or at offset 0). An append therefore re-chunks the file's tail chunk together with the new data instead of starting a chunk at the old end of file.
2. Its bytes are the writes laid over the old extents. The region is materialized incrementally (at most `max_size` bytes ahead of the current cut) and cut with UltraCDC. A later write of the group that the region reaches is merged into it.
3. Past the end of the last merged write, the region ends at the first cut that lands on the start of an old extent (resynchronization: from there the old cuts are what UltraCDC would produce again, so those extents are kept), or at the end of the data (end of file or a hole). If no cut has landed on an old boundary after `RESYNC_MAX_CHUNKS` (2) maximum chunk sizes, the region ends at the next old extent boundary.
4. Old extents that start inside a region are replaced by its chunks. A chunk identical to the old extent at the same offset (same start, length and hash) is left untouched, so rewriting identical data changes no extents or refcounts. Other extents get refcount `+1` for the new chunk and `-1` for the replaced one; replaced extents with no new extent at their offset are deleted.

Old extents a region can reach are loaded through the batch transaction once per segment (from one byte before the write to `(RESYNC_MAX_CHUNKS + 2) * max_size + 2 * max_extent_len` after it). A missing extent at the far end only looks like a hole at an extent start, which is a valid region end.

Cost: each FUSE write that extends a file re-reads (usually from the chunk cache) and re-stores the file's tail chunk. A random write re-chunks the extent before it, the extents it overwrites and normally one or two more until resynchronization.

### Inline end-of-file chunks

The chunk that ends a region at the end of the file is not a content-defined cut: the next append re-chunks it. Such a chunk, when new, is stored inline in metadata instead of a pack (`CHUNK_FLAG_INLINE`, `pack_id = INLINE_PACK_ID`, payload under `T` + hash; raw bytes, vault payloads encrypted with the record's nonce). Truncating down stores the kept prefix of the last extent the same way.

- Extents, refcounts, dedup and snapshots treat it like any chunk record.
- Reads take the record and the inline payload in one read transaction and verify the content hash.
- When an inline chunk's refcount drops to zero in a commit, the record and its payload are deleted in that commit; zero-ref inline records left by snapshot delete are deleted with their payload by the GC scan, which emits no discard record for them.
- Promotion: in the idle GC window, before the GC cycle, inline chunks that already existed at the previous pass (still alive and unchanged) are compressed (vault: moved as stored) and appended to the active pack under the read side of the write lock, and their records are repointed in one transaction per batch of 256. Chunks created since the previous pass stay inline, since they are probably still the open end of a file being written.

Effect (direct I/O, unique data, pack bytes per data byte): 1 MiB writes 1.016 -> 1.002, 128 KiB 1.122 -> 1.000, 32 KiB 1.503 -> 1.002, 8 KiB about 5 -> 1.002. The provisional tails go through the metadata WAL once instead of being compressed, packed and later compacted by GC.

## Runtime Layout

- `src/fs/mod.rs`
  - `VirtualFs` implementation
  - global mutation gate is now an async `RwLock`:
    - write-side lock for namespace/global metadata mutations (rename/unlink/rmdir/link/xattr/vault/snapshot)
    - read-side lock for file-content writes/truncates so unrelated inode writes can continue concurrently
  - per-inode async write locks are allocated lazily and weakly cached (`inode -> Weak<Mutex<()>>`); the map itself and the POSIX lock table are `parking_lot` mutexes because their critical sections are single map operations
  - blocking disk work never runs inline on a tokio worker: chunk payload reads on a cache miss (pack read plus decompression), the sync cycle's pack/WAL syncs and GC pack rewrites run under `block_in_place`, and the WAL hand-off pack sync runs in `spawn_blocking`
  - write path cuts file data into content-defined chunks, stages unique missing chunks, compresses them in parallel, appends to pack, then commits metadata
  - read path resolves the extents that overlap the request, loads the chunks missing from the cache in parallel (rayon, under one `block_in_place`), and resolves chunk location through pack-local hash index; bytes no extent covers are holes and read as zeros
  - read path verifies every stored copy before using it: payload CRC32 from `.idx`, vault AEAD, decompressed length and the plaintext's content hash; a damaged copy is rejected (logged and counted), the next indexed copy of the hash is tried, and when none is intact the read fails with EIO, never returning damaged bytes
  - a chunk with no intact copy is marked corrupt (in memory at once, persisted as `CHUNK_FLAG_CORRUPT` on its record by the next sync cycle); the next write of the same content stores a fresh copy instead of deduplicating against it and repoints the record, keeping its refcount, which repairs every file that references the chunk
  - bounded chunk metadata and chunk payload caches
  - committed metadata read caches (bounded `moka`, wrapped in `FillCache`) for inode, dirent and chunk-record lookups with post-commit invalidation on mutation paths. A fill takes a ticket (its key stripe's epoch) before reading the store and inserts only if no invalidation of that stripe happened meanwhile, under the stripe lock that invalidation also takes, so a reader racing a commit can never cache the pre-commit value
  - dedup hit/miss counters emitted in write-path debug logs
  - runtime counters for chunk cache hit/miss and read/write byte totals
  - `collect_stats` computes namespace-scoped logical size plus cache/memory/throughput metrics
  - stats include cumulative pack payload CRC32 read mismatch count
  - live scope is traversed from root while excluding `/.snapshots`, and excludes `/.vault` when vault is locked
  - snapshot logical and hidden-vault logical totals are computed separately, with an all-reachable-namespaces total
  - metadata consistency checks are computed in stats output:
    chunk refcount mismatches, extents referencing missing chunk records, and orphan extent records
  - stats output includes full `data_dir` size and disk delta `data_dir_size - live_logical_size`
  - stats are rendered as an aligned table for terminal readability
  - every completed scan is kept as a snapshot (in memory and in `<data_dir>/stats-snapshot.json`), so `stats` requests that can use a kept scan answer instantly, including after a reboot; see "Kept statistics scan"
  - range scans resolve each user key without a per-key point lookup when no range tombstone can exist (see the SurrealKV architecture guide), so whole-tree scans (statistics, records check, namespace walks) no longer repeat a full level search per key
  - read-only inode flag enforcement across mutating FUSE operations
  - exposes snapshot create/list/delete methods used by control socket server in mounted mode
  - enforces `.vault` lock-state visibility and access policy
  - blocks rename of top-level `/.vault` and blocks cross-boundary rename/link between vault and non-vault trees
  - marks vault inodes using inode flags and applies data encryption/decryption only for those inodes
  - provides `create_vault`, `unlock_vault`, and `lock_vault` runtime operations
  - background GC trigger integrated into periodic sync cycles with strict idle gating (recent activity checks plus write-lock contention checks before pack rewrite work)
  - directory handles now keep a per-`opendir` snapshot for stable pagination while the namespace is mutating (prevents recursive-delete entry skips)
  - file-handle read-plan cache uses bounded `moka` entries keyed by `fh`; for files up to 4 MiB a plan holds the file's whole extent list, version-checked against inode data version before reuse
  - session-level FUSE invalidation notifier is installed at mount and used for best-effort post-commit invalidation dispatch

- `src/fs/write.rs`
  - `apply_batch` folds all writes of one inode into a single `WriteGroup` of sorted, disjoint, non-adjacent byte ranges, in arrival order (a later write wins where writes overlap); writeback writes that arrive out of order or with gaps therefore still share one group. All groups of a batch are staged into one SurrealKV transaction and committed together
  - each group is re-chunked by `prepare_write_plan`/`chunk_region` (see "Content-Defined Chunking"); old chunk payloads are read only where a region keeps old bytes (the extent before a write, partly overwritten extents, resynchronization)
  - every read a write plan depends on (inode, old extents, dedup check) goes through the batch transaction; whatever the plan depends on is either in the transaction snapshot or rewritten by it (inode key, touched extents, chunk refcounts), so concurrent changes surface as commit conflicts, after which the batch is re-planned and retried
  - a group that fails to stage is reported failed and the remaining groups are re-staged on a fresh transaction; a write is acknowledged only after the transaction containing it commits
  - commits do not sync packs; crash ordering between pack data and metadata is enforced by the WAL durability barrier (see "Write Durability and Ordering")
  - truncate stages inode, extent deletions, the trimmed boundary chunk (the prefix of the extent holding the new last byte) and refcount deltas in one transaction with the same retry loop
- `src/meta/mod.rs`
  - bootstrap writes `SYS:storage_format` (currently 2) into a new data directory and refuses to open one with any other value or with no marker (the fixed 1 MiB block layout, format 1), with an explicit error; there is no migration from format 1
  - `write_txn` takes an `FnMut` closure and re-runs it on a fresh transaction when the commit fails with `TransactionWriteConflict` or `TransactionRetry` (up to `MAX_COMMIT_ATTEMPTS`); closures must derive all writes from reads through `txn` and have no other side effects
  - read-modify-write of metadata must always read through the committing transaction (never from caches or a separate read transaction)

- `src/write/batcher.rs`
  - queue ingestion no longer blocks on `sink.apply_batch`
  - ingestion flushes pending ops into an internal apply queue and immediately resumes receiving FUSE writes
  - apply worker preserves batch order, applies each batch, and resolves per-write completion channels
  - group commit: before applying, the apply worker merges every batch already queued (up to `batch_max_size_mb`) into one `apply_batch` call, so writes that arrived during the previous commit share the next one
  - `drain` and `shutdown` are implemented as ordered barriers in the apply queue; a control message ends a merged group and runs right after it
  - FUSE `read` drains the batcher only when fire-and-forget writes are pending (the same check as `flush`/`fsync`/`release`); every write is acknowledged after its commit, so an unconditional drain only stalled reads behind unrelated commits

- `crates/verfsnext-async-fusex/src/fuse_fs.rs`
  - fixed `readdir`/`readdirplus` cookie progression to use monotonic entry index cookies (`i + 1`)
  - `readdir` now stops filling once the reply buffer is full, matching `readdirplus` behavior and preserving correct continuation semantics
  - entry responses now carry separate entry TTL and attr TTL values end-to-end

- `crates/verfsnext-async-fusex/src/session.rs`
  - the two device reader threads read requests and spawn each one as its own tokio task, so a write waiting on its commit never blocks lookups, getattrs, readdirs or reads queued behind it (previously each reader `block_on`-ed its request, so two slow writes stalled every other request, e.g. a file manager's UI)
  - `max_background` is 32 (congestion threshold 24): up to 32 writeback writes are in flight and can join one group commit; the request buffer pool holds 16 more buffers than that so foreground requests always find one
  - each in-flight request holds an owned admission on the request gate; shutdown waits until every spawned request has replied
  - `Session::run` holds a buffer-pool receiver until that drain finishes: readers return at the closed gate and drop theirs, so admitted requests still return their buffers to an open pool (B034)
  - exposes `Session::notifier()` returning a cloneable session notifier handle
  - notifier supports kernel invalidations for inode attrs and directory entries (`invalidate_inode`, `invalidate_entry`)
  - notify writes are serialized through a mutex-protected FUSE device clone and use FUSE notify message framing with `unique = 0`

- `src/fs/fuse.rs` and `src/fs/write.rs`
  - namespace and inode mutations now trigger best-effort post-commit invalidation notifications (create/unlink/rmdir/rename/link/setattr/write/truncate/xattr paths)
  - notification failures are non-fatal and logged; correctness remains independent because TTL is still conservative

- `src/fs/mod.rs` and `src/fs/vault.rs`
  - control-plane namespace/visibility mutations now also dispatch invalidations (`create_snapshot`, `delete_snapshot`, `create_vault`, `unlock_vault`, `lock_vault`)
  - this keeps kernel entry/attr views coherent even when mutations happen outside direct FUSE syscall paths
  - snapshot create/delete now also invalidate all committed inode/dirent metadata caches to avoid stale inode reuse after recursive snapshot subtree updates/removals

- `src/vault/mod.rs`
  - Envelope wrapping metadata type (`VaultWrapRecord`) encoded with `rkyv`
  - Random 256-bit folder key generation
  - Argon2id KEK derivation
  - XChaCha20-Poly1305 wrapping for folder key and per-chunk data encryption/decryption helpers
  - Key-file generation/read helpers (`verfsnext.vault.key`)

- `src/snapshot/mod.rs`
  - Snapshot create/list/delete workflows over metadata
  - recursive tree cloning excluding `/.snapshots` subtree
  - hard links are preserved: an inode reached through several names is cloned once, and every further name becomes a dirent to that clone (its nlink counts the cloned names); delete handles each inode once
  - delete keeps a snapshot file that is still referenced (open handle or kernel lookup) as an orphan (nlink 0, orphan record, extents and chunk refs kept), like an unlinked open file; release/forget run the final delete and startup reclaims leftovers. Offline delete (nothing mounted) removes everything
  - chunk refcount delta handling on create/delete

- `src/gc/mod.rs`
  - `.DISCARD` binary header/record encode-decode and CRC32C validation
  - checkpoint-bounded record reads
  - atomic discard-file rewrite/rotation primitive used by GC pack stage

- `src/data/pack.rs`
  - Pack record format `VPK2` archived with `rkyv`
  - Sidecar index file per pack (`.idx`) uses fixed-size archived `rkyv` records including `payload_crc32`
  - Pack header and index record decode paths use checked `rkyv::access` for zero-copy validation
  - Automatic active-pack rollover on append when configured pack size target is reached; the pack subdirectory is fsynced after the new pack and index are opened (created), and `PackStore::open` fsyncs the packs directory and the active pack's subdirectory, so a pack that metadata can reference never loses its directory entry on power loss
  - Existing packs are discovered on startup; highest pack id becomes active if metadata lags
  - Hash lookup resolves the copy through the pack's in-memory lookup map (hash -> every indexed copy, oldest first), then reads the payload. The active pack's map is always resident and gets each entry at append time, so records not yet flushed to the `.idx` file are found however full the caches are. Sealed packs' maps are loaded from their `.idx` in one read the first time a pack is needed (single-flight) and kept in an LRU bounded by `pack_index_cache_capacity_entries` (total entries; the most recently used map is always kept). Lookups cost one hash-map probe, never an index-file scan
  - CRC32 is computed on stored payload bytes (ciphertext for vault chunks, compressed/raw payload for non-vault chunks)
  - Supports encrypted-payload reads (`read_chunk_payload`) for vault decrypt-then-decompress flow
  - At startup (and when rotation reuses an existing pack) every index must cover its pack: entries contiguous from offset 0 and the last one ending at the end of the pack, checked from the index file alone. Any other index (missing, torn, cut short by an interrupted rebuild, entries past the end, unindexed records after a crash) is rebuilt from the pack headers into `*.idx.rebuild`, synced and renamed over the old one, then the directory is synced, so an interruption never leaves a prefix
  - GC pack rewrite keeps every indexed copy of each hash that still has a chunk record (any refcount), verifies each payload against its index CRC32 (a damaged copy is dropped only when an intact copy of the same hash is kept, e.g. after a repair write; otherwise the rewrite aborts), and swaps files crash-safely: remove old index, rename pack, rename index, fsync the directory after each step; a crash in between leaves a pack without an index, which startup rebuilds; the rewrite refuses to run when a live hash (chunk record pointing at the pack) has no index entry, so an incomplete index can never make GC drop live data
  - every pack has its own swap lock: readers hold it shared across index lookup and payload read, the rewrite holds it exclusively only for the renames, directory syncs and the switch to the new generation (handles and the lookup map, which is built from the rewritten entries before the lock is taken). Reads of other packs never wait on a swap
  - for the active pack, the handles that keep appending are opened on the rewritten files before the swap starts; if any swap step fails (index removal, renames, directory syncs), the pack store records the failure and refuses every later append, sync and rewrite with an explicit error, so nothing is written through a handle detached from the canonical pack; a restart recovers the pack and rebuilds its index from disk
  - failed appends and index flushes roll back partial writes; startup truncates an incomplete record at the end of the active pack and drops an index that no longer matches it
  - a pack may hold several copies of one hash (commit retries, pre-B006 re-appends); vault copies differ because each append uses a fresh nonce. `read_chunk_with` tries the most recent copy and then the other indexed copies (newest to oldest) until the caller's acceptance check passes (vault: AEAD authentication with the record's nonce plus decompression; non-vault: decompression). An accepted older copy is moved to the front of the map so later reads try it first, and the resolution is logged at error level
  - copies are never collapsed: GC rewrite and pack-size migration carry every indexed copy of a hash, in order, into the same pack (`append_chunk_copies` may exceed the pack size target to keep them together)

- `src/migration/pack_size.rs`
  - Compatibility guard for persisted `SYS:pack_max_size_mb`
  - Offline pack rewrite flow for pack-size changes:
    - rewrites all chunk payloads into new packs using configured size
    - updates chunk metadata `pack_id` mappings
    - resets GC discard cursor/phase and truncates discard file
    - moves old pack/index files into a backup directory for manual cleanup

## Write Durability and Ordering

Pack data must reach the disk before any metadata that points at it, or a power loss could leave extents referencing chunks that are not on disk. Before B016 this was enforced by an `fdatasync` of the active pack and its index before every write commit. On a rotational disk that cost 100-170 ms per commit and capped writes at about 4 MB/s.

The ordering now lives in the metadata WAL, similar to ext4 `data=ordered`:

- `MetaStore::open_with_pack_barrier` installs a `PackBarrier` as the SurrealKV durability barrier. The pack store counts appended records (`append_watermark`); a sync records the highest count it covered (`durable_watermark`).
- The WAL keeps committed records in the process and tags each with the append watermark at commit time. Records reach the OS only once the pack data up to their watermark is durable.
- The hand-off task (`SyncService`, every 250 ms) syncs the packs from `spawn_blocking` (skipped when nothing new was appended) and then hands the durable prefix of the WAL buffer to the OS. It never syncs while holding the WAL lock, so commits do not wait on the disk.
- The periodic sync cycle syncs the packs and then hands off only the durable prefix, like the hand-off task. Flushing the whole buffer there would sync packs again under the WAL lock and pause commits every `sync_interval_ms` while writes stream in.
- A full WAL buffer (4 MiB), WAL rotation, `flush_wal` (full sync cycle, GC rewrite) and `fsync` hand everything off after an inline pack sync if needed.
- `fsync`, `fdatasync` and `fsyncdir` call `FsCore::sync_durably`: the packs are synced (`sync_all`, or `sync_data` for `fdatasync`), then the whole WAL buffer is written and fsynced, so the data and the metadata needed to read it back are durable on return. Explicit syncs never run GC or the periodic bookkeeping of the sync cycle.

Crash behavior:
- Power loss: the WAL on disk never references pack data that is not on disk. Un-fsynced work since the last durable WAL write may be lost, as before (the periodic sync cycle never fsynced the WAL).
- WAL torn tails (B033): recovery cuts a torn or zeroed tail off the highest segment and reopens the writer afterwards, so new records are never appended behind unreadable bytes. Segments that already hold damage followed by records are read with resync; a record after a damaged region is replayed only when it continues the replayed sequence, and the segment is rewritten from the replayed records after a backup copy. See the SurrealKV architecture guide ("Why Damage Appears Mid-Segment").
- Daemon crash (SIGKILL, panic): commits not yet handed off are lost. This is at most about 250 ms plus the duration of one pack sync. Before B016 each commit was handed to the OS immediately. Data still dirty in the kernel's FUSE page cache is lost on a daemon crash either way.

`PackStore::sync` holds the active-pack mutex only to flush buffered index entries and duplicate the file handles; the `fdatasync` runs on the duplicates, so appends and reads continue during it.

## Config

`config.toml` now includes Phase 5 tuning:

- `mount_point`
- `data_dir`
- `sync_interval_ms`
- `batch_max_size_mb`
- `batch_flush_interval_ms`
- `metadata_cache_capacity_entries`
- `chunk_cache_capacity_mb`
- `pack_index_cache_capacity_entries`
- `fuse_allow_other` (default `false`): adds the `allow_other` mount option so users other than the daemon's user can access the mount. Both mount paths (`fusermount` for non-root, direct `mount(2)` for root) always pass `default_permissions`, so the kernel enforces permission bits; the filesystem does not check them in its operations. A non-root mount with `allow_other` needs `user_allow_other` in `/etc/fuse.conf`, and a failed `fusermount` returns an error (it used to panic). Before this option existed the `fusermount` path always used `allow_other` and the direct root path passed neither `allow_other` nor `default_permissions`
- `fuse_attr_ttl_ms` and `fuse_entry_ttl_ms` are loaded from `config.toml` at mount (current defaults: 150ms each); effective runtime TTLs are zeroed automatically if kernel invalidation notifier is unavailable
- `pack_max_size_mb`
- `zstd_compression_level`
- `ultracdc_min_size_bytes`
- `ultracdc_avg_size_bytes`
- `ultracdc_max_size_bytes`
- `fuse_max_write_bytes`
- `fuse_direct_io`
- `fuse_fsname`
- `fuse_subtype`
- `fuse_allow_other`
- `fuse_attr_ttl_ms`
- `fuse_entry_ttl_ms`
- `gc_idle_min_ms`
- `gc_pack_rewrite_min_reclaim_bytes`
- `gc_pack_rewrite_min_reclaim_percent`
- `gc_discard_filename`
- `vault_enabled`
- `vault_argon2_mem_kib`
- `vault_argon2_iters`
- `vault_argon2_parallelism`

## Desktop App

Built with the default `gui` feature (Qt 6 Quick via cxx-qt 0.10, tray via `ksni`); `--profile headless --no-default-features` builds the headless daemon/CLI into `target/headless/` (never overwriting the desktop binary in `target/release/`) without Qt, D-Bus or tray libraries (the systemd installer does this). The UI follows the shared design of the other desktop apps (GravaAI, Lepramim, Celestial): same palette (`qml/VerfsTheme.qml`), flat `qml/` directory, every QML file listed in `build.rs`, Basic Quick Controls style, software rendering by default, QML written for Qt 6.2.

- `src/gui/mod.rs` — startup: single instance (`$XDG_RUNTIME_DIR/verfsnext/app.sock`; a second launch shows the open window, unless the open app runs another executable file — compared by device and inode through `/proc/<pid>/exe`, so a binary rebuilt or updated in place counts — in which case the launch sends it SIGTERM, waits up to 15 s for it to exit and takes over), waits up to 120 s for a StatusNotifier tray host, owns a 2-thread tokio runtime for control-socket requests, loads `qml/Main.qml`, QML-load watchdog.
- `src/gui/controller.rs` — the `AppController` QObject. Invokables run on the Qt GUI thread and never block: every process spawn, socket request, metadata scan and directory listing runs on a worker thread and comes back as an `Event` drained by `tick()` (100 ms QML timer). One user-visible operation (start/stop/restart/snapshot/vault/mode switch/folder change) runs at a time (`busy`).
- `src/gui/daemon.rs` — runs the filesystem in one of two modes. **User service**: `~/.config/systemd/user/verfsnext.service` (`ExecStart=<exe> --config <config>`, `KillSignal=SIGINT`, `TimeoutStopSec=3000`, `WantedBy=default.target`); the unit file existing is the single source of truth for the mode, and it is rewritten when the app binary moved. Installing writes, reloads and enables the unit and removes it again if systemd refuses it, before anything is stopped. **App**: the daemon is a child process (`--config <config>`, output appended to `~/.local/state/verfsnext/daemon.log`) stopped with SIGINT on quit; a daemon the app did not spawn is stopped through the control socket's peer PID (`SO_PEERCRED`). Status is probed every 2 s (control socket connect, `systemctl --user show ActiveState`, child `try_wait`, plus the cheap `status` request); a stale FUSE mount (`ENOTCONN`) is detected and repaired with `fusermount -u -z` before a start. A start that finds the filesystem already running checks the control socket peer's executable the same way; when it is not the app's own, that daemon is stopped gracefully (`systemctl --user stop` when it is the service's `MainPID`, otherwise SIGINT to the child or socket peer) and the current binary is started, so the folder is never left on old code after an update or rebuild. Location checks: absolute paths, mount point and data folder not nested, mount point empty, data folder empty or existing VerFSNext data (`metadata/` + `packs/`; a folder with other files is refused because startup normalizes permissions of the whole data tree), writable parent, free space.
- `src/gui/settings.rs` — friendly metadata (group, label, help, display unit/scale, advanced, setup-only) for every config key. Building the editor model or writing the file fails when a key has no entry, so new config options cannot silently go missing from the app. `pack_max_size_mb` and `gc_discard_filename` are setup-only (changing them later needs a migration or orphans files). Validation errors from `Config::validate` are shown with the friendly labels. The config is written atomically as commented TOML.
- `src/gui/paths.rs` — the managed config is `~/.config/verfsnext/config.toml` (the same file CLI discovery finds); app-only preferences (last vault key file) are in `~/.config/verfsnext/desktop.toml`.
- `src/gui/tray.rs` — status line, open folder, take snapshot, start/stop, control center, quit. Quit leaves a user service running; in app mode it stops the daemon (final sync) before exiting.
- `src/gui/desktop.rs` — app-menu and login-autostart entries (bare executable), icon install, desktop notifications for results that arrive while no window is open.
- First run (no config): welcome → background service or app-only → recommended settings (customizable) → mount point and data folder (in-app folder picker with New Folder) → summary → start. The window closes once the folder is mounted; the app stays in the tray.
- Control center pages: Overview (state, space saved, dedup/compression, health, activity, memory, every `stats` field; the page shows the daemon's last completed scan immediately — including after a reboot — and asks for a newer one at most every 30 s, re-reading every 2 s while the daemon measures it), Snapshots, Vault (create with key file / unlock / lock), Folders & Startup (move folders, service mode, login autostart, stale-mount repair), Settings (all config options, restart prompt when running).

## Metadata and Snapshot/GC Additions

- `ChunkRecord` keeps zero-ref entries at `refcount = 0` until metadata-stage GC consumes them.
- `ChunkRecord` now also stores vault encryption state (`flags`, `nonce`) for encrypted chunks.
- Snapshot metadata records are keyed under `S:<name>` and point to snapshot root inode.
- System keys now include:
  - `SYS:active_pack_id`
  - `SYS:pack_max_size_mb`
  - `SYS:gc.discard_checkpoint`
  - `SYS:gc.epoch`
  - `SYS:vault.state`
  - `SYS:vault.wrap`
  - `SYS:vault.policy`
- Inodes now carry `flags` with a read-only bit used for snapshot immutability.
  - Additional inode flags mark vault namespace and descendants.
- Unlinked-inode lifetime follows the FUSE contract: an inode stays valid until the
  kernel forgets it, not only until its last handle closes.
  - `FsCore` counts kernel lookup references per inode: entry replies from `lookup`,
    `create`, `mknod`, `mkdir`, `symlink` and `link` add one, and `FORGET`/`BATCH_FORGET`
    subtract `nlookup`. READDIRPLUS is not negotiated, so it adds no references.
  - Removing the last name of a regular file (unlink or rename-over) deletes it at once
    only if no open handle and no kernel reference remains. Otherwise the inode is kept
    with `nlink = 0`, and an orphan index key `O<ino>` is written in the same transaction.
  - `release` and `forget` run the final delete (payload, inode and orphan key) under
    the global write lock once both counts reach zero. A `FORGET` failure has no reply
    and is logged.
  - Before mounting, startup deletes every indexed orphan, because no kernel references
    survive a restart. Orphans left by builds before B013 have no index key and are not
    reclaimed.
  - Without this, the kernel's post-unlink timestamp `SETATTR` (writeback cache) got
    `ENOENT`, which it recorded as a writeback error that failed every later `syncfs`.

## Vault Data Path

1. `verfsnext crypt -c`:
   - the CLI process generates the key material and writes `verfsnext.vault.key` itself (`create_new`, mode `0600`, fsynced with its directory), so the file belongs to the calling user even when the daemon runs as another user; an existing key file is never overwritten, and the new file is removed if vault creation fails
   - the key material is sent to the daemon in the `vault_create` control request (or used directly in offline metadata mode)
   - the daemon generates a random 256-bit vault folder key and persists the wrapped folder key under `SYS:vault.wrap`
   - the daemon creates the top-level `/.vault` inode+dirent (root-only reserved path), mode `0700`, owned by the requesting user: the control socket peer credentials (`SO_PEERCRED`) in mounted mode, the CLI process's uid/gid in offline mode
2. While vault is locked:
   - `/.vault` is hidden from root `readdir`
   - `lookup` and direct inode operations against vault entries return inaccessible/not-found semantics
3. `verfsnext crypt -u`: the CLI reads the key file as the calling user and sends its content in the `vault_unlock` request; the daemon never opens key files. The daemon unwraps the folder key into process memory and flips runtime state to unlocked.
4. Vault writes:
   - chunk payload is compressed
   - compressed bytes are encrypted with XChaCha20-Poly1305 and random 192-bit nonce
   - encrypted payload is appended to pack
   - nonce + encryption flag are persisted in the vault-domain chunk record (`C` + `0xA7` + hash), which can never share a record with a plain chunk
5. Vault reads:
   - the vault-domain record is loaded from the pack (a plain record with the same 128-bit hash is a different record and is never consulted)
   - the record must carry `CHUNK_FLAG_ENCRYPTED`; otherwise the read fails as inconsistent metadata
   - encrypted payload is read from pack
   - payload is decrypted using in-memory folder key and the record's nonce; if the most recent copy of the chunk in its pack fails authentication, the other indexed copies are tried (see `src/data/pack.rs`)
   - decrypted compressed bytes are decompressed and returned
6. `verfsnext crypt -l` clears in-memory key material and invalidates vault-related caches.

## `.DISCARD` Flow

1. Metadata-stage GC scans for zero-ref chunks.
2. For each candidate chunk, GC emits a CRC-protected discard record (`pack_id`, `chunk_hash128`, `block_size_bytes`, `epoch_id`).
3. `SYS:gc.discard_checkpoint` is advanced only after `.DISCARD` append + sync.
4. Pack-stage GC reads records up to checkpoint, chooses packs by reclaim byte/percent thresholds, and briefly drains in-flight writes before reading live hashes, so every record already in the pack has its metadata committed (this also covers a pack that rotated while a write that appended to it was still committing). For the active pack that drain also fixes a cutoff offset: records below it are filtered, and records appended while the copy runs are carried over unchanged, with appends waiting only while that tail is copied and the files are swapped; the same pack id then stays open for appends. It fsyncs the metadata WAL, rewrites keeping every chunk that still has a metadata record (zero-ref records are reclaimed only after the scan stage deletes them, and every indexed copy of a live hash is kept so vault ciphertext still resolves to the copy that matches its nonce), and swaps rewritten pack/index files crash-safely. The bulk copy does not hold the read path's swap lock.
5. Consumed discard entries are removed via atomic discard-file rewrite and checkpoint reset to the new file length.
6. Offline rebuild command (`gc offline`) first deletes every zero-ref chunk record (the online scan-phase work, safe because the daemon is stopped and holds no metadata lock), then rewrites `.DISCARD` from scratch by walking pack indexes pack-by-pack and marking entries as dead when the chunk metadata is missing, zero-ref, or points to a different pack; it then sets `SYS:gc.phase = 1` so the next GC work starts at the pack stage.
7. `gc offline --run` immediately executes the pack-stage rewrite loop (no metadata scan phase), honoring the configured reclaim thresholds (`gc_pack_rewrite_min_reclaim_bytes` / `gc_pack_rewrite_min_reclaim_percent`).
   - The rewrite keeps every chunk that still has a metadata record (B006). Because step 6 deleted the zero-ref records first, the offline rewrite still reclaims chunks of files deleted since the last online scan.

## Pack-Size Compatibility and Migration

1. On startup, if `SYS:pack_max_size_mb` is missing, it is written from `config.pack_max_size_mb` (legacy upgrade path).
2. On startup, if persisted and configured pack size differ, mount fails fast and logs an error.
3. To change pack size safely:
   - stop daemon
   - run `verfsnext pack-size-migrate`
   - confirm prompt
4. Migration rewrites chunk payloads into newly allocated pack IDs (above existing max), updates `ChunkRecord.pack_id`, updates `SYS:active_pack_id`, and then moves previous pack files to a backup directory under `data_dir`.
5. Old packs are not deleted automatically; operator removes backup after validation.

## Pack-Index CRC32 Compatibility Migration

1. On startup, before `PackStore::open`, VerFS checks `SYS:pack_index_crc32_migration_v1`.
2. If the marker is missing, VerFS scans every pack file (`*.vpk`) and rewrites every pack index (`*.idx`) with the new fixed-size record format that includes `payload_crc32`.
3. The CRC32 is computed from the stored payload bytes in the pack record (so migration works even when `/.vault` is locked).
4. Each rewritten index is written to a temporary file and atomically renamed into place.
5. After all indexes are rewritten, VerFS writes the migration marker and initializes `SYS:pack_crc32_read_errors` if it does not already exist.

## Vault Chunk Key Compatibility Migration

1. On startup (and before offline snapshot/pack-size/GC commands touch chunks), VerFS checks `SYS:vault_chunk_keys_migration_v1`.
2. If the marker is missing and a vault exists (`SYS:vault.wrap` present), it scans chunk records in batches of 10,000. Records with `CHUNK_FLAG_ENCRYPTED` are moved from the plain key (`C` + hash) to the vault key (`C` + `0xA7` + hash) in the same transaction that removes the old key; an inline payload (`T` + hash) moves with its record.
3. No payload is decrypted, so the migration runs while the vault is locked. The plain key space is untouched, so plain chunks need no migration.
4. Each batch is atomic and already-tagged records are skipped, so a crash or a failed startup resumes safely on the next run. The marker is written only after the scan completes.
5. Files whose bytes were lost to the old cross-domain alias cannot be recovered by the migration; `verfsnext check` reports them as missing pieces after the upgrade. See `docs/migration-guide.md`.

## Benchmark: ComfyUI Profile (`bench_comfyui_profile`)

Mirrors the I/O profile of a typical ComfyUI installation folder (72K small files in a Python venv + 110 large model files):

| Phase | Files | Total Data | What It Measures |
|-------|-------|-----------|------------------|
| `sm_write_dura` | 510 small (1 KB – 1 MB) | ~23 MB | Write + fsync through FUSE |
| `sm_read` | 510 small | ~23 MB | Sequential readback + sha256sum |
| `lg_write_dura` | 2 (64 MB + 128 MB) | 192 MB | Large sequential write + fsync |
| `lg_read` | 2 | 192 MB | Large file readback + sha256sum |
| `sync_barrier` | — | — | System-wide `sync()` final barrier |

**Key design for accurate measurement:**
- Each file is written with `dd conv=fsync`, which triggers `batcher.drain()` + `sync_cycle()` through FUSE — guaranteeing data is on disk before the timer stops.
- A system-wide `sync()` at the end acts as a final barrier against kernel writeback caching.
- File sizes are deterministic (index-based formulas) and content uses a seeded LCG so dedup cannot collapse payloads.

**Run:**
```
VERFSNEXT_RUN_MOUNT_TESTS=1 cargo test bench_comfyui_profile --test rsync_integration -- --nocapture
```
Requires FUSE + Linux + `mountpoint`, `fusermount`, `bash`, `dd`, `sync`, `sha256sum`, `python3`.

## Validation Run

Executed after write-path concurrency/pipeline changes:

- `cargo build --release`
- `cargo test --release`

Build completed successfully in this repository state.

## Detached reliability validation

The OS-level harness in `scripts/resilience/` runs on a dedicated Proxmox LXC,
with an external persistent supervisor, fsynced expected manifests, SHA-256 checks,
concurrent readers, fsync-acknowledged POSIX mutations, interrupted copies, daemon
and container failures, kills during GC pack rewrites, snapshot churn and recovery,
snapshots, vault data, and idle GC windows. It freezes the test guest and preserves diagnostics
on failure. See [Detached LXC resilience test](resilience-test.md) for its durability
oracle, deployed resource limits, evidence, operating commands, and coverage limits.

That harness cannot lose unflushed writes: the Proxmox kernel keeps its page cache
across process kills and container stops. Power loss is tested separately in a VM by
`scripts/powercut/`: the data filesystem sits on `dm-log-writes`, and crash images are
rebuilt at sampled log positions with every write after the last flush kept, dropped
or torn. The daemon must recover each image with every fsync-acknowledged operation
intact, survive a second crash after recovery, and survive a second recorded power cut
during recovery. See [Power-cut crash-consistency test](powercut-test.md).

Normal mount shutdown first runs `syncfs` on the mount while requests are still
served, so pages held by the kernel writeback cache reach the filesystem. It then
closes the FUSE request gate and waits for admitted requests to finish (the
request buffer pool stays open until they have returned their buffers); requests
read after that are left unanswered and fail with `ECONNABORTED` when the daemon
releases the device, never with `EIO` from a closed metadata store. Only then does
it drain writes, sync packs and metadata, and explicitly detach the mount. Root uses `MNT_DETACH`,
consistent with non-root `fusermount -uz`, so open descriptors do not leave a stale
mount through `EBUSY`. Failures in signal handling, control-task completion, final
sync or unmount propagate to the daemon's exit status. An explicitly attempted
unmount relinquishes session ownership and is never retried in its destructor.

The reliability oracle uses strict filesystem traversal and records connection
errors separately from complete content mismatches. Every recovered mount is
verified against its external durable checkpoint before fault observations are
classified. Per-revision source paths and saved provenance preserve the exact
code behind each traceback. See B010 in [Bug fix history](bug-fix-history.md).

## Reachable data statistics

Deduplication uses all reachable file inodes in the live tree, snapshots and vault
(including the locked vault). Hard links count once per namespace inode.
`referenced_data_bytes` sums their whole CDC extent lengths, excluding sparse holes.
`stored_unique_uncompressed_bytes` and `stored_unique_compressed_bytes` sum each
reachable chunk once. Zero-reference and unreachable chunks are excluded even
before GC; consistency checks still audit every extent and chunk record.
Dedup bytes saved = referenced data - unique uncompressed data; the percentage
is that saving divided by referenced data (zero for an empty population).
Invalid negative dedup accounting is an explicit error, not clamped to zero.
Format 2 stores whole useful CDC chunks without block padding. This statistics
change does not modify storage or require a migration.

Compression bytes saved = reachable unique uncompressed bytes - reachable unique
stored payload bytes; percentage = saved / unique uncompressed (zero when empty).
Negative savings remain visible, including encryption overhead in vault payloads.
Deduplication, sparse holes, stale chunks, metadata and indexes are excluded.
Net bytes saved = all reachable logical bytes - allocated data-directory disk
bytes; percentage = saved / all logical (zero when empty). Disk consumption sums
`st_blocks * 512` for files and directories without following symlinks, including
metadata, indexes, pack headers, duplicates and garbage. Disk scans occur after
the metadata transaction and can reflect concurrent writes/GC.
`unreachable_chunk_compressed_bytes` measures only unreachable chunk records
still indexed in metadata. It is explicitly a partial garbage measure: obsolete
pack entries whose chunk records have already been removed or relocated also
consume disk until pack rewrite. It must not be presented as total reclaimable
space. GUI and CLI use the daemon's same signed savings and percentage fields.

## Kept statistics scan

A whole-tree statistics scan reads every namespace, extent and chunk record.
On the live dataset that scan was dominated by the metadata iterator: every
entry of every range scan resolved its key through a full point lookup, so the
scan took ~28 s of CPU for ~200 MiB of metadata. The iterator now skips that
lookup when the state it walks cannot hold a range tombstone and the newest
version the snapshot can see is a plain `Set`/`Replace` — the merge iterator
already yields that version first, so it is its own resolution. Merges, unknown
kinds and any state that may hold a range tombstone keep the old resolution (see
the SurrealKV architecture guide, "Read behavior"). On the live dataset the scan
now takes ~2.5 s.

Because a scan over a large tree can still take seconds, the daemon keeps the
last completed scan and answers from it:

- `collect_stats` (on the blocking pool) records every completed scan as a
  `StatsSnapshot` — `scanned_at_ms` plus the `VerFsStats` — in memory and in
  `<data_dir>/stats-snapshot.json`, written to a temporary file and renamed into
  place so a reader never sees a torn file. It is a cache, not metadata: a
  missing file (first run after setup) is normal, and an unreadable or
  unparseable one is logged and ignored, being replaced by the next scan.
- `stats { allow_cached: true }` (the desktop app) answers from the kept
  snapshot in milliseconds and starts one background scan when the kept one is
  at least 30 s old (`STATS_REFRESH_MIN_AGE_MS`). At most one background scan
  runs at a time; the answer carries the scan's timestamp, whether a refresh is
  running (`stats_refreshing`) and the last refresh failure
  (`stats_refresh_error`), which the app shows in place of the timestamp. The
  app re-asks every 2 s while a refresh runs, so the fresh numbers appear as
  soon as the scan finishes. With no kept snapshot at all — the first start
  after setup — the request waits for the first scan.
- `stats { allow_cached: false }` (the CLI `stats`) always waits for a scan
  taken after the request arrived, so the printed table is never stale.
- The file survives reboots, so the first window opened after a boot shows the
  numbers of the last session's scan, labelled with their time, and the daemon
  replaces them when the background scan finishes. A daemon restart loads the
  file at startup.

The scan is a read-only metadata transaction and runs on the blocking pool, so
it never blocks FUSE reads; a 128 MiB read and a 22k-entry `find` during a
background refresh were unaffected.

## Records Check and Damaged-File Cleanup

The Overview health tile reports counters (`missing_chunk_records_for_extents`,
`chunk_refcount_mismatch_count`, `orphan_extent_records`,
`pack_crc32_read_error_count`) but a counter does not tell the user which file
they lost. `src/fs/health.rs` turns those counters into a report that names the
files, and the desktop app shows it and can move the damaged files out of the
way.

- `verfsnext check` (socket only, like `stats`) sends `ControlRequest::CheckRecords`;
  the daemon answers with the rendered text and the structured
  `RecordsCheckReport` field. The desktop app calls the same request.
- `FsCore::check_records` runs one read-only metadata transaction with four
  passes: extents (reference count per chunk id — the owning inode's vault flag
  plus the extent hash — and orphan extents), chunk records (missing ids,
  reference-count mismatches, and chunks whose stored copy is damaged via
  `CHUNK_FLAG_CORRUPT` plus the in-memory `corrupt_chunks` set), affected
  extents grouped per owning inode, and a namespace walk that attaches the
  current path to every reachable affected inode (`path: null` when no
  directory entry reaches the inode). Matching extents and records by id is
  what reports a vault file whose chunk was aliased onto a plain record as a
  missing piece instead of a refcount coincidence.
- The counters are computed exactly like `collect_stats`, so the tile and the
  report never disagree. Each file list is sorted by path and capped at 200
  entries, with `list_truncated` set when the cap bites.
- Files with missing pieces are unrecoverable by the filesystem: the chunk
  record that told it where the data lived is gone. The report is therefore a
  repair list for the user (restore the file from a backup, or discard it), not
  something the filesystem can fix on its own. Reference-count drift and orphan
  extents are reported without an action: the data is readable and the
  bookkeeping is reclaimed by normal GC.

Cleanup runs in the GUI process with plain `std::fs` against the mount, not in
the daemon, so a move never has to read file data:

- `moveDamagedFiles` (QML passes the relative paths from the report) renames
  each file into `lost+found/<timestamp>/<relative path>` (created with
  `create_dir_all`), keeping its place in the tree. Vault files go to
  `.vault/lost+found/<timestamp>/…` instead, because a rename cannot cross the
  vault boundary (EXDEV). A rename is metadata-only, so it works on a file that
  cannot be read. Nothing is deleted. Paths with `.`/`..`/empty components are
  refused.
- The app only offers actions the user can carry out: files in `.snapshots/`
  (read-only), vault files while the vault is locked, and inodes without a
  folder entry show the reason instead of a button, and the backend refuses
  them with the same explanation.
- `deleteLostFoundFiles` permanently deletes files whose paths start with
  `lost+found/` or `.vault/lost+found/` (anything else is refused), then
  prunes the folders the move created, bottom-up, while they are empty.
  Deleting a quarantined file is what releases its extents and chunk
  references for GC.
- Both actions report per-file results: files that were already gone (a stale
  report) and files that could not be moved (for example one inside a read-only
  snapshot) are listed with their error instead of failing the whole batch, and
  the report and stats are refreshed afterwards.
- `qml/RecordsReport.qml` (opened by the Health tile) lists the files, explains
  what happened to each group and offers "Show in folder", "Move to
  lost+found", and — for files already waiting there — "Open lost+found
  folder" and "Delete permanently", both behind `ConfirmPopup`.

## Recent throughput

One daemon task samples cumulative successful FUSE read/write byte counters
every second. Rates are counter deltas divided by the actual monotonic elapsed
interval, returned as `throughput_sample_secs`, `read_throughput_bps` and
`write_throughput_bps` in both stats and cheap status. Delayed scheduling extends
the reported interval; no metadata, disk I/O or FUSE-path locks are involved.
A short metrics mutex only publishes/copies the sample. Sampling is independent
of clients, so requesting stats after a long idle period still reports recent
traffic, and multiple clients do not reset each other's baselines. Before the
first sample, the interval and rates are zero. A new daemon creates a fresh
baseline; idle samples return zero. These are FUSE transfer rates: reads served
by the kernel page cache do not increment the daemon's counters.

The same sampler keeps the last second that moved data — its two rates plus the
RSS, cache hit rate and approximate cache memory read at that moment — in
`DaemonStatus.last_activity`, with `ago_secs` computed when the response is
built. Cheap status therefore carries the live rates, the RSS, the cache hit
rate and the approximate cache memory as well. The desktop app shows the live
readings while a sample moves data and the kept ones afterwards, labelled with
how long ago they were taken, so the Memory card still shows the numbers of the
last copy after it ends and after the window is closed and reopened. The record
lives for the daemon's lifetime: restarting the daemon (or the app that owns it)
clears it, exactly like the cumulative totals.

The GUI reads rates from its existing two-second status poll, retaining the
30-second full metadata refresh. CLI stats displays the latest completed sample
and its exact interval. Cumulative I/O and uptime remain separately labeled;
the old traffic/uptime lifetime average is removed. No storage migration.
