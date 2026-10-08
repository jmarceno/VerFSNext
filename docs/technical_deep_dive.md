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
- Offline pack-size migration control path:
  - `verfsnext pack-size-migrate`
- Global config-file CLI option:
  - `verfsnext --config <path> ...`
  - `verfsnext -c <path> ...`
- Entry points (see "Desktop App"):
  - bare `verfsnext` opens the desktop app (builds with the default `gui` feature); a headless build (`--no-default-features`) mounts instead
  - `verfsnext mount` mounts with config discovery; `verfsnext --config <path>` (no command) mounts with that file
- The crate is a library (`src/lib.rs`, `verfsnext::run`) plus a thin `src/main.rs`. cxx-qt links its generated C++ objects into every target of the package, so the Qt bridge must live in a library that all targets (including `tests/`) link.
- Mounted control-plane socket at `<data_dir>/verfsnext.sock` accepts snapshot/crypt/stats commands from CLI control mode.
  - Protocol types and the client live in `src/control.rs`, shared by the CLI and the desktop app.
  - `stats` responses carry both the rendered table (`message`) and the structured `VerFsStats` (`stats`, serde-optional so old clients/daemons interoperate).
  - `status` is a cheap request for frequent polling (`DaemonStatus`: vault enabled/initialized/locked, GC running, uptime, I/O totals). It never scans metadata, unlike `stats`.
- The daemon shuts down gracefully (final sync, unmount) on SIGINT and on SIGTERM.
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
  - Pack stage: rewrites eligible non-active packs by reclaim threshold and atomically replaces pack + index files
  - Offline recovery path: `verfsnext gc offline [--run]` rebuilds `.DISCARD` by scanning pack indexes against current chunk metadata (including orphaned/stale pack entries) and can optionally run only the pack-stage rewrite loop afterward

## Runtime Layout

- `src/fs/mod.rs`
  - `VirtualFs` implementation
  - global mutation gate is now an async `RwLock`:
    - write-side lock for namespace/global metadata mutations (rename/unlink/rmdir/link/xattr/vault/snapshot)
    - read-side lock for file-content writes/truncates so unrelated inode writes can continue concurrently
  - per-inode async write locks are allocated lazily and weakly cached (`inode -> Weak<Mutex<()>>`)
  - write path stages unique missing chunks, compresses them in parallel, appends to pack, then commits metadata
  - read path resolves chunk location through pack-local hash index
  - read path validates pack payload CRC32 from `.idx`; mismatches are logged and counted and reads continue
  - bounded chunk metadata and chunk payload caches
  - committed metadata read caches (bounded `moka`) for inode and dirent lookups with post-commit invalidation on mutation paths
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
  - read-only inode flag enforcement across mutating FUSE operations
  - exposes snapshot create/list/delete methods used by control socket server in mounted mode
  - enforces `.vault` lock-state visibility and access policy
  - blocks rename of top-level `/.vault` and blocks cross-boundary rename/link between vault and non-vault trees
  - marks vault inodes using inode flags and applies data encryption/decryption only for those inodes
  - provides `create_vault`, `unlock_vault`, and `lock_vault` runtime operations
  - background GC trigger integrated into periodic sync cycles with strict idle gating (recent activity checks plus write-lock contention checks before pack rewrite work)
  - directory handles now keep a per-`opendir` snapshot for stable pagination while the namespace is mutating (prevents recursive-delete entry skips)
  - file-handle read-plan cache uses bounded `moka` entries keyed by `fh`; plans are version-checked against inode data version before reuse
  - session-level FUSE invalidation notifier is installed at mount and used for best-effort post-commit invalidation dispatch

- `src/fs/write.rs`
  - `apply_batch` coalesces adjacent writes, then splits the groups into rounds in which every inode appears at most once (per-inode order preserved); each round is staged into one SurrealKV transaction and committed before the next round starts
  - every read a write plan depends on (inode, old extents, dedup check) goes through the batch transaction; whatever the plan depends on is either in the transaction snapshot or rewritten by it (inode key, touched extents, chunk refcounts), so concurrent changes surface as commit conflicts, after which the round is re-planned and retried
  - a group that fails to stage is reported failed and the remaining groups are re-staged on a fresh transaction; a write is acknowledged only after the transaction containing it commits
  - packs are synced before every metadata commit; a sync failure fails the batch
  - truncate stages inode, extent deletions, boundary chunk and refcount deltas in one transaction with the same retry loop
- `src/meta/mod.rs`
  - `write_txn` takes an `FnMut` closure and re-runs it on a fresh transaction when the commit fails with `TransactionWriteConflict` or `TransactionRetry` (up to `MAX_COMMIT_ATTEMPTS`); closures must derive all writes from reads through `txn` and have no other side effects
  - read-modify-write of metadata must always read through the committing transaction (never from caches or a separate read transaction)

- `src/write/batcher.rs`
  - queue ingestion no longer blocks on `sink.apply_batch`
  - ingestion flushes pending ops into an internal apply queue and immediately resumes receiving FUSE writes
  - apply worker preserves batch order, applies each batch, and resolves per-write completion channels
  - `drain` and `shutdown` are implemented as ordered barriers in the apply queue

- `crates/verfsnext-async-fusex/src/fuse_fs.rs`
  - fixed `readdir`/`readdirplus` cookie progression to use monotonic entry index cookies (`i + 1`)
  - `readdir` now stops filling once the reply buffer is full, matching `readdirplus` behavior and preserving correct continuation semantics
  - entry responses now carry separate entry TTL and attr TTL values end-to-end

- `crates/verfsnext-async-fusex/src/session.rs`
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
  - chunk refcount delta handling on create/delete

- `src/gc/mod.rs`
  - `.DISCARD` binary header/record encode-decode and CRC32C validation
  - checkpoint-bounded record reads
  - atomic discard-file rewrite/rotation primitive used by GC pack stage

- `src/data/pack.rs`
  - Pack record format `VPK2` archived with `rkyv`
  - Sidecar index file per pack (`.idx`) uses fixed-size archived `rkyv` records including `payload_crc32`
  - Pack header and index record decode paths use checked `rkyv::access` for zero-copy validation
  - Automatic active-pack rollover on append when configured pack size target is reached
  - Existing packs are discovered on startup; highest pack id becomes active if metadata lags
  - Hash lookup path reads index entry first, then seeks pack payload
  - CRC32 is computed on stored payload bytes (ciphertext for vault chunks, compressed/raw payload for non-vault chunks)
  - Supports encrypted-payload reads (`read_chunk_payload`) for vault decrypt-then-decompress flow
  - Index is rebuilt from pack data when missing (including non-active packs loaded at startup)
  - GC pack rewrite keeps every indexed copy of each hash that still has a chunk record (any refcount), verifies each payload against its index CRC32 (aborts on mismatch), and swaps files crash-safely: remove old index, rename pack, rename index, fsync the directory after each step; a crash in between leaves a pack without an index, which startup rebuilds
  - readers hold a shared swap lock across index lookup and payload read; the rewrite holds it exclusively during the swap and cache invalidation
  - failed appends and index flushes roll back partial writes; startup truncates an incomplete record at the end of the active pack and drops an index that no longer matches it
  - index-file lookups use the most recent entry for a hash, matching the cache
  - a pack may hold several copies of one hash (commit retries, pre-B006 re-appends); vault copies differ because each append uses a fresh nonce. `read_chunk_with` tries the most recent copy and then the other indexed copies (newest to oldest) until the caller's acceptance check passes (vault: AEAD authentication with the record's nonce plus decompression; non-vault: decompression). The accepted copy is cached, and the resolution is logged at error level
  - copies are never collapsed: GC rewrite and pack-size migration carry every indexed copy of a hash, in order, into the same pack (`append_chunk_copies` may exceed the pack size target to keep them together)

- `src/migration/pack_size.rs`
  - Compatibility guard for persisted `SYS:pack_max_size_mb`
  - Offline pack rewrite flow for pack-size changes:
    - rewrites all chunk payloads into new packs using configured size
    - updates chunk metadata `pack_id` mappings
    - resets GC discard cursor/phase and truncates discard file
    - moves old pack/index files into a backup directory for manual cleanup

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

Built with the default `gui` feature (Qt 6 Quick via cxx-qt 0.10, tray via `ksni`); `--no-default-features` builds the headless daemon/CLI without Qt, D-Bus or tray libraries (the systemd installer does this). The UI follows the shared design of the other desktop apps (GravaAI, Lepramim, Celestial): same palette (`qml/VerfsTheme.qml`), flat `qml/` directory, every QML file listed in `build.rs`, Basic Quick Controls style, software rendering by default, QML written for Qt 6.2.

- `src/gui/mod.rs` — startup: single instance (`$XDG_RUNTIME_DIR/verfsnext/app.sock`; a second launch shows the open window), waits up to 120 s for a StatusNotifier tray host, owns a 2-thread tokio runtime for control-socket requests, loads `qml/Main.qml`, QML-load watchdog.
- `src/gui/controller.rs` — the `AppController` QObject. Invokables run on the Qt GUI thread and never block: every process spawn, socket request, metadata scan and directory listing runs on a worker thread and comes back as an `Event` drained by `tick()` (100 ms QML timer). One user-visible operation (start/stop/restart/snapshot/vault/mode switch/folder change) runs at a time (`busy`).
- `src/gui/daemon.rs` — runs the filesystem in one of two modes. **User service**: `~/.config/systemd/user/verfsnext.service` (`ExecStart=<exe> --config <config>`, `KillSignal=SIGINT`, `TimeoutStopSec=3000`, `WantedBy=default.target`); the unit file existing is the single source of truth for the mode, and it is rewritten when the app binary moved. Installing writes, reloads and enables the unit and removes it again if systemd refuses it, before anything is stopped. **App**: the daemon is a child process (`--config <config>`, output appended to `~/.local/state/verfsnext/daemon.log`) stopped with SIGINT on quit; a daemon the app did not spawn is stopped through the control socket's peer PID (`SO_PEERCRED`). Status is probed every 2 s (control socket connect, `systemctl --user show ActiveState`, child `try_wait`, plus the cheap `status` request); a stale FUSE mount (`ENOTCONN`) is detected and repaired with `fusermount -u -z` before a start. Location checks: absolute paths, mount point and data folder not nested, mount point empty, data folder empty or existing VerFSNext data (`metadata/` + `packs/`; a folder with other files is refused because startup normalizes permissions of the whole data tree), writable parent, free space.
- `src/gui/settings.rs` — friendly metadata (group, label, help, display unit/scale, advanced, setup-only) for every config key. Building the editor model or writing the file fails when a key has no entry, so new config options cannot silently go missing from the app. `pack_max_size_mb` and `gc_discard_filename` are setup-only (changing them later needs a migration or orphans files). Validation errors from `Config::validate` are shown with the friendly labels. The config is written atomically as commented TOML.
- `src/gui/paths.rs` — the managed config is `~/.config/verfsnext/config.toml` (the same file CLI discovery finds); app-only preferences (last vault key file) are in `~/.config/verfsnext/desktop.toml`.
- `src/gui/tray.rs` — status line, open folder, take snapshot, start/stop, control center, quit. Quit leaves a user service running; in app mode it stops the daemon (final sync) before exiting.
- `src/gui/desktop.rs` — app-menu and login-autostart entries (bare executable), icon install, desktop notifications for results that arrive while no window is open.
- First run (no config): welcome → background service or app-only → recommended settings (customizable) → mount point and data folder (in-app folder picker with New Folder) → summary → start. The window closes once the folder is mounted; the app stays in the tray.
- Control center pages: Overview (state, space saved, dedup/compression, health, activity, memory, every `stats` field; the full stats scan refreshes on demand and at most every 30 s while the page is visible), Snapshots, Vault (create with key file / unlock / lock), Folders & Startup (move folders, service mode, login autostart, stale-mount repair), Settings (all config options, restart prompt when running).

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
   - block payload is compressed
   - compressed bytes are encrypted with XChaCha20-Poly1305 and random 192-bit nonce
   - encrypted payload is appended to pack
   - nonce + encryption flag are persisted in chunk metadata
5. Vault reads:
   - encrypted payload is read from pack
   - payload is decrypted using in-memory folder key and the record's nonce; if the most recent copy of the chunk in its pack fails authentication, the other indexed copies are tried (see `src/data/pack.rs`)
   - decrypted compressed bytes are decompressed and returned
6. `verfsnext crypt -l` clears in-memory key material and invalidates vault-related caches.

## `.DISCARD` Flow

1. Metadata-stage GC scans for zero-ref chunks.
2. For each candidate chunk, GC emits a CRC-protected discard record (`pack_id`, `chunk_hash128`, `block_size_bytes`, `epoch_id`).
3. `SYS:gc.discard_checkpoint` is advanced only after `.DISCARD` append + sync.
4. Pack-stage GC reads records up to checkpoint, chooses packs by reclaim byte/percent thresholds, fsyncs the metadata WAL, rewrites keeping every chunk that still has a metadata record (zero-ref records are reclaimed only after the scan stage deletes them), and swaps rewritten pack/index files crash-safely.
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
concurrent readers, interrupted copies, daemon and container failures, snapshots,
vault data, and idle GC windows. It freezes the test guest and preserves diagnostics
on failure. See [Detached LXC resilience test](resilience-test.md) for its durability
oracle, deployed resource limits, evidence, operating commands, and coverage limits.

Normal mount shutdown retains the FUSE session while draining writes and syncing
packs and metadata, then explicitly detaches the mount. Root uses `MNT_DETACH`,
consistent with non-root `fusermount -uz`, so open descriptors do not leave a stale
mount through `EBUSY`. Failures in signal handling, control-task completion, final
sync or unmount propagate to the daemon's exit status. An explicitly attempted
unmount relinquishes session ownership and is never retried in its destructor.

The reliability oracle uses strict filesystem traversal and records connection
errors separately from complete content mismatches. Every recovered mount is
verified against its external durable checkpoint before fault observations are
classified. Per-revision source paths and saved provenance preserve the exact
code behind each traceback. See B010 in [Bug fix history](bug-fix-history.md).
