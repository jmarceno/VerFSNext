## B001 - Write-Read Data Integrity (Write-Read Coherency Fix) - May 01 2026

### Root Cause

Three interrelated bugs caused read-after-write data corruption (manifesting as `inflate: data stream error (unknown compression method)` during git clone):

1. **Write-path ordering bug** (`src/fs/write.rs:38-42` and `src/fs/write.rs:312-314`): In `apply_single_write` and `truncate_file_locked`, `bump_inode_data_version` was called **before** `invalidate_inode_cache`. This created a window where a concurrent read would see the new data version (plan cache miss → rebuild) but load a **stale inode** from the still-valid cache entry, returning truncated or zero-fill data.

2. **Stale SmallFileReadPlan returns zeros** (`src/fs/fuse.rs:798-810`): When a cached read plan's `file_size` didn't match the inode's actual size, the code returned zeros to the caller instead of rebuilding the plan with fresh metadata. This affected files ≤ 4 MiB.

3. **No batcher drain before read** (`src/fs/fuse.rs:705`): The FUSE `read()` handler never drained the write batcher. With FUSE writeback cache enabled (`FUSE_WRITEBACK_CACHE` in `crates/verfsnext-async-fusex/src/session.rs:97`), fire-and-forget writes (`FUSE_WRITE_CACHE` flag) could be pending in the batcher while a read returned stale data.

### Fixed: Write-Path Ordering

In `apply_single_write`, the order changed from:
```
commit → bump_version → invalidate_cache
```
to:
```
commit → invalidate_cache → bump_version → invalidate_attr
```

Same change applied to `truncate_file_locked` and `setattr` (in `fuse.rs`).

By invalidating the inode cache **before** bumping the data version, any concurrent read that observes the new version is guaranteed to load a fresh inode from metadata rather than a stale cached copy.

### Fixed: Stale Plan Returns Actual Data

When `SmallFileReadPlan.file_size != inode.size`, the code now rebuilds the plan from current metadata and returns correct data instead of zero-fill.

### Fixed: Batcher Drain on Read

The `read()` handler now calls `self.batcher.drain().await` after validating the file handle but before loading the inode. This ensures all fire-and-forget writes (from FUSE writeback cache) are committed before the read proceeds. The inode is loaded **after** the drain, so the read always operates on the latest committed state.

### Affected Files
- `src/fs/write.rs` — reorder invalidation before version bump (2 sites)
- `src/fs/fuse.rs` — drain batcher before read, rebuild stale plan instead of returning zeros, reorder invalidation before metadata update in setattr

## B002 - TOCTOU Race in Batcher Drain Counter - May 02 2026

### Root Cause

**B001's fix introduced a new bug.** The `can_skip_drain` optimization (added in B001) used an atomic counter (`writes_since_last_drain`) to track whether any fire-and-forget writes were pending. The counter was incremented in `mark_write_enqueued()` **before** `batcher.enqueue()`, creating this TOCTOU race:

```
Write thread:                    Read thread:
  mark_write_enqueued() → 1
                                   can_skip_drain() → false (counter=1)
                                   drain() → processes 0 writes
                                     (not yet enqueued!)
                                   mark_drained() → counter=0
  batcher.enqueue(op) → done
                                   (counter=0, writes pending!)

Next read: can_skip_drain() → true → SKIPS drain → returns stale data
```

For git clone, the stale data is zero-filled pages (for newly created pack files being written by index-pack). Git tries to inflate these zeros as zlib data and gets `inflate: data stream error (unknown compression method)`.

The race window is small but with large repos (29851 objects, 155 MiB) the probability is high enough to reproduce reliably.

### Fixed: Move Counter Ownership Into the Batcher

The pending-write counter was moved from `FsCore` (checked by `can_skip_drain` before the enqueue) into `WriteBatcher` itself, where it is incremented **after** the enqueue channel send succeeds. The drain method resets the counter to 0 **after** the drain completes.

Key changes:
1. `WriteBatcher` now owns `pending_count: Arc<AtomicU64>`
2. `enqueue()` increments `pending_count` AFTER `self.tx.send()` succeeds
3. `drain()` resets `pending_count` to 0 AFTER the drain response is received
4. `enqueue_and_wait()` does NOT increment the counter (the caller synchronously waits for the result)
5. All callers check `self.batcher.pending_write_count() > 0` instead of `self.core.can_skip_drain()`
6. Removed `writes_since_last_drain`, `mark_write_enqueued()`, `can_skip_drain()`, `mark_drained()` from `FsCore`

This eliminates the TOCTOU window entirely: `pending_count` is only incremented after the write is guaranteed to be visible to the drain flow, and only decremented after the drain has processed all writes.

### Regression Test

Added `git_clone_comfyui_manager_regression()` test that:
- Mounts VerFSNext with zero TTLs (no kernel caching, forces every read to go through FUSE)
- Clones `https://github.com/ltdrdata/ComfyUI-Manager` (29851 objects, ~155 MiB) onto the mount
- Verifies the clone succeeds (no inflate errors)
- Runs `git fsck --no-dangling` to verify repo integrity

### Affected Files
- `src/write/batcher.rs` — add `pending_count` field, increment on enqueue, reset on drain
- `src/fs/mod.rs` — remove `writes_since_last_drain` and related methods from `FsCore`
- `src/fs/fuse.rs` — remove `mark_write_enqueued()` from write handler, replace all `can_skip_drain`/`mark_drained` calls with `batcher.pending_write_count() > 0` check
- `tests/rsync_integration.rs` — add `git_clone_comfyui_manager_regression` test

## B003 - SurrealKV Write-Write Conflict Between CREATE and WRITE - May 02 2026

### Root Cause

The FUSE `create` handler uses a SurrealKV `write_txn` to create the inode, while the write handler's batched transaction (`apply_batch`) modifies the same inode key with `commit_prepared_write_txn`. Both run asynchronously on different tokio tasks, creating a write-write conflict window:

```
CREATE handler (tokio task A):        WRITE handler (tokio task B):
  begin_write → start_seq = N           begin_write → start_seq = N
  write inode_key(26)
  write dirent_key(...)                   
  write sys:next_inode                   
  commit_write_txn → seq++             
                                         write inode_key(26) (same key!)
                                         write extent_key(26, 0)
                                         commit_write_txn → conflict!
                                           inode_key(26) has seq N+3 > N
                                           → TransactionWriteConflict
```

The SurrealKV `check_keys_conflict` detects that a key in the write set was modified after the transaction started (seq of key > start_seq). The batch transaction aborts, and the inode metadata is never committed, leaving the file with `size=0` in SurrealKV. A subsequent read sees an empty file → `bad config line 1` in `.git/config`.

### Secondary Bug: Stale Chunk Cache on Retry

When the batch retried after a write conflict, `stage_chunk_if_missing` found the chunk hash in the in-memory `chunk_meta_cache` (populated by the first failed attempt's `materialize_pending_chunks`). It treated it as a dedup hit, skipping re-materialization. But the chunk record was never committed to SurrealKV (the first transaction was aborted), so `apply_ref_deltas_in_txn` failed with "missing chunk metadata" — or worse, on a subsequent commit attempt the extent was committed without the corresponding chunk record, rendering data inaccessible.

### Fixed: Transaction Retry with Rollback

`apply_batch` now retries up to 3 times on `TransactionWriteConflict`. On retry:
1. The active transaction is **rolled back** (clearing the write set) if any group op fails
2. A fresh SurrealKV transaction is created on retry, which sees a `start_seq` after the concurrent create handler's commit
3. A `tokio::task::yield_now()` is issued to ensure the concurrent handler's commit propagates

### Fixed: Move Cache Insertion After Commit

The root cause of the stale cache problem was that `materialize_pending_chunks` inserted into `chunk_meta_cache` before `commit_write_txn`. On retry, these uncommitted cache entries were treated as dedup hits, skipping re-materialization and leaving chunk records missing from KV.

**Fix: `materialize_pending_chunks` no longer inserts into `chunk_meta_cache`.** Instead, `apply_batch` collects the new chunk records from all writes and inserts them into the cache **after** `commit_write_txn` succeeds. This guarantees that the cache never contains entries from aborted transactions, and `stage_chunk_if_missing` can safely use the fast cache-only path without a SurrealKV verification.

A `known_new_hashes: HashSet<[u8; 16]>` tracks which hashes were confirmed as "new" in the first attempt, so retries skip redundant SurrealKV lookups and directly re-stage the chunks (the compressed data was already written to packs in the first attempt).

### Fixed: Move Version Bump After Metadata Commit

The `invalidate_inode_cache`, `bump_inode_data_version`, `mark_mutation`, and `invalidate_inode_attr_best_effort` calls were moved from inside `apply_single_write_in_txn` (which runs before `commit_write_txn`) to after `commit_write_txn` in `apply_batch`. This ensures concurrent readers that observe the new data version always find fresh metadata in SurrealKV.

### Affected Files
- `src/fs/write.rs` — retry on write conflict, move post-commit invalidation after commit, collect records and insert cache after commit
- `src/fs/chunk.rs` — remove cache insertion from `materialize_pending_chunks` (moved to post-commit in write.rs), keep `stage_chunk_if_missing` fast (no KV lookup)
- `tests/rsync_integration.rs` — `git_clone_comfyui_manager_regression` test

## B005 - FUSE Writeback Cache Fire-and-Forget Data Loss Window - May 03 2026

### Root Cause

With FUSE writeback cache enabled (default in `crates/verfsnext-async-fusex/src/session.rs:97`), the kernel caches `write()` data in its page cache and asynchronously flushes dirty pages via `FUSE_WRITE` requests with the `FUSE_WRITE_CACHE` flag. The FUSE write handler used `enqueue()` (fire-and-forget), which returned to the kernel **before** the batcher committed the data to SurrealKV and the pack file.

This created a window where:

1. Git writes pack data via `write()` → kernel page cache (dirty)
2. Kernel asynchronously flushes dirty page → `FUSE_WRITE` with `WRITE_CACHE` flag
3. FUSE handler calls `enqueue()` → returns immediately (data NOT committed)
4. Kernel considers write complete, may evict the page from its cache
5. Subsequent `read()` → kernel page cache miss → `FUSE_READ` sent
6. FUSE read handler drains batcher → but the fire-and-forget write may have been assigned a seq number but not yet reached the ingest worker's channel
7. Read returns stale metadata → git reads corrupt pack data → `inflate: data stream error`

The fundamental issue: the kernel's writeback pipeline and the batcher's commit pipeline are unsynchronized. The kernel considers the write durable when FUSE responds, but the batcher may not have committed the data yet.

**Why the existing drain did not prevent this**: The drain mechanism sends a `Drain` message through the same bounded channel as `Write` messages. When the channel buffer is congested (high writeback load), `enqueue().await` may suspend waiting for capacity. A concurrently arriving `Drain` message can enter the channel before the suspended `Write`, breaking the FIFO ordering guarantee that the drain relies on.

### Fixed: Synchronous Write Completion with Immediate Batch Flush

**Core fix**: All writes now use `enqueue_and_wait()` regardless of the `FUSE_WRITE_CACHE` flag. This guarantees the FUSE handler does not return to the kernel until the batcher has committed the data to SurrealKV and the pack file. The kernel's writeback pipeline is naturally synchronized: it sends `FUSE_WRITE`, waits for the response (now only after commit), and only then considers the page clean.

**Performance critical optimization**: `enqueue_and_wait()` creates a `oneshot` channel (`done_tx`) attached to the write. The batcher's ingest worker now detects writes with a `done` channel and **immediately dispatches the pending batch** instead of waiting for the timer (500ms) or size threshold (1024MB). This reduces the commit wait from up to 500ms to just the commit time (~2ms).

```
enqueue_and_wait flow (original, slow):
  Write → Channel → Ingest worker queues → Timer (500ms) → Dispatch → Commit (~2ms) → Response
                                                                  ↑ wait up to 500ms!

enqueue_and_wait flow (optimized):
  Write(done) → Channel → Ingest worker detects done → Immediate dispatch → Commit (~2ms) → Response
                                                                ↑ wait ~2ms only!
```

### Additional Fixes

- **`src/fs/write.rs`**: `packs.sync(true)` called **before** `commit_write_txn()`, ensuring pack data is on disk before extent pointers are visible to concurrent readers.
- **`src/fs/fuse.rs`**: Unconditional `drain()` in `read()` and `rename()` handlers (removed `pending_write_count() > 0` conditionals) as a safety net for any writes that might bypass the synchronous path.
- **`crates/verfsnext-async-fusex/src/session.rs`**: `FUSE_WRITEBACK_CACHE` remains **enabled** — the fix handles the consistency issue without disabling kernel caching.

### Performance

Full clone of `ComfyUI-Manager` (29851 objects, ~155 MiB):
- **Baseline** (no TTL, zero cache): 25s (original B001 fix)
- **Before fix** (release, 150ms TTL): 33% pass rate, ~8-9s on failure (early abort)
- **After fix** (release, 150ms TTL): 100% pass rate, 28-32s for full clone

The ~20% regression vs baseline is caused by the `packs.sync(true)` fsync before each batch commit. Without the sync, the original code synced after commit, creating a window where metadata pointed to unsynced data.

### Regression Test Updated

`git_clone_comfyui_manager_regression` now uses a **full clone** (removed `--depth=1`) to exercise the write-read coherency path with 29851 objects (~155 MiB). Full clone triggers the race condition that shallow clones avoid.

### Affected Files
- `src/fs/fuse.rs` — all writes use `enqueue_and_wait()`, unconditional drain in read/rename
- `src/write/batcher.rs` — immediate batch flush for writes with `done` channel
- `src/fs/write.rs` — `packs.sync(true)` before `commit_write_txn()`
- `tests/rsync_integration.rs` — full clone (no `--depth=1`)

## B004 - Root and .vault Directory Permissions/Ownership - May 04 2026

### Root Cause

Two permission/ownership issues:

1. **Root inode (`src/meta/mod.rs:39`)**: Created with `PERM_DIRECTORY_DEFAULT` (0o755), preventing unprivileged users from writing to the mount root. Should be 0o777 for easy multi-user access.

2. **`.vault` directory (`src/fs/vault.rs:125-126`)**: Created with `uid: root_inode.uid, gid: root_inode.gid`, inheriting the root inode's ownership instead of the `verfs crypt -c` caller's uid/gid. If a different user mounted the filesystem and another ran `crypt -c`, they couldn't unlock the vault.

### Fixed

1. Introduced `PERM_DIRECTORY_ROOT: u16 = 0o777` in `src/types/mod.rs` and used it for the root inode in `src/meta/mod.rs:39`. The `.snapshots` directory retains `PERM_DIRECTORY_DEFAULT` (0o755) since it is read-only by design.

2. `.vault` inode now uses `nix::unistd::getuid().as_raw()` and `nix::unistd::getgid().as_raw()` instead of `root_inode.uid` / `root_inode.gid`, ensuring the `.vault` directory is owned by the user who runs `verfs crypt -c`.

### Affected Files
- `src/types/mod.rs` — add `PERM_DIRECTORY_ROOT` (0o777)
- `src/meta/mod.rs` — import and use `PERM_DIRECTORY_ROOT` for root inode
- `src/fs/vault.rs` — use `getuid()/getgid()` instead of `root_inode.uid/gid`

## B006 - Delayed Data Corruption from Lost Metadata Updates and Unsafe GC - Oct 08 2026

### Symptom

Files that had been written correctly became unreadable (EIO, `missing chunk metadata`, `missing pack index entry`) or showed wrong/truncated content weeks after they were written. Nothing failed at write time; the damage surfaced only after GC had run on the affected packs.

### Root Cause

1. **Lost updates in SurrealKV commits** (`crates/verfsnext-surrealkv/src/transaction.rs`): `Transaction::commit()` validated write-write conflicts *before* taking the `write_admission` lock that serializes commits. Two transactions that wrote the same key could both pass validation and then both commit; the second silently overwrote the first. Chunk refcounts are read-modify-write counters touched concurrently by the write batcher, truncate, unlink/rename/release cleanup, snapshots and GC, so every lost update shifted a refcount. An under-counted chunk eventually reached refcount 0 while still referenced by extents, GC deleted its record and rewrote its pack without it, and every file referencing that chunk was destroyed. Inode records were affected the same way (e.g. a `setattr` reverting a size committed by a write). The memtable-history check also ran under different locks than the key check, so a background flush could hide a conflicting write.
2. **Read-modify-write outside the committing transaction** (`src/fs/fuse.rs`, `src/fs/write.rs`): `setattr` wrote back an inode loaded from the cache before its transaction; `truncate_file_locked` computed extent deletions and refcount deltas from a separate read transaction; the write path loaded the inode from the cache. Changes committed between the read and the transaction start were invisible to conflict detection and were overwritten (lost size changes, double refcount decrements).
3. **Same inode twice in one batch transaction** (`src/fs/write.rs`): every group was planned from committed state, so a second group for the same inode in the same transaction was planned without the first one: its bytes in a shared block were lost, the inode size could go backwards, and the old chunk of a block was decremented twice.
4. **Silent acknowledgement of rolled-back writes** (`src/fs/write.rs`): when a group failed on the last retry, `apply_batch` returned `Ok` for the other groups of the batch although their transaction had been rolled back. With the FUSE writeback cache this is silent data loss.
5. **Dedup trusted the in-memory chunk cache** (`src/fs/chunk.rs`): the cache can still hold a chunk GC has deleted (reads repopulate it), so a write could reference a chunk without metadata; on cache misses existing chunks were appended again, creating duplicate pack copies (for vault chunks with a different nonce than the committed record).
6. **GC pack rewrite** (`src/fs/gc.rs`, `src/data/pack.rs`):
   - only chunks with `refcount > 0` survived, so a writer that deduped against a zero-ref chunk while the pack was being rewritten referenced data that was then dropped;
   - the WAL was not fsynced before the destructive rewrite, so a power loss could roll metadata back to a state referencing chunks already removed from the pack;
   - the two renames (pack, then index) were not crash-safe; a crash in between left a new pack with an old index (reads fail with hash mismatch and the index is not rebuilt because it exists);
   - concurrent readers could pair an index offset from one pack generation with the file of the other;
   - dead index-cache entries were never invalidated (`invalidate_entries_if` fails without `support_invalidation_closures` and the error was discarded);
   - the first copy of a duplicated hash was kept instead of the copy the index points at, and payloads were re-checksummed without being verified, hiding bit rot.
7. **Pack append tearing** (`src/data/pack.rs`): a failed append (e.g. `ENOSPC`) left a partial record while `size_bytes` was not advanced, so every later record in that pack got a wrong offset in its index entry; a failed index flush left a partial entry that misaligned all later entries; a torn tail after a crash made the pack unparseable for sequential scanners once new records were appended after it.
8. **Unlinked file data freed while still open** (`src/fs/inode.rs`): `cleanup_unlinked_inode_if_closed` ran on every `release` without checking the open-handle count.
9. **Swallowed pack sync error** before the metadata commit, allowing metadata to point at unsynced pack data.

### Fixed

- SurrealKV: conflict validation runs under `write_admission`, and the memtable-history check is done under the same memtable locks as the key checks (`check_keys_conflict`).
- `MetaStore::write_txn` re-runs the closure on `TransactionWriteConflict` / `TransactionRetry` (closure is `FnMut`, up to `MAX_COMMIT_ATTEMPTS`); `is_retryable_commit_error` matches the typed error instead of a string.
- `setattr` applies attribute changes to the inode read inside its transaction and invalidates the inode cache after the commit.
- Truncate stages everything (inode, extents, boundary chunk) in one transaction, syncs packs before committing new chunks, and retries on conflicts.
- The write path reads the inode and old extents through the batch transaction; batches are split into rounds with at most one group per inode; a failing group is reported failed and the rest are re-staged on a fresh transaction; commits are retried on conflicts; a pack sync failure fails the batch.
- Dedup decisions read the chunk record through the committing transaction (`stage_chunk_if_missing`), so GC deletes and concurrent creators surface as conflicts.
- GC keeps every chunk that still has a record (any refcount), fsyncs the WAL before rewriting, keeps every indexed copy of a live hash in order, verifies each copied payload against its index CRC32 (aborts on mismatch), swaps files crash-safely (remove old index, rename pack, rename index, fsync the directory after each step), holds an exclusive swap lock that readers share, and invalidates the cache entries of every old index entry.
- Pack append and index flush roll back partial writes; startup truncates an incomplete record at the end of the active pack and removes an index that no longer matches it (rebuilt from the pack).
- Index-file lookups use the most recent entry for a hash, matching the cache.
- Unlinked inodes are cleaned up only once their open-handle count reaches zero.
- Refcount underflows and decrements of missing chunks are logged as errors.

### Follow-up: Offline GC Reclaim

Keeping zero-ref chunks in pack rewrites made `gc offline --run` reclaim much less, because it skips the scan phase that deletes zero-ref records. The offline command now deletes all zero-ref chunk records before rebuilding the discard list (`delete_all_zero_ref_records_offline` in `src/fs/gc.rs`). This is safe offline because no writer can dedup against those chunks while the daemon is stopped. The report prints `zero_ref_records_deleted`.

### Assessing Existing Damage

`verfsnext stats` reports chunk refcount mismatches, extents referencing missing chunk records, and orphan extents. Data already removed by GC cannot be recovered by this fix. Under-counted refcounts that have not yet reached zero remain wrong until recomputed; until then they are protected only while they stay above zero.

### Affected Files
- `crates/verfsnext-surrealkv/src/transaction.rs`, `crates/verfsnext-surrealkv/src/lsm.rs`
- `src/meta/mod.rs`
- `src/fs/write.rs`, `src/fs/chunk.rs`, `src/fs/fuse.rs`, `src/fs/inode.rs`, `src/fs/gc.rs`, `src/fs/mod.rs`
- `src/data/pack.rs`

## B007 - Vault Chunks Unreadable Because of Divergent Duplicate Copies - Oct 08 2026

### Root Cause

A pack can hold several copies of the same chunk hash. Before B006 they were created whenever the in-memory chunk cache missed for a chunk that already existed (the chunk was appended again while the refcount went to the existing record), and on every commit retry (the chunk was materialized again). For non-vault chunks the copies are byte-identical. Vault chunks are sealed with a random nonce per append, and the chunk record stores only one nonce, so the copies differ and only one of them decrypts.

Several places picked a copy without checking it against the record:
- the index cache and `prime_index_cache` use the most recent copy, while a cache miss scanned the index file and used the first copy;
- GC pack rewrite kept the first copy of each live hash and dropped the others;
- the offline pack-size migration copied only the copy the index cache pointed at.

When the selected copy was not the one matching the record, reads failed to decrypt (EIO, file "does not open"). When GC or migration dropped the matching copy, the chunk was lost.

### Fixed

- `PackStore::read_chunk_with` reads the most recent copy first and passes it to an acceptance check supplied by the caller. If the check fails, every other indexed copy of the hash in that pack is tried from newest to oldest, and the accepted copy becomes the cached entry. Vault reads accept a copy only if it authenticates (XChaCha20-Poly1305) with the record's nonce and decompresses; non-vault reads accept a copy that decompresses. A resolution is logged at error level, because it means stored data did not match its record.
- Chunks that are already affected become readable again as long as the matching copy is still in the pack. No separate repair pass is needed.
- GC pack rewrite keeps every indexed copy of a live hash (B006).
- `pack-size-migrate` copies every indexed copy of each chunk, with its original CRC32, into one target pack (`PackStore::append_chunk_copies`). That pack may exceed `pack_max_size_mb` when the copies do not fit together.
- Since B006, deduplication is decided through the committing transaction, so committed chunks are no longer appended again. The remaining sources of duplicates are retries (the committed record matches the newest copy) and two transactions creating the same new chunk concurrently (resolved by the read path).

### Not Recoverable

A chunk whose matching copy was already dropped by a GC rewrite or by a pack-size migration that ran before this fix cannot be recovered. Its reads fail with "no stored copy of chunk ... is usable". The backup directory left by an earlier pack-size migration still contains the original packs.

### Affected Files
- `src/data/pack.rs` — `read_chunk_with`, `read_indexed_copies`, `append_chunk_copies`; removed `read_chunk`, `read_chunk_payload`, `read_chunk_payload_with_index`, `append_chunk`
- `src/fs/chunk.rs` — chunk loads validate each candidate copy (decrypt and decompress)
- `src/migration/pack_size.rs` — migrates every indexed copy

## B008 - Vault Ownership, Key-File Handling and Control Socket Exposure in Service Mode - Oct 08 2026

### Root Cause

1. **`.vault` owned by the daemon user** (`src/fs/vault.rs`): B004 set the `/.vault` owner to `getuid()/getgid()`, but when the vault is created through the control socket that code runs in the daemon. Under the systemd service (`User=verfs`) `/.vault` (mode `0700`) belonged to `verfs`, so the user who created it could not open it.
2. **Key file written and read by the daemon** (`src/vault/mod.rs`, `src/main.rs`): the daemon wrote `verfsnext.vault.key` (mode `0600`) at a path sent by the client, so in service mode the file belonged to `verfs` and its owner could not read it, or the write failed for paths `verfs` cannot write. Unlock had the daemon open the caller's `0600` key file, which fails for the same reason. The file was also opened with `truncate(true)`, so creating a vault could overwrite an existing key file, and it was chmod-ed to `0600` only after being created with umask-default permissions. It was not fsynced.
3. **Control socket opened to all users** (`contrib/systemd/verfsnext-service.sh`): the installer ran `chmod 666` on the socket after starting the service, so every local user could run snapshot (including delete), crypt and stats commands, defeating the daemon's `0660` + `verfs` group model.
4. **Root directory without sticky bit** (`src/types/mod.rs`): the root inode was `0777`. With other users able to access the mount, any of them could delete or rename everyone else's top-level entries.

### Fixed

- The CLI performs all key-file I/O as the calling user. `crypt -c` generates the key material, writes the key file with `create_new` and mode `0600` at open time, fsyncs it and its directory, and sends the material in `vault_create`. If creation fails, the unused key file is removed. `crypt -u` reads the key file and sends the material in `vault_unlock`.
- The daemon takes the `/.vault` owner from the control socket peer credentials (`SO_PEERCRED`). In offline metadata mode the CLI process is the owner.
- `FsCore::create_vault` / `unlock_vault` take key material instead of paths; the daemon no longer touches key files.
- The installer no longer changes the socket mode; access requires membership in group `verfs`.
- `PERM_DIRECTORY_ROOT` is `0o1777`. With `default_permissions` the kernel enforces the sticky bit. This applies to newly created filesystems; no migration is provided (no existing deployments to migrate).

### Compatibility

The `vault_create` / `vault_unlock` control requests changed (key material instead of paths), so the CLI and the daemon must be the same version.

### Affected Files
- `src/main.rs`, `src/fs/vault.rs`, `src/fs/mod.rs`, `src/vault/mod.rs`, `src/types/mod.rs`
- `contrib/systemd/verfsnext-service.sh`

## B009 - SIGTERM Killed the Daemon Without Its Final Sync - Oct 08 2026

### Root Cause

The mount daemon only handled SIGINT (`tokio::signal::ctrl_c`). Any SIGTERM (a plain `kill`, `timeout`, a desktop session logging out, or a systemd unit without `KillSignal=SIGINT`) used the default action and terminated the process at once: no final sync of packs and metadata WAL, and the mount was left behind as "Transport endpoint is not connected". The shipped system unit masked it with `KillSignal=SIGINT`, but the new desktop app starts the daemon as a child of the desktop session, where logout sends SIGTERM. Found while testing the desktop app: `timeout` signalled the process group and the daemon died with a stale mount.

### Fix

`run_mount` (`src/lib.rs`) installs a SIGTERM handler next to SIGINT; both run the same graceful shutdown (`graceful_shutdown`, CRC32 report, cancel the FUSE session). The log line names the signal received.

### Affected Files
- `src/lib.rs` — SIGTERM handling in `run_mount`

## B010 - Busy Root Unmount and False Resilience Corruption Alarm - Oct 08 2026

### Root Cause

Root used normal `umount`, which fails with `EBUSY` when workload files remain
open. `Session::drop` logged the error and closed the FUSE connection while the
daemon returned success, leaving a disconnected mount. B009's final sync did run
in the observed incident. Separately, Python 3.13 `Path.rglob` suppressed the
disconnected mount's scan error and returned an empty manifest. The resilience
supervisor interpreted that observation as deleted files and froze the guest
before checking the recovered filesystem. Independent strict verification of
the preserved copy passed for 206 objects.

### Fix

- Root now uses `umount2(MNT_DETACH)`, matching the existing non-root lazy detach.
- The session run loop borrows its session; normal shutdown keeps the mount alive
  through final filesystem sync, then explicitly unmounts through a fallible API.
  Signal, control-task, sync and unmount failures are logged and make the command
  return failure. Abandoned sessions retain ownership-based startup cleanup;
  explicit unmount errors are not retried by `Drop`.
- The harness uses strict traversal, durable fault windows, explicit errno/path/
  operation observations and post-recovery verification. Completed hash or
  namespace mismatches remain failures. Only declared connection outages are
  classified as expected; `EIO` is not generally accepted.
- Release paths are immutable per revision, worker errors carry their actual
  cycle, and preflight/endurance progress and timestamps remain distinct.

### Compatibility and Validation

No persisted format, CLI or control-protocol change; no migration is required.
Validation uses the existing remote Rust suite, held-file SIGTERM reproduction
and six LXC fault scenarios before a fresh three-hour endurance run. Results and
binary/source hashes are retained in the deployed run's evidence and provenance.

### Affected Files

- `src/lib.rs`
- `crates/verfsnext-async-fusex/src/mount.rs`, `src/session.rs`
- `scripts/resilience/`, `docs/resilience-test.md`

## B011 - Pending FUSE Request Abort Misclassified by Resilience Harness - Oct 08 2026

### Root Cause

Cycle 38 killed the daemon as planned. A pending `os.open` of `/mnt/verfsnext`
returned `ECONNABORTED` (103) during the recorded recovery window, while the mount
was still attached. The harness recognized `ENOTCONN` but rejected this connection
abort and froze the LXC after post-recovery verification had passed. Independent
verification of a copy of the frozen data also passed for all 205 objects.

### Fix

Recognize `ECONNABORTED` for FUSE paths or descriptor operations, and the equivalent
rsync diagnostic for its destination, only during a matching injected fault cycle
and window. Connection errors with a known pathname outside the mount are rejected.
All observations remain logged; complete integrity mismatches, `EIO`, errors outside
the window and recovery failures still stop the run. The filesystem binary and
persisted data format are unchanged; no migration is required.

### Validation

Replay the recorded cycle-38 failure and rejection cases, then run the existing
six-scenario LXC preflight before starting a fresh detached three-hour run.

### Affected Files

- `scripts/resilience/guest.py`, `docs/resilience-test.md`

## B012 - In-Flight FUSE Requests Failed With EIO During Graceful Shutdown - Oct 09 2026

### Root Cause

`Session::run` returned as soon as the shutdown token was cancelled, but its FUSE
reader threads kept reading and dispatching requests. `graceful_shutdown` then
closed the metadata store under them, so requests in flight or arriving in that
window failed with `EIO` while the mount was still attached. Applications writing
during a logout or service stop saw I/O errors on `chmod`, `pwrite` and similar
calls. Pages held by the kernel writeback cache were also never flushed to the
daemon before its final sync. Found by the reviewed resilience harness: its
fsync-acknowledged writers hit `EIO` in the first `daemon_sigterm` preflight cycle.
Post-recovery verification passed, so no acknowledged data was lost. The old
harness missed this because its only writer had usually passed its last fsync when
SIGTERM arrived.

### Fix

- After cancellation, `Session::run` calls `syncfs` on the mount while requests are
  still served, so writeback-cached pages reach the filesystem before the final sync.
- A request gate (`RequestGate` in `crates/verfsnext-async-fusex/src/session.rs`) then
  closes and waits for admitted requests to finish. Reader threads leave requests read
  after closure unanswered and exit; the kernel aborts those requests with
  `ECONNABORTED` when the daemon releases the device, the same as after a daemon crash.
- Only then does `run_mount` perform the final sync and unmount. A `syncfs` failure
  is returned after draining, so the final sync still runs and the daemon exits nonzero.

### Compatibility and Validation

No persisted format, CLI or protocol change; no migration is required. An LXC
reproducer SIGTERMs the daemon while two writers loop `pwrite`/`chmod`/`fsync`. The
old binary returned `EIO` in 40 of 40 writer interruptions. Over 80 rounds the fixed
binary returned `ECONNABORTED` in 159 interruptions and `ENOENT` once, for a path
lookup after the lazy unmount; neither is `EIO`. This was followed by the nine-scenario
preflight and a six-hour LXC run.

### Affected Files

- `crates/verfsnext-async-fusex/src/session.rs`
- `docs/technical_deep_dive.md`, `docs/resilience-test.md`

## B013 - Unlink Made Every Later syncfs Fail With ENOENT; Crash Leaked Open-Unlinked Files - Oct 09 2026

### Root Cause

With the FUSE writeback cache, the kernel updates an unlinked file's ctime and flushes
it with `SETATTR(mtime|ctime)` before it forgets the inode. The daemon ignored
`FORGET` and deleted the inode as soon as no file handle was open, so that `SETATTR`
got `ENOENT`. The kernel records the error on the superblock's writeback error state,
so the next `syncfs(2)` (`sync -f`) on the mount, by any application, failed with
`ENOENT`. After every regular-file unlink or rename-over, such a `syncfs` failed. B012's
shutdown `syncfs` exposed it in the first resilience preflight cycle; a FUSE request
trace confirmed the sequence `UNLINK`, `SETATTR` (ENOENT), `FORGET`.

Separately, an unlinked file kept alive by an open handle was stored with `nlink = 0`
but nothing indexed it. If the daemon stopped before the handle closed, the inode and
its chunks were never reclaimed.

### Fix

- `FsCore` tracks kernel lookup references (entry replies minus `FORGET`/`BATCH_FORGET`).
- Unlink and rename-over keep an unlinked inode while it has an open handle or a kernel
  reference, writing orphan index key `O<ino>` in the same transaction.
- `release` and `forget` perform the final delete when both counts reach zero, under the
  global write lock that also serializes unlink; the final delete removes the orphan key.
- Startup deletes every indexed orphan before mounting.
- FUSE `write` failures are now logged with inode, offset, length and flags, because
  kernel writeback otherwise discards them.

### Compatibility and Validation

The orphan index is a new, additive key prefix. Older binaries ignore it; no migration
is required. Orphans created by older builds have no index key and are not reclaimed.
On the LXC, the old binary failed `sync -f` after unlink and after rename-over. The fixed
binary passed unlink, rename-over, hardlink unlink, open-unlinked read and `sync -f`, and
reclaimed an open-unlinked file after SIGKILL exactly once at the next start. This was
followed by the nine-scenario preflight and the six-hour LXC run.

### Affected Files

- `src/fs/fuse.rs`, `src/fs/inode.rs`, `src/fs/mod.rs`, `src/types/mod.rs`
- `docs/technical_deep_dive.md`

## B014 - Writeback Writes Failed With EIO During Snapshot Operations - Oct 09 2026

### Root Cause

`apply_write_round` (`src/fs/write.rs`) began its metadata transaction and planned the
write before taking the read side of the global mutation gate, and released the gate
inside `apply_single_write_in_txn` before committing. Snapshot create/delete (and other
namespace and vault mutations) hold the write side and commit large transactions that
rewrite chunk refcounts. They could therefore commit inside every write transaction's
window. While snapshots were created and deleted back to back, all 16 immediate commit
retries lost with `Transaction write conflict`, and the kernel's writeback WRITE failed;
applications saw `EIO` from `fsync`. Found by the resilience harness's `snapshot_op_kill`
scenario: two acknowledged-write workers got `EIO` from `fsync`, one of them before any
fault was injected. The new FUSE write-error log (B013) showed the cause. Post-recovery
verification found no corruption of acknowledged data.

### Fix

Each write-round attempt holds the read side of the mutation gate from `begin_write`
through commit, and the nested acquisition inside `apply_single_write_in_txn` is
removed. tokio's fair `RwLock` would deadlock on a nested read while a writer is
queued. No holder of the write side waits on the write batcher.

### Compatibility and Validation

No format or protocol change; no migration is required. A synthetic snapshot-churn
reproducer did not trigger the conflict on either binary; with the fix, writer throughput
under churn was higher (798 vs 659 fsyncs in 20 s). The decisive validation is the
harness `snapshot_op_kill` preflight cycle and its repetitions in the six-hour run.

### Affected Files

- `src/fs/write.rs`

## B015 - Resilience Resource Capture Raced GC Discard-File Replacement - Oct 09 2026

### Root Cause

The supervisor captured pack inventory with `find` while the daemon was atomically
rewriting `packs/.DISCARD` through `packs/.DISCARD.tmp`. The temporary directory entry
could disappear after `find` enumerated it but before `find` inspected its metadata.
That diagnostic command returned exit 1, marking the run failed and freezing the LXC
before cycle 41's fault was injected. Cycle 40 had passed its 215-object verification;
the failure contained no integrity mismatch or daemon error. This was a harness race,
not a product data-loss finding.

### Fix

Pack inventory now uses an explicit `scandir`/`stat` traversal in the guest. An entry
that disappears during the scan is listed in the resource report and recorded as a
`pack_inventory_entries_disappeared_during_scan` event. Errors opening directories or
reading other metadata still fail the capture with their original error.

### Compatibility and Validation

No product format or protocol change; no migration is required. The corrected harness
was syntax-checked with `uv` and its inventory command was run against the guest. A
fresh preflight passed all nine fault scenarios, including a GC rewrite hit, and the
first endurance cycle passed. No disappearance occurred during the new captures, so
the newly added race event path was not triggered in this run.

### Affected Files

- `scripts/resilience/supervisor.py`, `docs/resilience-test.md`

## B016 - Writes Ran at ~1-4 MB/s and Froze File Managers - Oct 13 2026

### Symptom

Copying files into the mount with Thunar ran at about 1.2 MB/s on a 160 MB/s disk, and Thunar's UI froze for the whole copy. Two 80 MB files took minutes.

### Root Cause

Measured on a rotational data disk (Seagate ST3000VM002), where an `fdatasync` plus cache flush costs 100-170 ms:

1. **Serialized FUSE dispatch** (`crates/verfsnext-async-fusex/src/session.rs`): two device reader threads each `block_on`-ed a request to completion. At most two requests ran at once. While both waited on write commits, every lookup, getattr and readdir from the file manager's UI thread queued behind them, which froze Thunar.
2. **One durable commit per FUSE write** (`src/write/batcher.rs`, `src/fs/write.rs`): every write waits for its commit, the ingest worker dispatched each such write immediately, and the apply worker committed them one by one. Each commit synced the active pack and its index before the metadata commit, so each FUSE write paid 100-170 ms of disk flushes.
3. **Same-inode groups split into separate commits** (`src/fs/write.rs`): writeback writes for one file that were not contiguous in arrival order were placed in separate rounds, each with its own transaction and pack sync.
4. **Kernel dirty throttling**: FUSE's strict per-mount dirty limit kept only one or two writes in flight while their pages waited on the reply. More concurrency alone could not amortize the sync.

### Fix

- FUSE requests are spawned as tasks; `max_background` raised to 32 and the buffer pool sized above it (`session.rs`).
- Group commit in the batcher apply worker; all writes of an inode in a batch fold into one multi-range `WriteGroup`, so a batch is one transaction (`batcher.rs`, `write.rs`). Full-block overwrites no longer read the old block.
- The per-commit pack sync is gone. Pack-before-metadata ordering is enforced by a SurrealKV WAL durability barrier with watermarks plus a 250 ms hand-off task (see "Write Durability and Ordering" in the technical deep dive). Truncate's per-commit pack sync was removed for the same reason.
- `PackStore::sync` no longer holds the active-pack mutex during `fdatasync`.
- The periodic (non-full) sync cycle hands off only the WAL prefix whose pack data is durable, instead of flushing the whole buffer. A full flush synced packs again under the WAL lock and paused commits every 5 s during long copies.
- `read` no longer drains the batcher unconditionally (see B005): all writes are acknowledged after their commit, so the drain only stalled reads behind unrelated commits. It now drains when fire-and-forget writes are pending, like `flush`/`fsync`/`release`.
- Tokio hygiene: blocking pack reads on cache misses, sync-cycle syncs and GC pack rewrites run under `block_in_place`; the inode-lock map and POSIX lock table moved from tokio mutexes to `parking_lot`.

### Behavior Change

A daemon crash now loses commits made in roughly the last 250 ms plus one pack sync. Before, each commit reached the OS immediately. Power-loss guarantees and `fsync` semantics are unchanged. Dirty pages in the kernel's FUSE cache were already lost on a daemon crash.

### Compatibility and Validation

No on-disk format change (the WAL bytes are identical, only buffered), so no migration is needed. Measured on the same HDD with an isolated instance (80 MB files, old -> new): `gio copy` (Thunar's copy path) 20.4 s -> 0.38 s; `dd bs=1M conv=fsync` 19.5 s -> 0.92 s; `dd bs=64K conv=fsync` 15.9 s -> 0.79 s. `rsync -a` of 2000 small files (60 MB) 227.5 s -> 3.5 s. All copies read back byte-identical, and the rsync'd tree is identical. 14 SIGKILL-during-copy cycles: every closed or fsynced file intact, in-flight files a clean prefix of the source, metadata consistency check clean (0 refcount mismatches, missing chunks or orphan extents). `verfsnext` lib tests pass. SurrealKV lib tests pass except 4 `sstable_tests` that fail identically on the base commit.

### Affected Files

- `crates/verfsnext-async-fusex/src/session.rs`
- `crates/verfsnext-surrealkv/src/{lib.rs,lsm.rs,wal/mod.rs,wal/manager.rs,wal/writer.rs,wal/reader.rs}`
- `src/write/batcher.rs`, `src/fs/write.rs`, `src/fs/fuse.rs`, `src/fs/chunk.rs`, `src/fs/gc.rs`, `src/fs/mod.rs`
- `src/meta/mod.rs`, `src/data/pack.rs`, `src/sync/mod.rs`

## B017 - Launching an Updated App Kept the Old Filesystem Running - Oct 13 2026

### Root Cause

`daemon::start` (`src/gui/daemon.rs`) returned as soon as the control socket answered, without checking which executable served it. After a rebuild or update, opening the new app reattached to the daemon still running the old (deleted) binary, so the folder kept old code. B016's performance testing on the live mount ran against the pre-B016 daemon because of this. A second launch while an old app window was open only activated that window, which kept old code too (`src/gui/single_instance.rs`).

### Fix

- `paths::runs_current_exe` compares `/proc/<pid>/exe` with `/proc/self/exe` by device and inode.
- `daemon::start` stops a running daemon whose executable differs and starts the current one. It uses `systemctl --user stop` when that daemon is the service's `MainPID`, otherwise the app-mode stop (child or control-socket peer, SIGINT). The stop includes the final sync.
- Single instance: when the primary runs another executable, the new launch sends it SIGTERM, waits up to 15 s for it to exit and becomes the primary. The app holds no filesystem state, and the new instance then restarts an outdated daemon as above.

### Compatibility and Validation

No data or protocol change. On the live desktop with the user service:
- Launching the new binary with only an outdated daemon running stopped it (final sync logged) and restarted the service on the current binary.
- After rebuilding in place with both the app and the daemon outdated, launching the binary replaced the app, then the daemon (new service `MainPID`), and the mount stayed available.
- Afterwards the live metadata consistency check was clean, every file on the mount (1.87 GiB) read back without errors, and the user's test files matched their sources.

### Affected Files

- `src/gui/daemon.rs`, `src/gui/paths.rs`, `src/gui/single_instance.rs`


## B018 - Content-Defined Chunking Was Never Implemented; Files Were Stored as Fixed 1 MiB Blocks - Oct 14 2026

### Root Cause

The system was designed around UltraCDC content-defined chunking, and the README, configuration, desktop settings and this documentation all described it, but the storage path never used it. `src/fs/write.rs` split every file into fixed 1 MiB blocks (`BLOCK_SIZE` in `src/types/mod.rs`), hashed whole blocks and zero-padded partial ones; every pack record of an inspected live pack had `uncompressed_len = 1048576`. History (all refs, not shallow):

- `b398a9c` (2026-02-17) added `UltraStreamChunker`, but production only called `ultracdc_chunk_count()`, which threw the spans away and logged their count. Content-defined boundaries never reached storage.
- `27539db` (2026-02-17) raised `BLOCK_SIZE` from 4 KiB to 1 MiB for FUSE throughput and derived the default `fuse_max_write_bytes` from it, coupling the FUSE request size to the stored block size. It also changed the CDC defaults to 16/32/64 KiB, which only affected the telemetry.
- `503ef1b` (2026-05-01) and `41c5716` (2026-05-02) removed the telemetry and `src/data/chunker.rs`.

So no commit disabled working CDC; the `ultracdc_*` settings never controlled anything. Any shift of data inside or between files defeated dedup, and identical regions smaller than 1 MiB were never shared.

### Fix

- `src/data/chunker.rs`: UltraCDC (8-byte window Hamming distance against `0xAA..AA`, masks `0x2F`/`0x2C` before/after the normal size, low-entropy run detection, min/max bounds), driven by the `ultracdc_*` settings.
- Storage format 2: extents are keyed by byte offset and map whole variable-length chunks (`ExtentRecord { chunk_hash, len }`); holes are gaps between extents. `BLOCK_SIZE` is gone and `fuse_max_write_bytes` has its own 1 MiB default.
- `prepare_write_plan` / `chunk_region` re-chunk each write as part of a region that starts at the extent before it and ends where a cut resynchronizes with an old extent boundary (or at end of data, or at the next old boundary after 2 maximum chunk sizes). Unchanged chunks keep their extents and refcounts.
- Truncate keeps the prefix of the extent holding the new last byte and deletes the rest.
- Reads assemble overlapping extents and load uncached chunks in parallel; the per-handle read plan of small files holds the extent list.
- `SYS:max_extent_len` bounds extent lengths so the extent covering an offset is found with a forward scan.
- `SYS:storage_format` marks new data directories; directories without it (format 1, fixed blocks) are refused with an explicit error. As agreed, there is no migration: all existing data was disposable test data.
- Pack records are written with one `write` call each (records are now ~32x more numerous). The pack format itself is unchanged; it was already chunk-size agnostic.
- Config validation: `ultracdc_min_size_bytes >= 64`, `ultracdc_max_size_bytes <= 4 MiB`.

### Compatibility and Validation

Breaking metadata change without migration (agreed: no real deployments). Validation:
- Library unit tests (43) and all 7 functional FUSE integration tests in `tests/rsync_integration.rs` pass.
- Manual OS-command checks on a scratch mount against a reference copy: sequential writes in mixed request sizes, 300 random overwrites (1 B to 300 KB), sparse writes 50 MiB past EOF and into the hole, truncate down/up with writes after each, snapshot isolation after an in-place edit, vault write/truncate/lock/unlock, unmount/remount (all SHA-256 sums matched), offline GC followed by remount (data and vault intact). Metadata consistency stayed clean (0 refcount mismatches, 0 missing chunks, 0 orphan extents) and there were no CRC32 errors.
- Dedup: a 32 MiB random file copied again with 8 bytes prepended stored 3.6 MB of new chunk data instead of 32 MiB; most of that is superseded tail chunks (below).
- `bench_comfyui_profile` against the previous commit, two runs each: totals 114.2 s / 100.6 s (new) vs 93.4 s / 102.9 s (old). The difference is in `sm_write_dura`, which is dominated by 3000 `fsync` calls and varied from 43 s to 69 s between runs of the same binary; a separate run of that phase alone gave 60-64 s (new) vs 58-68 s (old). Reads were faster in both runs (`lg_read_seq` 2.8/1.7 s vs 4.7/2.0 s, `sm_read_seq` 1.5/1.3 s vs 2.4/2.0 s); `mod_write_dura` was 17.3/14.6 s vs 18.6/12.8 s.

Known cost: each write that extends a file re-chunks and re-stores the file's tail chunk, and the superseded tail is garbage until GC (about 5% extra pack writes when copying a 32 MiB file).

### Affected Files

- `src/data/chunker.rs` (new), `src/data/mod.rs`, `src/data/compress.rs`, `src/data/pack.rs`
- `src/fs/write.rs`, `src/fs/chunk.rs`, `src/fs/fuse.rs`, `src/fs/mod.rs`
- `src/types/mod.rs`, `src/meta/mod.rs`, `src/config.rs`, `src/gui/settings.rs`
- `README.md`, `config.toml.example`, `docs/technical_deep_dive.md`

## B019 - Dedup Statistics Counted Unreachable Chunks - Oct 14 2026

The dashboard compared all chunk records, including obsolete data, with logical
file sizes and clamped negative savings to zero. Dedup now compares reachable
extent data with reachable unique CDC data, across live files, snapshots and
locked/unlocked vault. Sparse holes and unreachable chunks do not inflate the
result. CLI and GUI show saved bytes and percentage; integrity auditing retains
its full scope. No storage change or migration. Validation: 43 existing library tests passed
on the remote builder. On a temporary FUSE mount, two identical 8 MiB + 123 B
random files reported exactly 8,388,731 bytes / 50% saved; SHA-256 matched both
copies and all three metadata consistency counters were zero.

## B020 - Compression Savings Included Obsolete Data and Hid Expansion - Oct 14 2026

Compression now compares only useful reachable unique CDC bytes with their
stored payload, with shared daemon calculations for GUI and CLI. Signed bytes
and percentages expose expansion instead of clamping it away. Net savings
include all namespaces and actual allocated disk blocks, including metadata,
indexes and garbage. Unreachable indexed payload is reported separately and
explicitly marked as a partial garbage measure. Format 2 has no block padding;
no storage change or migration is needed.

Validation: 43 existing library tests passed remotely. A temporary FUSE workflow
used incompressible and compressible files, 128 MiB sparse holes, a snapshot,
delete and a locked/unlocked vault. Compression and dedup totals remained
identical across vault lock transitions; disk bytes matched `du -s -B1`; hashes
matched and all metadata consistency counters were zero. CLI rendered the same
byte and percentage fields as the control RPC consumed by the GUI.

## B021 - Transfer Speed Averaged All Idle Time Since Startup - Oct 14 2026

Traffic divided by daemon uptime made fast transfers appear slow after long
idle periods. A daemon-owned one-second monotonic counter sampler now supplies
the latest read/write rates and actual interval to stats and status. It starts
from zero on each daemon instance and reports zero when idle. GUI speed uses
existing cheap two-second status polls; full metadata scans remain every
30 seconds. CLI labels the recent rate and sampling interval; totals stay
separate. No storage change or migration.

Validation: 43 existing library tests passed remotely. The existing mounted
`rsync_copy_remount_and_delete_persistence` integration test passed using its
remote-compiled executable on the local FUSE-capable user service. Real repeated
copies and direct reads produced recent peaks of 31.4/33.9 MB/s over measured
1.000-1.001 s intervals, then zero idle rates. Restart reset both totals and the
sampling baseline; file SHA-256 and metadata consistency remained correct.
The actual OverviewPage QML rendered offscreen with real daemon RPC data and
showed the shared savings and recent speed fields. No new tests were added.

## B022 - Idle Cleanup Could Not Reclaim Garbage in the Active Pack - Oct 14 2026

Pack rewrite skipped the pack writers were still appending to, and that pack
rotates only at its size limit. Deleted and overwritten data therefore stayed
in the open pack until it reached that limit, and the space report folded
those bytes into the same excess as metadata.

When a pack meets the existing reclaim threshold, idle and offline cleanup now
compact it in place, the active pack included. GC briefly takes the write
lock so in-flight writes commit, and for the active pack records the current
size as a cutoff. Live hashes are read after that, so a chunk appended but not
yet committed cannot be dropped. Records below the cutoff are filtered without
any lock; appends then wait only while the records written during the copy are
carried over unchanged and the files are swapped, and the same pack id is
reopened for appends. The active pack therefore still grows to its size limit:
cleanup never rotates it or creates smaller packs. Every indexed copy of a
hash that still has metadata is kept, including vault ciphertext. `.DISCARD`
bytes waiting for that compaction are reported on their own, separate from
metadata bookkeeping. No pack or metadata format change, so no migration.

The first attempt sealed (rotated) the active pack before rewriting it. That
left packs far below the size limit, one per cleanup, and was replaced before
release. The write-lock drain also closes an older window: a pack that rotated
while a write that had appended to it was still committing could be rewritten
without that chunk.

Validation: 43 library tests passed on the remote builder. At the user's
request, two mount integration tests were added in `tests/rsync_integration.rs`:
`gc_compacts_active_pack_in_place_under_concurrent_io` compacts the active pack
while reading a live file, appending fsynced data during the copy (the test
fails if the copy finished first), re-creating the content of deleted plain and
vault files, and overwriting a file. It checks that the pack keeps its id,
shrinks, and then grows on new writes, and that every file matches after a
vault lock and unlock and after a remount, with metadata consistency ok and
zero checksum errors. `gc_active_pack_compaction_survives_crash` sends SIGKILL
to the daemon during compaction right after an fsynced append, then checks that
the data survives, that cleanup completes after restart without leaving a
temporary pack, and that the data survives a second remount. Both tests passed
four runs in a row. The full mount suite apart from the benchmarks (9 tests)
passed. A manual mount with a 600 MiB file and 200 MiB deleted also matched
every checksum after compaction and after a remount, and pack 1 stayed the
only pack.

## B023 - Reads Returned Damaged Chunk Data and Writes Could Not Repair It - Oct 15 2026

### Root Cause
The read path compared each stored payload with its pack-index CRC32 but only
logged and counted a mismatch, then decoded the damaged bytes. Raw chunks have
no other check, and zstd frames are written without a content checksum, so a
single flipped byte in a pack was returned to applications as file content.
Nothing compared the decoded plaintext with the chunk's content hash. Because
the CRC never rejected a copy, the duplicate-copy fallback was never tried.
Writing the correct content again did not help: it deduplicated against the
damaged chunk record, so the new file was damaged too.

### Fix
- A copy is accepted only if its CRC32, vault AEAD, decoded length and
  plaintext content hash all match. A rejected copy falls through to the next
  indexed copy; with none intact the read fails with EIO.
- A chunk with no intact copy is marked corrupt in memory and persisted as
  `CHUNK_FLAG_CORRUPT` by the sync cycle. Staging treats such a chunk as
  missing, stores a fresh copy and the commit repoints the existing record
  (refcount kept), repairing every file that shares it.
- GC drops a damaged copy only when an intact copy of the same hash survives
  the rewrite; otherwise it still refuses to rewrite the pack.
- The flag uses a free bit of the existing `flags` byte: no format change.

### Validation
Scratch mount, one byte flipped in a raw chunk: reads fail with EIO; copying
the correct content to a new name heals both the new and the original file,
also when a restart separates detection and rewrite (persisted flag). Offline
GC with the damaged and the fresh copy in the same pack drops the damaged copy
and all files stay intact after remount.

### Affected Files
`src/fs/chunk.rs`, `src/fs/gc.rs`, `src/fs/mod.rs`, `src/fs/vault.rs`,
`src/fs/write.rs`, `src/vault/mod.rs`, `src/data/pack.rs`

## B024 - fdatasync Returned Before the Metadata WAL Was on Disk - Oct 15 2026

### Root Cause
FUSE `fsync`/`fsyncdir` called `sync_cycle(!datasync)`. For `fdatasync`
(`datasync = true`) that was the periodic, non-full cycle: it synced the packs
but only handed the WAL prefix to the OS cache, without an fsync. The inode
size, extents and chunk records needed to read the synced data back were not
durable when `fdatasync` returned, so a power loss could lose data an
application had explicitly synced (databases and journals use `fdatasync`).
The call also ran the idle-GC check of the periodic cycle.

### Fix
`FsCore::sync_durably(data_only)` serves every explicit sync: packs
`sync_all` (`sync_data` for `fdatasync`), then `flush_wal(true)`, which writes
and fsyncs the whole WAL buffer. The periodic cycle and the 250 ms hand-off are
unchanged. No format change.

### Validation
Scratch mount: write 3 MB, `fdatasync`, `fsync` on the directory, SIGKILL the
daemon, remount: the file matches. (Power loss cannot be simulated with OS
commands; the call chain now ends in a WAL fsync.)

### Affected Files
`src/fs/mod.rs`, `src/fs/fuse.rs`

## B025 - Incomplete Pack Index Was Trusted and Let GC Delete Live Data - Oct 15 2026

### Root Cause
Index rebuild wrote straight into the canonical `.idx` (truncate, one entry at
a time, sync at the end). An interruption left a non-empty prefix, and startup
rebuilt only missing or empty indexes, so the prefix was trusted. The active
pack check only verified that entries ended inside the pack, not that they
covered it. Chunks past the prefix returned "missing pack index entry", and GC
merely logged live hashes absent from the index, then rewrote the pack keeping
only indexed records: the live payloads were deleted (reproduced by removing
the last index entry and running offline GC).

### Fix
- `ensure_index_covers_pack` runs for every pack at startup, before any handle
  is opened, and when rotation reuses an existing pack: the index must be a
  contiguous run of entries from offset 0 to the end of the pack (index file
  only, cheap). Otherwise the index is rebuilt.
- `rebuild_index` writes `*.idx.rebuild`, syncs it, renames it over the index
  and syncs the directory.
- GC refuses to rewrite a pack when a live hash has no index entry.
- The partial index check in `repair_active_pack_tail` is replaced by the
  coverage check. No format change.

### Validation
Scratch mount: 8 MiB file, 32 KiB survivor, a deleted file; last 40-byte index
entry removed and a stale `.idx.rebuild` left behind. Restart restores the
index to full length and every file reads back; offline GC then rewrites the
pack and every file still matches after remount.

### Affected Files
`src/data/pack.rs`

## B026 - Failed Active-Pack Swap Left Appends on a Detached File - Oct 15 2026

### Root Cause
In-place compaction of the active pack removes the old index, renames the new
pack over the old one, renames the new index and syncs the directory, and only
then reopens the active-pack handles. Each step returned early on error. A
failure after the pack rename (directory sync, index rename, reopen) left the
active handles on the old, now unlinked pack. The sync loop only logged the
error, so later writes were appended to and synced on the detached file, and
WAL records could durably point at bytes absent from the canonical pack: a
restart would lose those acknowledged writes.

### Fix
- The replacement handles are opened on the rewritten files before the first
  destructive step and follow them through the renames.
- The destructive steps run in `swap_rewritten_pack`; any error there records
  a swap failure in `PackStore`, and every later append, sync, cutoff and
  rewrite fails with an explicit error naming it (writes fail with EIO).
  Restart recovers: startup rebuilds a missing or incomplete index.
- No format change.

### Validation
Mount suite without benchmarks (9 tests, including the active-pack compaction
and compaction-crash tests) passed. The failure path itself needs fault
injection at the directory-sync/rename steps and was verified by review.

### Affected Files
`src/data/pack.rs`

## B027 - Pack Index Lookups: Unflushed Entries Lost on Eviction, Full Scans per Miss, Priming Under the Swap Lock - Oct 15 2026

### Root Cause
- Index entries of the active pack were kept only in a bounded Moka cache
  until the next index flush (sync every 250 ms or 5 s). An evicted entry
  made a committed chunk unreadable: with small caches an 8 MiB write read
  back before any sync failed with EIO (reproduced; the same read passed after
  fsync).
- A cache miss scanned the whole `.idx` file with one `pread` per 40-byte
  entry, to find one hash: O(chunks x pack entries) on large datasets.
- GC held one global swap lock while it fsynced the directory three times and
  re-primed the cache entry by entry, so compacting one pack stalled reads of
  every pack.

### Fix
Per-pack lookup maps replace the global (pack, hash) cache. The active pack's
map is resident and updated on append (covers unflushed entries). Sealed maps
are loaded in one read and kept in an LRU bounded by
`pack_index_cache_capacity_entries`. GC builds the new map from the rewritten
entries before taking the lock, and swap locks are per pack. Duplicate copies
are tried newest first from the map, including unflushed ones. No format
change.

### Validation
- Unflushed-entry scenario (chunk cache 1 MiB, index cache 8 entries, 8 MiB
  in 512 KiB writes, read before sync, three partial overwrites, remount):
  byte-identical; the previous build fails the first read with EIO.
- Cold reads of 1 GiB spread over 81 packs with small caches: 41 MiB/s before,
  189 MiB/s after (two runs each).
- Mount suite without benchmarks (9 tests) and 43 library tests passed.

### Affected Files
`src/data/pack.rs`

## B028 - Metadata Cache Fills Could Resurrect Invalidated Values - Oct 15 2026

### Root Cause
`load_inode_cached`, `load_dirent_cached` and `load_chunk_record` read the
store on a miss and then inserted the result. Mutations commit and then
invalidate. A lookup/getattr/read that read the store before a commit and
inserted after its invalidation cached the old value, and the caches have no
TTL: a stale inode size made reads stop short, and a stale dirent after
rename-over-existing served the replaced file. Requests run as independent
tasks and these paths take no write lock, so the interleaving is legal; the
window is microseconds (not hit in 9000 stress iterations).

### Fix
`FillCache` (src/fs/fill_cache.rs) wraps the three caches: 64 striped epochs;
invalidation bumps the key's stripe epoch under its lock; a fill records the
epoch before the store read and inserts under the same lock only if it is
unchanged. `invalidate_all` bumps every stripe. No TTL was added.

### Validation
Stress scripts (stat during appends, rename-over-existing with concurrent
lookups) stay clean; mount suite passes. The interleaving itself is closed by
construction (no deterministic hook).

### Affected Files
`src/fs/fill_cache.rs`, `src/fs/mod.rs`, `src/fs/chunk.rs`

## B029 - Snapshot Delete Broke Open Files; Snapshots Split Hard Links - Oct 15 2026

### Root Cause
- `remove_snapshot_subtree` hard-deleted every inode and its extents and
  dropped the chunk refcounts, ignoring open handles and kernel lookups. A
  file open inside a snapshot failed with ENOENT on read and fstat right after
  `snapshot delete` (reproduced), and GC could then reclaim its chunks.
- `clone_dir_children` cloned every dirent as a new inode with the source
  nlink: a hard-linked file became N separate inodes, each claiming nlink N.

### Fix
- Delete takes a `referenced` predicate (`inode_referenced` when mounted,
  always false offline). A referenced file keeps its inode and data with
  nlink 0 and an orphan record, and the existing release/forget/startup paths
  finish it. Every inode is processed once.
- Create maps source inode -> clone for hard-linked files; further names get a
  dirent to the clone and bump its nlink. No format change; older snapshots
  stay split.

### Validation
Scratch mount: three names of one file appear in the snapshot as one inode
with nlink 3. A snapshot file opened before `snapshot delete` reads back in
full through the open fd (fstat nlink 0); metadata consistency stays ok after
close, after remount, and after deleting a second hard-link snapshot and the
source files.

### Affected Files
`src/snapshot/mod.rs`, `src/fs/mod.rs`, `src/lib.rs`

## B030 - New Pack Files Were Not Made Durable in Their Directory - Oct 15 2026

### Root Cause
Rotation and startup created pack and index files and later synced only the
file handles. POSIX does not make a new name durable without an fsync of its
directory, yet the WAL barrier treats synced pack data as durable, so after a
power loss metadata could reference a pack whose directory entry was lost
(ext4/XFS journaling usually covers it; other filesystems may not).

### Fix
`rotate_active_pack` syncs the pack subdirectory after opening the next pack
and index; `PackStore::open` syncs the packs directory after creating the
subdirectories and the active pack's subdirectory after opening its files.

### Validation
Scratch mount with 8 MiB packs: a 30 MB file rotates through 5 packs and
matches after remount. Power loss itself cannot be simulated with OS commands.

### Affected Files
`src/data/pack.rs`

## B031 - RENAME_EXCHANGE Left Parent Directory Link Counts Wrong - Oct 15 2026

### Root Cause
The exchange branch of `rename` updated the moved inodes' parents but touched
both parent directories with an nlink delta of 0. Exchanging a directory with
a non-directory across parents left the losing parent one link too high and
the receiving parent one too low (reproduced: xa 3/xb 2 instead of 2/3).

### Fix
When the parents differ, the old parent's delta is (target is dir) - (source
is dir) and the new parent gets the opposite.

### Validation
renameat2(RENAME_EXCHANGE) of xa/sub (dir) with xb/file and back: nlinks
2/3 and then 3/2 as expected.

### Affected Files
`src/fs/fuse.rs`

## B032 - Every Write Request Stored a Throw-Away End-of-File Chunk in the Pack - Oct 15 2026

### Root Cause
A write region that ends at the end of the file cuts its last chunk there, not
at a content-defined boundary. That chunk was compressed and appended to the
active pack, and the next append re-chunked it, so it immediately became
garbage: pack bytes per data byte were 1.016 (1 MiB writes), 1.122 (128 KiB),
1.503 (32 KiB) and about 5 (8 KiB), plus compression CPU and GC compaction
of the garbage later.

### Fix
End-of-file chunks are stored inline in metadata (`CHUNK_FLAG_INLINE`, payload
key `T` + hash) and deleted in the same commit when their refcount reaches
zero. Inline chunks that survive one idle GC pass are moved to the active pack
by the next one. Readers handle both kinds. Storage format change without
migration: older data stays valid; data written by this version is not
readable by older versions (owner decision: the existing data is test data).

### Validation
- Pack growth at 1 MiB / 128 KiB / 32 KiB / 8 KiB direct writes: 1.002 /
  1.000 / 1.002 / 1.002, contents identical.
- 40 plain files and 17 vault and truncated files written in 8 KiB requests:
  identical after idle promotion (packs grew by the promoted tails), remount,
  vault lock/unlock with a promotion pass while locked, and SIGKILL after an
  fsynced append; metadata consistency ok.
- Mount suite without benchmarks (9 tests) passed. ComfyUI benchmark, two runs
  each, noisy disk: sm_write_dura 34.3 s vs 51.1 s before (average),
  mod_write_dura 9.7 s vs 14.3 s, other phases unchanged.

### Affected Files
`src/fs/chunk.rs`, `src/fs/write.rs`, `src/fs/gc.rs`, `src/fs/mod.rs`,
`src/types/mod.rs`, `src/vault/mod.rs`

## B033 - New WAL Records Were Appended Behind a Torn Tail and Then Truncated Away - Oct 16 2026

### Root Cause
A crash left a zeroed tail on the active segment
`metadata/wal/00000000000000000002.wal` (offset 44471756). Replay treats a
zeroed region (`Empty` header) and a partial record at the end as a clean end
of segment, and `Wal::open_with_min_log_number` reopened that segment in append
mode, so every transaction acknowledged after the restart was written *behind*
the zeros, in the same block. At the next startup the strict reader failed with
"non-zero byte in padding area", and `repair_corrupted_wal_segment` rewrote the
segment with only the records before the zeros, dropping the ~180 KB of
acknowledged records after them. Two chunk records were lost, one file read
back with EIO, and the Overview health tile only showed counters.

Two more defects sat on the same path:
- The WAL writer was opened before replay and never reopened. When recovery
  repaired (rename over) or removed the active segment, the writer kept
  appending through a descriptor to the replaced inode, so everything written
  until the next rotation would vanish on a crash.
- The first fix attempt read past damage at the next CRC-valid record without
  any ordering check. Records that survive *after* a crash-damaged region can be
  older, never-acknowledged writes; replaying them without the destroyed ones
  before them breaks prefix order (and repair copied them back into the file).

Reproduced on a scratch mount: write 20 files, `kill -9`, append 3000 zero
bytes to the segment, start the pre-fix binary, write 20 more files, `kill -9`,
restart: the pre-fix binary returns mismatching content.

### Fix
- Torn tails (`recovery.rs`, `lsm.rs`): `replay_wal_outcome` reports the end of
  the last replayed record of the highest segment and
  `truncate_wal_segment_tail` cuts the non-record bytes after it (fsynced)
  before anything is appended. `Core::reopen_wal_after_recovery` reopens the
  writer after replay, at startup and after a restore.
- Resync (`reader.rs`): `Reader::set_resyncing(true)` continues at the next
  `Full`/`First`/`SetCompressionType` header whose CRC matches, and
  `Reader::take_gap` reports that non-record bytes (a resync or an `Empty`
  region) preceded a record. The replay accepts a record after a gap only when
  its `starting_seq_num` continues the replayed sequence; others are logged and
  rejected. This recovers segments damaged by older versions (records written
  after the restart continue the recovered sequence) without replaying
  unacknowledged leftovers.
- Repair: every segment with skipped bytes, rejected records or a gap followed
  by records is rewritten from exactly the records replay accepted, after an
  fsynced copy to `<segment-id>.corrupt-<millis>` (ignored by segment listing).
  A stale `repair_temp/` is removed first. A repair failure now fails startup
  instead of continuing on a damaged active segment.
- Records check (`src/fs/health.rs`, `verfsnext check`,
  `ControlRequest::CheckRecords`, `RecordsCheckReport`): names the files behind
  every consistency counter, including chunks whose stored copy is damaged,
  with the path relative to the folder root (`null` when no folder entry
  reaches the inode).
- Desktop app: the Health tile is clickable and names the problem in words. The
  dialog lists each affected file and can move it to `lost+found/<timestamp>/`
  — `.vault/lost+found/<timestamp>/` for vault files, since a rename cannot
  cross the vault boundary — and permanently delete files already waiting
  there. Files the user cannot act on (in a read-only snapshot, in a locked
  vault, or without a folder entry) show the reason instead of a button, and
  the backend refuses them with the same message instead of counting a locked
  vault as "already gone". Paths with `.`/`..` components are refused.

### Validation
- `cargo remote -h -- test -p verfsnext-surrealkv`: 928 passed, 2 ignored; only
  the 4 `sstable_tests` that already fail on the base commit fail.
- Scratch-mount crash simulation (`kill -9` + 3000 zero bytes appended to the
  active segment, 2×20 files of 300 KB):
  - pre-fix binary for all runs: content mismatch (the bug);
  - pre-fix binary writes behind the torn tail, fixed binary recovers: all 40
    files intact, the segment is rewritten from 124 replayed records after a
    backup, records check consistent;
  - fixed binary for all runs: the 3000-byte tail is cut at startup, no repair
    needed, all 40 files intact.
- The records check on the live mount named the file that had been failing
  reads with EIO (2 lost pieces); the lost chunk records were already gone from
  the earlier truncation and are unrecoverable.

### Affected Files
`crates/verfsnext-surrealkv/src/wal/reader.rs`,
`crates/verfsnext-surrealkv/src/wal/recovery.rs`,
`crates/verfsnext-surrealkv/src/lsm.rs`, `src/fs/health.rs`,
`src/fs/mod.rs`, `src/control.rs`, `src/lib.rs`, `src/gui/controller.rs`,
`qml/RecordsReport.qml`, `qml/RecordsFileRow.qml`, `qml/StatTile.qml`,
`qml/OverviewPage.qml`

## B034 - Graceful Shutdown Panicked When an Admitted Request Returned Its Buffer - Oct 16 2026

### Root Cause
Found by the power-cut harness, which SIGTERMs the daemon while workers run
fsync-heavy POSIX ops:
`panicked at crates/verfsnext-async-fusex/src/session.rs:426:9: failed to put
the buffer back to buffer pool ... sending on a disconnected channel`.

`Session::run` dropped its pool receiver right after spawning the two device
reader threads, so the readers held the only receivers. Once shutdown closed the
request gate (B012/B016), each reader that read one more request returned at the
gate and dropped its receiver, while `close_and_drain` was still waiting for
admitted requests (e.g. an `fsync` inside `sync_durably` /
`rewrite_pack_durably`). Those requests replied, then sent their buffer back to
a pool with no receivers and panicked. The reply had already been sent and the
`Admission` was released while unwinding, so the drain finished, the process
exited 0 and no acknowledged data was lost; the panic was still a shutdown-path
failure.

### Fix
`Session::run` keeps its pool receiver until `close_and_drain` has returned, so
the pool stays open for every admitted request and is dropped only after the
last one returned its buffer. Both buffer returns in `process_fuse_request`
(normal reply and unknown-opcode `ENOSYS` reply) go through `return_buffer`,
which panics on a closed pool: that is now a broken invariant. The
unknown-opcode path previously only logged and dropped the buffer.

### Validation
- Scratch mount, 12 concurrent workers (`dd conv=fsync`, `os.fsync`, `mv`),
  SIGTERM after 1–3 s, repeated: pre-fix binary panicked in 8/10 runs, fixed
  binary 0/30, all exits 0.
- Mount suite (`rsync_integration`, benches skipped, test binary built with
  `cargo remote` and run locally because the builder has no `/dev/fuse`): 9/9
  passed.
- `cargo remote -h -- test --profile headless --no-default-features`: all
  passed.

### Affected Files
`crates/verfsnext-async-fusex/src/session.rs`

## B035 - A Zeroed Active-Pack Tail After Power Loss Prevented Startup - Oct 16 2026

### Root Cause
Found by the power-cut harness (docs/powercut-test.md), on the pre-B033 binary and
again on HEAD 84d20a4. After a power cut the active pack ended in 1,594 zero bytes
up to a 4 KiB boundary: ext4 had committed the larger file size, but the last
partially written data block never reached disk, the same shape as B033's WAL tail.
`repair_active_pack_tail` only truncated a record cut short at the end; on a header
without the record magic it bailed, so startup failed with "corrupt pack record
magic" and the filesystem could not be mounted until the pack was cut by hand.

### Fix
A successful pack sync makes every byte up to that length durable, and payloads are
synced before metadata points at them, so after a power loss every missing, zeroed
or torn byte lies past the last sync and nothing committed references it.
`repair_active_pack_tail` now cuts the pack at the first header that is not a whole
record (undecodable, wrong magic, or running past the end), fsyncs it, and logs a
WARN. Non-zero cut bytes are first kept in `<pack>.vpk.tail-<millis>` (synced,
ignored by pack listing). If a record header appears after the damaged offset, the
damage may not be a power-loss tail (bit rot inside synced records), so the cut is
logged as an ERROR: reads of any chunk that was lost fail with EIO, mark it corrupt,
and the records check names the files. No format change.

### Validation
- VM reproduction: 20 files of 300 KB, directory fsync, `kill -9`, active pack padded
  with zeros to the next 4 KiB boundary. HEAD 84d20a4: "corrupt pack record magic at
  offset 5660793", no mount. Fixed: mounts, all 20 files match.
- Power-cut harness run with all faults (see docs/powercut-test.md, run history).

### Affected Files
`src/data/pack.rs`
