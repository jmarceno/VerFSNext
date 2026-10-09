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
