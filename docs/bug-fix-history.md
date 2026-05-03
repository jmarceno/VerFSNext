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
