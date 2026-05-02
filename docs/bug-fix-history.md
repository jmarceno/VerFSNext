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
