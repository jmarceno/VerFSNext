# FUSE TTL and Read-Path Improvement Plan

## Goal

Reduce metadata overhead for workloads dominated by many small files without weakening correctness, transaction integrity, or crash behavior.

This plan covers two related tracks:

1. Replace `ATTR_TTL = 0` with explicit kernel invalidation plus bounded TTLs.
2. Add the next read-path improvements that become safe and worthwhile once invalidation and handle plumbing exist.

## Non-Negotiables

- Never return stale data or stale namespace state after a committed mutation.
- Never publish cache state before the corresponding metadata transaction commits.
- Never invalidate kernel state for a mutation that did not commit.
- Never rely on TTL alone for correctness.
- Keep read-path CRC validation, vault checks, and metadata transactional semantics unchanged.

## Current Problem

Today `src/fs/mod.rs` uses `ATTR_TTL = Duration::ZERO` because VerFS does not send FUSE invalidation notifications.

That is safe, but it forces the kernel to reissue a large number of:

- `lookup`
- `getattr`
- pathname resolution steps

For cargo-like workloads, these metadata round-trips often dominate the actual file I/O.

## High-Level Delivery Order

1. Add invalidate-notify support to `vendor/async-fusex`.
2. Wire VerFS namespace and inode invalidations to committed mutations.
3. Raise attr TTL first.
4. Raise entry TTL after namespace invalidation is proven correct.
5. Extend `async-fusex` so `read` receives `fh`.
6. Add handle-local read plans.
7. Add committed metadata caches with strict invalidation.

## Phase 1 - Add Invalidation Support to `async-fusex`

### Objective

Expose the kernel notification primitives needed to invalidate cached dentries and inode attributes after a successful mutation.

### Required API Surface

Add session-level methods in `vendor/async-fusex` for:

- `invalidate_inode(ino, off, len)`
- `invalidate_entry(parent, name)`
- optional later: `invalidate_entry_range` if batching becomes useful

### Implementation Notes

- Store the low-level FUSE session/channel handle where filesystem code can reach it safely after mount initialization.
- Make notification methods return structured errors but treat `ENOENT`/already-gone cases as non-fatal at call sites.
- Ensure notification calls are safe from async contexts and do not require blocking the main request path.

### Deliverables

- `async_fusex` API for inode invalidation
- `async_fusex` API for dentry invalidation
- basic vendored tests if feasible inside `async_fusex`

## Phase 2 - Add VerFS Invalidation Dispatcher

### Objective

Centralize all kernel invalidation requests behind one VerFS component so mutation code stays simple and ordering stays correct.

### New VerFS Component

Add an invalidation helper in VerFS, for example in `src/fs/mod.rs` or a new `src/fs/invalidation.rs`, with methods like:

- `invalidate_inode_attr(ino)`
- `invalidate_dentry(parent, name)`
- `invalidate_rename(old_parent, old_name, new_parent, new_name, ino)`

### Rules

- Call only after metadata commit success.
- Never hold the metadata transaction open while sending notifications.
- Best-effort delivery is acceptable, but correctness must not depend on delivery succeeding; TTL remains bounded as a backup.

## Phase 3 - Mutation-by-Mutation Invalidation Matrix

### Namespace Mutations

#### `create` / `mknod` / `mkdir` / `symlink`

After commit:

- invalidate parent entry for created name
- invalidate parent inode attrs
- invalidate created inode attrs

#### `unlink`

After commit:

- invalidate parent entry for removed name
- invalidate parent inode attrs
- invalidate target inode attrs

#### `rmdir`

After commit:

- invalidate parent entry for removed directory name
- invalidate parent inode attrs
- invalidate removed inode attrs

#### `rename`

After commit:

- invalidate old parent entry for old name
- invalidate new parent entry for new name
- invalidate old parent inode attrs
- invalidate new parent inode attrs
- invalidate moved inode attrs
- if overwrite happened, invalidate replaced inode attrs too

#### `link`

After commit:

- invalidate new parent entry for new name
- invalidate new parent inode attrs
- invalidate linked inode attrs

### Inode/Data Mutations

#### `setattr`

After commit:

- invalidate target inode attrs

#### `write`

After commit if size/mtime/ctime can change:

- invalidate target inode attrs

#### `truncate`

After commit:

- invalidate target inode attrs

### Directory Snapshot Caches

Existing `readdir` handle snapshots should also be invalidated or naturally bypassed on namespace version mismatch once that versioning is added.

## Phase 4 - TTL Rollout

### Step 4.1 - Attr TTL Only

Keep entry TTL at zero initially.

Set:

- `attr_ttl = 100ms` to `250ms`
- `entry_ttl = 0ms`

Reason:

- attr invalidation is simpler than namespace invalidation
- this gives an immediate reduction in repeated `getattr`

### Step 4.2 - Entry TTL Enablement

Once create/unlink/rename/link invalidations are verified:

- raise entry TTL to `100ms` to `250ms`

### Step 4.3 - Make TTL Configurable

Add config values for:

- `fuse_attr_ttl_ms`
- `fuse_entry_ttl_ms`

Defaults should remain conservative.

### Step 4.4 - Operational Guardrail

Add a mount-time log line showing active TTL values and whether invalidation notifications are available.

## Phase 5 - Extend `async_fusex` so `read` Receives `fh`

### Objective

Allow VerFS to reuse open-file state on read instead of rebuilding the plan from inode and metadata each time.

### Required API Change

Change the virtual filesystem trait so `read` includes the file handle:

- current: `read(ino, offset, size, buf)`
- target: `read(ino, fh, offset, size, buf)`

Propagate the handle from kernel request parsing through dispatch into the trait call.

### Why This Matters

For many small files the open-read-close pattern is common. With `fh` available, VerFS can attach read-side state to the open handle and avoid repeated:

- inode reloads
- extent scans
- chunk metadata lookups for stable files

## Phase 6 - Handle-Local Read Plans

### Objective

Cache per-open read metadata that is valid for the lifetime of a file handle unless the inode data version changes.

### New Handle State

Add a file-handle map in VerFS with entries such as:

- `ino`
- `inode_data_version`
- `inode_size`
- `vault_state marker if needed`
- optional single-block extent map or small extent vector

### Read Behavior

On `open`:

- create handle state
- optionally precompute a small-file read plan lazily or eagerly

On `read`:

- compare current inode data version with handle version
- if unchanged, reuse plan
- if changed, rebuild plan and update handle state

On `release`:

- drop handle state

### Scope Limit

The first version should optimize only metadata planning, not data correctness logic.

## Phase 7 - Committed Metadata Read Cache

### Objective

Cut repeated SurrealKV read-only transaction creation for hot metadata lookups.

### Cache Candidates

- inode cache: `ino -> InodeRecord`
- dirent cache: `(parent, name) -> DirentRecord`
- optional negative dirent cache: `(parent, name) -> ENOENT`

### Correctness Rules

- populate only from committed state
- invalidate synchronously after successful commit
- never mutate cached records in place
- namespace mutations invalidate affected dirent keys immediately
- inode mutations invalidate affected inode keys immediately

### Versioning

For inode data-related reuse, keep using `inode_data_versions` as the invalidation source of truth.

## Phase 8 - Small-File Read Specialization

### Objective

Exploit the fact that many cargo files are tiny.

### First Safe Specializations

- detect files fully covered by one logical block
- cache the single-block extent mapping in handle-local state
- skip extent range scan when handle state is valid
- continue using existing chunk data cache and CRC/decrypt/decompress logic

### Explicit Non-Goals for First Pass

- no new compression format behavior
- no weakened validation
- no write-path semantic changes

## Testing Plan

## 1. `async_fusex` Invalidation Coverage

- add tests around notification API plumbing if possible
- verify invalidation calls do not crash when inode/dentry no longer exists

## 2. VerFS Integration Coverage

Add or extend mount integration tests to verify:

- `create` becomes visible without waiting for TTL expiration
- `unlink` disappears immediately
- `rename` does not leave stale names visible
- repeated `stat`/`open` on hot files does not regress correctness

## 3. Cargo-Like Workload Benchmark

Use the cargo-like integration workload as a correctness gate, and add a separate benchmark or timed harness to compare:

- zero TTL baseline
- attr TTL only
- attr + entry TTL with invalidation

Measure:

- wall-clock completion time
- count of `lookup` and `getattr` requests if observable
- cache hit rates

## 4. Failure Injection

Specifically test crashes between:

- metadata commit and notification dispatch
- rename commit and multi-entry invalidation

The system must remain correct after remount even if a notification was never sent.

## Rollout Recommendation

### Stage A

- merge `async_fusex` invalidation support
- keep TTL at zero
- enable logging-only instrumentation of where invalidations would fire

### Stage B

- enable real invalidations
- set attr TTL > 0, keep entry TTL = 0

### Stage C

- enable entry TTL > 0
- benchmark cargo-like workload again

### Stage D

- add `fh` to `read`
- implement handle-local read plans

### Stage E

- add committed metadata caches
- add small-file specialization

## Concrete File-Level Work List

### `vendor/async-fusex`

- add invalidate notify primitives to session/channel layer
- expose safe filesystem-facing API
- thread file handle through `read`

### `src/fs/mod.rs`

- replace single `ATTR_TTL` constant with attr and entry TTL values
- add invalidation dispatcher

### `src/fs/fuse.rs`

- return entry TTL and attr TTL separately where supported
- fire post-commit invalidations for namespace and inode mutations
- update `read` signature to include `fh`

### `src/fs/write.rs`

- ensure write/truncate completion paths invalidate inode attrs after commit

### `src/fs/inode.rs`

- add metadata cache access helpers if phase 7 proceeds

### `src/fs/*handle*` or new module

- manage file-handle read plans and version checks

### `tests/rsync_integration.rs`

- keep cargo-like CRUD workload as a regression gate
- add invalidation-sensitive rename/create/delete assertions if needed

## Expected Outcome

If implemented in this order, VerFS should get the largest safe small-file read win from reduced lookup/getattr churn first, then get a second win from handle reuse and metadata caching, all without weakening transaction integrity or compatibility.
