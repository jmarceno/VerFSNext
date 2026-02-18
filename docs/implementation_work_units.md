# VerFSNext — Execution Units (Large Phases, Manual-First Validation)

This document converts the architecture plan into **large implementable units** with fast manual checkpoints.
It is optimized for:

- Earliest possible real mount + copy workflow.
- Minimal phase count (no micro-phases).
- Each phase ending with a **quick manual gate** you can run yourself.

---

## Delivery Strategy

- Total phases: **5** (intentionally large).
- First mount/copy validation: **end of Phase 1**.
- First full operation surface (all async-fusex ops implemented): **end of Phase 2**.
- Dedup/compression/two-index/GC architecture hardened: **end of Phase 4**.
- `.vault` encryption + CLI + lock behavior: **end of Phase 5**.

---

## Phase 1 — Bootable Vertical Slice (Mount, Real File I/O, Crash-Safe Skeleton)

## Goal
Deliver a usable filesystem you can mount and copy files into immediately.

## Scope (single-pass implementation)

- Program bootstrap: `config.toml`, mount lifecycle, SIGINT shutdown wiring.
- SurrealKV initialization and baseline metadata schema (`inode`, `dirent`, attrs, handles).
- Core path and file operations needed for real use:
  - `lookup`, `getattr`, `setattr`, `mkdir`, `create`, `open`, `read`, `write`, `readdir`, `unlink`, `rmdir`, `rename`, `flush`, `release`, `statfs`.
- Baseline pack writer/reader with hash-keyed records (no dedup optimization yet beyond index lookup).
- Write batcher skeleton (`3000` blocks OR `500ms`) with synchronous syscall completion contract.
- Background sync service skeleton from config + shutdown full-sync barrier.

## Output

- Filesystem mounts on Linux.
- You can copy files in/out, list directories, rename, delete, and unmount cleanly.
- Data survives remount.

## Manual Gate (5-10 minutes)

1. Start filesystem and mount.
2. Run:
   - `cp /etc/hosts <mount>/hosts.copy`
   - `mkdir <mount>/d1 && mv <mount>/hosts.copy <mount>/d1/h`
   - `cat <mount>/d1/h`
   - `rm <mount>/d1/h && rmdir <mount>/d1`
3. Ctrl+C the daemon; verify graceful shutdown.
4. Remount and verify previous expected state is preserved.

## Exit Criteria

- No kernel panic/FUSE deadlock.
- Reads/writes functional for normal shell workflow.
- Shutdown waits for final sync before process exit.

---

## Phase 2 — Full FUSE Surface + POSIX/rsync Operational Correctness

## Goal
Meet the hard requirement that every operation exposed by vendored async-fusex is implemented and usable.

## Scope (single-pass implementation)

- Complete all remaining async-fusex operations (including xattrs, locking, dir fsync variants, ioctl/copy-range/poll if exposed).
- Normalize errno behavior for POSIX correctness.
- Finish ownership/permissions/timestamps/link-count semantics.
- Harden rename/link/unlink ordering and atomicity expectations.
- Ensure rsync critical workflows (`temp file + rename`, `inplace`, metadata preservation).

## Output

- “No exceptions” async-fusex operation coverage.
- Stable behavior under shell tools and rsync.

## Automated Testing
- Add an integration test covering basic rsync copy.
    - Test must always invoke and do operations through mounted VerFSNext, using Linux commands (`cp`, `mv`, `rsync`) to interact with the filesystem.
    - Internal commands and APIs should not be used for this test, as the goal is to validate real-world behavior and compatibility with standard tools.
    - Test should invoke linux `rsync` binary against mounted VerFSNext and validate file integrity (`sha256sum` through linux `sha256sum` command) metadata correctness after copy, then it should unmount and remount to validate data persistence and integrity (`sha256sum` command) after remount, them it should delete the copied files and validate deletion through `ls` and `stat` commands, then it should unmount and remount to validate deletion persistence through `ls` and `stat` commands.
    - External commands should always be run with `timeout` to prevent hanging tests in case of deadlocks or stalls, and the test should fail if any command does not complete within a reasonable time frame (e.g., 120 seconds).

## Manual Gate (10-15 minutes)

1. Permission/metadata checks:
   - `touch`, `chmod`, `chown` (if run as root), `setfattr/getfattr`.
2. Hardlink/symlink checks:
   - `ln`, `ln -s`, rename linked files, validate `stat` link counts.
3. rsync checks:
   - `rsync -a <src>/ <mount>/t1/`
   - `rsync --inplace -a <src>/ <mount>/t1/`
   - `rsync --delete -a <src>/ <mount>/t1/`
4. Validate output/attrs by comparing source and mounted tree.

## Exit Criteria

- All async-fusex operations return implemented behavior.
- rsync runs complete without semantic breakage.
- POSIX metadata semantics are consistent in manual validation.

---

## Phase 3 — Inline Data Plane Performance Features (Dedup, Compression, Hash-Only Two-Index)

## Goal
Activate full inline chunking + dedup + compression pipeline with current hash-authoritative locator model.

## Scope (single-pass implementation)

- UltraCDC chunking in streaming mode (no whole-file memory buffering).
- XXH3-128 hashing with collision-ignore policy (as specified).
- zstd compression + rayon parallel workers for data blocks (blocks are compressed in parallel and added to the pack as they complete).
- Hash-only two-index design finalized:
  - Metadata `C:{chunk_hash128} -> pack_id`
  - Pack-local index `chunk_hash128 -> byte offset`
- Moka caches for metadata/chunk/pack-index hot paths. (Always bounded to entry count or memory limit, no unbounded growth.)
- Writer batching fully active (`3000` blocks or `500ms`) while preserving synchronous syscall contract.

## Output

- Repeated content deduplicates.
- Pack storage compressed.
- Read path resolves via pack-local hash index.

## Manual Gate (10 minutes)

1. Copy same large file twice into different paths.
2. Observe metrics/log counters for dedup hits increasing on second copy.
3. Compare logical file sizes vs physical data growth in pack directory.
4. Read both copies and compare checksums (`sha256sum`) to confirm integrity.

## Exit Criteria

- Dedup and compression are both active in normal writes.
- No full-file memory buffering behavior.
- Hash-based pack index lookup is used for reads.

---

## Phase 4 — COW, Snapshots, Non-Blocking Reads, Two-Stage GC (`.DISCARD`)

## Goal
Deliver core filesystem semantics and lifecycle behavior under mutation pressure.

## Scope (single-pass implementation)

- COW versioned extent model for mutating writes.
- Snapshot creation/listing/access via `/.snapshots` behavior + CLI.
  - .verfsnext snapshot create "name"
  - .verfsnext snapshot list
  - .verfsnext snapshot delete "name"
  - `find <mount>/.snapshots/name -type f` lists files in snapshot
  - `cat <mount>/.snapshots/name/path/to/file` reads file from snapshot
- Read concurrency model where reads never block on writers.
- Two-stage GC:
  1. Metadata stage consumes SurrealKV GC output for zero-ref entries, removes refs, appends `.DISCARD` records.
  2. Pack stage selects reclaimable packs by configured threshold, rewrites survivors, atomically renames replacement pack.
- `.DISCARD` binary format and checkpoint handling (`SYS:gc.discard_checkpoint`) in production path.
- Recovery flow for interrupted GC/rewrite.

## Output

- Snapshots are usable and stable.
- Reader latency remains stable under active writes.
- GC reclaims space through metadata prune + pack rewrite without metadata offset updates.

## Manual Gate (15 minutes)

1. Create dataset, take snapshot, mutate/delete live files.
2. Verify snapshot still exposes original content.
3. Run parallel stress:
   - Terminal A loops reads (`find`, `cat`, checksum scan).
   - Terminal B performs continuous writes/renames.
   - Confirm read operations keep progressing.
4. Trigger GC; verify `.DISCARD` creation and later reclaimed pack space.
5. Remount and verify integrity after GC cycle.

## Exit Criteria

- Snapshot isolation works.
- Reads do not stall behind writers.
- GC completes both stages and safely reclaims space.

---

## Phase 5 — `.vault` Encryption Productization (CLI + Visibility + Cache Purge)

## Goal
Ship complete encrypted folder behavior with strict operational semantics.

## Scope (single-pass implementation)

- `verfs crypt` CLI:
  - `-c` create - takes a password and a path where the key will be saved once it is created, no path creates the key at working directory. Key is always named `verfsnext.vault.key`.
  - `-u` unlock - takes a password and a path to the key file created with `-c` to unlock the vault.
  - `-l` lock - takes no arguments, locks the vault, and invalidates all active vault handles.
- `.vault` root-only behavior:
  - not auto-created
  - invisible/inaccessible when locked
  - visible hidden folder when unlocked
  - rename blocked or auto-reverted policy enforced
- Envelope encryption implementation:
  - random 256-bit folder key
  - Argon2id KEK derivation
  - wrapped key stored in metadata system config
  - data-only chunk encryption via XChaCha20-Poly1305 (192-bit nonce)
  - metadata stays unencrypted for indexing, performance and simplicity reasons
- `ChunkBackend` interception for `/.vault` paths.
- Lock transition hard invalidation:
  - clear vault-related caches
  - drop sensitive in-memory materials
  - invalidate active vault handles by policy.

## Output

- `.vault` behaves like a controlled encrypted namespace with lock/unlock semantics.
- Non-vault paths remain unaffected.

## Manual Gate (10-15 minutes)

1. `verfs crypt -c -p ... -path ...` then mount; verify `/.vault` exists only after create.
2. Lock state: `ls -la <mount>` shows no `.vault`; direct access fails.
3. Unlock state: `.vault` appears; create/read files inside.
4. Lock again; confirm invisibility + cache/session invalidation behavior.
5. Restart/remount and verify wrapped-key based unlock still works.

## Exit Criteria

- All lock/unlock/create command paths are functional.
- No cleartext key/password persistence.
- `.vault` policy and POSIX behavior (when unlocked) are correct.

---

## Global “Do Not Advance” Gates

A phase does not advance unless:

1. Manual gate passes end-to-end on your machine.
2. Mount/unmount cycle is clean.
3. No read-stall regression is observed in ad-hoc concurrent checks.
4. Data integrity spot checks pass after remount.

---

## Why This Split Fits Your Constraints

- **Few, large phases**: 5 units only, each substantial.
- **Fast real usability**: mount/copy workflow at Phase 1.
- **Manual-first validation**: every phase ends with quick, operator-driven checks.
- **Risk pulled left**: operation coverage and real rsync behavior come before deep optimization/encryption hardening.
