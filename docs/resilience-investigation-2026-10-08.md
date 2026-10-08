# Resilience investigation — 2026-10-08

The initial three-hour run stopped at **18:06:43 -03**, during cycle **21**
(`daemon_sigterm`), and LXC **101** was frozen at **18:06:47**. It completed about
24 minutes of endurance work: **12 completed endurance cycles**, in addition to
the six preflight verifications. This run does **not** establish three-hour
resilience.

The reported loss of all seven stable files was a **false corruption alarm in
the test harness**. Independent verification of a storage copy recovered all
expected data. A separate, reproducible root FUSE unmount bug leaves a disconnected
mount when files are open during shutdown.

## Evidence and scope

- Tested source: `43014ab759ac0c5d243d91c65b18c6feb91cee88`.
- Executable SHA-256:
  `b602212a9378f29bb787459e75da83f937ee9dbf24de6036f120b4a7e12e0a4e`.
- Original at investigation: LXC 101 **FROZEN**; supervisor service **failed**.
- Preserved host artifacts:
  `/var/lib/verfsnext-resilience-investigation/101-20261008/`.
- Local diagnostic artifacts:
  `target/resilience/investigation-20261008/` (`report.json`,
  `raw-storage-comparison.json`, `clone-daemon.log`, `status.json`,
  `mismatch.json`, `journal-000021-after.log`, `running-supervisor.py`,
  `verify-copy.py`, `forensic-service-tail.log`).
- No changes to the product or running harness were applied during the diagnostic
  investigation. The subsequent authorized fix and redeployment archive this run
  before reusing LXC 101.

The diagnostic used the exact original executable on a working copy, inside a
private mount namespace on Proxmox, bounded to 768 MiB and 50% CPU. It did not
continue the LXC workload. A separate pristine storage copy remains untouched.
SHA-256 comparison confirmed the original's **92 regular storage files**
(26,207,420 bytes) still match the pristine copy. This comparison also includes
directory names and the control socket type.

## Failure timeline

All times are 2026-10-08, UTC-03:00.

| Time | Observation |
| --- | --- |
| 18:06:29.107 | Last successful verification, cycle 20. |
| 18:06:31.021 | Cycle 21 durable checkpoint; snapshots 20 and 21 retained. |
| 18:06:34.969 | Supervisor records shutdown intent; background copy is writing. |
| 18:06:35.596 | Daemon receives the injected SIGTERM. |
| 18:06:35.607 | Full sync completes; CRC32 counters remain zero. Root unmount fails with `EBUSY`. |
| 18:06:35.670 | Concurrent reader reports all seven stable entries missing. |
| 18:06:35.824 | Writer reports `ENOTCONN` on the disconnected FUSE connection. |
| 18:06:39.941 | Recovery mount becomes ready. |
| 18:06:43.749 | Supervisor aborts on the old mismatch marker before verifying recovered data. |
| 18:06:47.455 | Original LXC is frozen to preserve evidence. |

No nonzero CRC32 counters or OOM events were found. At the failure checkpoint,
rootfs free space was about 3 GB and evidence occupied about 12 MB; resource
exhaustion did not explain the failure.

## Confirmed causes

1. `scripts/resilience/guest.py:97` builds its manifest with `Path.rglob()`.
   Python 3.13 suppresses filesystem-scanning `OSError` in glob/rglob. On the
   disconnected mount it returns no paths, which the harness interprets as
   deleted files. The independent reproducer returned `ENOTCONN` from
   `os.scandir()` and `[]` from `rglob()` on the **same directory**.
   See the [Python 3.13 pathlib documentation](https://docs.python.org/3.13/library/pathlib.html#pathlib.Path.rglob).
2. `scripts/resilience/supervisor.py:206` checks `mismatch.json` immediately after
   remounting, before `verify`. The transient reader observation therefore stops
   the run without establishing persistent loss. The exact loaded supervisor
   source is preserved as `running-supervisor.py`; later edits to its on-disk
   file made traceback source lines misleading.
3. `crates/verfsnext-async-fusex/src/mount.rs:72` uses normal `umount` for root,
   while non-root uses lazy `fusermount -uz`. Active file descriptors make the
   root unmount fail with `EBUSY`. `Session::drop` logs this error and then closes
   the FUSE descriptor, leaving a disconnected mount. The diagnostic reproduced
   this with a stable file held open; the daemon still returned status zero.

This is distinct from B009 in `docs/bug-fix-history.md`: the SIGTERM handler and
final sync ran successfully in this incident.

## Independent data verification

The copy passed strict OS traversal and comparison against the durable external
oracle: **206 objects**, including **185 regular-file paths**, covering live
stable files, the mutable tree, vault files, two snapshots, and the atomic probe.
Checks include SHA-256, size, mode, ownership, xattrs, symlink targets and live
hardlink identity. Snapshot hardlink identity remains the previously recorded
known limitation; snapshot content and metadata were checked.

The atomic probe retained the cycle-20 acknowledged value, which is valid:
cycle 21 had not acknowledged its replacement. No corruption was detected in
the retained corpus; this does not establish correctness for untested workloads
or the unfinished three-hour run.

The forensic service exited with status **1** when closing the deliberately held
descriptor after disconnection (`ENOTCONN`). This cleanup error was logged after
the successful integrity report and the false-alarm reproduction were saved;
it must not be presented as a clean service exit.

## Correction scope approved after diagnosis

1. **Make traversal strict.** Replace `rglob` and error-ignoring `os.walk` with
   traversal that propagates scan/stat/read errors. Classify object types from
   the collected `lstat`, and log path, operation, errno, cycle and fault phase.
   A failed scan must never produce a successful empty manifest.
2. **Separate availability observations from verified corruption.** Record the
   fault/recovery interval durably before injection. Preserve every reader
   observation; classify connection loss only when it matches the declared
   fault interval. Completed hash/content mismatches remain failures. Always
   perform strict verification after recovery to establish whether data was
   lost; do not silently discard observations or broadly accept `EIO`.
3. **Correct root shutdown unmount.** After successful write drain and full
   sync, use `umount2(MNT_DETACH)` as the root unmount policy, consistent with the
   existing non-root lazy detach. This detaches a busy mount from the namespace;
   do not use forced unmount. Make explicit shutdown cleanup return its errors
   through a fallible path rather than reporting success with failed unmount
   only in `Drop`. See [Linux umount semantics](https://man7.org/linux/man-pages/man2/umount.2.html).
4. **Make provenance and progress unambiguous.** Run an immutable supervisor
   source per run, include the real workload cycle in errors, clear stale
   completion timestamps when arming, and report preflight/endurance counts
   separately. Preserve this failed run as evidence.

Validation after implementation should first repeat the held-descriptor
reproducer and the six existing fault scenarios, then start a **fresh three-hour
LXC run** with the same 4 GiB disk / 2 GiB memory limits. Rust changes must be
built remotely with `cargo remote -h -- build --release --no-default-features`.
No storage-format change or migration is required. The implementation is recorded
under B010 in the bug-fix history; active deployment and validation evidence live
in the run's provenance and status files. The diagnostic findings above refer to
the original failed revision.
