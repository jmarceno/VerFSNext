# Detached LXC resilience test

The reliability harness lives in `scripts/resilience/`. It runs synthetic data through
OS file operations and `rsync`, with the reference corpus on the LXC's ordinary ext4
filesystem. It does not change the daemon or use its internal Rust APIs.

## Deployment

- Proxmox node: `pve`, reached with the workstation's existing `ssh proxmox` identity.
- Dedicated LXC: **101**, `verfsnext-resilience`, Debian 13, unprivileged,
  FUSE and nesting enabled, starts on boot.
- Limits: **4 GiB root disk, 2048 MiB RAM, no swap**, two cores capped at one CPU.
- Initial failed run source: Git `43014ab759ac0c5d243d91c65b18c6feb91cee88`.
- Initial executable SHA-256:
  `b602212a9378f29bb787459e75da83f937ee9dbf24de6036f120b4a7e12e0a4e`.
- Build: `cargo remote -h -- build --release --no-default-features`.
  The original local desktop executable was retained; the headless artifact is
  `target/resilience/verfsnext`.
- Daemon data: `/var/lib/verfsnext-soak/data` inside LXC 101.
- Mount: `/mnt/verfsnext` inside LXC 101.
- Supervisor, status and evidence: `/var/lib/verfsnext-resilience/101` on `pve`.
  Its `evidence` subdirectory is bind mounted at
  `/var/lib/verfsnext-soak/evidence` in LXC 101. No other guest data is accessed.

The supervisor is a persistent system service on **Proxmox**, not on the workstation.
`scripts/resilience/chain.sh RELEASE CYCLES INTERVAL` runs as a transient systemd unit
on Proxmox: it runs the preflight, records `preflight-completed.json`, and only on a
completed preflight arms six endurance hours and starts the enabled service. Nothing in
the sequence depends on the workstation or an agent session.
Logging out, closing Codex, losing SSH, or rebooting the workstation does not stop
the run. Both LXC boot and the supervisor service are enabled for Proxmox startup.
The daemon is deliberately started by the supervisor, so recovery verification
occurs before further workload mutations.

Each deployment stores the supervisor, guest workload and executable in a directory
named for its Git revision, or for the revision plus a hash of the harness files when
the harness is deployed ahead of a commit. The services expand `release.env` to the exact revision
path, so replacing a later release cannot change a running script's traceback
source. The active run's `provenance.json` records the revision and file hashes.

Each endurance run is armed for a fixed number of active hours after a preflight
that runs every fault scenario once. It completes the current cycle, verifies
everything once more, and gracefully stops the daemon. Data and evidence remain for
review. No further endurance run is scheduled automatically.

### Run history

- **2026-10-08, revision 43014ab**: stopped after 12 endurance cycles by a harness
  traversal false alarm and a busy root unmount; see the
  [investigation](resilience-investigation-2026-10-08.md) and B010.
- **2026-10-08, revision 966013d**: stopped at cycle 38 by a misclassified
  `ECONNABORTED` (B011); independent verification passed.
- **2026-10-08 22:12 to 2026-10-09 01:14, revision d19936e**: **completed**; 96
  cycles (6 preflight, 90 endurance), 16 of each of the six original faults, no
  daemon errors or CRC32 errors. Archived as `archives/101-20261009-011448-3h-completed`.
- **2026-10-09, release `d19936e-harness-c85df24f`**: same daemon binary, reviewed
  harness with acknowledged-write workers, unacknowledged-byte checks, GC-rewrite,
  snapshot-churn and double-kill faults, seeded random timing and the daemon-log
  oracle. Preflight stopped at cycle 3 (`daemon_sigterm`): acknowledged-write workers
  received `EIO` during graceful shutdown (B012). Verification passed. Archived as
  `archives/101-20261009-063226-b012-preflight-failed`.
- **2026-10-09, releases `d19936e-b012-882835dc`, `d19936e-b012diag*`**: the B012 fix's
  shutdown `syncfs` failed with `ENOENT` after any unlink, which exposed B013. Diagnostic
  runs were archived under `archives/101-20261009-0659-*` and `-0713-*`/`-0715-*`.
- **2026-10-09, release `d19936e-b013-7adac9e5`**: B012 and B013 fixed. Preflight passed
  `copy_sigkill` and `daemon_sigkill`, and the daemon behaved correctly under SIGTERM, but
  the harness treated an ack writer's exit after a classified outage as a failure. Fixed:
  helpers now end cleanly after a classified outage, and the observation is still judged.
- **2026-10-09, release `d19936e-b013-36672cb3`**: preflight cycles 1–7 passed, including
  a kill during a GC pack rewrite. Cycle 8 (`snapshot_op_kill`) exposed B014: writeback
  writes failed with a transaction conflict during snapshot churn, and writers got `EIO`
  from `fsync`.
- **2026-10-09, release `d19936e-b014-b61459be`**: B012, B013 and B014 fixed (remote
  headless build, SHA-256 `13145043…0165b`) with the reviewed harness. All nine
  preflight scenarios passed. `chain.sh` armed **six hours** of endurance at 07:54 -03
  and started `verfsnext-resilience-101.service`, detached on Proxmox. Consult the active
  `status.json`, `preflight-completed.json` and `provenance.json`.

## Workload and failure schedule

Every cycle creates a durable checkpoint, saves its expected manifest outside
VerFSNext, starts concurrent readers, a rate-limited `rsync --inplace --fsync` copy,
an atomic overwrite writer and two acknowledged-write workers. Fault scenarios
rotate in this order:

1. SIGKILL only the copy process; the readers and writer must finish successfully.
2. SIGKILL the VerFSNext daemon while workload is active.
3. SIGTERM the daemon while workload is active (same signal as session shutdown).
4. Graceful LXC shutdown followed by startup.
5. Abrupt `pct stop` of the LXC followed by startup.
6. Let the workload finish, wait a random 1–8 s of idle time for GC, and SIGKILL the
   daemon.
7. Let the workload finish, then SIGKILL the daemon from inside the guest as soon as a
   GC pack rewrite temp file (`*.rewrite`) appears (polled every 3 ms, 30 s limit).
   Hits and misses are counted in `gc_rewrite_hits` / `gc_rewrite_misses`.
8. Churn `snapshot create` / `snapshot delete` in a loop during the workload and SIGKILL
   the daemon. Any surviving `stress-*` snapshot must be complete.
9. SIGKILL the daemon during the workload, start it again and SIGKILL it a random
   0.02–1.0 s later, during metadata/WAL recovery or right after mounting.

Injection delays are drawn from a per-run seed (`seed` in `status.json`) and the
cycle number, uniformly over 0–3 s after the workload starts, so they span probe
writes, file fsync, rename, acknowledgement and copy/ack-worker-only traffic. The
actual delay, observed phase and byte count are recorded with each fault intent.

Each acknowledged-write worker owns `acked/wN` and performs serial creates,
overwrites (in place or extending), truncates, renames (including over existing
names), unlinks and chmod/xattr changes. Half of each file's 64 KiB segments come
from a shared pool, so deletes and overwrites move dedup reference counts. Every
operation is recorded as pending on the LXC's ext4 before it starts and is
acknowledged only after the file and directory fsyncs return. Reference contents
are stored by SHA-256 next to the model, outside VerFSNext.

The bounded corpus exercises zero-byte, small, boundary-sized and MiB files;
compression and shared chunks; permission bits, ownership, xattrs, symlinks and
hardlinks; rename, overwrite, truncate and delete; two retained snapshots; and
encrypted `.vault` contents. An open-unlinked-file check closes a second handle
first, then reads the surviving handle. Source contents are generated deterministically
with SHAKE-256 labels. SHA-256 manifests are checked against the source corpus,
not generated from the destination being tested.

Pack size is 8 MiB, caches and write batches are limited to fit 2 GiB RAM, and GC
uses a short idle threshold and low rewrite threshold. Full manifest checks occur
after fresh remounts and again after the idle interval. The first avoids verifying
only kernel-cached contents; the second can detect damage that appears after GC.

## Durability oracle

Confirmed data is fsynced through the mount, including containing directories,
before the external manifest checkpoint is fsynced. Confirmed files, directory
entries, metadata, live hardlink relationships and snapshot contents must survive every fault.

Preflight identified an existing structural limitation: snapshot creation clones
each hardlink name to a separate inode, retaining its file contents and attributes.
This is visible in `preflight-snapshot-hardlinks.json` and is recorded in the run's
`known_findings` field. Snapshot verification checks all file hashes, sizes,
permissions, ownership, xattrs, symlinks and namespace entries, but does not require
live inode-sharing relationships inside a snapshot. Live hardlinks are checked
strictly. This limitation was not repaired or hidden by changing the filesystem.

An interrupted copy is kept in a separate unconfirmed namespace. Its incomplete
bytes are observable, but are not falsely counted as loss of acknowledged data.
The atomic overwrite protocol records both the prior hash and the next complete
hash before the operation. Until file fsync, rename and parent-directory fsync have
all succeeded and the external acknowledgement has been recorded, recovery may
yield either complete version. After acknowledgement only the new hash is accepted.
Missing or partial confirmed data fails the test.

For acknowledged-write workers, every acknowledged file must match its recorded
hash, size, mode and xattr exactly, and no unrecorded name may appear. Only the one
pending operation per worker may have either outcome: an interrupted write may be
torn, but every byte must come from the old or the new version (bytes past an end
count as zero); a truncate must have the old or new size with old content; a rename
must show exactly the old or new namespace; an attribute change may show either
value. After verification the observed valid outcome becomes the new model.

Unacknowledged files (`inflight/copy.bin`, `probe.tmp`) may be short or zero-filled,
but every byte must match the source or be zero; foreign bytes or a read error fail.
When `rsync --fsync` exits 0 and the directory fsync succeeds, the copy is
acknowledged and must survive exactly.

The `.snapshots` namespace must contain every retained snapshot and nothing besides
`stress-*` snapshots, which must be complete and are then deleted.

Every daemon journal line is inspected. `ERROR`, a panic or a nonzero CRC32 counter
fails the run. A torn WAL tail message is recorded (`wal_tail_repairs`) instead only
in the recovery capture after an abrupt-stop fault, because the data oracle decides
whether anything acknowledged was lost. `WARN` lines are recorded in
`daemon_warnings` and the event log.

## Evidence and stopping behavior

On `pve`, the run directory contains:

- `status.json`: run state, active time, cycle, phase, fault counts and last verification.
- `host-events.jsonl`: durable fault intentions, applications, recoveries and exceptions,
  with wall-clock nanoseconds, monotonic time and Proxmox boot ID.
- `checkpoint-NNNNNN.json`: expected manifest saved before each fault.
- `journal-NNNNNN-before.log` / `after.log`: daemon and workload logs, including
  SIGTERM handling, WAL recovery, backtraces and CRC32 error counters.
- `resources-NNNNNN-*.json`: root disk space, cgroup memory/OOM counters, pack sizes
  and modification times, useful for locating GC activity.
- `evidence/guest-events.jsonl`: workload stages, fsync acknowledgements, copy exit
  codes, concurrent-reader results and errors, with LXC boot IDs.
- `evidence/progress.json`, `pending.json`, `probe-ack.json`: overwrite phase and
  acknowledgement evidence at the fault boundary.
- `evidence/verified-NNNNNN.json`: successful verification count and manifest hash.
- `evidence/mismatch.json`: expected versus actual objects/hashes if comparison fails.
- `evidence/fault-window.json`: durable injection/recovery boundaries and fault identity.
- `evidence/observation-NNNNNN-*.json`: reader/worker exceptions, syscall context,
  errno, mount state and classification; every observation remains available.

Manifest and durability traversal use strict `os.scandir` and `lstat` information.
Scan, stat, read and xattr errors propagate instead of producing partial manifests.
Connection loss is classified as an injected outage only for the matching cycle
inside its declared fault interval. This includes `ENOTCONN` and `ECONNABORTED`
on FUSE paths, or descriptor operations without a pathname. Pending requests may
return `ECONNABORTED` when the daemon is killed, even while the mount remains attached.
`ENOENT` requires the FUSE mount to be detached
and the missing path to lie below its mountpoint; `EIO` is never broadly accepted.
Rsync file-I/O exits 11/23 require an explicit connection-loss message (including
"Software caused connection abort" naming the FUSE mount), or a
missing destination under a detached mount, within that same fault interval.
Completed content/metadata mismatches always fail, even if recovery reads pass.
Recovery verification runs before observations are judged, including after a
supervisor/Proxmox restart, preserving the distinction between an unavailable
connection and missing durable data. Graceful SIGTERM cycles also require a
successful daemon service result.

A failure sets `status=failed`, collects diagnostics and **freezes LXC 101 using
`lxc-freeze`**, preserving the data and process state instead of cleaning up or
continuing over the damaged corpus. Errors are explicit. A frozen guest must be
unfrozen with `lxc-unfreeze -n 101` only when ready to investigate it.

The supervisor stops before another cycle if root free space falls below 768 MiB
or external evidence exceeds 64 MiB. The LXC journal is capped at 64 MiB. These
limits preserve room for diagnostics; a resource-budget stop is reported as a
failed run, not a passing integrity result. Extending to multiple days should first
review pack growth and evidence size.

The logs bracket **when corruption became observable** between the last successful
verification and the failing read, with the operation/fault context. They do not
by themselves establish a root cause; retained packs, metadata and traces are the
debugging evidence.

## Review commands

From the workstation:

```bash
ssh proxmox 'cat /var/lib/verfsnext-resilience/101/status.json'
ssh proxmox 'systemctl status verfsnext-resilience-101.service --no-pager'
ssh proxmox 'tail -20 /var/lib/verfsnext-resilience/101/host-events.jsonl'
ssh proxmox 'journalctl -u verfsnext-resilience-101.service --no-pager -n 50'
```

`completed` is passing only after the final verification and daemon shutdown.
`running` is an unfinished run. `failed` requires investigation before resuming.
Review the retained preflight status and per-cycle logs along with the main run.

To syntax-check the scripts locally without installing Python packages:

```bash
uv run --offline python -m py_compile scripts/resilience/guest.py scripts/resilience/supervisor.py
```

No pip or external Python libraries are required. The guest uses Debian's `python3`,
`rsync`, `fuse3`, `attr`, coreutils and systemd.

After the user approves a longer run and the current run is **completed**, arm an
additional duration (example: 72 hours), then restart the existing service:

```bash
ssh proxmox '. /var/lib/verfsnext-resilience/101/release.env && python3 $RESILIENCE_RELEASE/supervisor.py --ct 101 --root /var/lib/verfsnext-resilience/101 --guest-script $RESILIENCE_GUEST --extend-hours 72 --arm-only'
ssh proxmox 'systemctl restart verfsnext-resilience-101.service'
```

The extension retains the corpus, previous events and cycle counter. The required
active duration is persisted, so subsequent service restarts do not rearm another
72 hours. Preflight and endurance verification counters are separate and the old
completion timestamp is removed when arming. An unexpected Proxmox restart during checkpoint preparation is marked
inconclusive and fails instead of misclassifying intentional incomplete mutations.
A restart during a fault/recovery verifies the retained checkpoint before proceeding.

## Scope limits

An LXC shares the Proxmox kernel. `pct stop` kills guest processes abruptly but does
not cut power to the host or discard host storage caches. This run tests process
crashes, interrupted copies, container restart/recovery, ordinary POSIX operations,
snapshot and vault integrity, and idle GC behavior. It does not validate physical
power loss, a Proxmox kernel crash, storage-controller failure, injected bit rot,
ENOSPC, a forced OOM, or production-scale timing. No physical host or other guest is
rebooted by the harness. Because `pct stop` keeps the host page cache, data that was
written but never fsynced survives it; a missing `fsync` in VerFSNext's own write path
can therefore pass here. Validating that requires a VM whose virtual disk drops
unflushed writes (for example QEMU with `cache=unsafe` killed abruptly, or
`dm-log-writes` replay).
