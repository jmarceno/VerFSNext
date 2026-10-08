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
Logging out, closing Codex, losing SSH, or rebooting the workstation does not stop
the run. Both LXC boot and the supervisor service are enabled for Proxmox startup.
The daemon is deliberately started by the supervisor, so recovery verification
occurs before further workload mutations.

Each deployment stores the supervisor, guest workload and executable in a directory
named for its Git revision. The services expand `release.env` to the exact revision
path, so replacing a later release cannot change a running script's traceback
source. The active run's `provenance.json` records the revision and file hashes.

The initial endurance run is configured for **three additional hours of active
cycles after preflight**. It completes the current cycle, verifies everything once
more, and gracefully stops the daemon. Data and evidence remain for review. No
further endurance run is scheduled automatically.

The six-scenario preflight completed successfully. The endurance run was armed on
**2026-10-08 at 17:42 America/Sao_Paulo**, but stopped at **18:06** during cycle 21,
after 12 completed endurance cycles. The three-hour run was incomplete. Independent verification of
a preserved storage copy passed for 206 objects; the reported missing stable
files were caused by a harness traversal error during FUSE disconnection.
See the [investigation and proposed corrections](resilience-investigation-2026-10-08.md)
for the timeline, reproduction, separate root unmount bug, and preserved evidence.
Before redeployment, this run and its storage are archived separately. The corrected
run repeats six preflight scenarios before arming three additional hours. Consult
the active `status.json` for its current state and `armed_at` time.

## Workload and failure schedule

Every cycle creates a durable checkpoint, saves its expected manifest outside
VerFSNext, starts concurrent readers, a rate-limited `rsync --inplace` copy, and an
atomic overwrite writer. Fault scenarios rotate in this order:

1. SIGKILL only the copy process; the readers and writer must finish successfully.
2. SIGKILL the VerFSNext daemon while workload is active.
3. SIGTERM the daemon while workload is active (same signal as session shutdown).
4. Graceful LXC shutdown followed by startup.
5. Abrupt `pct stop` of the LXC followed by startup.
6. Let the workload finish, allow six seconds of idle time for GC, and SIGKILL the
   daemon. This probes recovery after an idle GC opportunity; it does **not** claim
   that every injection lands within an individual pack rewrite.

Injection delays rotate through write, file-fsync, rename, and post-acknowledgement
windows. The actual observed phase and byte count are recorded immediately before
the fault. Timing is intentionally unrelated to throughput measurements.

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
inside its declared fault interval. `ENOENT` requires the FUSE mount to be detached
and the missing path to lie below its mountpoint; `EIO` is never broadly accepted.
Rsync file-I/O exits 11/23 require an explicit connection-loss message, or a
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
or external evidence exceeds 32 MiB. The LXC journal is capped at 64 MiB. These
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
ssh proxmox 'python3 /var/lib/verfsnext-resilience/101/supervisor.py --ct 101 --root /var/lib/verfsnext-resilience/101 --guest-script /opt/verfsnext-resilience/guest.py --extend-hours 72 --arm-only'
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
rebooted by the harness.
