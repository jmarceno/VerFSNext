# Power-cut crash-consistency test

`scripts/powercut/` simulates **power loss**, which the LXC harness in
[resilience-test.md](resilience-test.md) cannot. That harness kills processes and
stops the container, but the Proxmox kernel keeps its page cache, so every byte the
daemon handed to the kernel survives. B024 (fdatasync before the WAL was on disk),
B030 (pack directory entries not fsynced) and B033 (records appended behind a torn
WAL tail after the 2026-10-15 power loss) all passed it for that reason.

## How it works

The test runs in a dedicated VM with its own kernel, so it can use device mapper:

```
VerFSNext ─► ext4 ─► dm-log-writes ─► origin disk
                         └─► log disk: every write, flush, FUA and mark, in completion order
```

**Record.** The daemon's data directory lives on an ext4 filesystem on top of
`dm-log-writes`. Three workers (two plain, one inside the unlocked `.vault`) and a
snapshot worker run POSIX operations against the mount:

- create, pwrite overwrite/extend, append with `fdatasync`, truncate up and down,
  `mmap` write + `msync`, chmod/xattr set and remove;
- rename (also over existing names and across directories), `renameat2`
  `RENAME_EXCHANGE`, hard links, symlinks, unlink;
- occasional 6–20 MiB files, so packs rotate and GC has garbage to compact;
- snapshot create/delete.

Contents mix unique, shared (dedup) and compressible 128 KiB segments. Before each
operation the worker writes it to a journal on the VM root disk. Once the operation's
last `fsync`/`fdatasync`/`msync` and directory fsync have returned, it inserts a mark
`a.<worker>.<seq>` into the write log. That mark is the acknowledgement.

Between bursts the scheduler injects, at random times while workers are active:
SIGKILL of the daemon, SIGKILL during startup recovery (double kill), SIGTERM, ENOSPC
(a ballast file fills the ext4 to 0–48 MiB free for 3–12 s), and idle pauses that let
GC rewrite packs. After every fault the daemon is restarted and the live state is
**settled**: it is verified against the journal (an interrupted operation may have
either outcome), each file and directory is fsynced, and a mark `s.<id>` records the
adopted state. Errors are expected only inside a fault window: `ENOTCONN`,
`ECONNABORTED`, `ENOENT` (mount detached) and, only once the signal has been sent,
`EIO` (the kernel fails pending writeback when the connection aborts) for kills;
`ENOSPC`/`EIO` for ENOSPC. Any other error fails the run, for example a daemon that
keeps refusing writes after the space came back.

**Replay.** The record stops at 80 % of the log disk or after `--record-seconds`.
Then about `--points` crash points are sampled: uniformly, near fault, idle and settle
marks, and just before flushes (where the most unflushed data is at risk). For each
point P the disk a power cut could have left is rebuilt:

- a base disk holds every write up to the last flush at or before P (it is advanced
  monotonically, never rebuilt);
- a dm-snapshot over the base gets the writes after that flush up to P, in one of
  three modes: `prefix` (all of them), `drop` (each kept at a random rate) or `torn`
  (each 4 KiB piece kept at a random rate). FUA writes are always kept.

This is the real contract of a disk with a volatile cache: data is durable only
after a flush (or with FUA), and anything later may be lost, reordered or torn.

**Check each point.** ext4 is mounted (its own journal recovery runs), the daemon is
started on the image and the vault unlocked. Then:

1. **Cold check.** Every operation acknowledged before P must be there exactly:
   content hash, size, mode, xattr, namespace, hard-link groups and `nlink`. Each
   worker's one in-flight operation may have the old or the new outcome. An
   interrupted write may be torn, but every byte must come from the old or the new
   version. Renames, exchanges, links and unlinks are all-or-nothing. The static
   corpus (in the live tree, in the vault and in every snapshot) must be intact.
   `verfsnext check` must report the records consistent. Every file is read in
   full, and a read error fails the point.
2. **Second life.** The recovered state is settled, the workers run for 1–6 s, the
   daemon is SIGKILLed mid-operation and started again. Everything acknowledged
   must still be there. This is the B033 sequence (crash, recover, write, crash).
3. **Nested power cut** (every `--nested-every` points). The second life runs on a
   second `dm-log-writes` layered over the snapshot, and 4 points of that log are
   replayed and checked the same way. This covers power loss during startup
   recovery (WAL tail cuts and repairs, pack index rebuilds) and right after it.

Daemon log: a panic, or an `ERROR` line after the mount completed, fails a point.
`ERROR` lines during startup recovery (WAL corruption reports, repairs) are counted
in `recovery_errors`, because after a power cut they can be legitimate. The data
checks decide whether anything acknowledged was lost.

When all points of a run pass, the base disk is advanced to the end of the log and
must be byte-identical to the origin disk. This proves the replayer reproduces what
was recorded. Then a new run starts on fresh disks, until `--hours` have elapsed.

## Failures

The first finding stops the service. The VM keeps the disks and a small run directory
`/var/lib/powercut/runs/<run>/` (the next start deletes it):

- `events.jsonl`: fault events, record summary, every passed point, and the finding
  with its full context (point, mode, seed, last flush, nested level-2 point);
- `journal.jsonl`: every operation with its contents' hashes (the contents themselves,
  `refs/`, are deleted when the run ends);
- `failed-point/`: the daemon logs of the failing point, its level-2 journal and
  `context.json`;
- `record-daemon.log`: the daemon log of the recording.

Replay is deterministic for a level-1 point: base disk at the last flush, then
`WriteLog.plan_tail(P, mode, Random(seed))`. The log disk holds everything needed to
rebuild the image, as long as no new run has started.

## Deployment

- VM **103** `verfsnext-powercut` on `pve`: Debian 13 cloud image, 4 vCPU, 6 GiB
  RAM, `local-lvm` thin disks with discard: root 32 GiB, origin 10 GiB, log 24 GiB,
  base 10 GiB, cow 4 GiB, log2 8 GiB, cow2 4 GiB (virtio serials `pc-*`, listed in
  `/etc/powercut/devices.json`). It does not start on boot.
- `provision-vm.sh` creates the VM (once). `deploy.sh --setup` installs packages,
  loads `dm_log_writes`/`dm_snapshot` and installs `powercut.service`.
- `deploy.sh` builds this checkout headless (`cargo remote -h -- build --profile
  headless --no-default-features`, never into `target/release/`) and installs it with
  the harness under `/opt/powercut/releases/<label>-harness-<hash>`, with
  `provenance.json`. `--binary PATH --label L` installs another build.
- The VM's address is found from its MAC (`deploy.sh` sweeps the LAN from `pve`).
  Set `POWERCUT_HOST=powercut@<ip>` to skip that.

```bash
scripts/powercut/deploy.sh
scripts/powercut/deploy.sh --start "--hours 6"
scripts/powercut/deploy.sh --status
```

Options of `powercut.py run`: `--hours` (6), `--record-seconds` (900),
`--log-budget` (0.8), `--points` (400 per run), `--nested-every` (8),
`--faults` (comma-separated subset of `idle,kill,doublekill,term,enospc,none`, all by
default) and `--no-records-check` (for binaries older than B033, which have no
`verfsnext check`).

Syntax check without installing anything:

```bash
uv run --offline python -m py_compile scripts/powercut/powercut.py
```

## Scope

Covered: lost, reordered and torn writes after the last flush, at any moment of
operation, GC, pack rotation, snapshot churn, ENOSPC, SIGTERM shutdown and startup
recovery, plus a second power cut during or after recovery. The host filesystem is
ext4 (`data=ordered`, the default).

Not covered: a disk that lies about flushes (acknowledges a flush and then loses the
data), silent bit rot of data the disk reported as written, other host filesystems
(XFS, btrfs), and I/O errors from the device (EIO) other than ENOSPC.

## Run history

- **2026-10-16, HEAD `84d20a4`, smoke run** (90 s record, 6 points): passed. About 4,600
  acknowledged ops, kill and double-kill faults, 6 points plus 8 nested points; the full
  replay reproduced the recorded device byte for byte.
- **2026-10-16, pre-B033 `555e4fe`, all faults**: stopped in the recording by a daemon
  panic during SIGTERM shutdown (`session.rs:426`, "failed to put the buffer back to
  buffer pool": a request task outlives the pool receiver). Data was intact. The same
  code is in HEAD.
- **2026-10-16, pre-B033 `555e4fe`, `--faults idle,kill,doublekill,enospc,none
  --no-records-check`**: stopped at the 4th point (nested power cut). The active pack
  ended in 1,594 zero bytes up to a 4 KiB boundary (ext4 had recorded the larger file
  size, but the data block never reached disk). `repair_active_pack_tail` only trims a
  short final record, so startup failed with "corrupt pack record magic" and the
  filesystem could not be mounted. The same code is in HEAD.
- **2026-10-16, HEAD `84d20a4`, same faults**: the first attempt was a harness false
  alarm (`EIO` from `fsync` after SIGKILL, now classified). The rerun passed 31 points
  and 28 nested points (all three replay modes), then hit the same zeroed active-pack
  tail on a nested power cut: "corrupt pack record magic at offset 4137010 in
  .../pack-00000000000000000052.vpk", and the daemon did not start. **Open: HEAD cannot
  mount after a power loss that leaves a zeroed pack tail.**
- **2026-10-16 20:00, `9b03d72`, all faults, 6 h**: the harness died after 15 minutes of
  recording, when the VM root disk filled (the reference stores of earlier failed
  runs). It was not a VerFSNext finding. The harness now stops explicitly below
  3 GiB free.
- **2026-10-17 08:21, `1c0a0d2`, all faults, 6 h**: 141 points and 68 nested points passed
  (all modes, 0 recovery errors, no finding), then the harness stopped at the 3 GiB
  guard: every point's second life and observations had added 9.3 GiB to the run's
  reference store. Each point now keeps its own store, removed with the point.
- **2026-10-17 15:21, `69c9a03`, all faults, 6 h**: 94 points and 44 nested points passed,
  then `daemon_stop_failed`: the crash point lay inside an ENOSPC window, so the replayed
  image still held the ballast (845 MiB of data plus a 2.2 GiB ballast on 2.9 GiB). The
  cold check passed on the full disk; in the second life, SIGTERM shutdown could not flush
  an 85 MB memtable to an SST ("No space left on device") and exited with an explicit
  error, with the data still in the WAL. A harness artifact, not a VerFSNext finding: the
  ballast is now removed after the cold check, before the second life.
- **2026-10-17 19:50, `4ac877e`**: stopped to grow the disks (origin and base 10 GiB, log 24 GiB) and to keep only logs and journals as evidence.
- **2026-10-17 19:56, `852fccb`, all faults, 6 h**: passed. 3 full runs (each 400 points and
  200 nested points, all replay modes, every run's full replay byte-identical to the origin
  disk), 0 recovery errors, no finding. Finished 2026-10-18 02:36.
