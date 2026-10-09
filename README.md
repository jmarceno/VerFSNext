<p align="center">
  <img src="assets/verfsnext.svg" width="160" alt="VerFSNext logo: stacked folders with a V" />
</p>

<h1 align="center">VerFSNext</h1>

<p align="center">
  <strong>Keep versions. Store shared content once.</strong><br />
  A copy-on-write Linux filesystem with inline deduplication, compression, snapshots, and an encrypted vault.<br />
  Manage it from a Qt Quick desktop app, or run it from the terminal through FUSE.
</p>

<p align="center">
  <a href="#why-verfsnext">Why VerFSNext</a> ·
  <a href="#desktop-app">Desktop app</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#mount-quickstart-terminal">Terminal guide</a> ·
  <a href="#snapshots">Snapshots</a> ·
  <a href="#encryption-vault">Vault</a> ·
  <a href="docs/technical_deep_dive.md">Technical guide</a>
</p>

<p align="center">
  <a href="LICENSE.txt"><img src="https://img.shields.io/badge/license-Apache--2.0-2fb3a3" alt="Apache-2.0 license" /></a>
  <img src="https://img.shields.io/badge/filesystem-Linux%20%2B%20FUSE-167d72" alt="Linux FUSE filesystem" />
  <img src="https://img.shields.io/badge/desktop-Qt%20Quick-167d72" alt="Qt Quick desktop app" />
</p>

---

## Why VerFSNext

Use your file manager, editor, and command-line tools as usual. VerFSNext
mounts as a normal folder while storing its contents as deduplicated,
compressed chunks in a separate data directory.

| What you want | What VerFSNext gives you |
| --- | --- |
| **Less repeated data on disk** | Inline UltraCDC chunking identifies shared content so duplicate chunks can reuse stored data. Chunk sizes are configurable. |
| **Compression as you write** | Zstandard compresses chunks before they reach storage, with a configurable compression level. |
| **Earlier versions you can browse** | Create read-only snapshots and open their files through `.snapshots` at the mount root. |
| **Private files in their own space** | The optional `.vault` namespace uses Argon2id key derivation and XChaCha20-Poly1305 authenticated encryption. It is hidden and inaccessible while locked. |
| **A clear view of your storage** | The desktop control center shows logical size, storage used, space savings, throughput, and detailed runtime statistics. |
| **Your choice of workflow** | A setup assistant and tray controls for your desktop; a CLI and systemd service for terminal and server use. |

Space savings depend on your data: repeated content benefits from deduplication,
and compressible content benefits from Zstandard. Background garbage collection
reclaims eligible unused storage while the filesystem is idle.

> **Project status:** VerFSNext is a personal project used by its author for
> everyday data. It is not recommended for critical data; use it at your own risk.

## Desktop app

The desktop app is included in the default build. Launch `verfsnext` with no
arguments to open the first-run assistant or return to the system tray.

### Set up once

1. Choose whether VerFSNext should run as a **background user service** or
   **only while the app is open**. The user service needs no root privileges.
2. Review the recommended settings, or customize them.
3. Choose the **VerFSNext folder** you will use for your files and the separate
   **data folder** where VerFSNext stores metadata and packs.
4. Finish setup. The filesystem starts, the window closes to the tray, and an
   applications-menu entry is added. Login startup is optional.

### Manage your filesystem visually

Click the tray icon to open the control center:

| Page | What you can do |
| --- | --- |
| **Overview** | Check whether the filesystem is running, inspect space savings and I/O statistics, open the mounted folder, and take a snapshot. |
| **Snapshots** | Create, browse, and delete snapshots without remembering CLI commands. |
| **Vault** | Create the encrypted vault, select its key file, unlock it, open it, and lock it again. |
| **Folders & Startup** | Review folder locations and choose how VerFSNext starts. |
| **Settings** | Adjust storage and resource settings; the app tells you when a restart is needed. |

The tray menu also opens the folder, takes a snapshot, starts or stops the
filesystem, and quits. Closing the control window keeps the app in the tray.
After setup, later launches start the filesystem and wait in the tray.

The app manages `~/.config/verfsnext/config.toml`, the same configuration the
CLI can discover. A **StatusNotifierItem system tray** is required; GNOME
needs the AppIndicator extension. Background user-service mode also requires
`systemctl --user`.

## Quick start

### Build the desktop app

Install Rust and the FUSE userspace tools for your distribution. The desktop
build also needs Qt 6 development packages and `lld`. On Debian, Ubuntu, or
Pop!_OS, the build and Qt dependencies are:

```bash
sudo apt install build-essential clang lld cmake pkg-config libdbus-1-dev \
  qt6-base-dev qt6-base-dev-tools qt6-declarative-dev libqt6svg6 \
  qml6-module-qtquick qml6-module-qtquick-controls qml6-module-qtquick-layouts \
  qml6-module-qtquick-templates qml6-module-qtquick-window qml6-module-qtqml-workerscript
```

Then build and launch:

```bash
git clone https://github.com/jmarceno/VerFSNext.git
cd VerFSNext
cargo build --release
./target/release/verfsnext
```

Follow the setup assistant and start using the mounted folder. Keep the
executable at a stable location: desktop and service entries refer to it.
The mounted folder is where you work with your files; the data directory
contains VerFSNext's internal storage.

### Prefer the terminal or a server?

Build without the desktop app to omit Qt, D-Bus, and tray dependencies:

```bash
cargo build --release --no-default-features
```

Then follow the [terminal mount guide](#mount-quickstart-terminal) below.
A bare headless binary mounts the filesystem; a bare GUI-enabled binary opens
the desktop app. `verfsnext mount` explicitly chooses terminal mounting in
either build.

## Mount Quickstart (terminal)

1. Set your paths in `config.toml`:
   - `mount_point`: existing mount directory (for example `/mnt/verfs`)
   - `data_dir`: existing data directory (for example `/mnt/work/verfs`)
   - Optional mount behavior:
     - `fuse_direct_io = true` to bypass kernel page cache
     - `fuse_fsname`, `fuse_subtype` for filesystem labeling/identity
     - `fuse_allow_other = true` to let other users access the mount (see below)

   **Who can access the mount.** By default (`fuse_allow_other = false`) only the user running the daemon can access the mounted filesystem; the kernel rejects everyone else, root included. No root privileges are needed to run or mount in this mode: the daemon uses the system `fusermount` helper, and you only need write access to `mount_point` and `data_dir`.
   With `fuse_allow_other = true` other users can access it too, subject to normal file permission bits, which the kernel enforces (`default_permissions` is always set). A daemon not running as root then needs `user_allow_other` in `/etc/fuse.conf` (a one-time root change); without it the mount fails with an error that says so. The filesystem root directory is mode `1777` (like `/tmp`): with this option any local user can create entries at the top level, but only an entry's owner can delete or rename it.

2. Start the filesystem daemon from the repo root:
   ```bash
   ./target/release/verfsnext mount
   ```
   (A bare `./target/release/verfsnext` opens the desktop app instead, unless it was built with `--no-default-features`.)
   Config resolution order is:
   - `./config.toml`
   - `~/.config/verfsnext/config.toml`
   - `/etc/verfsnext/config.toml`
   If found by search, the CLI shows the selected file and asks for confirmation (auto-accept after 5 seconds).

   You can bypass discovery and confirmation with an explicit config file:
   ```bash
   ./target/release/verfsnext --config /etc/verfsnext/config.toml
   # or
   ./target/release/verfsnext -c /etc/verfsnext/config.toml
   ```

3. Use the mount normally:
   ```bash
   ls -la /mnt/verfs
   cp /etc/hosts /mnt/verfs/hosts.copy
   ```

4. Unmount cleanly:
   - Press `Ctrl+C` in the daemon terminal (or send it SIGTERM).
   - The process performs graceful shutdown and final sync before exit.

5. If a manual unmount is needed:
   ```bash
   fusermount -uz /mnt/verfs
   ```

## Snapshots

Use snapshots through the control CLI:

```bash
# Create
./target/release/verfsnext snapshot create snap1
# List
./target/release/verfsnext snapshot list
# Delete
./target/release/verfsnext snapshot delete snap1
```

To force a specific config file for any control command:
```bash
./target/release/verfsnext --config /etc/verfsnext/config.toml snapshot list
```
Mounted snapshot view:
- Snapshot roots appear under `/.snapshots` and can be accessed like normal directories.

## Stats

Read live runtime/internal statistics from the mounted daemon through the control socket:

```bash
./target/release/verfsnext stats
```

Stats namespace behavior:
- Live logical size excludes `/.snapshots`.
- If vault is locked, live logical size also excludes hidden `/.vault` and reports it separately as hidden vault logical size.
- The report includes metadata consistency checks (chunk refcount mismatches, missing chunk records, orphan extents).

## Encryption (`.vault`)

`/.vault` is a reserved encrypted namespace at filesystem root.

- Not created automatically
- Hidden/inaccessible while locked
- Visible and usable only when unlocked

### Initialize vault (one-time)

```bash
./target/release/verfsnext crypt -c -p "your-password" -path /secure/key/dir
```
- Creates `verfsnext.vault.key` (mode `0600`) in the provided directory. The `verfsnext` command you run writes it, so it belongs to you even when the daemon runs as another user (e.g. the `verfs` service user). An existing key file is never overwritten; if creating the vault fails, the new key file is removed.
- Creates `/.vault` metadata in the filesystem, with `/.vault` (mode `0700`) owned by the user who ran the command

If `-path` is omitted, the key is created in the current working directory.
Losing the key file means losing access to the vault: keep a copy somewhere safe.

### Unlock vault

```bash
./target/release/verfsnext crypt -u -p "your-password" -k /secure/key/dir/verfsnext.vault.key
```
The command reads the key file as you and sends its content to the daemon over the control socket; the daemon never opens your key file. After unlock `.vault` becomes visible and accessible for normal file operations. The vault remains unlocked and usable until a lock command is issued or the daemon is restarted.

### Lock vault

```bash
./target/release/verfsnext crypt -l
```

After lock:
- `/.vault` disappears from directory listings
- Direct access to `/.vault/*` fails until next unlock

## Pack Size Migration

To migrate to a new pack size:

```bash
./target/release/verfsnext pack-size-migrate
```

Notes:
- This command must run while the daemon is stopped.
- It rewrites all packs and updates chunk metadata pack mappings.
- Old packs are moved to a backup directory under `data_dir`; remove that backup only after validation.
- Every stored copy of a chunk is migrated, and all copies of one chunk land in the same new pack, which may then exceed the new size target.

## Offline GC Recovery / Full Rebuild

Use this when you want to rebuild `.DISCARD` from scratch (instead of relying on the existing file), for example after large deletions or to catch data left behind by a previous GC run.

```bash
# Rebuild .DISCARD only
./target/release/verfsnext gc offline

# Rebuild .DISCARD and immediately run the pack-rewrite GC phase
./target/release/verfsnext gc offline --run
```

Notes:
- This command must run while the daemon is stopped.
- `gc offline --run` then only runs the second GC phase (pack rewrite), because the discard list was just rebuilt.
- Pack rewrite decisions still honor `gc_pack_rewrite_min_reclaim_bytes` and `gc_pack_rewrite_min_reclaim_percent` from `config.toml`.
- Before rebuilding the discard list, the command deletes every chunk record whose refcount is 0 (the work of the online GC scan phase, which it otherwise skips), so `--run` reclaims chunks of files deleted since the last online scan as well. This is safe only because the daemon is stopped.

## Run As A Systemd Service

This is the system-wide service (runs as user `verfs`, needs root to install). For a personal setup without root, use the desktop app's background user service instead.

1. Install everything (build, binary, user, config, mount/data dirs, unit, enable/start). The installer builds the headless binary (`--no-default-features`), so the server needs no Qt:
   ```bash
   ./contrib/systemd/verfsnext-service.sh install
   ```
   The installer also adds the invoking user to group `verfs` and prints a highlighted reminder that control commands require `verfs` group membership (`newgrp verfs` or re-login required). The control socket (`<data_dir>/verfsnext.sock`) is mode `0660`, owned by `verfs:verfs`; only members of group `verfs` can run snapshot, crypt and stats commands against the service.
   The service starts with `--config /etc/verfsnext/config.toml`.
   The service runs as user `verfs`, so the installer sets `fuse_allow_other = true` in a newly installed config and adds `user_allow_other` to `/etc/fuse.conf` when it is missing. An existing config is left as is, except that a config written before the option existed gets `fuse_allow_other = true` added (by `install` and by `update-bin`), which keeps the previous behavior. If you set it to `false`, only the `verfs` user can access the service mount.

2. If already installed, update only the executable:
   ```bash
   ./contrib/systemd/verfsnext-service.sh update-bin
   ```

3. Preview actions without changing the system:
   ```bash
   ./contrib/systemd/verfsnext-service.sh --dry-run install
   ```

4. Useful commands:
   ```bash
   sudo systemctl status verfsnext
   sudo journalctl -u verfsnext -f
   sudo systemctl restart verfsnext
   sudo systemctl stop verfsnext
   ```

5. Uninstall service artifacts (keeps `data_dir` untouched):
   ```bash
   ./contrib/systemd/verfsnext-uninstall.sh
   ```

If startup fails with FUSE permission errors, verify `/dev/fuse` access and that the `fuse` group exists.

## FAQ

### Why does this exist?

VerFSNext is the public release of a project I've been developing for years.
It began with my curiosity about storage appliances in **2011**, followed by
the first Python implementation in **2015**. Around **2020**, a later version
added features including replication; that iteration is archived as
[VerFS](https://github.com/jmarceno/VerFS).

After further iterations, VerFSNext focuses on the features that make sense
for my daily use.

### Why maintain the FUSE and metadata crates in-tree?

Over the years, I've tried many metadata databases and FUSE bindings, and
implemented metadata storage from scratch. The internal forks of
`async_fusex` and `surrealkv` let me change the filesystem interface and
storage engine together as the project evolves.

## Documentation and development

- [Technical deep dive](docs/technical_deep_dive.md) — storage layout, write and read paths, snapshots, vault, garbage collection, and the desktop architecture.
- [Bug-fix history](docs/bug-fix-history.md) — fixes, their impact, and regression context.
- [Resilience validation](docs/resilience-test.md) — workload and fault-injection procedures, scope, and recorded results.
- [Configuration example](config.toml) — annotated storage, cache, FUSE, and vault settings.

## License

VerFSNext is released under the [Apache License 2.0](LICENSE.txt).

Have an idea or found a bug? [Open an issue](https://github.com/jmarceno/VerFSNext/issues)
with your setup, what happened, and the relevant logs. If VerFSNext is useful
to you, a star helps other Linux users discover it.
