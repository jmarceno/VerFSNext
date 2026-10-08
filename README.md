# VerFSNext


VerFSNext is a **Copy-on-Write (COW) Linux userspace file system** built on top of **FUSE**.

## ✨ Features

* 📸 **Snapshots**
  Navigable and transparently accessible through the `.snapshots` directory at the filesystem root.

* 🧩 **Inline Deduplication**
  Powered by **UltraCDC** with configurable chunk sizes.

* 🗜️ **Inline Compression**
  Uses **ZSTD** with configurable compression levels.

* 🔐 **Encryption**
  Data can be stored in a dedicated hidden folder (`.vault`) at the root.
  Uses **Argon2id** for key derivation and **XChaCha20-Poly1305** for authenticated encryption.

---
⚠️ DISCLAIMER: Althogh I've been using for my own data with no issues, this is a personal project and is not recommended for critical data. Use at your own risk.

⚠️ As of 20 Feb 2026, development and ALL tests were made on Manjaro Linux with kernel 6.12.68-1. I have no plans to support other platforms, but contributions are welcome (Just open a discussion, so we can chat and I can help).

---

## Desktop App

The easiest way to use VerFSNext. Run the binary with no arguments (double-click it, or `./target/release/verfsnext`):

- **First run**: a short assistant asks whether VerFSNext should run as a background user service (recommended: starts at login, no root needed) or only while the app is open, shows the recommended settings (and lets you customize them), lets you pick the VerFSNext folder and the data folder with a folder browser, then starts it. The window closes and VerFSNext stays in the system tray. It also adds VerFSNext to your applications menu and, if you keep that option on, to your login items.
- **Afterwards**: the app starts the filesystem and waits in the tray. Click the icon for the control center: status, space savings and every statistic, snapshots, the encrypted vault, folders and startup options, and all settings. The tray menu also opens the folder, takes a snapshot, starts/stops VerFSNext and quits.

The app manages `~/.config/verfsnext/config.toml` (the same file the terminal commands find). It needs a system tray (StatusNotifierItem; GNOME needs the AppIndicator extension) and `systemctl --user` for the background service.

Building the app needs Qt 6 development packages and `lld` (Debian/Ubuntu/Pop!_OS):

```bash
sudo apt install build-essential clang lld cmake pkg-config libdbus-1-dev \
  qt6-base-dev qt6-base-dev-tools qt6-declarative-dev libqt6svg6 \
  qml6-module-qtquick qml6-module-qtquick-controls qml6-module-qtquick-layouts \
  qml6-module-qtquick-templates qml6-module-qtquick-window qml6-module-qtqml-workerscript
```

For servers, build without the app (no Qt, D-Bus or tray libraries; a bare `verfsnext` then mounts):

```bash
cargo build --release --no-default-features
```

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

# FAQ

## Why this exists?

**VerFSNext** is the public release of a passion project I’ve been developing for many years.
It started with my curiosity about high-end storage appliances back in **2011**, which led to the first Python implementation in **2015**.

Around **2020**, I built a much more feature-rich version, including replication support. That version is archived here:
👉 [https://github.com/jmarceno/VerFS](https://github.com/jmarceno/VerFS)

After two additional iterations, this current version represents the most refined and focused evolution of the project, with only the features
that makes sense for my daily use.  

## Why vendor `async_fusex` and `surrealkv` instead of other options?

Over the years, I’ve tried dozens of databases for metadata storage (and even implemented it from scratch), as well as nearly every FUSE binding I could find — if you can name it, I’ve probably tried it.
Because of that, I wanted something I could modify freely, without restrictions or external constraints. This approach turned out to be the best option for that goal.
