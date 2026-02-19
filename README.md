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

The main branch is fairly stable and I use it on a daily basis to keep my AI models.
Altohgt I've had no data loss or any issues in the last months, I still do not recommend for critical data.

And as obviuous, I'm not responsible for any data loss!

## Mount Quickstart

1. Set your paths in `config.toml`:
   - `mount_point`: existing mount directory (for example `/mnt/verfs`)
   - `data_dir`: existing data directory (for example `/mnt/work/verfs`)
   - Optional mount behavior:
     - `fuse_direct_io = true` to bypass kernel page cache
     - `fuse_fsname`, `fuse_subtype` for filesystem labeling/identity

2. Start the filesystem daemon from the repo root:
   ```bash
   ./target/release/verfsnext
   ```
   It reads `./config.toml`, mounts at `mount_point`, and stays in the foreground.

3. Use the mount normally:
   ```bash
   ls -la /mnt/verfs
   cp /etc/hosts /mnt/verfs/hosts.copy
   ```

4. Unmount cleanly:
   - Press `Ctrl+C` in the daemon terminal.
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
Mounted snapshot view:
- Snapshot roots appear under `/.snapshots` and can be accessed like normal directories.

## Stats

Read live runtime/internal statistics from the mounted daemon through the control socket:

```bash
./target/release/verfsnext stats
```

## Encryption (`.vault`)

`/.vault` is a reserved encrypted namespace at filesystem root.

- Not created automatically
- Hidden/inaccessible while locked
- Visible and usable only when unlocked

### Initialize vault (one-time)

```bash
./target/release/verfsnext crypt -c -p "your-password" -path /secure/key/dir
```
- Creates `verfsnext.vault.key` in the provided directory
- Creates `/.vault` metadata in the filesystem

If `-path` is omitted, the key is created in the current working directory.

### Unlock vault

```bash
./target/release/verfsnext crypt -u -p "your-password" -k /secure/key/dir/verfsnext.vault.key
```
After unlock `.vault` becomes visible and accessible for normal file operations. The vault remains unlocked and usable until a lock command is issued or the daemon is restarted.

### Lock vault

```bash
./target/release/verfsnext crypt -l
```

After lock:
- `/.vault` disappears from directory listings
- Direct access to `/.vault/*` fails until next unlock

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
