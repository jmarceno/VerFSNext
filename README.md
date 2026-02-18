# VerFSNext

## Mount Quickstart

1. Set your paths in `config.toml`:
   - `mount_point`: existing mount directory (for example `/mnt/verfs`)
   - `data_dir`: existing data directory (for example `/mnt/work/verfs`)

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

The report includes:
- Total logical size
- Unique compressed/uncompressed chunk sizes and compression rate
- Metadata size
- Deduplication savings
- Cache hit rate
- Used memory (RSS)
- Read/write throughput and totals
- Full `data_dir` disk size and disk-usage delta computed as `data_dir_size - logical_size`

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
