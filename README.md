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
