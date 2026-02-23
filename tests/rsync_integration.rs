use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};

struct MountDaemon {
    mount_point: PathBuf,
    child: Option<Child>,
}

impl MountDaemon {
    fn start(workdir: &Path, mount_point: PathBuf) -> Result<Self> {
        let bin = std::env::var_os("CARGO_BIN_EXE_verfsnext")
            .context("CARGO_BIN_EXE_verfsnext is not set")?;

        let stdout = File::create(workdir.join("daemon.stdout.log"))
            .context("failed to create daemon stdout log")?;
        let stderr = File::create(workdir.join("daemon.stderr.log"))
            .context("failed to create daemon stderr log")?;

        let child = Command::new(bin)
            .current_dir(workdir)
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .context("failed to start verfsnext daemon")?;

        Ok(Self {
            mount_point,
            child: Some(child),
        })
    }

    fn wait_until_mounted(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mount_s = self.mount_point.to_string_lossy().into_owned();
        while Instant::now() < deadline {
            let out = run_cmd_raw(5, None, "mountpoint", &["-q", &mount_s])?;
            if out.status.success() {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(200));
        }
        bail!(
            "mountpoint {} did not become ready",
            self.mount_point.display()
        )
    }

    fn stop_graceful(&mut self) -> Result<()> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };

        if child.try_wait()?.is_some() {
            self.child = None;
            return Ok(());
        }

        let pid_s = child.id().to_string();
        let _ = run_cmd_raw(10, None, "kill", &["-INT", &pid_s]);

        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if child.try_wait()?.is_some() {
                self.child = None;
                let mount_s = self.mount_point.to_string_lossy().into_owned();
                let _ = run_cmd_raw(10, None, "fusermount", &["-uz", &mount_s]);
                return Ok(());
            }
            thread::sleep(Duration::from_millis(100));
        }

        let _ = run_cmd_raw(5, None, "kill", &["-KILL", &pid_s]);
        let _ = child.wait();
        self.child = None;

        let mount_s = self.mount_point.to_string_lossy().into_owned();
        let _ = run_cmd_raw(10, None, "fusermount", &["-uz", &mount_s]);
        bail!("daemon did not exit after SIGINT")
    }
}

impl Drop for MountDaemon {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            if child.try_wait().ok().flatten().is_none() {
                let pid_s = child.id().to_string();
                let _ = run_cmd_raw(5, None, "kill", &["-KILL", &pid_s]);
                let _ = child.wait();
            }
        }
        self.child = None;
        let mount_s = self.mount_point.to_string_lossy().into_owned();
        let _ = run_cmd_raw(5, None, "fusermount", &["-uz", &mount_s]);
    }
}

fn run_cmd_raw(timeout_secs: u64, cwd: Option<&Path>, cmd: &str, args: &[&str]) -> Result<Output> {
    let mut command = Command::new("timeout");
    command
        .arg("--signal=KILL")
        .arg(timeout_secs.to_string())
        .arg(cmd)
        .args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
        .output()
        .with_context(|| format!("failed to run command: {} {:?}", cmd, args))
}

fn run_cmd(timeout_secs: u64, cwd: Option<&Path>, cmd: &str, args: &[&str]) -> Result<Output> {
    let output = run_cmd_raw(timeout_secs, cwd, cmd, args)?;
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "command failed: {} {:?}\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
            cmd,
            args,
            output.status.code(),
            stdout,
            stderr
        );
    }
    Ok(output)
}

fn require_tool(name: &str) -> Result<()> {
    let out = run_cmd_raw(5, None, "which", &[name])?;
    if out.status.success() {
        return Ok(());
    }
    bail!("missing required tool: {}", name)
}

fn sha256(path: &Path) -> Result<String> {
    let p = path.to_string_lossy().into_owned();
    let out = run_cmd(120, None, "sha256sum", &[&p])?;
    let text = String::from_utf8_lossy(&out.stdout);
    text.split_whitespace()
        .next()
        .map(ToOwned::to_owned)
        .context("sha256sum output missing hash")
}

fn stat_signature(path: &Path) -> Result<String> {
    let p = path.to_string_lossy().into_owned();
    let out = run_cmd(120, None, "stat", &["-c", "%f %u %g %s %Y", &p])?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

fn unique_test_root() -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!("verfsnext-rsync-{}-{}", std::process::id(), now))
}

fn setup_source_tree(src: &Path) -> Result<()> {
    fs::create_dir_all(src.join("nested")).context("failed to create nested src dir")?;

    let mut alpha = File::create(src.join("alpha.txt")).context("failed to create alpha.txt")?;
    writeln!(alpha, "VerFSNext rsync validation")?;
    writeln!(alpha, "timestamp-seed=1700000000")?;

    let mut beta =
        File::create(src.join("nested/beta.bin")).context("failed to create beta.bin")?;
    for i in 0..8192_u32 {
        beta.write_all(&i.to_le_bytes())?;
    }

    std::os::unix::fs::symlink("../alpha.txt", src.join("nested/alpha.link"))
        .context("failed to create symlink")?;

    Ok(())
}

fn setup_large_source_file(src: &Path) -> Result<PathBuf> {
    fs::create_dir_all(src).context("failed to create large source dir")?;
    let large = src.join("large.bin");
    let mut file = File::create(&large).context("failed to create large.bin")?;
    // Generate deterministic high-entropy bytes so dedup/compression does not
    // collapse the 4 MiB payload into a tiny physical write.
    let mut block = vec![0_u8; 1024 * 1024];
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    for _ in 0..4 {
        for byte in &mut block {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (state >> 56) as u8;
        }
        file.write_all(&block)
            .context("failed to write large.bin block")?;
    }
    Ok(large)
}

fn count_files_with_extension_recursive(root: &Path, extension: &str) -> Result<usize> {
    let mut stack = vec![root.to_path_buf()];
    let mut count = 0_usize;

    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(&path)
            .with_context(|| format!("failed to read directory {}", path.display()))?
        {
            let entry = entry?;
            let entry_path = entry.path();
            if entry_path.is_dir() {
                stack.push(entry_path);
                continue;
            }
            let matches = entry_path
                .extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext == extension)
                .unwrap_or(false);
            if matches {
                count = count.saturating_add(1);
            }
        }
    }

    Ok(count)
}

fn create_recursive_delete_fanout(mount_point: &Path, dir_count: usize) -> Result<PathBuf> {
    let target = mount_point.join("rm_fanout");
    let target_s = target.to_string_lossy().into_owned();
    let script = format!(
        "set -euo pipefail; target='{}'; mkdir -p \"$target\"; for i in $(seq 1 {}); do d=\"$target/dir_$i\"; mkdir -p \"$d\"; printf 'payload-%s' \"$i\" > \"$d/file.txt\"; done",
        target_s, dir_count
    );
    run_cmd(240, None, "bash", &["-c", &script])?;
    Ok(target)
}

#[test]
fn rsync_copy_remount_and_delete_persistence() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in [
        "timeout",
        "rsync",
        "sha256sum",
        "mountpoint",
        "fusermount",
        "cp",
        "mv",
        "rm",
        "ls",
        "stat",
    ] {
        if require_tool(tool).is_err() {
            return Ok(());
        }
    }

    let root = unique_test_root();
    let mount_point = root.join("mnt");
    let data_dir = root.join("data");
    let src_dir = root.join("src");
    fs::create_dir_all(&mount_point).context("failed to create mount dir")?;
    fs::create_dir_all(&data_dir).context("failed to create data dir")?;
    fs::create_dir_all(&src_dir).context("failed to create source dir")?;

    setup_source_tree(&src_dir)?;

    let config = format!(
        "mount_point = \"{}\"\ndata_dir = \"{}\"\nsync_interval_ms = 1000\nbatch_max_blocks = 3000\nbatch_flush_interval_ms = 500\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), config).context("failed to write config.toml")?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let src_alpha = src_dir.join("alpha.txt");
    let cp_probe = mount_point.join("cp_probe.txt");
    let mv_probe = mount_point.join("mv_probe.txt");
    let src_alpha_s = src_alpha.to_string_lossy().into_owned();
    let cp_probe_s = cp_probe.to_string_lossy().into_owned();
    let mv_probe_s = mv_probe.to_string_lossy().into_owned();
    run_cmd(120, None, "cp", &[&src_alpha_s, &cp_probe_s])?;
    run_cmd(120, None, "mv", &[&cp_probe_s, &mv_probe_s])?;

    let src_rsync = format!("{}/", src_dir.display());
    let dst_rsync = format!("{}/", mount_point.join("t1").display());
    run_cmd(120, None, "rsync", &["-a", &src_rsync, &dst_rsync])?;

    let dst_alpha = mount_point.join("t1/alpha.txt");
    let dst_beta = mount_point.join("t1/nested/beta.bin");

    let src_alpha_hash = sha256(&src_alpha)?;
    let dst_alpha_hash = sha256(&dst_alpha)?;
    if src_alpha_hash != dst_alpha_hash {
        bail!("alpha checksum mismatch after rsync copy")
    }

    let src_beta_hash = sha256(&src_dir.join("nested/beta.bin"))?;
    let dst_beta_hash = sha256(&dst_beta)?;
    if src_beta_hash != dst_beta_hash {
        bail!("beta checksum mismatch after rsync copy")
    }

    let src_alpha_stat = stat_signature(&src_alpha)?;
    let dst_alpha_stat = stat_signature(&dst_alpha)?;
    if src_alpha_stat != dst_alpha_stat {
        bail!("alpha stat mismatch after rsync copy")
    }

    daemon.stop_graceful()?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let remount_alpha_hash = sha256(&dst_alpha)?;
    let remount_beta_hash = sha256(&dst_beta)?;
    if remount_alpha_hash != src_alpha_hash || remount_beta_hash != src_beta_hash {
        bail!("checksum mismatch after remount")
    }

    let rm_target = mount_point.join("t1");
    let rm_target_s = rm_target.to_string_lossy().into_owned();
    run_cmd(120, None, "rm", &["-rf", &rm_target_s])?;

    let mount_ls_s = mount_point.to_string_lossy().into_owned();
    let ls_out = run_cmd(120, None, "ls", &["-la", &mount_ls_s])?;
    let ls_text = String::from_utf8_lossy(&ls_out.stdout);
    if ls_text.contains("t1") {
        bail!("t1 still appears in ls output after deletion")
    }

    let stat_out = run_cmd_raw(120, None, "stat", &[&rm_target_s])?;
    if stat_out.status.success() {
        bail!("stat unexpectedly succeeded for deleted path")
    }

    daemon.stop_graceful()?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let ls_out_post = run_cmd(120, None, "ls", &["-la", &mount_ls_s])?;
    let ls_text_post = String::from_utf8_lossy(&ls_out_post.stdout);
    if ls_text_post.contains("t1") {
        bail!("t1 reappeared after remount")
    }

    let stat_out_post = run_cmd_raw(120, None, "stat", &[&rm_target_s])?;
    if stat_out_post.status.success() {
        bail!("deleted path exists again after remount")
    }

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);

    Ok(())
}

#[test]
fn rsync_large_file_rollover_multpack_persists() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in [
        "timeout",
        "rsync",
        "sha256sum",
        "mountpoint",
        "fusermount",
        "ls",
    ] {
        if require_tool(tool).is_err() {
            return Ok(());
        }
    }

    let root = unique_test_root();
    let mount_point = root.join("mnt");
    let data_dir = root.join("data");
    let src_dir = root.join("src");
    fs::create_dir_all(&mount_point).context("failed to create mount dir")?;
    fs::create_dir_all(&data_dir).context("failed to create data dir")?;
    fs::create_dir_all(&src_dir).context("failed to create source dir")?;
    let src_large = setup_large_source_file(&src_dir)?;

    let config = format!(
        "mount_point = \"{}\"\ndata_dir = \"{}\"\nsync_interval_ms = 1000\nbatch_max_blocks = 3000\nbatch_flush_interval_ms = 500\npack_max_size_mb = 1\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), config).context("failed to write config.toml")?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let src_rsync = format!("{}/", src_dir.display());
    let dst_rsync = format!("{}/", mount_point.join("roll").display());
    run_cmd(240, None, "rsync", &["-a", &src_rsync, &dst_rsync])?;

    let dst_large = mount_point.join("roll/large.bin");
    let src_hash = sha256(&src_large)?;
    let dst_hash = sha256(&dst_large)?;
    if src_hash != dst_hash {
        bail!("large file checksum mismatch after rsync")
    }

    daemon.stop_graceful()?;

    let packs_dir = data_dir.join("packs");
    let pack_files = count_files_with_extension_recursive(&packs_dir, "vpk")?;
    if pack_files < 2 {
        bail!(
            "expected rollover to create multiple packs, found {}",
            pack_files
        );
    }

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;
    let remount_hash = sha256(&dst_large)?;
    if remount_hash != src_hash {
        bail!("large file checksum mismatch after remount")
    }

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn rm_rf_large_fanout_directory_succeeds() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in ["timeout", "mountpoint", "fusermount", "rm", "stat", "bash"] {
        if require_tool(tool).is_err() {
            return Ok(());
        }
    }

    let root = unique_test_root();
    let mount_point = root.join("mnt");
    let data_dir = root.join("data");
    fs::create_dir_all(&mount_point).context("failed to create mount dir")?;
    fs::create_dir_all(&data_dir).context("failed to create data dir")?;

    let config = format!(
        "mount_point = \"{}\"\ndata_dir = \"{}\"\nsync_interval_ms = 1000\nbatch_max_blocks = 3000\nbatch_flush_interval_ms = 500\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), config).context("failed to write config.toml")?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let rm_target = create_recursive_delete_fanout(&mount_point, 1200)?;
    let rm_target_s = rm_target.to_string_lossy().into_owned();
    run_cmd(240, None, "rm", &["-rf", &rm_target_s])?;

    let stat_out = run_cmd_raw(120, None, "stat", &[&rm_target_s])?;
    if stat_out.status.success() {
        bail!("rm_fanout still exists after rm -rf")
    }

    daemon.stop_graceful()?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;
    let stat_out_post = run_cmd_raw(120, None, "stat", &[&rm_target_s])?;
    if stat_out_post.status.success() {
        bail!("rm_fanout reappeared after remount")
    }

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);
    Ok(())
}
