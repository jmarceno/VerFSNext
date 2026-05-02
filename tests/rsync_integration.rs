use std::collections::BTreeSet;
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

fn worker_hash(worker: usize) -> String {
    format!("{worker:02}{worker:02}{worker:02}{worker:02}")
}

fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create parent dir for {}", path.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("failed to write {}", path.display()))
}

fn build_expected_cargo_like_tree(
    root: &Path,
    workers: usize,
    final_iteration: usize,
) -> Result<()> {
    let registry_prefix = "index.crates.io-1949cf8c6b5b557f";

    fs::create_dir_all(root.join("workspace"))?;
    fs::create_dir_all(root.join("registry/src").join(registry_prefix))?;
    fs::create_dir_all(root.join("target/debug/.fingerprint"))?;
    fs::create_dir_all(root.join("target/debug/deps"))?;
    fs::create_dir_all(root.join("target/debug/build"))?;
    fs::create_dir_all(root.join("target/debug/incremental"))?;
    fs::create_dir_all(root.join("target/tmp"))?;

    for worker in 1..=workers {
        let hash = worker_hash(worker);
        let crate_name = format!("member_{worker}");

        write_text_file(
            &root.join("workspace").join(&crate_name).join("Cargo.toml"),
            &format!(
                "[package]\nname = \"{crate_name}\"\nversion = \"0.{final_iteration}.0\"\nedition = \"2021\"\n[lib]\npath = \"src/lib.rs\"\n"
            ),
        )?;
        write_text_file(
            &root.join("workspace").join(&crate_name).join("src/lib.rs"),
            &format!(
                "pub const WORKER: usize = {worker};\npub const ITERATION: usize = {final_iteration};\npub fn marker() -> &'static str {{ \"worker-{worker}-iter-{final_iteration}\" }}\n"
            ),
        )?;
        write_text_file(
            &root
                .join("registry/src")
                .join(registry_prefix)
                .join(format!("{crate_name}-0.1.0"))
                .join("src/lib.rs"),
            &format!(
                "pub const REGISTRY_MARKER: &str = \"registry-{worker}-{final_iteration}\";\n"
            ),
        )?;
        write_text_file(
            &root
                .join("target/debug/deps")
                .join(format!("lib{crate_name}.d")),
            &format!(
                "workspace/{crate_name}/src/lib.rs: /deps/shared-{worker}-{final_iteration}\n"
            ),
        )?;
        write_text_file(
            &root
                .join("target/debug/.fingerprint")
                .join(format!("{crate_name}-{hash}"))
                .join("invoked.timestamp"),
            &format!(
                "{{\"worker\":{worker},\"iteration\":{final_iteration},\"crate\":\"{crate_name}\"}}\n"
            ),
        )?;
        write_text_file(
            &root
                .join("target/debug/build")
                .join(format!("{crate_name}-{hash}"))
                .join("out/generated.rs"),
            &format!("pub const GENERATED: &str = \"gen-{worker}-{final_iteration}\";\n"),
        )?;
        write_text_file(
            &root
                .join("target/debug/incremental")
                .join(&crate_name)
                .join(format!("s-{hash}"))
                .join("work-products.txt"),
            &format!("work-product=member_{worker} iteration={final_iteration}\n"),
        )?;
    }

    Ok(())
}

fn collect_tree_entries(root: &Path) -> Result<(BTreeSet<String>, BTreeSet<String>)> {
    let mut dirs = BTreeSet::new();
    let mut files = BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(&path)
            .with_context(|| format!("failed to read directory {}", path.display()))?
        {
            let entry = entry?;
            let entry_path = entry.path();
            let rel = entry_path
                .strip_prefix(root)
                .with_context(|| format!("failed to strip prefix {}", root.display()))?
                .to_string_lossy()
                .replace('\u{5c}', "/");
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                dirs.insert(rel);
                stack.push(entry_path);
            } else if file_type.is_file() {
                files.insert(rel);
            } else {
                bail!("unexpected non-file entry under {}", root.display());
            }
        }
    }

    Ok((dirs, files))
}

fn assert_trees_match(expected_root: &Path, actual_root: &Path) -> Result<()> {
    let (expected_dirs, expected_files) = collect_tree_entries(expected_root)?;
    let (actual_dirs, actual_files) = collect_tree_entries(actual_root)?;

    if expected_dirs != actual_dirs {
        bail!(
            "directory layout mismatch under {}\nexpected: {:?}\nactual: {:?}",
            actual_root.display(),
            expected_dirs,
            actual_dirs
        );
    }
    if expected_files != actual_files {
        bail!(
            "file layout mismatch under {}\nexpected: {:?}\nactual: {:?}",
            actual_root.display(),
            expected_files,
            actual_files
        );
    }

    for rel in expected_files {
        let expected = expected_root.join(&rel);
        let actual = actual_root.join(&rel);
        let expected_s = expected.to_string_lossy().into_owned();
        let actual_s = actual.to_string_lossy().into_owned();
        run_cmd(30, None, "cmp", &["-s", &expected_s, &actual_s])?;
    }

    Ok(())
}

fn verify_cargo_like_mount_state(expected_root: &Path, mount_point: &Path) -> Result<()> {
    for subtree in ["workspace", "registry", "target"] {
        assert_trees_match(&expected_root.join(subtree), &mount_point.join(subtree))?;
    }
    Ok(())
}

fn cargo_like_workload_script(
    mount_point: &Path,
    workers: usize,
    iterations: usize,
    readers: usize,
) -> String {
    let mut script = String::new();
    script.push_str("set -euo pipefail\n");
    script.push_str(&format!("mnt={:?}\n", mount_point.to_string_lossy()));
    script.push_str(&format!("workers={}\n", workers));
    script.push_str(&format!("iterations={}\n", iterations));
    script.push_str(&format!("readers={}\n", readers));
    script.push_str(
        "registry_root=\"$mnt/registry/src/index.crates.io-1949cf8c6b5b557f\"\nworkspace_root=\"$mnt/workspace\"\nfingerprint_root=\"$mnt/target/debug/.fingerprint\"\ndeps_root=\"$mnt/target/debug/deps\"\nbuild_root=\"$mnt/target/debug/build\"\nincremental_root=\"$mnt/target/debug/incremental\"\nscratch_root=\"$mnt/target/tmp\"\nmkdir -p \"$registry_root\" \"$workspace_root\" \"$fingerprint_root\" \"$deps_root\" \"$build_root\" \"$incremental_root\" \"$scratch_root\"\n",
    );
    script.push_str(
        r#"
worker() {
  w="$1"
  crate="member_${w}"
  hash=$(printf '%02d%02d%02d%02d' "$w" "$w" "$w" "$w")
  crate_dir="$workspace_root/$crate"
  registry_dir="$registry_root/$crate-0.1.0"
  fp_dir="$fingerprint_root/$crate-$hash"
  build_out="$build_root/$crate-$hash/out"
  incr_dir="$incremental_root/$crate/s-$hash"

  for i in $(seq 1 "$iterations"); do
    if [ $((i % 4)) -eq 0 ]; then
      rm -rf "$build_out" "$incr_dir"
    fi
    mkdir -p "$crate_dir/src" "$registry_dir/src" "$fp_dir" "$build_out" "$incr_dir"

    cargo_tmp="$crate_dir/Cargo.toml.tmp.$i.$$"
    lib_tmp="$crate_dir/src/lib.rs.tmp.$i.$$"
    registry_tmp="$registry_dir/src/lib.rs.tmp.$i.$$"
    dep_tmp="$deps_root/lib${crate}.d.tmp.$i.$$"
    fp_tmp="$fp_dir/invoked.timestamp.tmp.$i.$$"
    build_tmp="$build_out/generated.rs.tmp.$i.$$"
    incr_tmp="$incr_dir/work-products.txt.tmp.$i.$$"

    printf '[package]\nname = "%s"\nversion = "0.%s.0"\nedition = "2021"\n[lib]\npath = "src/lib.rs"\n' "$crate" "$i" > "$cargo_tmp"
    printf 'pub const WORKER: usize = %s;\npub const ITERATION: usize = %s;\npub fn marker() -> &'"'"'static str { "worker-%s-iter-%s" }\n' "$w" "$i" "$w" "$i" > "$lib_tmp"
    printf 'pub const REGISTRY_MARKER: &str = "registry-%s-%s";\n' "$w" "$i" > "$registry_tmp"
    printf 'workspace/%s/src/lib.rs: /deps/shared-%s-%s\n' "$crate" "$w" "$i" > "$dep_tmp"
    printf '{"worker":%s,"iteration":%s,"crate":"%s"}\n' "$w" "$i" "$crate" > "$fp_tmp"
    printf 'pub const GENERATED: &str = "gen-%s-%s";\n' "$w" "$i" > "$build_tmp"
    printf 'work-product=member_%s iteration=%s\n' "$w" "$i" > "$incr_tmp"

    if [ $((i % 3)) -eq 0 ]; then
      rm -f "$deps_root/lib${crate}.d" "$fp_dir/invoked.timestamp"
    fi

    mv "$cargo_tmp" "$crate_dir/Cargo.toml"
    mv "$lib_tmp" "$crate_dir/src/lib.rs"
    mv "$registry_tmp" "$registry_dir/src/lib.rs"
    mv "$dep_tmp" "$deps_root/lib${crate}.d"
    mv "$fp_tmp" "$fp_dir/invoked.timestamp"
    mv "$build_tmp" "$build_out/generated.rs"
    mv "$incr_tmp" "$incr_dir/work-products.txt"

    cat "$crate_dir/Cargo.toml" "$crate_dir/src/lib.rs" "$registry_dir/src/lib.rs" >/dev/null
    cat "$deps_root/lib${crate}.d" "$fp_dir/invoked.timestamp" "$build_out/generated.rs" "$incr_dir/work-products.txt" >/dev/null
    sha256sum "$crate_dir/Cargo.toml" "$crate_dir/src/lib.rs" "$registry_dir/src/lib.rs" >/dev/null

    scratch="$scratch_root/$crate-$i"
    mkdir -p "$scratch/dir_a" "$scratch/dir_b"
    printf 'scratch-%s-%s-a\n' "$w" "$i" > "$scratch/dir_a/a.txt"
    printf 'scratch-%s-%s-b\n' "$w" "$i" > "$scratch/dir_b/b.txt"
    cat "$scratch/dir_a/a.txt" "$scratch/dir_b/b.txt" >/dev/null
    rm -rf "$scratch"
  done
}

reader() {
  for round in $(seq 1 $((iterations * 6))); do
    for w in $(seq 1 "$workers"); do
      crate="member_${w}"
      hash=$(printf '%02d%02d%02d%02d' "$w" "$w" "$w" "$w")
      crate_dir="$workspace_root/$crate"
      registry_dir="$registry_root/$crate-0.1.0"
      fp_dir="$fingerprint_root/$crate-$hash"
      build_out="$build_root/$crate-$hash/out"
      incr_dir="$incremental_root/$crate/s-$hash"

      ls "$crate_dir" >/dev/null 2>&1 || true
      cat "$crate_dir/Cargo.toml" >/dev/null 2>&1 || true
      cat "$crate_dir/src/lib.rs" >/dev/null 2>&1 || true
      cat "$registry_dir/src/lib.rs" >/dev/null 2>&1 || true
      cat "$deps_root/lib${crate}.d" >/dev/null 2>&1 || true
      cat "$fp_dir/invoked.timestamp" >/dev/null 2>&1 || true
      cat "$build_out/generated.rs" >/dev/null 2>&1 || true
      cat "$incr_dir/work-products.txt" >/dev/null 2>&1 || true
    done

    ls "$workspace_root" >/dev/null 2>&1 || true
    ls "$deps_root" >/dev/null 2>&1 || true
    ls "$fingerprint_root" >/dev/null 2>&1 || true
    ls "$build_root" >/dev/null 2>&1 || true
    ls "$incremental_root" >/dev/null 2>&1 || true
  done
}

pids=""
for w in $(seq 1 "$workers"); do
  worker "$w" &
  pids="$pids $!"
done
for r in $(seq 1 "$readers"); do
  reader &
  pids="$pids $!"
done
for pid in $pids; do
  wait "$pid"
done
"#,
    );
    script
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

#[test]
fn cargo_like_concurrent_crud_integrity_persists() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in [
        "timeout",
        "mountpoint",
        "fusermount",
        "bash",
        "cat",
        "cmp",
        "ls",
        "sha256sum",
    ] {
        if require_tool(tool).is_err() {
            return Ok(());
        }
    }

    let workers = 6_usize;
    let iterations = 12_usize;
    let readers = 3_usize;

    let root = unique_test_root();
    let mount_point = root.join("mnt");
    let data_dir = root.join("data");
    let expected_dir = root.join("expected");
    fs::create_dir_all(&mount_point).context("failed to create mount dir")?;
    fs::create_dir_all(&data_dir).context("failed to create data dir")?;
    fs::create_dir_all(&expected_dir).context("failed to create expected dir")?;
    build_expected_cargo_like_tree(&expected_dir, workers, iterations)?;

    let config = format!(
        "mount_point = \"{}\"\ndata_dir = \"{}\"\nsync_interval_ms = 1000\nbatch_max_blocks = 3000\nbatch_flush_interval_ms = 250\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), config).context("failed to write config.toml")?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let workload = cargo_like_workload_script(&mount_point, workers, iterations, readers);
    run_cmd(300, None, "bash", &["-c", &workload])?;

    verify_cargo_like_mount_state(&expected_dir, &mount_point)?;

    daemon.stop_graceful()?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    verify_cargo_like_mount_state(&expected_dir, &mount_point)?;

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn invalidation_sensitive_visibility_with_entry_ttl() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in [
        "timeout",
        "mountpoint",
        "fusermount",
        "bash",
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
    fs::create_dir_all(&mount_point).context("failed to create mount dir")?;
    fs::create_dir_all(&data_dir).context("failed to create data dir")?;

    let config = format!(
        "mount_point = \"{}\"\ndata_dir = \"{}\"\nsync_interval_ms = 1000\nbatch_max_blocks = 3000\nbatch_flush_interval_ms = 250\nfuse_attr_ttl_ms = 4000\nfuse_entry_ttl_ms = 4000\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), config).context("failed to write config.toml")?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let vis_dir = mount_point.join("vis");
    let vis_dir_s = vis_dir.to_string_lossy().into_owned();
    run_cmd(
        30,
        None,
        "bash",
        &["-c", &format!("mkdir -p '{}'", vis_dir_s)],
    )?;

    let created = vis_dir.join("created.txt");
    let created_s = created.to_string_lossy().into_owned();
    let created_stat_before = run_cmd_raw(10, None, "stat", &[&created_s])?;
    if created_stat_before.status.success() {
        bail!("created.txt unexpectedly existed before create")
    }

    run_cmd(
        30,
        None,
        "bash",
        &["-c", &format!("printf 'create-visible' > '{}'", created_s)],
    )?;

    let created_stat_after = run_cmd_raw(10, None, "stat", &[&created_s])?;
    if !created_stat_after.status.success() {
        bail!("created.txt not visible immediately after create with entry TTL")
    }

    run_cmd(30, None, "rm", &["-f", &created_s])?;
    let created_stat_deleted = run_cmd_raw(10, None, "stat", &[&created_s])?;
    if created_stat_deleted.status.success() {
        bail!("created.txt still visible immediately after unlink with entry TTL")
    }

    let old_name = vis_dir.join("old.txt");
    let new_name = vis_dir.join("new.txt");
    let old_s = old_name.to_string_lossy().into_owned();
    let new_s = new_name.to_string_lossy().into_owned();

    run_cmd(
        30,
        None,
        "bash",
        &["-c", &format!("printf 'rename-source' > '{}'", old_s)],
    )?;

    let old_stat_before = run_cmd_raw(10, None, "stat", &[&old_s])?;
    if !old_stat_before.status.success() {
        bail!("old.txt missing before rename")
    }
    let new_stat_before = run_cmd_raw(10, None, "stat", &[&new_s])?;
    if new_stat_before.status.success() {
        bail!("new.txt unexpectedly existed before rename")
    }

    run_cmd(30, None, "mv", &[&old_s, &new_s])?;

    let old_stat_after = run_cmd_raw(10, None, "stat", &[&old_s])?;
    if old_stat_after.status.success() {
        bail!("old.txt still visible immediately after rename with entry TTL")
    }
    let new_stat_after = run_cmd_raw(10, None, "stat", &[&new_s])?;
    if !new_stat_after.status.success() {
        bail!("new.txt not visible immediately after rename with entry TTL")
    }

    let ls_out = run_cmd(30, None, "ls", &["-1", &vis_dir_s])?;
    let ls_text = String::from_utf8_lossy(&ls_out.stdout);
    if ls_text.lines().any(|line| line == "old.txt") {
        bail!("old.txt still listed after rename with entry TTL")
    }
    if !ls_text.lines().any(|line| line == "new.txt") {
        bail!("new.txt missing from directory listing after rename with entry TTL")
    }

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

#[test]
fn invalidation_sensitive_hardlink_visibility_with_entry_ttl() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in [
        "timeout",
        "mountpoint",
        "fusermount",
        "bash",
        "ln",
        "rm",
        "stat",
    ] {
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
        "mount_point = \"{}\"\ndata_dir = \"{}\"\nsync_interval_ms = 1000\nbatch_max_blocks = 3000\nbatch_flush_interval_ms = 250\nfuse_attr_ttl_ms = 4000\nfuse_entry_ttl_ms = 4000\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), config).context("failed to write config.toml")?;

    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let hardlink_dir = mount_point.join("hardlink_vis");
    let hardlink_dir_s = hardlink_dir.to_string_lossy().into_owned();
    run_cmd(
        30,
        None,
        "bash",
        &["-c", &format!("mkdir -p '{}'", hardlink_dir_s)],
    )?;

    let base = hardlink_dir.join("base.txt");
    let alias = hardlink_dir.join("alias.txt");
    let base_s = base.to_string_lossy().into_owned();
    let alias_s = alias.to_string_lossy().into_owned();

    run_cmd(
        30,
        None,
        "bash",
        &["-c", &format!("printf 'hardlink-source' > '{}'", base_s)],
    )?;

    let alias_before = run_cmd_raw(10, None, "stat", &[&alias_s])?;
    if alias_before.status.success() {
        bail!("alias.txt unexpectedly existed before link")
    }

    run_cmd(30, None, "ln", &[&base_s, &alias_s])?;

    let alias_after = run_cmd_raw(10, None, "stat", &[&alias_s])?;
    if !alias_after.status.success() {
        bail!("alias.txt not visible immediately after hardlink with entry TTL")
    }

    let base_stat = run_cmd(10, None, "stat", &["-c", "%i %h", &base_s])?;
    let alias_stat = run_cmd(10, None, "stat", &["-c", "%i %h", &alias_s])?;
    let base_sig = String::from_utf8_lossy(&base_stat.stdout).trim().to_owned();
    let alias_sig = String::from_utf8_lossy(&alias_stat.stdout)
        .trim()
        .to_owned();
    if base_sig != alias_sig {
        bail!("hardlink inode/link-count mismatch: base={base_sig} alias={alias_sig}")
    }

    run_cmd(30, None, "rm", &["-f", &alias_s])?;
    let alias_deleted = run_cmd_raw(10, None, "stat", &[&alias_s])?;
    if alias_deleted.status.success() {
        bail!("alias.txt still visible immediately after unlink with entry TTL")
    }

    let base_after = run_cmd_raw(10, None, "stat", &[&base_s])?;
    if !base_after.status.success() {
        bail!("base.txt missing after alias unlink")
    }

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

/// Benchmark copying files with the same profile as the ComfyUI install at
/// `/mnt/verfs/comfyui/`. That dataset has three tiers:
///
///   Tier       Files   Size     Profile
///   ────────────────────────────────────────
///   comfy-env  72K      9 GB    Python venv: many tiny files <64KB
///   ComfyUI    14K    2+ GB     App code: mostly <16KB
///   models    110     14 GB     Model weights: 124 MB – 3.4 GB
///
/// We downsample to ~500 small files (23 MB) + 2 large files (192 MB) so the
/// benchmark finishes in ~10 s but still exercises the same I/O patterns.
///
/// **Durability measurement** – every file is individually `fsync`'d through
/// Heavy ComfyUI-like profile benchmark.
///
/// Phases (all with UNIQUE content per file, no dedup collapsing):
///   1. sm_write_dura  — Write 3000 small files (~274 MB unique) to a site-packages layout
///   2. sm_read_seq    — Sequential read (hot cache)
///   3. lg_write_dura  — Write 3 large models (768+384+192 = 1344 MB unique) — evicts small chunks
///   4. sm_read_rnd    — Random-order read of small files (COLD — chunks evicted by phase 3)
///   5. lg_read_seq    — Sequential large-file read (hot chunks)
///   6. lg_read_rev    — Reverse-order large-file read (defeats sequential readahead)
///   7. mod_write_dura — Overwrite 750 files with NEW unique content
///   8. mod_read       — Read modified files
///   9. sync_barrier   — system-wide sync
///
/// Total unique data written: ~1.7 GB (exceeds default 1 GB chunk cache).
/// Timing is wall-clock via `date +%s%N` inside the FUSE mount.
#[test]
fn bench_comfyui_profile() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in [
        "timeout",
        "mountpoint",
        "fusermount",
        "bash",
        "dd",
        "sync",
        "sha256sum",
        "shuf",
    ] {
        if require_tool(tool).is_err() {
            return Ok(());
        }
    }

    let root = unique_test_root();
    let mount_point = root.join("mnt");
    let data_dir = root.join("data");
    fs::create_dir_all(&mount_point).context("mount dir")?;
    fs::create_dir_all(&data_dir).context("data dir")?;

    let config = format!(
        "mount_point = \"{}\"\ndata_dir = \"{}\"\n\
         sync_interval_ms = 1000\nbatch_max_blocks = 3000\n\
         batch_flush_interval_ms = 250\n\
         fuse_attr_ttl_ms = 0\nfuse_entry_ttl_ms = 0\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), &config).context("config")?;

    // ── Seed content ──────────────────────────────────────────────────────
    // 2 GB deterministic high-entropy seed so every file can use unique data.
    let seed_file = root.join("seed.bin");
    let mut sf = File::create(&seed_file).context("seed file")?;
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut block = vec![0u8; 1024 * 1024];
    for _ in 0..2048 {
        for byte in &mut block {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (state >> 56) as u8;
        }
        sf.write_all(&block).context("seed chunk")?;
    }
    drop(sf);
    let seed_s = seed_file.to_string_lossy();

    // ── Launch daemon ─────────────────────────────────────────────────────
    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let mnt_s = mount_point.to_string_lossy();

    // ── Benchmark script ───────────────────────────────────────────────────
    // Heavier, cache-defeating workload:
    //   - 3000 small files with UNIQUE content (different seed offsets, no dedup)
    //   - 3 large model files totaling ~1.3 GB of unique data
    //   - Random-order & reverse-order reads to defeat OS page cache + FUSE cache
    //   - Overwrite phase (750 modified files with NEW unique content)
    //   - Total unique data written exceeds default 1 GB chunk cache
    //   - Phase ordering guarantees small file chunks are evicted before cold read
    let script = format!(
        r#"set -euo pipefail
SEED={seed_s}
MNT={mnt_s}

phase() {{
    local name="$1" start="$2" end="$3"
    echo "BENCH:$name:$(( end - start ))"
}}

SITE="$MNT/lib/python3.13/site-packages"
BINDIR="$MNT/bin"
MODELS="$MNT/models"
mkdir -p "$SITE" "$BINDIR" "$MODELS"

# ---- Phase 1: Write small files with UNIQUE content --------------------------
# 300 packages x 10 files = 3000 files
# Each file reads from a unique, non-overlapping seed offset so no dedup occurs.
# Tier breakdown:
#   tier-1  35%: 1050 files, <1 KB (config / __init__)
#   tier-2  50%: 1500 files, 1-64 KB (source modules)
#   tier-3  15%: 450 files, 64 KB-1 MB (compiled extensions / data)

p1_start=$(date +%s%N)
cumulative_kb=0

for pkg in $(seq 1 300); do
    pdir="$SITE/package_$(printf '%04d' "$pkg")"
    mkdir -p "$pdir"
    for slot in 0 1 2 3 4 5 6 7 8 9; do
        idx=$(( (pkg - 1) * 10 + slot + 1 ))
        if [ "$idx" -le 1050 ]; then
            # tier-1: <1 KB → round to 1 KB
            size_kb=1
        elif [ "$idx" -le 2550 ]; then
            # tier-2: 1-64 KB
            size_bytes=$(( 1024 + (idx * 13) % 64512 ))
            size_kb=$(( (size_bytes + 1023) / 1024 ))
        else
            # tier-3: 64 KB-1 MB
            size_bytes=$(( 65536 + (idx * 17) % 983040 ))
            size_kb=$(( (size_bytes + 1023) / 1024 ))
        fi
        fname="mod_$(printf '%02d' "$slot").py"
        [ "$slot" -eq 0 ] && fname="__init__.py"
        dd if="$SEED" bs=1024 count="$size_kb" skip="$cumulative_kb" of="$pdir/$fname" conv=fsync 2>/dev/null
        cumulative_kb=$(( cumulative_kb + size_kb ))
    done
done

# Bin scripts (20 small scripts with unique content)
for i in $(seq 1 20); do
    size_kb=$(( 1 + (i * 11) % 15 ))
    dd if="$SEED" bs=1024 count="$size_kb" skip="$cumulative_kb" of="$BINDIR/script_$(printf '%04d' "$i")" conv=fsync 2>/dev/null
    cumulative_kb=$(( cumulative_kb + size_kb ))
done

p1_end=$(date +%s%N)
phase "sm_write_dura" "$p1_start" "$p1_end"

# ---- Phase 2: Read small files sequentially (HOT cache) ----------------------
p2_start=$(date +%s%N)
find "$SITE" -type f -exec sha256sum {{}} + > /dev/null
sha256sum "$BINDIR"/* > /dev/null
p2_end=$(date +%s%N)
phase "sm_read_seq" "$p2_start" "$p2_end"

# ---- Phase 3: Large files with UNIQUE content (EVICTS small file chunks) -----
# 768 MB checkpoint + 384 MB diffusion + 192 MB vae = 1344 MB total
# Reads from unique seed regions well past the small-file data.
# This write overflows the default 1 GB chunk cache, evicting small-file chunks.
p3_start=$(date +%s%N)
lgb_mb=400
dd if="$SEED" bs=1M count=768 skip=$lgb_mb of="$MODELS/checkpoint.bin" conv=fsync 2>/dev/null
lgb_mb=$(( lgb_mb + 768 ))
dd if="$SEED" bs=1M count=384 skip=$lgb_mb of="$MODELS/diffusion.bin" conv=fsync 2>/dev/null
lgb_mb=$(( lgb_mb + 384 ))
dd if="$SEED" bs=1M count=192 skip=$lgb_mb of="$MODELS/vae.bin" conv=fsync 2>/dev/null
p3_end=$(date +%s%N)
phase "lg_write_dura" "$p3_start" "$p3_end"

# ---- Phase 4: Read small files in random order (COLD - chunks evicted) -------
# The large file write evicted small file chunks from cache.
# shuf randomizes access order, defeating any remaining OS page cache.
p4_start=$(date +%s%N)
find "$SITE" -type f -print0 | shuf -z | xargs -0 -n1 sha256sum > /dev/null
sha256sum "$BINDIR"/* > /dev/null
p4_end=$(date +%s%N)
phase "sm_read_rnd" "$p4_start" "$p4_end"

# ---- Phase 5: Read large files sequentially (HOT chunks) ---------------------
p5_start=$(date +%s%N)
sha256sum "$MODELS/checkpoint.bin" > /dev/null
sha256sum "$MODELS/diffusion.bin" > /dev/null
sha256sum "$MODELS/vae.bin" > /dev/null
p5_end=$(date +%s%N)
phase "lg_read_seq" "$p5_start" "$p5_end"

# ---- Phase 6: Read large files in REVERSE order (defeats seq readahead) ------
p6_start=$(date +%s%N)
sha256sum "$MODELS/vae.bin" > /dev/null
sha256sum "$MODELS/diffusion.bin" > /dev/null
sha256sum "$MODELS/checkpoint.bin" > /dev/null
p6_end=$(date +%s%N)
phase "lg_read_rev" "$p6_start" "$p6_end"

# ---- Phase 7: Overwrite 50% of small files with NEW unique content ------------
# Every other package (150 of 300), 5 files each = 750 modified files.
# Reads from seeds at offset well past all prior regions.
p7_start=$(date +%s%N)
ovw_kb=$(( cumulative_kb + 4096 ))
for pkg in $(seq 2 2 300); do
    pdir="$SITE/package_$(printf '%04d' "$pkg")"
    for slot in 5 6 7 8 9; do
        idx=$(( (pkg - 1) * 10 + slot + 1 ))
        size_bytes=$(( 2048 + (idx * 23) % 64512 ))
        size_kb=$(( (size_bytes + 1023) / 1024 ))
        fname="mod_$(printf '%02d' "$slot").py"
        dd if="$SEED" bs=1024 count="$size_kb" skip="$ovw_kb" of="$pdir/$fname" conv=fsync 2>/dev/null
        ovw_kb=$(( ovw_kb + size_kb ))
    done
done
p7_end=$(date +%s%N)
phase "mod_write_dura" "$p7_start" "$p7_end"

# ---- Phase 8: Read modified files --------------------------------------------
p8_start=$(date +%s%N)
for pkg in $(seq 2 2 300); do
    pdir="$SITE/package_$(printf '%04d' "$pkg")"
    sha256sum "$pdir"/* > /dev/null
done
p8_end=$(date +%s%N)
phase "mod_read" "$p8_start" "$p8_end"

# ---- Phase 9: Final durability barrier (system-wide sync) ---------------------
p9_start=$(date +%s%N)
sync
p9_end=$(date +%s%N)
phase "sync_barrier" "$p9_start" "$p9_end"

echo "---"
echo "BENCH:summary_total:$(( p9_end - p1_start ))"
"#,
    );

    let out = run_cmd(900, Some(&root), "bash", &["-c", &script])?;
    let stdout = String::from_utf8_lossy(&out.stdout);

    for line in stdout.lines() {
        if line.starts_with("BENCH:") {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 3 {
                let name = parts[1];
                let ns: u128 = parts[2].parse().unwrap_or(0);
                let ms = ns as f64 / 1_000_000.0;
                println!("  {name:20}  {ms:>8.1} ms");
            }
        }
    }

    // Sanity checks: count all small files (pkg files under lib/python + bin scripts)
    let sm_lines = run_cmd(30, None, "find", &[
        mnt_s.as_ref(),
        "-type",
        "f",
        "(",
        "-path",
        "*/lib/python*",
        "-o",
        "-path",
        "*/bin/*",
        ")",
    ])?;
    let sm_count = String::from_utf8_lossy(&sm_lines.stdout)
        .lines()
        .count();
    anyhow::ensure!(
        sm_count == 3020,
        "expected 3020 small files (3000 pkg + 20 bin), got {}",
        sm_count
    );

    let lg_lines = run_cmd(30, None, "find", &[
        mnt_s.as_ref(),
        "-type",
        "f",
        "-path",
        "*/models/*",
    ])?;
    let lg_count = String::from_utf8_lossy(&lg_lines.stdout)
        .lines()
        .count();
    anyhow::ensure!(lg_count == 3, "expected 3 large files, got {}", lg_count);

    for path in &["models/checkpoint.bin", "models/diffusion.bin", "models/vae.bin"] {
        let stat_out = run_cmd(
            30,
            None,
            "stat",
            &["-c", "%s", &format!("{}/{}", mnt_s, path)],
        )?;
        let size: u64 = String::from_utf8_lossy(&stat_out.stdout)
            .trim()
            .parse()
            .context("stat size")?;
        anyhow::ensure!(size > 0, "{} is empty", path);
    }

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);
    Ok(())
}

/// Scaled-down version of bench_comfyui_profile for quick iteration.
/// 600 small files (~50 MB) + 3 large files (128+64+32 = 224 MB) + overwrite/rnd phases.
/// Same cache-defeating structure (unique content, random/reverse reads).
/// Total unique data: ~300 MB.
#[test]
fn bench_comfyui_profile_fast() -> Result<()> {
    if std::env::consts::OS != "linux" {
        return Ok(());
    }
    if std::env::var("VERFSNEXT_RUN_MOUNT_TESTS").ok().as_deref() != Some("1") {
        return Ok(());
    }

    for tool in [
        "timeout",
        "mountpoint",
        "fusermount",
        "bash",
        "dd",
        "sync",
        "sha256sum",
        "shuf",
    ] {
        if require_tool(tool).is_err() {
            return Ok(());
        }
    }

    let root = unique_test_root();
    let mount_point = root.join("mnt");
    let data_dir = root.join("data");
    fs::create_dir_all(&mount_point).context("mount dir")?;
    fs::create_dir_all(&data_dir).context("data dir")?;

    let config = format!(
        "mount_point = \"{}\"\ndata_dir = \"{}\"\n\
         sync_interval_ms = 1000\nbatch_max_blocks = 3000\n\
         batch_flush_interval_ms = 250\n\
         fuse_attr_ttl_ms = 0\nfuse_entry_ttl_ms = 0\n",
        mount_point.display(),
        data_dir.display()
    );
    fs::write(root.join("config.toml"), &config).context("config")?;

    // Seed content - deterministic high-entropy block
    let seed_file = root.join("seed.bin");
    let mut sf = File::create(&seed_file).context("seed file")?;
    let mut state = 0x9E37_79B9_7F4A_7C15_u64;
    let mut block = vec![0u8; 1024 * 1024];
    for _ in 0..384 {
        for byte in &mut block {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (state >> 56) as u8;
        }
        sf.write_all(&block).context("seed chunk")?;
    }
    drop(sf);
    let seed_s = seed_file.to_string_lossy();

    // Launch daemon
    let mut daemon = MountDaemon::start(&root, mount_point.clone())?;
    daemon.wait_until_mounted(Duration::from_secs(30))?;

    let mnt_s = mount_point.to_string_lossy();

    // ── Benchmark script ───────────────────────────────────────────────────
    // Scaled-down but same structure as the full benchmark.
    // 600 small files with unique content + 3 large files (224 MB total) + overwrite/reverse.
    let script = format!(
        r#"set -euo pipefail
SEED={seed_s}
MNT={mnt_s}

phase() {{
    local name="$1" start="$2" end="$3"
    echo "BENCH:$name:$(( end - start ))"
}}

SITE="$MNT/lib/python3.13/site-packages"
BINDIR="$MNT/bin"
MODELS="$MNT/models"
mkdir -p "$SITE" "$BINDIR" "$MODELS"

# ---- Phase 1: Small files with unique content ----
p1_start=$(date +%s%N)
cumulative_kb=0

for pkg in $(seq 1 100); do
    pdir="$SITE/package_$(printf '%04d' "$pkg")"
    mkdir -p "$pdir"
    for slot in 0 1 2 3 4 5; do
        idx=$(( (pkg - 1) * 6 + slot + 1 ))
        if [ "$idx" -le 210 ]; then
            size_kb=1
        elif [ "$idx" -le 510 ]; then
            size_bytes=$(( 1024 + (idx * 13) % 16384 ))
            size_kb=$(( (size_bytes + 1023) / 1024 ))
        else
            size_bytes=$(( 32768 + (idx * 17) % 229376 ))
            size_kb=$(( (size_bytes + 1023) / 1024 ))
        fi
        fname="mod_$(printf '%02d' "$slot").py"
        [ "$slot" -eq 0 ] && fname="__init__.py"
        dd if="$SEED" bs=1024 count="$size_kb" skip="$cumulative_kb" of="$pdir/$fname" conv=fsync 2>/dev/null
        cumulative_kb=$(( cumulative_kb + size_kb ))
    done
done

for i in $(seq 1 10); do
    size_kb=$(( 1 + (i * 7) % 10 ))
    dd if="$SEED" bs=1024 count="$size_kb" skip="$cumulative_kb" of="$BINDIR/script_$(printf '%04d' "$i")" conv=fsync 2>/dev/null
    cumulative_kb=$(( cumulative_kb + size_kb ))
done

p1_end=$(date +%s%N)
phase "sm_write_dura" "$p1_start" "$p1_end"

# ---- Phase 2: Read small files sequentially (HOT) ----
p2_start=$(date +%s%N)
find "$SITE" -type f -exec sha256sum {{}} + > /dev/null
sha256sum "$BINDIR"/* > /dev/null
p2_end=$(date +%s%N)
phase "sm_read_seq" "$p2_start" "$p2_end"

# ---- Phase 3: Large files (unique content, evicts small chunks) ----
p3_start=$(date +%s%N)
lgb_mb=100
dd if="$SEED" bs=1M count=128 skip=$lgb_mb of="$MODELS/checkpoint.bin" conv=fsync 2>/dev/null
lgb_mb=$(( lgb_mb + 128 ))
dd if="$SEED" bs=1M count=64 skip=$lgb_mb of="$MODELS/diffusion.bin" conv=fsync 2>/dev/null
lgb_mb=$(( lgb_mb + 64 ))
dd if="$SEED" bs=1M count=32 skip=$lgb_mb of="$MODELS/vae.bin" conv=fsync 2>/dev/null
p3_end=$(date +%s%N)
phase "lg_write_dura" "$p3_start" "$p3_end"

# ---- Phase 4: Read small files in random order (COLD) ----
p4_start=$(date +%s%N)
find "$SITE" -type f -print0 | shuf -z | xargs -0 -n1 sha256sum > /dev/null
sha256sum "$BINDIR"/* > /dev/null
p4_end=$(date +%s%N)
phase "sm_read_rnd" "$p4_start" "$p4_end"

# ---- Phase 5: Read large files sequentially ----
p5_start=$(date +%s%N)
sha256sum "$MODELS/checkpoint.bin" > /dev/null
sha256sum "$MODELS/diffusion.bin" > /dev/null
sha256sum "$MODELS/vae.bin" > /dev/null
p5_end=$(date +%s%N)
phase "lg_read_seq" "$p5_start" "$p5_end"

# ---- Phase 6: Read large files in REVERSE order ----
p6_start=$(date +%s%N)
sha256sum "$MODELS/vae.bin" > /dev/null
sha256sum "$MODELS/diffusion.bin" > /dev/null
sha256sum "$MODELS/checkpoint.bin" > /dev/null
p6_end=$(date +%s%N)
phase "lg_read_rev" "$p6_start" "$p6_end"

# ---- Phase 7: Overwrite 50% of small files ----
p7_start=$(date +%s%N)
ovw_kb=$(( cumulative_kb + 512 ))
for pkg in $(seq 2 2 100); do
    pdir="$SITE/package_$(printf '%04d' "$pkg")"
    for slot in 3 4 5; do
        size_bytes=$(( 2048 + (pkg * 17) % 16384 ))
        size_kb=$(( (size_bytes + 1023) / 1024 ))
        fname="mod_$(printf '%02d' "$slot").py"
        dd if="$SEED" bs=1024 count="$size_kb" skip="$ovw_kb" of="$pdir/$fname" conv=fsync 2>/dev/null
        ovw_kb=$(( ovw_kb + size_kb ))
    done
done
p7_end=$(date +%s%N)
phase "mod_write_dura" "$p7_start" "$p7_end"

# ---- Phase 8: Read modified files ----
p8_start=$(date +%s%N)
for pkg in $(seq 2 2 100); do
    pdir="$SITE/package_$(printf '%04d' "$pkg")"
    sha256sum "$pdir"/* > /dev/null
done
p8_end=$(date +%s%N)
phase "mod_read" "$p8_start" "$p8_end"

# ---- Phase 9: Sync barrier ----
p9_start=$(date +%s%N)
sync
p9_end=$(date +%s%N)
phase "sync_barrier" "$p9_start" "$p9_end"

echo "---"
echo "BENCH:summary_total:$(( p9_end - p1_start ))"
"#,
    );

    let out = run_cmd(600, Some(&root), "bash", &["-c", &script])?;
    let stdout = String::from_utf8_lossy(&out.stdout);

    for line in stdout.lines() {
        if line.starts_with("BENCH:") {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 3 {
                let name = parts[1];
                let ns: u128 = parts[2].parse().unwrap_or(0);
                let ms = ns as f64 / 1_000_000.0;
                println!("  {name:20}  {ms:>8.1} ms");
            }
        }
    }

    // Sanity checks
    let sm_lines = run_cmd(30, None, "find", &[
        mnt_s.as_ref(),
        "-type",
        "f",
        "(",
        "-path",
        "*/lib/python*",
        "-o",
        "-path",
        "*/bin/*",
        ")",
    ])?;
    let sm_count = String::from_utf8_lossy(&sm_lines.stdout)
        .lines()
        .count();
    anyhow::ensure!(
        sm_count == 610,
        "expected 610 small files (600 pkg + 10 bin), got {}",
        sm_count
    );

    let lg_lines = run_cmd(30, None, "find", &[
        mnt_s.as_ref(),
        "-type",
        "f",
        "-path",
        "*/models/*",
    ])?;
    let lg_count = String::from_utf8_lossy(&lg_lines.stdout)
        .lines()
        .count();
    anyhow::ensure!(lg_count == 3, "expected 3 large files, got {}", lg_count);

    for path in &["models/checkpoint.bin", "models/diffusion.bin", "models/vae.bin"] {
        let stat_out = run_cmd(
            30,
            None,
            "stat",
            &["-c", "%s", &format!("{}/{}", mnt_s, path)],
        )?;
        let size: u64 = String::from_utf8_lossy(&stat_out.stdout)
            .trim()
            .parse()
            .context("stat size")?;
        anyhow::ensure!(size > 0, "{} is empty", path);
    }

    daemon.stop_graceful()?;
    let _ = fs::remove_dir_all(&root);
    Ok(())
}
