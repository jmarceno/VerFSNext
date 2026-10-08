//! Running the filesystem for the desktop app, in one of two modes:
//!
//! - **User service**: a systemd *user* unit (`~/.config/systemd/user/
//!   verfsnext.service`). No root needed; it starts at login and keeps the
//!   folder available when the app is closed. The unit file existing is the
//!   single source of truth for this mode.
//! - **App**: the app starts the daemon as its own child process and stops it
//!   on quit.
//!
//! Every function here may block (process spawns, waits); call them from
//! worker threads, never from the Qt GUI thread.

use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::net::UnixStream;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde_json::json;

use super::paths;
use crate::config::Config;

pub const UNIT_NAME: &str = "verfsnext.service";
/// Daemon startup may run one-time migrations over all packs.
const START_TIMEOUT: Duration = Duration::from_secs(600);
/// Matches the user unit's TimeoutStopSec: a final sync during GC can be long.
const STOP_TIMEOUT: Duration = Duration::from_secs(3000);

pub type SharedChild = Arc<Mutex<Option<Child>>>;

pub fn unit_path() -> Result<PathBuf> {
    Ok(paths::config_home()?.join("systemd/user").join(UNIT_NAME))
}

pub fn service_installed() -> Result<bool> {
    Ok(unit_path()?.is_file())
}

/// Quote one `ExecStart=` argument (systemd also expands `%` specifiers).
fn unit_arg(path: &Path) -> String {
    let s = path.to_string_lossy();
    format!(
        "\"{}\"",
        s.replace('\\', "\\\\").replace('"', "\\\"").replace('%', "%%")
    )
}

fn unit_contents(exe: &Path, config_path: &Path) -> String {
    format!(
        "# Written by the VerFSNext app. Turn it off in the app (Folders & Startup > Run in the background).\n\
         [Unit]\n\
         Description=VerFSNext filesystem\n\
         \n\
         [Service]\n\
         Type=simple\n\
         ExecStart={exe} --config {config}\n\
         Restart=on-failure\n\
         RestartSec=2\n\
         # The daemon shuts down gracefully (final sync) on SIGINT.\n\
         KillSignal=SIGINT\n\
         TimeoutStopSec=3000\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        exe = unit_arg(exe),
        config = unit_arg(config_path),
    )
}

fn systemctl(args: &[&str]) -> Result<String> {
    let output = Command::new("systemctl")
        .arg("--user")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .context("failed to run systemctl (is systemd available?)")?;
    if !output.status.success() {
        bail!(
            "systemctl --user {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

/// Writes, loads and enables the unit (does not start it). When systemd
/// rejects it the file is removed again, so the app never ends up in
/// service mode with a unit systemd doesn't know.
pub fn install_service(exe: &Path, config_path: &Path) -> Result<()> {
    let path = unit_path()?;
    super::write_atomic(&path, unit_contents(exe, config_path).as_bytes())?;
    let registered = systemctl(&["daemon-reload"]).and_then(|_| systemctl(&["enable", UNIT_NAME]));
    if let Err(err) = registered {
        std::fs::remove_file(&path).with_context(|| {
            format!("{err:#}; also failed to remove {}", path.display())
        })?;
        return Err(err.context("systemd did not accept the VerFSNext user service"));
    }
    Ok(())
}

/// Disables and removes the unit. The service must already be stopped.
pub fn remove_service() -> Result<()> {
    systemctl(&["disable", UNIT_NAME])?;
    let path = unit_path()?;
    std::fs::remove_file(&path).with_context(|| format!("failed to remove {}", path.display()))?;
    systemctl(&["daemon-reload"])?;
    Ok(())
}

pub fn stop_service() -> Result<()> {
    systemctl(&["stop", UNIT_NAME])?;
    Ok(())
}

/// Rewrites the unit when it points at another executable (the app binary
/// was moved or replaced by a build in another place).
pub fn refresh_service_unit(exe: &Path, config_path: &Path) -> Result<bool> {
    let path = unit_path()?;
    let expected = unit_contents(exe, config_path);
    let current =
        std::fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    if current == expected {
        return Ok(false);
    }
    super::write_atomic(&path, expected.as_bytes())?;
    systemctl(&["daemon-reload"])?;
    Ok(true)
}

/// `active`, `activating`, `deactivating`, `failed`, `inactive`, ...
pub fn service_state() -> Result<String> {
    systemctl(&["show", "--property=ActiveState", "--value", UNIT_NAME])
}

pub fn service_log_tail() -> Result<String> {
    let output = Command::new("journalctl")
        .args(["--user", "--unit", UNIT_NAME, "--lines", "30", "--no-pager", "--output", "cat"])
        .stdin(Stdio::null())
        .output()
        .context("failed to run journalctl")?;
    if !output.status.success() {
        bail!(
            "journalctl failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

pub fn file_log_tail(path: &Path) -> Result<String> {
    const TAIL: u64 = 6 * 1024;
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    let len = file.metadata()?.len();
    file.seek(SeekFrom::Start(len.saturating_sub(TAIL)))?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let text = String::from_utf8_lossy(&buf);
    let lines: Vec<&str> = text.lines().collect();
    Ok(lines[lines.len().saturating_sub(30)..].join("\n"))
}

fn spawn_child(exe: &Path, config_path: &Path) -> Result<Child> {
    let log_path = paths::daemon_log_path()?;
    let parent = log_path.parent().context("log path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .with_context(|| format!("failed to open {}", log_path.display()))?;
    let log_err = log.try_clone().context("failed to clone log handle")?;
    Command::new(exe)
        .arg("--config")
        .arg(config_path)
        .stdin(Stdio::null())
        .stdout(log)
        .stderr(log_err)
        .spawn()
        .with_context(|| format!("failed to start {}", exe.display()))
}

/// Graceful stop: SIGINT makes the daemon sync and unmount before exiting.
fn stop_child(child: &mut Child) -> Result<()> {
    let pid = i32::try_from(child.id()).context("child pid out of range")?;
    match kill(Pid::from_raw(pid), Signal::SIGINT) {
        Ok(()) => {}
        // Already exited; `wait` below reaps it.
        Err(nix::errno::Errno::ESRCH) => {}
        Err(err) => return Err(err).context("failed to signal the filesystem process"),
    }
    let status = child.wait().context("failed waiting for the filesystem to stop")?;
    if !status.success() {
        bail!("the filesystem stopped with {status}");
    }
    Ok(())
}

pub fn socket_live(socket: &Path) -> bool {
    UnixStream::connect(socket).is_ok()
}

/// A FUSE mount whose daemon died ("Transport endpoint is not connected").
pub fn mount_is_stale(mount_point: &Path) -> bool {
    matches!(
        std::fs::metadata(mount_point),
        Err(err) if err.raw_os_error() == Some(nix::libc::ENOTCONN)
    )
}

pub fn repair_mount(mount_point: &Path) -> Result<()> {
    let output = Command::new("fusermount")
        .arg("-u")
        .arg("-z")
        .arg(mount_point)
        .stdin(Stdio::null())
        .output()
        .context("failed to run fusermount")?;
    if !output.status.success() {
        bail!(
            "fusermount -u -z {} failed: {}",
            mount_point.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(())
}

/// Starts the filesystem in the current mode and waits until it serves the
/// control socket (i.e. it is mounted).
pub fn start(config: &Config, config_path: &Path, child: &SharedChild) -> Result<()> {
    let socket = config.control_socket_path();
    if socket_live(&socket) {
        return Ok(());
    }
    if mount_is_stale(&config.mount_point) {
        tracing::warn!(mount_point = %config.mount_point.display(), "repairing stale mount left by a previous run");
        repair_mount(&config.mount_point)?;
    }
    std::fs::create_dir_all(&config.mount_point)
        .with_context(|| format!("failed to create {}", config.mount_point.display()))?;

    let service = service_installed()?;
    if service {
        systemctl(&["start", UNIT_NAME])?;
    } else {
        let exe = paths::current_exe()?;
        let spawned = spawn_child(&exe, config_path)?;
        *child.lock().expect("child lock poisoned") = Some(spawned);
    }

    let deadline = Instant::now() + START_TIMEOUT;
    loop {
        if socket_live(&socket) {
            return Ok(());
        }
        if service {
            let state = service_state()?;
            if state == "failed" || state == "inactive" {
                bail!("VerFSNext could not start:\n{}", service_log_tail()?);
            }
        } else if let Some(status) = child
            .lock()
            .expect("child lock poisoned")
            .as_mut()
            .map(Child::try_wait)
            .transpose()
            .context("failed to check the filesystem process")?
            .flatten()
        {
            bail!(
                "VerFSNext could not start ({status}):\n{}",
                file_log_tail(&paths::daemon_log_path()?)?
            );
        }
        if Instant::now() >= deadline {
            bail!("VerFSNext did not finish starting within {} minutes", START_TIMEOUT.as_secs() / 60);
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// Stops the filesystem (blocks until the final sync is done).
pub fn stop(config: &Config, child: &SharedChild) -> Result<()> {
    if service_installed()? {
        return stop_service();
    }
    stop_app_mode(config, child)
}

/// Stops a daemon that is not run by the user service.
pub fn stop_app_mode(config: &Config, child: &SharedChild) -> Result<()> {
    let taken = child.lock().expect("child lock poisoned").take();
    match taken {
        Some(mut running) => stop_child(&mut running),
        // Started elsewhere (a terminal, or an earlier app session that was
        // killed): find it through the control socket.
        None => stop_by_socket_peer(&config.control_socket_path()),
    }
}

/// PID of the process serving the control socket (SO_PEERCRED).
fn daemon_pid(socket: &Path) -> Result<Pid> {
    use std::os::unix::io::AsRawFd;

    let stream = UnixStream::connect(socket)
        .with_context(|| format!("failed to connect {}", socket.display()))?;
    let cred = nix::sys::socket::getsockopt(
        stream.as_raw_fd(),
        nix::sys::socket::sockopt::PeerCredentials,
    )
    .context("failed to read the filesystem process id")?;
    Ok(Pid::from_raw(cred.pid()))
}

fn stop_by_socket_peer(socket: &Path) -> Result<()> {
    let pid = daemon_pid(socket)?;
    kill(pid, Signal::SIGINT).context("failed to signal the filesystem process")?;
    let deadline = Instant::now() + STOP_TIMEOUT;
    loop {
        match kill(pid, None) {
            Err(nix::errno::Errno::ESRCH) => return Ok(()),
            Ok(()) => {}
            Err(err) => return Err(err).context("failed to check the filesystem process"),
        }
        if Instant::now() >= deadline {
            bail!("VerFSNext did not finish stopping within {} minutes", STOP_TIMEOUT.as_secs() / 60);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// What the app shows about the filesystem process.
pub struct Probe {
    pub state: &'static str,
    pub detail: String,
    pub stale_mount: bool,
}

pub fn probe(config: &Config, child: &SharedChild) -> Result<Probe> {
    if socket_live(&config.control_socket_path()) {
        return Ok(Probe {
            state: "running",
            detail: String::new(),
            stale_mount: false,
        });
    }
    let stale_mount = mount_is_stale(&config.mount_point);
    if service_installed()? {
        let state = service_state()?;
        return Ok(match state.as_str() {
            "activating" => Probe {
                state: "starting",
                detail: String::new(),
                stale_mount,
            },
            "deactivating" => Probe {
                state: "stopping",
                detail: String::new(),
                stale_mount,
            },
            "failed" => Probe {
                state: "failed",
                detail: service_log_tail()?,
                stale_mount,
            },
            _ => Probe {
                state: "stopped",
                detail: String::new(),
                stale_mount,
            },
        });
    }
    let mut guard = child.lock().expect("child lock poisoned");
    let exited = match guard.as_mut() {
        Some(running) => running
            .try_wait()
            .context("failed to check the filesystem process")?,
        None => None,
    };
    if let Some(status) = exited {
        *guard = None;
        drop(guard);
        if !status.success() {
            return Ok(Probe {
                state: "failed",
                detail: file_log_tail(&paths::daemon_log_path()?)?,
                stale_mount,
            });
        }
        return Ok(Probe {
            state: "stopped",
            detail: String::new(),
            stale_mount,
        });
    }
    let starting = guard.is_some();
    Ok(Probe {
        state: if starting { "starting" } else { "stopped" },
        detail: String::new(),
        stale_mount,
    })
}

fn has_parent_refs(path: &Path) -> bool {
    path.components().any(|c| matches!(c, Component::ParentDir))
}

fn nearest_existing(path: &Path) -> Option<&Path> {
    path.ancestors().find(|p| p.exists())
}

fn free_space(path: &Path) -> Result<u64> {
    let existing = nearest_existing(path).context("no existing parent folder")?;
    let stat = nix::sys::statvfs::statvfs(existing)
        .with_context(|| format!("failed to read free space of {}", existing.display()))?;
    Ok(stat.blocks_available() as u64 * stat.fragment_size() as u64)
}

fn writable_parent(path: &Path) -> Result<()> {
    let existing = nearest_existing(path).context("no existing parent folder")?;
    nix::unistd::access(existing, nix::unistd::AccessFlags::W_OK)
        .with_context(|| format!("you can't create files in {}", existing.display()))
}

fn dir_is_empty(path: &Path) -> Result<bool> {
    Ok(std::fs::read_dir(path)
        .with_context(|| format!("failed to read {}", path.display()))?
        .next()
        .is_none())
}

/// Checks a mount point / data folder pair. Returns JSON with per-field
/// problems (`mountError`, `dataError`), a `dataNote` and `dataFree` bytes.
/// `current` is the active config: its own paths are accepted as they are.
pub fn check_locations(mount: &str, data: &str, current: Option<&Config>) -> serde_json::Value {
    let mount = PathBuf::from(mount.trim());
    let data = PathBuf::from(data.trim());
    let mut mount_error = String::new();
    let mut data_error = String::new();
    let mut data_note = String::new();

    for (path, error) in [(&mount, &mut mount_error), (&data, &mut data_error)] {
        if path.as_os_str().is_empty() {
            *error = "Choose a folder.".into();
        } else if !path.is_absolute() || has_parent_refs(path) {
            *error = "Use a full path, like /home/you/Folder.".into();
        }
    }
    if mount_error.is_empty() && data_error.is_empty() {
        if mount == data {
            mount_error = "Use a different folder than the data folder.".into();
        } else if mount.starts_with(&data) {
            mount_error = "This folder can't be inside the data folder.".into();
        } else if data.starts_with(&mount) {
            data_error = "The data folder can't be inside your VerFSNext folder.".into();
        }
    }

    let same_mount = current.is_some_and(|c| c.mount_point == mount);
    if mount_error.is_empty() && !same_mount {
        mount_error = if mount_is_stale(&mount) {
            "This folder is still attached to a stopped filesystem. Start VerFSNext once to repair it, or pick another folder.".into()
        } else if mount.exists() {
            if !mount.is_dir() {
                "This is a file, not a folder.".into()
            } else {
                match dir_is_empty(&mount) {
                    Ok(true) => String::new(),
                    Ok(false) => "This folder isn't empty. Pick an empty folder or a new name.".into(),
                    Err(err) => format!("{err:#}"),
                }
            }
        } else {
            match writable_parent(&mount) {
                Ok(()) => String::new(),
                Err(err) => format!("{err:#}"),
            }
        };
    }

    let same_data = current.is_some_and(|c| c.data_dir == data);
    if data_error.is_empty() && !same_data {
        if data.exists() {
            if !data.is_dir() {
                data_error = "This is a file, not a folder.".into();
            } else if data.join("metadata").is_dir() && data.join("packs").is_dir() {
                data_note = "Existing VerFSNext data found here. It will be used.".into();
            } else {
                match dir_is_empty(&data) {
                    Ok(true) => {}
                    Ok(false) => {
                        data_error = "This folder already has other files. Pick an empty folder or a new name.".into()
                    }
                    Err(err) => data_error = format!("{err:#}"),
                }
            }
        } else if let Err(err) = writable_parent(&data) {
            data_error = format!("{err:#}");
        }
    }

    let data_free = if data_error.is_empty() {
        match free_space(&data) {
            Ok(bytes) => json!(bytes),
            Err(err) => {
                data_error = format!("{err:#}");
                json!(null)
            }
        }
    } else {
        json!(null)
    };

    json!({
        "mountError": mount_error,
        "dataError": data_error,
        "dataNote": data_note,
        "dataFree": data_free,
    })
}

/// Opens a folder in the user's file manager.
pub fn open_path(path: &Path) -> Result<()> {
    let mut child = Command::new("xdg-open")
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to run xdg-open")?;
    let status = child.wait().context("failed waiting for xdg-open")?;
    if !status.success() {
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stderr.take() {
            pipe.read_to_string(&mut stderr)?;
        }
        bail!("could not open {}: {}", path.display(), stderr.trim());
    }
    Ok(())
}
