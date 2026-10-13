//! One running desktop app per user session.
//!
//! The first (primary) instance binds `app.sock` under the runtime dir and
//! listens. A later launch connects, writes `activate\n` and exits; the
//! primary turns that into [`Activation`] so the UI can show its window.
//! A stale socket file left by a crash is detected (connect fails) and
//! replaced. A primary running another executable (the app was rebuilt or
//! updated while it ran) is terminated, and the new launch takes over.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;

const ACTIVATE_MSG: &[u8] = b"activate\n";
/// How long an outdated primary gets to exit after SIGTERM.
const REPLACE_TIMEOUT: Duration = Duration::from_secs(15);

/// Delivered to the primary when another launch happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Activation;

pub enum AcquireOutcome {
    /// Hold the guard for the app lifetime.
    Primary(Guard),
    /// Another instance is running and was told to activate; exit.
    Secondary,
}

/// Owns the socket path; dropping it removes the socket file.
pub struct Guard {
    socket_path: PathBuf,
    rx: crossbeam_channel::Receiver<Activation>,
}

impl Guard {
    pub fn receiver(&self) -> crossbeam_channel::Receiver<Activation> {
        self.rx.clone()
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_file(&self.socket_path) {
            tracing::warn!(path = %self.socket_path.display(), error = %err, "failed to remove single-instance socket");
        }
    }
}

pub fn acquire() -> Result<AcquireOutcome> {
    acquire_on(&super::paths::app_runtime_dir().join("app.sock"))
}

fn acquire_on(path: &Path) -> Result<AcquireOutcome> {
    use std::os::unix::fs::PermissionsExt;

    let parent = path.parent().context("socket path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))
        .with_context(|| format!("failed to restrict {}", parent.display()))?;

    if let Some(pid) = outdated_primary(path)? {
        replace_primary(pid)?;
    } else if notify(path) {
        return Ok(AcquireOutcome::Secondary);
    }
    // Only reached when nothing accepts connections: the file is stale.
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to remove stale {}", path.display()))
        }
    }
    let listener = match UnixListener::bind(path) {
        Ok(l) => l,
        // Lost a race with another starting primary.
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && notify(path) => {
            return Ok(AcquireOutcome::Secondary);
        }
        Err(e) => return Err(e).with_context(|| format!("failed to bind {}", path.display())),
    };
    let (tx, rx) = crossbeam_channel::unbounded();
    std::thread::Builder::new()
        .name("single-instance".into())
        .spawn(move || accept_loop(listener, tx))
        .context("failed to spawn single-instance listener")?;
    Ok(AcquireOutcome::Primary(Guard {
        socket_path: path.to_path_buf(),
        rx,
    }))
}

/// PID of a live primary that runs another executable than this launch.
fn outdated_primary(path: &Path) -> Result<Option<Pid>> {
    use std::os::unix::io::AsRawFd;

    let Ok(stream) = UnixStream::connect(path) else {
        return Ok(None);
    };
    let cred = nix::sys::socket::getsockopt(
        stream.as_raw_fd(),
        nix::sys::socket::sockopt::PeerCredentials,
    )
    .context("failed to read the running app's process id")?;
    if super::paths::runs_current_exe(cred.pid())? {
        return Ok(None);
    }
    Ok(Some(Pid::from_raw(cred.pid())))
}

/// Terminates an outdated primary and waits for it to exit. The app holds no
/// filesystem state: the filesystem runs in its own process, and the new
/// instance restarts it when it is outdated too.
fn replace_primary(pid: Pid) -> Result<()> {
    tracing::info!(pid = pid.as_raw(), "replacing a running app that uses an outdated executable");
    kill(pid, Signal::SIGTERM).context("failed to stop the outdated app")?;
    let deadline = std::time::Instant::now() + REPLACE_TIMEOUT;
    loop {
        match kill(pid, None) {
            Err(nix::errno::Errno::ESRCH) => return Ok(()),
            Ok(()) => {}
            Err(err) => return Err(err).context("failed to check the outdated app"),
        }
        if std::time::Instant::now() >= deadline {
            bail!("the outdated VerFSNext app (pid {pid}) did not exit");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// True when a live primary accepted the activation message.
fn notify(path: &Path) -> bool {
    let Ok(mut stream) = UnixStream::connect(path) else {
        return false;
    };
    if let Err(err) = stream.set_write_timeout(Some(Duration::from_secs(2))) {
        tracing::warn!(error = %err, "failed to set activation write timeout");
    }
    stream.write_all(ACTIVATE_MSG).is_ok()
}

fn accept_loop(listener: UnixListener, tx: crossbeam_channel::Sender<Activation>) {
    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(stream) => stream,
            Err(err) => {
                tracing::warn!(error = %err, "single-instance accept failed");
                continue;
            }
        };
        if let Err(err) = stream.set_read_timeout(Some(Duration::from_secs(2))) {
            tracing::warn!(error = %err, "failed to set activation read timeout");
        }
        let mut buf = [0u8; 64];
        // Zero bytes = bare liveness probe; do not wake the UI for it.
        if matches!(stream.read(&mut buf), Ok(n) if n > 0) && tx.send(Activation).is_err() {
            break;
        }
    }
}
