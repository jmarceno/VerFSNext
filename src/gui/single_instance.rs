//! One running desktop app per user session.
//!
//! The first (primary) instance binds `app.sock` under the runtime dir and
//! listens. A later launch connects, writes `activate\n` and exits; the
//! primary turns that into [`Activation`] so the UI can show its window.
//! A stale socket file left by a crash is detected (connect fails) and
//! replaced.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};

const ACTIVATE_MSG: &[u8] = b"activate\n";

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

    if notify(path) {
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
