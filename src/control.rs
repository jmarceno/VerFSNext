//! Control-socket protocol (`<data_dir>/verfsnext.sock`) and its client side,
//! shared by the CLI control commands and the desktop app.
//!
//! One JSON request line in, one JSON response line out. New response fields
//! carry `#[serde(default)]` so older clients and daemons keep interoperating.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::config::Config;
use crate::fs::{DaemonStatus, VaultOwner, VerFs, VerFsStats};
use crate::vault::{generate_key_file_material, read_key_file, resolve_create_key_path, write_key_file};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ControlRequest {
    SnapshotCreate {
        name: String,
    },
    SnapshotList,
    SnapshotDelete {
        name: String,
    },
    /// The client generates the key material and writes the key file itself,
    /// so the file belongs to the caller; the daemon takes the `/.vault` owner
    /// from the socket peer credentials.
    VaultCreate {
        password: String,
        key_material: [u8; 32],
    },
    /// The client reads the caller's key file and sends its content, so the
    /// daemon never opens files on behalf of the caller.
    VaultUnlock {
        password: String,
        key_material: [u8; 32],
    },
    VaultLock,
    /// Full report: scans all metadata, so it is expensive on large trees.
    Stats,
    /// Cheap state for frequent polling.
    Status,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ControlResponse {
    pub ok: bool,
    pub names: Vec<String>,
    pub message: String,
    pub error: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stats: Option<VerFsStats>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DaemonStatus>,
}

impl ControlResponse {
    pub fn success() -> Self {
        Self {
            ok: true,
            ..Self::default()
        }
    }

    pub fn failure(err: &anyhow::Error) -> Self {
        Self {
            ok: false,
            error: format!("{err:#}"),
            ..Self::default()
        }
    }
}

pub fn socket_unavailable(err: &std::io::Error) -> bool {
    err.kind() == ErrorKind::NotFound
        || err.kind() == ErrorKind::ConnectionRefused
        || err.kind() == ErrorKind::ConnectionReset
}

/// Sends one request to the mounted daemon. Returns `None` when no daemon is
/// listening on the control socket; a daemon-side failure is an error.
pub async fn send_control_request(
    socket_path: &Path,
    req: &ControlRequest,
) -> Result<Option<ControlResponse>> {
    let mut stream = match UnixStream::connect(socket_path).await {
        Ok(stream) => stream,
        Err(err) if socket_unavailable(&err) => return Ok(None),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to connect control socket {}", socket_path.display())
            });
        }
    };

    let payload = serde_json::to_vec(req).context("failed to encode control request")?;
    stream
        .write_all(&payload)
        .await
        .context("failed writing control request")?;
    stream
        .write_all(b"\n")
        .await
        .context("failed writing control request terminator")?;
    stream
        .flush()
        .await
        .context("failed flushing control request")?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .await
        .context("failed reading control response")?;
    if read == 0 {
        bail!("empty control response");
    }
    let resp: ControlResponse =
        serde_json::from_str(line.trim_end()).context("invalid control response payload")?;
    if !resp.ok {
        bail!(resp.error);
    }
    Ok(Some(resp))
}

/// Like [`send_control_request`], for callers that need a running daemon.
pub async fn require_daemon(config: &Config, req: &ControlRequest) -> Result<ControlResponse> {
    send_control_request(&config.control_socket_path(), req)
        .await?
        .with_context(|| {
            format!(
                "VerFSNext is not running (control socket {} unavailable)",
                config.control_socket_path().display()
            )
        })
}

pub async fn is_daemon_reachable(config: &Config) -> Result<bool> {
    let socket_path = config.control_socket_path();
    match UnixStream::connect(&socket_path).await {
        Ok(_stream) => Ok(true),
        Err(err) if socket_unavailable(&err) => Ok(false),
        Err(err) => Err(err)
            .with_context(|| format!("failed to connect control socket {}", socket_path.display())),
    }
}

/// Creates the vault and its key file, returning the key file path. Key files
/// are always written by this process, i.e. by the calling user, never by the
/// daemon, which may run as a different system user. Without a running daemon
/// the vault is created directly in the metadata store, owned by the caller.
pub async fn create_vault(
    config: &Config,
    password: &str,
    key_path: Option<&Path>,
) -> Result<PathBuf> {
    let key_file = resolve_create_key_path(key_path)?;
    let key_material = generate_key_file_material();
    write_key_file(&key_file, &key_material)?;
    if let Err(err) = create_vault_with_key(config, password, &key_material).await {
        // The key file only unlocks the vault this call failed to create, so
        // it must not be left behind.
        if let Err(remove_err) = std::fs::remove_file(&key_file) {
            return Err(err.context(format!(
                "also failed to remove the unused key file {}: {remove_err}",
                key_file.display()
            )));
        }
        return Err(err);
    }
    Ok(key_file)
}

async fn create_vault_with_key(
    config: &Config,
    password: &str,
    key_material: &[u8; 32],
) -> Result<()> {
    let req = ControlRequest::VaultCreate {
        password: password.to_owned(),
        key_material: *key_material,
    };
    if send_control_request(&config.control_socket_path(), &req)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let fs = VerFs::new(config.clone()).await?;
    let owner = VaultOwner {
        uid: nix::unistd::getuid().as_raw(),
        gid: nix::unistd::getgid().as_raw(),
    };
    let result = fs.create_vault(password, key_material, owner).await;
    fs.graceful_shutdown().await?;
    result
}

/// Reads the caller's key file and asks the mounted daemon to unlock.
pub async fn unlock_vault(config: &Config, password: &str, key_file: &Path) -> Result<()> {
    let key_material = read_key_file(key_file)?;
    let req = ControlRequest::VaultUnlock {
        password: password.to_owned(),
        key_material,
    };
    require_daemon(config, &req).await?;
    Ok(())
}

/// Locks through the mounted daemon, or directly in the metadata store when
/// no daemon is running.
pub async fn lock_vault(config: &Config) -> Result<()> {
    if send_control_request(&config.control_socket_path(), &ControlRequest::VaultLock)
        .await?
        .is_some()
    {
        return Ok(());
    }
    let fs = VerFs::new(config.clone()).await?;
    let result = fs.lock_vault().await;
    fs.graceful_shutdown().await?;
    result
}
