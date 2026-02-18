mod config;
mod data;
mod fs;
mod gc;
mod meta;
mod snapshot;
mod sync;
mod types;
mod write;

use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_fusex::{
    session::{new_session, SessionConfig},
    FuseFs, VirtualFs,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::Config;
use crate::fs::VerFs;
use crate::meta::MetaStore;
use crate::snapshot::SnapshotManager;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if !args.is_empty() {
        return run_control_command(args).await;
    }

    run_mount().await
}

async fn run_mount() -> Result<()> {
    let config_path = Path::new("config.toml");
    let config = Config::load_from_file(config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;

    let fs = Arc::new(VerFs::new(config.clone()).await?);
    let virtual_fs: Arc<dyn VirtualFs> = fs.clone();
    let fuse_fs = FuseFs::new(virtual_fs);

    let session = new_session(
        &config.mount_point,
        fuse_fs,
        SessionConfig {
            max_write_bytes: config.fuse_max_write_bytes,
        },
    )
    .await
    .with_context(|| {
        format!(
            "failed to mount FUSE filesystem at {}",
            config.mount_point.display()
        )
    })?;

    let cancel = CancellationToken::new();
    let cancel_for_signal = cancel.clone();
    let fs_for_signal = fs.clone();
    let control_socket_path = config.control_socket_path();
    let control_listener = bind_control_socket(&control_socket_path)?;
    let control_server = tokio::spawn(run_control_socket_server(
        fs.clone(),
        control_listener,
        control_socket_path.clone(),
        cancel.clone(),
    ));

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("SIGINT received, starting graceful shutdown");
            if let Err(err) = fs_for_signal.graceful_shutdown().await {
                error!(error = %err, "graceful shutdown failed during SIGINT handling");
            }
            cancel_for_signal.cancel();
        }
    });

    info!(
        mount_point = %config.mount_point.display(),
        data_dir = %config.data_dir.display(),
        "VerFSNext Phase 1 mounted"
    );

    let run_result = session.run(cancel.clone()).await;
    cancel.cancel();
    if let Err(join_err) = control_server.await {
        error!(error = %join_err, "control socket task join failed");
    }

    if let Err(err) = fs.graceful_shutdown().await {
        error!(error = %err, "graceful shutdown after run loop failed");
    }

    run_result.context("FUSE run loop failed")
}

async fn run_control_command(args: Vec<String>) -> Result<()> {
    let mut idx = 0_usize;
    let mut config_path = "config.toml".to_owned();
    if args.len() >= 2 && args[0] == "--config" {
        config_path = args[1].clone();
        idx = 2;
    }

    if idx >= args.len() {
        bail!("missing control command");
    }

    let config = Config::load_from_file(Path::new(&config_path))
        .with_context(|| format!("failed to load {}", config_path))?;
    config.ensure_dirs()?;

    match args[idx].as_str() {
        "snapshot" => {
            let cmd = parse_snapshot_cmd(&args[(idx + 1)..])?;
            if !try_run_snapshot_via_socket(&config, &cmd).await? {
                run_snapshot_via_metadata(&config, &cmd).await?;
            }
        }
        other => bail!("unknown control command '{other}'"),
    }

    Ok(())
}

enum SnapshotCommand {
    Create(String),
    List,
    Delete(String),
}

fn parse_snapshot_cmd(args: &[String]) -> Result<SnapshotCommand> {
    if args.is_empty() {
        bail!("missing snapshot subcommand (create|list|delete)");
    }
    match args[0].as_str() {
        "create" => {
            if args.len() < 2 {
                bail!("missing snapshot name");
            }
            Ok(SnapshotCommand::Create(args[1].clone()))
        }
        "list" => Ok(SnapshotCommand::List),
        "delete" => {
            if args.len() < 2 {
                bail!("missing snapshot name");
            }
            Ok(SnapshotCommand::Delete(args[1].clone()))
        }
        other => bail!("unknown snapshot subcommand '{other}'"),
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum SnapshotControlRequest {
    Create { name: String },
    List,
    Delete { name: String },
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotControlResponse {
    ok: bool,
    names: Vec<String>,
    error: String,
}

async fn run_control_socket_server(
    fs: Arc<VerFs>,
    listener: UnixListener,
    socket_path: PathBuf,
    cancel: CancellationToken,
) -> Result<()> {
    info!(path = %socket_path.display(), "control socket listening");

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                break;
            }
            incoming = listener.accept() => {
                let (stream, _) = match incoming {
                    Ok(pair) => pair,
                    Err(err) => {
                        error!(error = %err, "control socket accept failed");
                        continue;
                    }
                };
                if let Err(err) = handle_control_client(fs.clone(), stream).await {
                    error!(error = %err, "control socket request failed");
                }
            }
        }
    }

    if let Err(err) = std::fs::remove_file(&socket_path) {
        if err.kind() != ErrorKind::NotFound {
            error!(
                error = %err,
                path = %socket_path.display(),
                "failed to remove control socket"
            );
        }
    }
    Ok(())
}

fn bind_control_socket(socket_path: &Path) -> Result<UnixListener> {
    if socket_path.exists() {
        match std::fs::remove_file(socket_path) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to remove stale socket {}", socket_path.display())
                });
            }
        }
    }
    UnixListener::bind(socket_path)
        .with_context(|| format!("failed to bind control socket {}", socket_path.display()))
}

async fn handle_control_client(fs: Arc<VerFs>, stream: UnixStream) -> Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .await
        .context("failed reading control request")?;
    if read == 0 {
        return Ok(());
    }

    let req: SnapshotControlRequest =
        serde_json::from_str(line.trim_end()).context("invalid control request payload")?;

    let resp = match req {
        SnapshotControlRequest::Create { name } => match fs.create_snapshot(&name).await {
            Ok(()) => SnapshotControlResponse {
                ok: true,
                names: Vec::new(),
                error: String::new(),
            },
            Err(err) => SnapshotControlResponse {
                ok: false,
                names: Vec::new(),
                error: err.to_string(),
            },
        },
        SnapshotControlRequest::List => match fs.list_snapshots() {
            Ok(names) => SnapshotControlResponse {
                ok: true,
                names,
                error: String::new(),
            },
            Err(err) => SnapshotControlResponse {
                ok: false,
                names: Vec::new(),
                error: err.to_string(),
            },
        },
        SnapshotControlRequest::Delete { name } => match fs.delete_snapshot(&name).await {
            Ok(()) => SnapshotControlResponse {
                ok: true,
                names: Vec::new(),
                error: String::new(),
            },
            Err(err) => SnapshotControlResponse {
                ok: false,
                names: Vec::new(),
                error: err.to_string(),
            },
        },
    };

    let payload = serde_json::to_vec(&resp).context("failed to encode control response")?;
    writer
        .write_all(&payload)
        .await
        .context("failed writing control response")?;
    writer
        .write_all(b"\n")
        .await
        .context("failed writing control response terminator")?;
    writer
        .flush()
        .await
        .context("failed flushing control response")?;
    Ok(())
}

async fn try_run_snapshot_via_socket(config: &Config, cmd: &SnapshotCommand) -> Result<bool> {
    let socket_path = config.control_socket_path();
    let mut stream = match UnixStream::connect(&socket_path).await {
        Ok(stream) => stream,
        Err(err) if socket_unavailable(&err) => return Ok(false),
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to connect control socket {}", socket_path.display())
            });
        }
    };

    let req = match cmd {
        SnapshotCommand::Create(name) => SnapshotControlRequest::Create { name: name.clone() },
        SnapshotCommand::List => SnapshotControlRequest::List,
        SnapshotCommand::Delete(name) => SnapshotControlRequest::Delete { name: name.clone() },
    };

    let payload = serde_json::to_vec(&req).context("failed to encode control request")?;
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
    let resp: SnapshotControlResponse =
        serde_json::from_str(line.trim_end()).context("invalid control response payload")?;
    if !resp.ok {
        bail!(resp.error);
    }
    if matches!(cmd, SnapshotCommand::List) {
        for name in resp.names {
            println!("{name}");
        }
    }
    Ok(true)
}

fn socket_unavailable(err: &std::io::Error) -> bool {
    err.kind() == ErrorKind::NotFound
        || err.kind() == ErrorKind::ConnectionRefused
        || err.kind() == ErrorKind::ConnectionReset
}

async fn run_snapshot_via_metadata(config: &Config, cmd: &SnapshotCommand) -> Result<()> {
    let meta = MetaStore::open(&config.metadata_dir()).await?;
    let snapshots = SnapshotManager::new(&meta);

    let action_result = match cmd {
        SnapshotCommand::Create(name) => snapshots.create(name).await,
        SnapshotCommand::List => {
            let names = snapshots.list()?;
            for name in names {
                println!("{name}");
            }
            Ok(())
        }
        SnapshotCommand::Delete(name) => snapshots.delete(name).await,
    };
    let close_result = meta.close().await;

    action_result?;
    close_result?;
    Ok(())
}

fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
}
