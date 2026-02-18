mod config;
mod data;
mod fs;
mod gc;
mod meta;
mod snapshot;
mod sync;
mod types;
mod vault;
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
use crate::fs::{VerFs, VerFsStats};
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
        "stats" => {
            if !args[(idx + 1)..].is_empty() {
                bail!("stats does not take arguments");
            }
            if !try_run_stats_via_socket(&config).await? {
                bail!("stats requires a mounted daemon (control socket unavailable)");
            }
        }
        "crypt" => {
            let cmd = parse_crypt_cmd(&args[(idx + 1)..])?;
            if !try_run_crypt_via_socket(&config, &cmd).await? {
                run_crypt_via_metadata(&config, &cmd).await?;
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

enum CryptCommand {
    Create {
        password: String,
        key_path: Option<PathBuf>,
    },
    Unlock {
        password: String,
        key_file: PathBuf,
    },
    Lock,
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

fn parse_crypt_cmd(args: &[String]) -> Result<CryptCommand> {
    if args.is_empty() {
        bail!("missing crypt arguments");
    }
    let mut create = false;
    let mut unlock = false;
    let mut lock = false;
    let mut password: Option<String> = None;
    let mut key_path: Option<PathBuf> = None;
    let mut unlock_key: Option<PathBuf> = None;

    let mut idx = 0_usize;
    while idx < args.len() {
        match args[idx].as_str() {
            "-c" => create = true,
            "-u" => unlock = true,
            "-l" => lock = true,
            "-p" => {
                idx = idx.saturating_add(1);
                let Some(value) = args.get(idx) else {
                    bail!("missing value for -p");
                };
                password = Some(value.clone());
            }
            "-path" => {
                idx = idx.saturating_add(1);
                let Some(value) = args.get(idx) else {
                    bail!("missing value for -path");
                };
                key_path = Some(PathBuf::from(value));
            }
            "-k" => {
                idx = idx.saturating_add(1);
                let Some(value) = args.get(idx) else {
                    bail!("missing value for -k");
                };
                unlock_key = Some(PathBuf::from(value));
            }
            other => bail!("unknown crypt argument '{other}'"),
        }
        idx = idx.saturating_add(1);
    }

    let selected = [create, unlock, lock].into_iter().filter(|v| *v).count();
    if selected != 1 {
        bail!("exactly one of -c, -u, -l is required");
    }

    if create {
        let password = password.context("missing -p password for -c")?;
        return Ok(CryptCommand::Create { password, key_path });
    }
    if unlock {
        let password = password.context("missing -p password for -u")?;
        let key_file = unlock_key.context("missing -k key file for -u")?;
        return Ok(CryptCommand::Unlock { password, key_file });
    }
    if password.is_some() || key_path.is_some() || unlock_key.is_some() {
        bail!("-l does not take -p, -path, or -k");
    }
    Ok(CryptCommand::Lock)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ControlRequest {
    SnapshotCreate {
        name: String,
    },
    SnapshotList,
    SnapshotDelete {
        name: String,
    },
    VaultCreate {
        password: String,
        key_path: Option<String>,
    },
    VaultUnlock {
        password: String,
        key_file: String,
    },
    VaultLock,
    Stats,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlResponse {
    ok: bool,
    names: Vec<String>,
    message: String,
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

    let req: ControlRequest =
        serde_json::from_str(line.trim_end()).context("invalid control request payload")?;

    let resp = match req {
        ControlRequest::SnapshotCreate { name } => match fs.create_snapshot(&name).await {
            Ok(()) => ControlResponse {
                ok: true,
                names: Vec::new(),
                message: String::new(),
                error: String::new(),
            },
            Err(err) => ControlResponse {
                ok: false,
                names: Vec::new(),
                message: String::new(),
                error: err.to_string(),
            },
        },
        ControlRequest::SnapshotList => match fs.list_snapshots() {
            Ok(names) => ControlResponse {
                ok: true,
                names,
                message: String::new(),
                error: String::new(),
            },
            Err(err) => ControlResponse {
                ok: false,
                names: Vec::new(),
                message: String::new(),
                error: err.to_string(),
            },
        },
        ControlRequest::SnapshotDelete { name } => match fs.delete_snapshot(&name).await {
            Ok(()) => ControlResponse {
                ok: true,
                names: Vec::new(),
                message: String::new(),
                error: String::new(),
            },
            Err(err) => ControlResponse {
                ok: false,
                names: Vec::new(),
                message: String::new(),
                error: err.to_string(),
            },
        },
        ControlRequest::VaultCreate { password, key_path } => {
            let path = key_path.map(PathBuf::from);
            match fs.create_vault(&password, path.as_deref()).await {
                Ok(written_path) => ControlResponse {
                    ok: true,
                    names: Vec::new(),
                    message: written_path.display().to_string(),
                    error: String::new(),
                },
                Err(err) => ControlResponse {
                    ok: false,
                    names: Vec::new(),
                    message: String::new(),
                    error: err.to_string(),
                },
            }
        }
        ControlRequest::VaultUnlock { password, key_file } => {
            match fs.unlock_vault(&password, Path::new(&key_file)).await {
                Ok(()) => ControlResponse {
                    ok: true,
                    names: Vec::new(),
                    message: String::new(),
                    error: String::new(),
                },
                Err(err) => ControlResponse {
                    ok: false,
                    names: Vec::new(),
                    message: String::new(),
                    error: err.to_string(),
                },
            }
        }
        ControlRequest::VaultLock => match fs.lock_vault().await {
            Ok(()) => ControlResponse {
                ok: true,
                names: Vec::new(),
                message: String::new(),
                error: String::new(),
            },
            Err(err) => ControlResponse {
                ok: false,
                names: Vec::new(),
                message: String::new(),
                error: err.to_string(),
            },
        },
        ControlRequest::Stats => match fs.collect_stats() {
            Ok(stats) => ControlResponse {
                ok: true,
                names: Vec::new(),
                message: format_stats_report(&stats),
                error: String::new(),
            },
            Err(err) => ControlResponse {
                ok: false,
                names: Vec::new(),
                message: String::new(),
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
        SnapshotCommand::Create(name) => ControlRequest::SnapshotCreate { name: name.clone() },
        SnapshotCommand::List => ControlRequest::SnapshotList,
        SnapshotCommand::Delete(name) => ControlRequest::SnapshotDelete { name: name.clone() },
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
    let resp: ControlResponse =
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

async fn try_run_crypt_via_socket(config: &Config, cmd: &CryptCommand) -> Result<bool> {
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
        CryptCommand::Create { password, key_path } => ControlRequest::VaultCreate {
            password: password.clone(),
            key_path: key_path
                .as_ref()
                .map(|p| std::fs::canonicalize(p).unwrap_or_else(|_| p.clone()))
                .map(|p| p.to_string_lossy().to_string()),
        },
        CryptCommand::Unlock { password, key_file } => ControlRequest::VaultUnlock {
            password: password.clone(),
            key_file: std::fs::canonicalize(key_file)
                .unwrap_or_else(|_| key_file.clone())
                .to_string_lossy()
                .to_string(),
        },
        CryptCommand::Lock => ControlRequest::VaultLock,
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
    let resp: ControlResponse =
        serde_json::from_str(line.trim_end()).context("invalid control response payload")?;
    if !resp.ok {
        bail!(resp.error);
    }
    if !resp.message.is_empty() {
        println!("{}", resp.message);
    }
    Ok(true)
}

async fn try_run_stats_via_socket(config: &Config) -> Result<bool> {
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

    let payload =
        serde_json::to_vec(&ControlRequest::Stats).context("failed to encode control request")?;
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
    if !resp.message.is_empty() {
        println!("{}", resp.message);
    }
    Ok(true)
}

fn format_stats_report(stats: &VerFsStats) -> String {
    let compression_rate = percent(stats.unique_compressed_bytes, stats.unique_uncompressed_bytes);
    let dedup_ratio = if stats.referenced_compressed_bytes == 0 {
        0.0
    } else {
        stats.dedup_savings_bytes as f64 / stats.referenced_compressed_bytes as f64
    };
    let disk_vs_logical = stats.data_dir_size_bytes as i128 - stats.logical_size_bytes as i128;

    let mut lines = Vec::new();
    lines.push(format!(
        "total logical size: {} ({})",
        stats.logical_size_bytes,
        human_bytes(stats.logical_size_bytes)
    ));
    lines.push(format!(
        "compressed unique chunk size: {} ({})",
        stats.unique_compressed_bytes,
        human_bytes(stats.unique_compressed_bytes)
    ));
    lines.push(format!(
        "uncompressed unique chunk size: {} ({})",
        stats.unique_uncompressed_bytes,
        human_bytes(stats.unique_uncompressed_bytes)
    ));
    lines.push(format!(
        "uncompressed referenced chunk size: {} ({})",
        stats.referenced_uncompressed_bytes,
        human_bytes(stats.referenced_uncompressed_bytes)
    ));
    lines.push(format!("compression rate: {:.2}%", compression_rate * 100.0));
    lines.push(format!(
        "metadata size: {} ({})",
        stats.metadata_size_bytes,
        human_bytes(stats.metadata_size_bytes)
    ));
    lines.push(format!(
        "deduplication savings: {} ({}, {:.2}% of referenced compressed)",
        stats.dedup_savings_bytes,
        human_bytes(stats.dedup_savings_bytes),
        dedup_ratio * 100.0
    ));
    lines.push(format!(
        "cache hit rate: {:.2}% ({}/{})",
        stats.cache_hit_rate * 100.0,
        stats.cache_hits,
        stats.cache_requests
    ));
    lines.push(format!(
        "used memory (RSS): {} ({})",
        stats.used_memory_bytes,
        human_bytes(stats.used_memory_bytes)
    ));
    lines.push(format!(
        "throughput: read {}/s, write {}/s, uptime {:.1}s",
        human_bytes(stats.read_throughput_bps as u64),
        human_bytes(stats.write_throughput_bps as u64),
        stats.uptime_secs
    ));
    lines.push(format!(
        "io totals: read {} ({}), write {} ({})",
        stats.read_bytes_total,
        human_bytes(stats.read_bytes_total),
        stats.write_bytes_total,
        human_bytes(stats.write_bytes_total)
    ));
    lines.push(format!(
        "full data dir size: {} ({})",
        stats.data_dir_size_bytes,
        human_bytes(stats.data_dir_size_bytes)
    ));
    lines.push(format!(
        "disk usage delta (data dir - logical): {} ({})",
        disk_vs_logical,
        human_bytes_signed(disk_vs_logical)
    ));
    lines.push(format!(
        "cache entries: metadata {}, chunk-data {}",
        stats.metadata_cache_entries, stats.chunk_cache_entries
    ));
    lines.push(format!(
        "approx cache memory: {} ({})",
        stats.approx_cache_memory_bytes,
        human_bytes(stats.approx_cache_memory_bytes)
    ));
    lines.join("\n")
}

fn percent(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut value = bytes as f64;
    let mut unit = 0_usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}

fn human_bytes_signed(bytes: i128) -> String {
    if bytes < 0 {
        let abs = u64::try_from(bytes.unsigned_abs()).unwrap_or(u64::MAX);
        format!("-{}", human_bytes(abs))
    } else {
        human_bytes(u64::try_from(bytes).unwrap_or(u64::MAX))
    }
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

async fn run_crypt_via_metadata(config: &Config, cmd: &CryptCommand) -> Result<()> {
    let fs = VerFs::new(config.clone()).await?;
    match cmd {
        CryptCommand::Create { password, key_path } => {
            let path = fs.create_vault(password, key_path.as_deref()).await?;
            println!("{}", path.display());
            fs.graceful_shutdown().await?;
            Ok(())
        }
        CryptCommand::Unlock { .. } => {
            fs.graceful_shutdown().await?;
            bail!("unlock requires a mounted daemon (control socket unavailable)")
        }
        CryptCommand::Lock => {
            let result = fs.lock_vault().await;
            fs.graceful_shutdown().await?;
            result
        }
    }
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
