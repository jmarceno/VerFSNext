mod config;
mod data;
mod fs;
mod gc;
mod meta;
mod migration;
mod permissions;
mod snapshot;
mod sync;
mod types;
mod vault;
mod write;

use std::io::ErrorKind;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use async_fusex::{
    mount::MountConfig,
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
use crate::migration::pack_size::run_pack_size_migration;
use crate::permissions::set_socket_mode;
use crate::snapshot::SnapshotManager;

type LogHandle =
    tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;

#[tokio::main]
async fn main() -> Result<()> {
    let log_handle = init_logging();

    let parsed = parse_global_cli_options(std::env::args().skip(1).collect::<Vec<_>>())?;
    let selection = resolve_config_path(parsed.explicit_config.as_deref())?;

    if parsed.explicit_config.is_none() {
        confirm_config_selection(&selection)?;
    }

    if !parsed.command_args.is_empty() {
        return run_control_command(selection.path.clone(), parsed.command_args).await;
    }

    run_mount(log_handle, selection.path).await
}

async fn run_mount(log_handle: LogHandle, config_path: PathBuf) -> Result<()> {
    let config = Config::load_from_file(&config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;

    let fs = Arc::new(VerFs::new(config.clone()).await?);
    let virtual_fs: Arc<dyn VirtualFs> = fs.clone();
    let fuse_fs = FuseFs::new(virtual_fs).with_direct_io(config.fuse_direct_io);

    let session = new_session(
        &config.mount_point,
        fuse_fs,
        SessionConfig {
            max_write_bytes: config.fuse_max_write_bytes,
            mount_config: MountConfig {
                direct_io: config.fuse_direct_io,
                fs_name: config.fuse_fsname.clone(),
                subtype: config.fuse_subtype.clone(),
            },
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
        Some(log_handle.clone()),
    ));
    let log_handle_for_signal = log_handle.clone();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            let _ = log_handle_for_signal
                .modify(|filter| *filter = tracing_subscriber::EnvFilter::new("info"));
            info!("SIGINT received, starting graceful shutdown");
            if fs_for_signal.is_gc_in_progress() {
                info!("Garbage collection is currently in progress, shutdown might take a while");
            }
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

async fn run_control_command(config_path: PathBuf, args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        bail!("missing control command");
    }

    let config = Config::load_from_file(&config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;
    config.ensure_dirs()?;

    match args[0].as_str() {
        "snapshot" => {
            let cmd = parse_snapshot_cmd(&args[1..])?;
            if !try_run_snapshot_via_socket(&config, &cmd).await? {
                run_snapshot_via_metadata(&config, &cmd).await?;
            }
        }
        "stats" => {
            if !args[1..].is_empty() {
                bail!("stats does not take arguments");
            }
            if !try_run_stats_via_socket(&config).await? {
                bail!("stats requires a mounted daemon (control socket unavailable)");
            }
        }
        "crypt" => {
            let cmd = parse_crypt_cmd(&args[1..])?;
            if !try_run_crypt_via_socket(&config, &cmd).await? {
                run_crypt_via_metadata(&config, &cmd).await?;
            }
        }
        "pack-size-migrate" => {
            if !args[1..].is_empty() {
                bail!("pack-size-migrate does not take arguments");
            }
            run_pack_size_migration(&config).await?;
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
    log_handle: Option<LogHandle>,
) -> Result<()> {
    info!(path = %socket_path.display(), "control socket listening");

    if let Some(handle) = log_handle {
        let _ = handle.modify(|filter| *filter = tracing_subscriber::EnvFilter::new("error"));
    }

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
    let listener = UnixListener::bind(socket_path)
        .with_context(|| format!("failed to bind control socket {}", socket_path.display()))?;
    set_socket_mode(socket_path)?;
    Ok(listener)
}

struct ParsedCli {
    explicit_config: Option<PathBuf>,
    command_args: Vec<String>,
}

#[derive(Clone)]
enum ConfigSource {
    Explicit,
    WorkingDirectory,
    UserConfig,
    SystemConfig,
}

impl ConfigSource {
    fn label(&self) -> &'static str {
        match self {
            Self::Explicit => "from --config/-c",
            Self::WorkingDirectory => "from current working directory",
            Self::UserConfig => "from ~/.config/verfsnext",
            Self::SystemConfig => "from /etc/verfsnext",
        }
    }
}

struct ConfigSelection {
    path: PathBuf,
    source: ConfigSource,
}

fn parse_global_cli_options(args: Vec<String>) -> Result<ParsedCli> {
    if args.is_empty() {
        return Ok(ParsedCli {
            explicit_config: None,
            command_args: Vec::new(),
        });
    }

    if args[0] == "--config" || args[0] == "-c" {
        let Some(path) = args.get(1) else {
            bail!(
                "missing value for {} (expected path to config file)",
                args[0]
            );
        };
        return Ok(ParsedCli {
            explicit_config: Some(PathBuf::from(path)),
            command_args: args[2..].to_vec(),
        });
    }

    Ok(ParsedCli {
        explicit_config: None,
        command_args: args,
    })
}

fn resolve_config_path(explicit: Option<&Path>) -> Result<ConfigSelection> {
    if let Some(path) = explicit {
        return Ok(ConfigSelection {
            path: path.to_path_buf(),
            source: ConfigSource::Explicit,
        });
    }

    let cwd_candidate = std::env::current_dir()
        .context("failed to resolve current working directory")?
        .join("config.toml");
    if cwd_candidate.is_file() {
        return Ok(ConfigSelection {
            path: cwd_candidate,
            source: ConfigSource::WorkingDirectory,
        });
    }

    if let Some(home) = std::env::var_os("HOME") {
        let user_candidate = PathBuf::from(home)
            .join(".config")
            .join("verfsnext")
            .join("config.toml");
        if user_candidate.is_file() {
            return Ok(ConfigSelection {
                path: user_candidate,
                source: ConfigSource::UserConfig,
            });
        }
    }

    let system_candidate = PathBuf::from("/etc/verfsnext/config.toml");
    if system_candidate.is_file() {
        return Ok(ConfigSelection {
            path: system_candidate,
            source: ConfigSource::SystemConfig,
        });
    }

    bail!(
        "no config.toml found. searched in: {}, ~/.config/verfsnext/config.toml, /etc/verfsnext/config.toml",
        cwd_candidate.display()
    );
}

fn confirm_config_selection(selection: &ConfigSelection) -> Result<()> {
    println!(
        "Using config file: {} ({})",
        selection.path.display(),
        selection.source.label()
    );
    print!("Proceed? [Y/n] (auto-accept in 5 seconds): ");
    std::io::stdout()
        .flush()
        .context("failed to flush confirmation prompt")?;

    let (tx, rx) = mpsc::channel::<Option<String>>();
    std::thread::spawn(move || {
        let mut input = String::new();
        let read_result = std::io::stdin().read_line(&mut input);
        if read_result.is_ok() {
            let _ = tx.send(Some(input));
        } else {
            let _ = tx.send(None);
        }
    });

    match rx.recv_timeout(Duration::from_secs(5)) {
        Ok(Some(input)) => {
            let normalized = input.trim().to_ascii_lowercase();
            if normalized.is_empty() || normalized == "y" || normalized == "yes" {
                Ok(())
            } else if normalized == "n" || normalized == "no" {
                bail!("aborted by user confirmation");
            } else {
                bail!("invalid confirmation answer '{}'", input.trim());
            }
        }
        Ok(None)
        | Err(mpsc::RecvTimeoutError::Timeout)
        | Err(mpsc::RecvTimeoutError::Disconnected) => {
            println!("\nNo confirmation received in time, proceeding.");
            Ok(())
        }
    }
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
        ControlRequest::Stats => match fs.collect_stats().await {
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
    let stored_compression_ratio = ratio(
        stats.stored_unique_compressed_bytes,
        stats.stored_unique_uncompressed_bytes,
    );
    let stored_compression_savings_ratio = (1.0 - stored_compression_ratio).max(0.0);
    let disk_vs_live_logical =
        stats.data_dir_size_bytes as i128 - stats.live_logical_size_bytes as i128;
    let disk_delta_note = if disk_vs_live_logical < 0 {
        format!(
            "saving {} vs live logical",
            human_bytes(u64::try_from((-disk_vs_live_logical) as u128).unwrap_or(u64::MAX))
        )
    } else if disk_vs_live_logical > 0 {
        format!(
            "overhead {} vs live logical",
            human_bytes(u64::try_from(disk_vs_live_logical as u128).unwrap_or(u64::MAX))
        )
    } else {
        "exactly equal to live logical".to_owned()
    };
    let all_namespaces_note = if stats.vault_locked {
        "live + snapshots + hidden /.vault"
    } else {
        "live + snapshots"
    };
    let live_scope_note = if stats.vault_locked {
        "locked /.vault excluded from live scope"
    } else {
        "includes /.vault when unlocked"
    };
    let consistency_ok = stats.chunk_refcount_mismatch_count == 0
        && stats.missing_chunk_records_for_extents == 0
        && stats.orphan_extent_records == 0;

    let mut rows = vec![
        (
            "Live Logical Size (/.snapshots excluded)".to_owned(),
            human_bytes(stats.live_logical_size_bytes),
            format!(
                "{} bytes, {}",
                stats.live_logical_size_bytes, live_scope_note
            ),
        ),
        (
            "Logical Size in Snapshots".to_owned(),
            human_bytes(stats.snapshots_logical_size_bytes),
            format!("{} bytes", stats.snapshots_logical_size_bytes),
        ),
        (
            "Logical Size (All Reachable Namespaces)".to_owned(),
            human_bytes(stats.all_logical_size_bytes),
            format!(
                "{} bytes ({})",
                stats.all_logical_size_bytes, all_namespaces_note
            ),
        ),
        (
            "Compression (Stored Unique Chunks)".to_owned(),
            format!("{:.2}% smaller", stored_compression_savings_ratio * 100.0),
            format!(
                "compressed/original: {:.2}%",
                stored_compression_ratio * 100.0
            ),
        ),
        (
            "Stored Unique Uncompressed (All Chunks)".to_owned(),
            human_bytes(stats.stored_unique_uncompressed_bytes),
            format!("{} bytes", stats.stored_unique_uncompressed_bytes),
        ),
        (
            "Stored Unique Compressed (All Chunks)".to_owned(),
            human_bytes(stats.stored_unique_compressed_bytes),
            format!("{} bytes", stats.stored_unique_compressed_bytes),
        ),
        (
            "Metadata Consistency".to_owned(),
            if consistency_ok { "ok" } else { "warning" }.to_owned(),
            format!(
                "refcount mismatches {}, missing chunks {}, orphan extents {}",
                stats.chunk_refcount_mismatch_count,
                stats.missing_chunk_records_for_extents,
                stats.orphan_extent_records
            ),
        ),
        (
            "Pack CRC32 Read Errors".to_owned(),
            stats.pack_crc32_read_error_count.to_string(),
            "read-time checksum mismatches (read continues)".to_owned(),
        ),
        (
            "Metadata Size".to_owned(),
            human_bytes(stats.metadata_size_bytes),
            format!("{} bytes", stats.metadata_size_bytes),
        ),
        (
            "Data Dir Size (On Disk)".to_owned(),
            human_bytes(stats.data_dir_size_bytes),
            format!("{} bytes", stats.data_dir_size_bytes),
        ),
        (
            "Disk Delta (data_dir - live_logical)".to_owned(),
            human_bytes_signed(disk_vs_live_logical),
            disk_delta_note,
        ),
        (
            "Cache Hit Rate".to_owned(),
            format!("{:.2}%", stats.cache_hit_rate * 100.0),
            format!(
                "{} hits / {} requests",
                stats.cache_hits, stats.cache_requests
            ),
        ),
        (
            "Process Private Memory".to_owned(),
            human_bytes(stats.process_private_memory_bytes),
            format!("{} bytes", stats.process_private_memory_bytes),
        ),
        (
            "Process RSS".to_owned(),
            human_bytes(stats.process_rss_bytes),
            format!("{} bytes", stats.process_rss_bytes),
        ),
        (
            "Approx Cache Memory".to_owned(),
            human_bytes(stats.approx_cache_memory_bytes),
            format!("{} bytes", stats.approx_cache_memory_bytes),
        ),
        (
            "Cache Entries".to_owned(),
            format!(
                "metadata {}, chunk-data {}",
                stats.metadata_cache_entries, stats.chunk_cache_entries
            ),
            "active cache keys".to_owned(),
        ),
        (
            "Average Throughput".to_owned(),
            format!(
                "read {}/s | write {}/s",
                human_bytes(stats.read_throughput_bps as u64),
                human_bytes(stats.write_throughput_bps as u64)
            ),
            format!("uptime {:.1}s", stats.uptime_secs),
        ),
        (
            "I/O Totals".to_owned(),
            format!(
                "read {} | write {}",
                human_bytes(stats.read_bytes_total),
                human_bytes(stats.write_bytes_total)
            ),
            format!(
                "{} / {} bytes",
                stats.read_bytes_total, stats.write_bytes_total
            ),
        ),
    ];
    if stats.vault_locked {
        rows.insert(
            1,
            (
                "Hidden Vault Logical Size".to_owned(),
                human_bytes(stats.hidden_vault_logical_size_bytes),
                format!(
                    "{} bytes (excluded from live while locked)",
                    stats.hidden_vault_logical_size_bytes
                ),
            ),
        );
    }

    let table = render_stats_table(&rows);
    format!(
        "VerFSNext Stats\n(scoped to mounted live tree unless explicitly marked otherwise)\n{}",
        table
    )
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn render_stats_table(rows: &[(String, String, String)]) -> String {
    let metric_header = "Metric";
    let value_header = "Value";
    let details_header = "Details";

    let metric_width = rows
        .iter()
        .map(|(metric, _, _)| metric.len())
        .max()
        .unwrap_or(metric_header.len())
        .max(metric_header.len());
    let value_width = rows
        .iter()
        .map(|(_, value, _)| value.len())
        .max()
        .unwrap_or(value_header.len())
        .max(value_header.len());
    let details_width = rows
        .iter()
        .map(|(_, _, details)| details.len())
        .max()
        .unwrap_or(details_header.len())
        .max(details_header.len());

    let sep = format!(
        "+-{:-<metric_width$}-+-{:-<value_width$}-+-{:-<details_width$}-+",
        "", "", ""
    );

    let mut out = String::new();
    out.push_str(&sep);
    out.push('\n');
    out.push_str(&format!(
        "| {:<metric_width$} | {:<value_width$} | {:<details_width$} |",
        metric_header, value_header, details_header
    ));
    out.push('\n');
    out.push_str(&sep);
    out.push('\n');

    for (metric, value, details) in rows {
        out.push_str(&format!(
            "| {:<metric_width$} | {:<value_width$} | {:<details_width$} |",
            metric, value, details
        ));
        out.push('\n');
    }

    out.push_str(&sep);
    out
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

fn init_logging() -> LogHandle {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(env_filter);
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().compact().with_target(true))
        .init();
    reload_handle
}

#[cfg(test)]
mod tests {
    use super::{
        format_stats_report, human_bytes_signed, parse_global_cli_options, render_stats_table,
        resolve_config_path, ConfigSource,
    };
    use crate::fs::VerFsStats;
    use std::path::Path;

    fn sample_stats() -> VerFsStats {
        VerFsStats {
            live_logical_size_bytes: 10_000,
            snapshots_logical_size_bytes: 2_000,
            hidden_vault_logical_size_bytes: 0,
            all_logical_size_bytes: 12_000,
            stored_unique_uncompressed_bytes: 11_000,
            stored_unique_compressed_bytes: 5_000,
            metadata_size_bytes: 1_000,
            data_dir_size_bytes: 7_500,
            vault_locked: false,
            chunk_refcount_mismatch_count: 0,
            missing_chunk_records_for_extents: 0,
            orphan_extent_records: 0,
            pack_crc32_read_error_count: 0,
            cache_hits: 9,
            cache_requests: 10,
            cache_hit_rate: 0.9,
            process_private_memory_bytes: 2_048,
            process_rss_bytes: 4_096,
            read_bytes_total: 100_000,
            write_bytes_total: 50_000,
            uptime_secs: 10.0,
            read_throughput_bps: 10_000.0,
            write_throughput_bps: 5_000.0,
            metadata_cache_entries: 123,
            chunk_cache_entries: 456,
            approx_cache_memory_bytes: 8_192,
        }
    }

    #[test]
    fn stats_report_contains_expected_sections_and_rows() {
        let report = format_stats_report(&sample_stats());
        assert!(report.contains("VerFSNext Stats"));
        assert!(report.contains("Metric"));
        assert!(report.contains("Live Logical Size (/.snapshots excluded)"));
        assert!(report.contains("Compression (Stored Unique Chunks)"));
        assert!(report.contains("Metadata Consistency"));
        assert!(report.contains("Pack CRC32 Read Errors"));
        assert!(report.contains("Average Throughput"));
        assert!(report.contains("I/O Totals"));
    }

    #[test]
    fn stats_report_formats_negative_disk_delta() {
        let mut stats = sample_stats();
        stats.data_dir_size_bytes = 1_000;
        stats.live_logical_size_bytes = 10_000;
        let report = format_stats_report(&stats);
        assert!(report.contains("Disk Delta (data_dir - live_logical)"));
        assert!(report.contains("saving"));
    }

    #[test]
    fn stats_report_shows_hidden_vault_row_when_locked() {
        let mut stats = sample_stats();
        stats.vault_locked = true;
        stats.hidden_vault_logical_size_bytes = 2_048;
        let report = format_stats_report(&stats);
        assert!(report.contains("Hidden Vault Logical Size"));
        assert!(report.contains("excluded from live while locked"));
    }

    #[test]
    fn stats_report_marks_metadata_warning_on_inconsistency() {
        let mut stats = sample_stats();
        stats.chunk_refcount_mismatch_count = 1;
        let report = format_stats_report(&stats);
        assert!(report.contains("Metadata Consistency"));
        assert!(report.contains("warning"));
    }

    #[test]
    fn stats_table_renders_headers_and_borders() {
        let rows = vec![
            ("A".to_owned(), "1".to_owned(), "x".to_owned()),
            ("Long Metric".to_owned(), "2".to_owned(), "y".to_owned()),
        ];
        let table = render_stats_table(&rows);
        assert!(table.contains("| Metric"));
        assert!(table.contains("| Value"));
        assert!(table.contains("| Details"));
        assert!(table.lines().next().unwrap_or_default().starts_with("+-"));
    }

    #[test]
    fn signed_human_bytes_has_minus_prefix_for_negative() {
        assert!(human_bytes_signed(-1024).starts_with('-'));
        assert!(!human_bytes_signed(1024).starts_with('-'));
    }

    #[test]
    fn global_config_option_is_parsed_when_present() {
        let parsed = parse_global_cli_options(vec![
            "--config".to_owned(),
            "/etc/verfsnext/config.toml".to_owned(),
            "stats".to_owned(),
        ])
        .expect("parse should succeed");
        assert_eq!(
            parsed.explicit_config.as_deref(),
            Some(Path::new("/etc/verfsnext/config.toml"))
        );
        assert_eq!(parsed.command_args, vec!["stats"]);
    }

    #[test]
    fn global_short_config_option_is_parsed_when_present() {
        let parsed = parse_global_cli_options(vec![
            "-c".to_owned(),
            "/tmp/custom.toml".to_owned(),
            "crypt".to_owned(),
            "-l".to_owned(),
        ])
        .expect("parse should succeed");
        assert_eq!(
            parsed.explicit_config.as_deref(),
            Some(Path::new("/tmp/custom.toml"))
        );
        assert_eq!(parsed.command_args, vec!["crypt", "-l"]);
    }

    #[test]
    fn explicit_config_resolution_skips_search() {
        let selected =
            resolve_config_path(Some(Path::new("/etc/verfsnext/config.toml"))).expect("resolve");
        assert_eq!(selected.path, Path::new("/etc/verfsnext/config.toml"));
        assert!(matches!(selected.source, ConfigSource::Explicit));
    }
}
