mod config;
mod data;
mod fs;
mod gc;
mod meta;
mod snapshot;
mod sync;
mod types;
mod write;

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use async_fusex::{
    session::{new_session, SessionConfig},
    FuseFs, VirtualFs,
};
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
    let meta = MetaStore::open(&config.metadata_dir()).await?;
    let snapshots = SnapshotManager::new(&meta);

    match args[idx].as_str() {
        "snapshot" => {
            idx += 1;
            if idx >= args.len() {
                bail!("missing snapshot subcommand (create|list|delete)");
            }
            match args[idx].as_str() {
                "create" => {
                    idx += 1;
                    if idx >= args.len() {
                        bail!("missing snapshot name");
                    }
                    snapshots.create(&args[idx]).await?;
                }
                "list" => {
                    let names = snapshots.list()?;
                    for name in names {
                        println!("{name}");
                    }
                }
                "delete" => {
                    idx += 1;
                    if idx >= args.len() {
                        bail!("missing snapshot name");
                    }
                    snapshots.delete(&args[idx]).await?;
                }
                other => bail!("unknown snapshot subcommand '{other}'"),
            }
        }
        other => bail!("unknown control command '{other}'"),
    }

    meta.close().await?;
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
