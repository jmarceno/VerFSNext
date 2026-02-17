mod config;
mod data;
mod fs;
mod meta;
mod sync;
mod types;
mod write;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use async_fusex::{FuseFs, VirtualFs, session::new_session};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::config::Config;
use crate::fs::VerFs;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging();

    let config_path = Path::new("config.toml");
    let config = Config::load_from_file(config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;

    let fs = Arc::new(VerFs::new(config.clone()).await?);
    let virtual_fs: Arc<dyn VirtualFs> = fs.clone();
    let fuse_fs = FuseFs::new(virtual_fs);

    let session = new_session(&config.mount_point, fuse_fs)
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

fn init_logging() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(true)
        .compact()
        .init();
}
