use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

/// How often committed metadata is handed to the OS. Metadata commits stay in
/// the process until the pack data they reference is durable, so this bounds
/// how much committed work a daemon crash can lose.
const WAL_HANDOFF_INTERVAL: Duration = Duration::from_millis(250);

#[async_trait]
pub trait SyncTarget: Send + Sync {
    async fn sync_cycle(&self, full: bool) -> Result<()>;
    /// Syncs pack data and hands the metadata that references it to the OS.
    async fn hand_off_wal(&self) -> Result<()>;
}

pub struct SyncService {
    shutdown: CancellationToken,
    worker: Mutex<Option<JoinHandle<()>>>,
    handoff_worker: Mutex<Option<JoinHandle<()>>>,
    target: Arc<dyn SyncTarget>,
}

impl SyncService {
    pub fn start(target: Arc<dyn SyncTarget>, interval: Duration) -> Self {
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let target_for_worker = Arc::clone(&target);

        // A separate task, so a long sync cycle (GC included) never delays the
        // hand-off of committed metadata.
        let handoff_shutdown = shutdown.clone();
        let handoff_target = Arc::clone(&target);
        let handoff_worker = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(WAL_HANDOFF_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = handoff_shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        if let Err(err) = handoff_target.hand_off_wal().await {
                            error!(error = %format!("{err:#}"), "metadata WAL hand-off failed");
                        }
                    }
                }
            }
        });

        let worker = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = worker_shutdown.cancelled() => break,
                    _ = ticker.tick() => {
                        if let Err(err) = target_for_worker.sync_cycle(false).await {
                            warn!(error = %err, "background sync cycle failed");
                        }
                    }
                }
            }
        });

        Self {
            shutdown,
            worker: Mutex::new(Some(worker)),
            handoff_worker: Mutex::new(Some(handoff_worker)),
            target,
        }
    }

    pub async fn shutdown_with_full_sync(&self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(worker) = self.worker.lock().await.take() {
            if let Err(err) = worker.await {
                error!(error = %err, "background sync worker failed");
            }
        }
        if let Some(worker) = self.handoff_worker.lock().await.take() {
            if let Err(err) = worker.await {
                error!(error = %err, "WAL hand-off worker failed");
            }
        }

        if let Err(err) = self.target.sync_cycle(true).await {
            error!(error = %err, "final full sync failed");
            return Err(err);
        }

        Ok(())
    }
}
