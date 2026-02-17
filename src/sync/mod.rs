use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

#[async_trait]
pub trait SyncTarget: Send + Sync {
    async fn sync_cycle(&self, full: bool) -> Result<()>;
}

pub struct SyncService {
    shutdown: CancellationToken,
    worker: Mutex<Option<JoinHandle<()>>>,
    target: Arc<dyn SyncTarget>,
}

impl SyncService {
    pub fn start(target: Arc<dyn SyncTarget>, interval: Duration) -> Self {
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();
        let target_for_worker = Arc::clone(&target);

        let worker = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
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
            target,
        }
    }

    pub async fn shutdown_with_full_sync(&self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }

        if let Err(err) = self.target.sync_cycle(true).await {
            error!(error = %err, "final full sync failed");
            return Err(err);
        }

        Ok(())
    }
}
