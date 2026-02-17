use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct WriteOp {
    pub ino: u64,
    pub offset: u64,
    pub data: Vec<u8>,
}

#[async_trait]
pub trait WriteApply: Send + Sync {
    async fn apply_batch(&self, ops: Vec<WriteOp>) -> Vec<Result<()>>;
}

struct QueuedWrite {
    op: WriteOp,
    blocks: usize,
    done: oneshot::Sender<Result<()>>,
}

pub struct WriteBatcher {
    tx: mpsc::Sender<QueuedWrite>,
    shutdown: CancellationToken,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl WriteBatcher {
    pub fn new(
        sink: Arc<dyn WriteApply>,
        max_blocks: usize,
        flush_interval: Duration,
        queue_capacity: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<QueuedWrite>(queue_capacity);
        let shutdown = CancellationToken::new();
        let worker_shutdown = shutdown.clone();

        let handle = tokio::spawn(async move {
            let mut pending = Vec::<QueuedWrite>::new();
            let mut pending_blocks = 0_usize;
            let mut ticker = tokio::time::interval(flush_interval);

            loop {
                tokio::select! {
                    _ = worker_shutdown.cancelled() => {
                        flush_pending(&sink, &mut pending, &mut pending_blocks).await;
                        break;
                    }
                    maybe_msg = rx.recv() => {
                        match maybe_msg {
                            Some(msg) => {
                                pending_blocks = pending_blocks.saturating_add(msg.blocks);
                                pending.push(msg);
                                if pending_blocks >= max_blocks {
                                    flush_pending(&sink, &mut pending, &mut pending_blocks).await;
                                }
                            }
                            None => {
                                flush_pending(&sink, &mut pending, &mut pending_blocks).await;
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        flush_pending(&sink, &mut pending, &mut pending_blocks).await;
                    }
                }
            }
        });

        Self {
            tx,
            shutdown,
            worker: Mutex::new(Some(handle)),
        }
    }

    pub async fn enqueue(&self, op: WriteOp, touched_blocks: usize) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueuedWrite {
                op,
                blocks: touched_blocks.max(1),
                done: done_tx,
            })
            .await
            .map_err(|_| anyhow!("write batcher is closed"))?;

        done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before replying"))?
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutdown.cancel();
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }
        Ok(())
    }
}

async fn flush_pending(
    sink: &Arc<dyn WriteApply>,
    pending: &mut Vec<QueuedWrite>,
    pending_blocks: &mut usize,
) {
    if pending.is_empty() {
        return;
    }

    let ops = pending.iter().map(|entry| entry.op.clone()).collect::<Vec<_>>();
    let results = sink.apply_batch(ops).await;

    if results.len() != pending.len() {
        let err = anyhow!(
            "write sink returned {} results for {} queued writes",
            results.len(),
            pending.len()
        );
        for entry in pending.drain(..) {
            let _ = entry.done.send(Err(anyhow!(err.to_string())));
        }
        *pending_blocks = 0;
        return;
    }

    for (entry, result) in pending.drain(..).zip(results.into_iter()) {
        let _ = entry.done.send(result);
    }
    *pending_blocks = 0;
}
