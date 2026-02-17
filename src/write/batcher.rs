use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

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
    done: Option<oneshot::Sender<Result<()>>>,
}

enum QueueMessage {
    Write(QueuedWrite),
    Drain(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<Result<()>>),
}

pub struct WriteBatcher {
    tx: mpsc::Sender<QueueMessage>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl WriteBatcher {
    pub fn new(
        sink: Arc<dyn WriteApply>,
        max_blocks: usize,
        flush_interval: Duration,
        queue_capacity: usize,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<QueueMessage>(queue_capacity);

        let handle = tokio::spawn(async move {
            let mut pending = Vec::<QueuedWrite>::new();
            let mut pending_blocks = 0_usize;
            let mut ticker = tokio::time::interval(flush_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut pending_error: Option<anyhow::Error> = None;

            loop {
                tokio::select! {
                    maybe_msg = rx.recv() => {
                        match maybe_msg {
                            Some(QueueMessage::Write(msg)) => {
                                pending_blocks = pending_blocks.saturating_add(msg.blocks.max(1));
                                pending.push(msg);
                                if pending_blocks >= max_blocks.max(1) {
                                    flush_pending(&sink, &mut pending, &mut pending_blocks, &mut pending_error).await;
                                }
                            }
                            Some(QueueMessage::Drain(done_tx)) => {
                                flush_pending(&sink, &mut pending, &mut pending_blocks, &mut pending_error).await;
                                let _ = done_tx.send(match pending_error.take() {
                                    Some(err) => Err(err),
                                    None => Ok(()),
                                });
                            }
                            Some(QueueMessage::Shutdown(done_tx)) => {
                                flush_pending(&sink, &mut pending, &mut pending_blocks, &mut pending_error).await;
                                let _ = done_tx.send(match pending_error.take() {
                                    Some(err) => Err(err),
                                    None => Ok(()),
                                });
                                break;
                            }
                            None => {
                                flush_pending(&sink, &mut pending, &mut pending_blocks, &mut pending_error).await;
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        flush_pending(&sink, &mut pending, &mut pending_blocks, &mut pending_error).await;
                    }
                }
            }
        });

        Self {
            tx,
            worker: Mutex::new(Some(handle)),
        }
    }

    pub async fn enqueue(&self, op: WriteOp, touched_blocks: usize) -> Result<()> {
        self.tx
            .send(QueueMessage::Write(QueuedWrite {
                op,
                blocks: touched_blocks.max(1),
                done: None,
            }))
            .await
            .map_err(|_| anyhow!("write batcher is closed"))
    }

    pub async fn enqueue_and_wait(&self, op: WriteOp, touched_blocks: usize) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueueMessage::Write(QueuedWrite {
                op,
                blocks: touched_blocks.max(1),
                done: Some(done_tx),
            }))
            .await
            .map_err(|_| anyhow!("write batcher is closed"))?;

        done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before replying"))?
    }

    pub async fn drain(&self) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueueMessage::Drain(done_tx))
            .await
            .map_err(|_| anyhow!("write batcher is closed"))?;
        done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before drain reply"))?
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueueMessage::Shutdown(done_tx))
            .await
            .map_err(|_| anyhow!("write batcher is closed"))?;
        let flush_result = done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before shutdown reply"))?;
        if let Some(worker) = self.worker.lock().await.take() {
            let _ = worker.await;
        }
        flush_result
    }
}

async fn flush_pending(
    sink: &Arc<dyn WriteApply>,
    pending: &mut Vec<QueuedWrite>,
    pending_blocks: &mut usize,
    pending_error: &mut Option<anyhow::Error>,
) {
    if pending.is_empty() {
        return;
    }

    let ops = pending
        .iter()
        .map(|entry| entry.op.clone())
        .collect::<Vec<_>>();
    let results = sink.apply_batch(ops).await;

    if results.len() != pending.len() {
        let err = anyhow!(
            "write sink returned {} results for {} queued writes",
            results.len(),
            pending.len()
        );
        for entry in pending.drain(..) {
            if let Some(done) = entry.done {
                let _ = done.send(Err(anyhow!(err.to_string())));
            }
        }
        if pending_error.is_none() {
            *pending_error = Some(err);
        }
        *pending_blocks = 0;
        return;
    }

    for (entry, result) in pending.drain(..).zip(results.into_iter()) {
        match result {
            Ok(()) => {
                if let Some(done) = entry.done {
                    let _ = done.send(Ok(()));
                }
            }
            Err(err) => {
                if pending_error.is_none() {
                    *pending_error = Some(anyhow!(err.to_string()));
                }
                if let Some(done) = entry.done {
                    let _ = done.send(Err(err));
                }
            }
        }
    }
    *pending_blocks = 0;
}
