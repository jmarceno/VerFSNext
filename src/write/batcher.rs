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
    bytes: usize,
    /// Resolves this write's own result. Every queued write has one: there is
    /// no fire-and-forget producer, so no error can be left unowned (B042).
    done: oneshot::Sender<Result<()>>,
}

enum QueueMessage {
    Write(QueuedWrite),
    Shutdown(oneshot::Sender<Result<()>>),
}

enum ApplyMessage {
    Batch(Vec<QueuedWrite>),
    Shutdown(oneshot::Sender<Result<()>>),
}

pub struct WriteBatcher {
    tx: mpsc::UnboundedSender<QueueMessage>,
    ingest_worker: Mutex<Option<JoinHandle<()>>>,
    apply_worker: Mutex<Option<JoinHandle<()>>>,
}

impl WriteBatcher {
    pub fn new(
        sink: Arc<dyn WriteApply>,
        max_size_bytes: usize,
        flush_interval: Duration,
    ) -> Self {
        // Use an unbounded channel so a Write never waits for capacity and a
        // Shutdown can never overtake a Write that is still queued: the ingest
        // worker must dispatch every write before it answers the shutdown.
        let (tx, mut rx) = mpsc::unbounded_channel::<QueueMessage>();
        let (apply_tx, mut apply_rx) = mpsc::unbounded_channel::<ApplyMessage>();

        let apply_sink = Arc::clone(&sink);
        let apply_handle = tokio::spawn(async move {
            let mut deferred: Option<ApplyMessage> = None;

            loop {
                let msg = match deferred.take() {
                    Some(msg) => msg,
                    None => match apply_rx.recv().await {
                        Some(msg) => msg,
                        None => break,
                    },
                };
                match msg {
                    ApplyMessage::Batch(mut batch) => {
                        // Group commit: writes that queued up while the previous
                        // commit was running share this commit and its pack sync.
                        // A control message ends the group and runs right after it.
                        let mut batch_bytes = batch.iter().map(|entry| entry.bytes).sum::<usize>();
                        while batch_bytes < max_size_bytes {
                            match apply_rx.try_recv() {
                                Ok(ApplyMessage::Batch(more)) => {
                                    batch_bytes += more.iter().map(|entry| entry.bytes).sum::<usize>();
                                    batch.extend(more);
                                }
                                Ok(other) => {
                                    deferred = Some(other);
                                    break;
                                }
                                Err(_) => break,
                            }
                        }
                        apply_queued_batch(&apply_sink, batch).await;
                    }
                    ApplyMessage::Shutdown(done_tx) => {
                        let _ = done_tx.send(Ok(()));
                        break;
                    }
                }
            }
        });

        let ingest_handle = tokio::spawn(async move {
            let mut pending = Vec::<QueuedWrite>::new();
            let mut pending_bytes = 0_usize;
            let mut ticker = tokio::time::interval(flush_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    maybe_msg = rx.recv() => {
                        match maybe_msg {
                            Some(QueueMessage::Write(msg)) => {
                                pending_bytes = pending_bytes.saturating_add(msg.bytes);
                                pending.push(msg);
                                // Dispatch right away: every write carries a
                                // waiter, and the apply worker's group commit
                                // merges whatever queued up meanwhile.
                                dispatch_pending(&apply_tx, &mut pending, &mut pending_bytes);
                            }
                            Some(QueueMessage::Shutdown(done_tx)) => {
                                dispatch_pending(&apply_tx, &mut pending, &mut pending_bytes);
                                if let Err(err) = apply_tx.send(ApplyMessage::Shutdown(done_tx)) {
                                    match err.0 {
                                        ApplyMessage::Shutdown(done_tx) => {
                                            let _ = done_tx.send(Err(anyhow!("write apply worker is closed")));
                                        }
                                        _ => unreachable!("apply control channel mismatch"),
                                    }
                                }
                                break;
                            }
                            None => {
                                dispatch_pending(&apply_tx, &mut pending, &mut pending_bytes);
                                break;
                            }
                        }
                    }
                    _ = ticker.tick() => {
                        dispatch_pending(&apply_tx, &mut pending, &mut pending_bytes);
                    }
                }
            }
            drop(apply_tx);
        });

        Self {
            tx,
            ingest_worker: Mutex::new(Some(ingest_handle)),
            apply_worker: Mutex::new(Some(apply_handle)),
        }
    }

    /// Applies one write and resolves only after the batch containing it was
    /// applied, so a successful return means the write is committed and its
    /// error, if any, is reported to this caller. This is the batcher's only
    /// write entry point: there is no fire-and-forget path (B042).
    pub async fn enqueue_and_wait(&self, op: WriteOp, bytes: usize) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueueMessage::Write(QueuedWrite {
                op,
                bytes,
                done: done_tx,
            }))
            .map_err(|_| anyhow!("write batcher is closed"))?;

        done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before replying"))?
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueueMessage::Shutdown(done_tx))
            .map_err(|_| anyhow!("write batcher is closed"))?;
        let flush_result = done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before shutdown reply"))?;
        if let Some(worker) = self.ingest_worker.lock().await.take() {
            let _ = worker.await;
        }
        if let Some(worker) = self.apply_worker.lock().await.take() {
            let _ = worker.await;
        }
        flush_result
    }
}

fn dispatch_pending(
    apply_tx: &mpsc::UnboundedSender<ApplyMessage>,
    pending: &mut Vec<QueuedWrite>,
    pending_bytes: &mut usize,
) {
    if pending.is_empty() {
        return;
    }

    let batch = std::mem::take(pending);
    *pending_bytes = 0;
    if let Err(err) = apply_tx.send(ApplyMessage::Batch(batch)) {
        match err.0 {
            ApplyMessage::Batch(batch) => {
                fail_batch(batch, anyhow!("write apply worker is closed"))
            }
            _ => unreachable!("apply data channel mismatch"),
        }
    }
}

fn fail_batch(batch: Vec<QueuedWrite>, err: anyhow::Error) {
    let err_text = err.to_string();
    for entry in batch {
        let _ = entry.done.send(Err(anyhow!(err_text.clone())));
    }
}

async fn apply_queued_batch(
    sink: &Arc<dyn WriteApply>,
    mut batch: Vec<QueuedWrite>,
) {
    if batch.is_empty() {
        return;
    }

    // Extract WriteOps without cloning the data Vec — take ownership instead.
    let ops: Vec<WriteOp> = batch
        .iter_mut()
        .map(|entry| WriteOp {
            ino: entry.op.ino,
            offset: entry.op.offset,
            data: std::mem::take(&mut entry.op.data),
        })
        .collect();
    let results = sink.apply_batch(ops).await;

    if results.len() != batch.len() {
        let err = anyhow!(
            "write sink returned {} results for {} queued writes",
            results.len(),
            batch.len()
        );
        for entry in batch {
            let _ = entry.done.send(Err(anyhow!(err.to_string())));
        }
        return;
    }

    for (entry, result) in batch.into_iter().zip(results) {
        match result {
            Ok(()) => {
                let _ = entry.done.send(Ok(()));
            }
            Err(err) => {
                let _ = entry.done.send(Err(err));
            }
        }
    }
}
