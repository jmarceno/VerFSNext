use std::sync::atomic::{AtomicU64, Ordering};
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
    done: Option<oneshot::Sender<Result<()>>>,
}

enum QueueMessage {
    Write(QueuedWrite),
    Drain(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<Result<()>>),
}

enum ApplyMessage {
    Batch(Vec<QueuedWrite>),
    Drain(oneshot::Sender<Result<()>>),
    Shutdown(oneshot::Sender<Result<()>>),
}

pub struct WriteBatcher {
    tx: mpsc::UnboundedSender<QueueMessage>,
    ingest_worker: Mutex<Option<JoinHandle<()>>>,
    apply_worker: Mutex<Option<JoinHandle<()>>>,
    /// Tracks the number of fire-and-forget writes enqueued since the last drain.
    /// Incremented atomically after each enqueue, reset after each drain completes.
    /// Used by readers to decide whether a drain is necessary before reading.
    pending_count: Arc<AtomicU64>,
}

impl WriteBatcher {
    /// Returns the number of fire-and-forget writes enqueued since the last drain.
    /// A return value of 0 means no writes are pending and the drain can be skipped.
    pub fn pending_write_count(&self) -> u64 {
        self.pending_count.load(Ordering::Acquire)
    }
}

impl WriteBatcher {
    pub fn new(
        sink: Arc<dyn WriteApply>,
        max_size_bytes: usize,
        flush_interval: Duration,
    ) -> Self {
        // Use an unbounded channel so Write and Drain messages are never reordered.
        // A bounded channel could cause a Drain to overtake a suspended Write
        // when the channel is full, breaking the FIFO ordering that the drain
        // mechanism relies on for read-after-write coherency.
        let (tx, mut rx) = mpsc::unbounded_channel::<QueueMessage>();
        let (apply_tx, mut apply_rx) = mpsc::unbounded_channel::<ApplyMessage>();

        let apply_sink = Arc::clone(&sink);
        let apply_handle = tokio::spawn(async move {
            let mut pending_error: Option<anyhow::Error> = None;

            while let Some(msg) = apply_rx.recv().await {
                match msg {
                    ApplyMessage::Batch(batch) => {
                        apply_queued_batch(&apply_sink, batch, &mut pending_error).await;
                    }
                    ApplyMessage::Drain(done_tx) => {
                        let _ = done_tx.send(take_pending_error(&mut pending_error));
                    }
                    ApplyMessage::Shutdown(done_tx) => {
                        let _ = done_tx.send(take_pending_error(&mut pending_error));
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
                                let needs_immediate_flush = msg.done.is_some();
                                pending_bytes = pending_bytes.saturating_add(msg.bytes);
                                pending.push(msg);
                                if needs_immediate_flush || pending_bytes >= max_size_bytes {
                                    dispatch_pending(&apply_tx, &mut pending, &mut pending_bytes);
                                }
                            }
                            Some(QueueMessage::Drain(done_tx)) => {
                                dispatch_pending(&apply_tx, &mut pending, &mut pending_bytes);
                                if let Err(err) = apply_tx.send(ApplyMessage::Drain(done_tx)) {
                                    match err.0 {
                                        ApplyMessage::Drain(done_tx) => {
                                            let _ = done_tx.send(Err(anyhow!("write apply worker is closed")));
                                        }
                                        _ => unreachable!("apply control channel mismatch"),
                                    }
                                }
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

        let pending_count = Arc::new(AtomicU64::new(0));
        Self {
            tx,
            ingest_worker: Mutex::new(Some(ingest_handle)),
            apply_worker: Mutex::new(Some(apply_handle)),
            pending_count,
        }
    }

    pub async fn enqueue(&self, op: WriteOp, bytes: usize) -> Result<()> {
        self.tx
            .send(QueueMessage::Write(QueuedWrite {
                op,
                bytes,
                done: None,
            }))
            .map_err(|_| anyhow!("write batcher is closed"))?;
        self.pending_count.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub async fn enqueue_and_wait(&self, op: WriteOp, bytes: usize) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueueMessage::Write(QueuedWrite {
                op,
                bytes,
                done: Some(done_tx),
            }))
            .map_err(|_| anyhow!("write batcher is closed"))?;

        done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before replying"))?
    }

    pub async fn drain(&self) -> Result<()> {
        let (done_tx, done_rx) = oneshot::channel();
        self.tx
            .send(QueueMessage::Drain(done_tx))
            .map_err(|_| anyhow!("write batcher is closed"))?;
        let result = done_rx
            .await
            .map_err(|_| anyhow!("write batcher worker exited before drain reply"))?;
        self.pending_count.store(0, Ordering::Release);
        result
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
        if let Some(done) = entry.done {
            let _ = done.send(Err(anyhow!(err_text.clone())));
        }
    }
}

fn take_pending_error(pending_error: &mut Option<anyhow::Error>) -> Result<()> {
    match pending_error.take() {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

async fn apply_queued_batch(
    sink: &Arc<dyn WriteApply>,
    mut batch: Vec<QueuedWrite>,
    pending_error: &mut Option<anyhow::Error>,
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
            if let Some(done) = entry.done {
                let _ = done.send(Err(anyhow!(err.to_string())));
            }
        }
        if pending_error.is_none() {
            *pending_error = Some(err);
        }
        return;
    }

    for (entry, result) in batch.into_iter().zip(results) {
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
}
