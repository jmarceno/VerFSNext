use super::*;
use crate::fs::chunk::{NewChunk, StagedChunk};
use crate::fs::FsCore;

/// All writes to one inode within a batch, kept as sorted, disjoint and
/// non-adjacent byte ranges. Writes are folded in arrival order, so a later
/// write wins where writes overlap.
pub(crate) struct WriteGroup {
    ino: u64,
    segments: Vec<WriteOp>,
    indices: Vec<usize>,
}

impl WriteGroup {
    fn new(ino: u64) -> Self {
        Self {
            ino,
            segments: Vec::new(),
            indices: Vec::new(),
        }
    }

    fn add(&mut self, idx: usize, op: WriteOp) {
        self.indices.push(idx);
        if op.data.is_empty() {
            return;
        }
        let first = self
            .segments
            .iter()
            .position(|segment| can_coalesce_write(segment, &op) || segment.offset > op.offset);
        let Some(first) = first else {
            self.segments.push(op);
            return;
        };
        let touching = self.segments[first..]
            .iter()
            .take_while(|segment| can_coalesce_write(segment, &op))
            .count();
        if touching == 0 {
            self.segments.insert(first, op);
            return;
        }
        let mut touched = self.segments.drain(first..first + touching);
        let mut merged = touched.next().expect("touching segment count is non-zero");
        for segment in touched {
            merge_write_op(&mut merged, segment);
        }
        merge_write_op(&mut merged, op);
        self.segments.insert(first, merged);
    }

    fn first_offset(&self) -> u64 {
        self.segments.first().map_or(0, |segment| segment.offset)
    }

    fn data_len(&self) -> u64 {
        self.segments
            .iter()
            .map(|segment| segment.data.len() as u64)
            .sum()
    }
}

/// How far past the last written byte a region keeps re-chunking unchanged
/// data while waiting for a cut to land on an existing extent boundary, in
/// maximum chunk sizes. Past it the region ends at the next existing
/// boundary.
const RESYNC_MAX_CHUNKS: u64 = 2;

struct PreparedWritePlan {
    write_end: u64,
    dedup_hits: u64,
    dedup_misses: u64,
    /// Starts of extents that are removed without a replacement at the
    /// same offset.
    extent_deletes: Vec<u64>,
    extent_sets: Vec<FileExtent>,
    ref_deltas: HashMap<ChunkId, i64>,
    new_chunks: HashMap<ChunkId, NewChunk>,
}

/// A chunk cut from a region, with the bytes it holds.
struct CutChunk {
    extent: FileExtent,
    data: Arc<Vec<u8>>,
    /// Cut at the end of the file rather than at a content-defined boundary:
    /// the next append re-chunks it, so it is stored inline.
    eof_tail: bool,
}

fn segment_end(segment: &WriteOp) -> u64 {
    segment.offset + segment.data.len() as u64
}

/// The extent of `extents` that holds byte `offset`.
fn covering_extent(extents: &BTreeMap<u64, FileExtent>, offset: u64) -> Option<FileExtent> {
    extents
        .range(..=offset)
        .next_back()
        .map(|(_, extent)| *extent)
        .filter(|extent| extent.end() > offset)
}

/// Moves `next` past every segment that ends at or before `offset`.
fn skip_segments_before(segments: &[WriteOp], next: &mut usize, offset: u64) {
    while segments
        .get(*next)
        .is_some_and(|segment| segment_end(segment) <= offset)
    {
        *next += 1;
    }
}

impl FsCore {
    fn ensure_write_target_inode(&self, ino: u64, inode: &InodeRecord) -> Result<()> {
        self.ensure_inode_vault_access(inode, "write")?;
        if inode.kind != INODE_KIND_FILE {
            return Err(anyhow_errno(
                Errno::EISDIR,
                format!("write target inode {} is not a regular file", ino),
            ));
        }
        FsCore::ensure_inode_writable(inode, "write")
    }

    /// Plans the extents of one inode after its batched writes.
    ///
    /// Chunk boundaries are content defined and independent of how the
    /// kernel split the data into requests. Every write is re-chunked as part
    /// of a region of the file that starts at the extent holding the byte
    /// before the write, so an append continues the tail chunk instead of
    /// starting a new one at the old end of file. A region runs past the end
    /// of its writes until a cut lands on an existing extent boundary, from
    /// where the existing extents are kept as they are.
    async fn prepare_write_plan(
        &self,
        group: &WriteGroup,
        inode: &InodeRecord,
        txn: &verfsnext_surrealkv::Transaction,
    ) -> Result<PreparedWritePlan> {
        let mut write_end = 0_u64;
        for segment in &group.segments {
            let end = segment
                .offset
                .checked_add(segment.data.len() as u64)
                .ok_or_else(|| anyhow_errno(Errno::EOVERFLOW, "write offset overflow"))?;
            write_end = write_end.max(end);
        }

        // Extents a region can read: the one before each write, the ones it
        // overwrites and the ones it re-chunks while resynchronizing.
        let max_chunk = self.chunker.max_size() as u64;
        let reach = (RESYNC_MAX_CHUNKS + 2) * max_chunk + 2 * self.max_extent_len;
        let file_end = inode.size.max(write_end);
        let mut old_extents = BTreeMap::<u64, FileExtent>::new();
        if inode.size != 0 {
            for segment in &group.segments {
                let from = segment.offset.saturating_sub(1);
                let to = segment_end(segment).saturating_add(reach);
                for extent in self.load_extents_in_txn(txn, group.ino, from, to)? {
                    old_extents.insert(extent.start, extent);
                }
            }
        }

        let mut replaced = HashMap::<u64, FileExtent>::new();
        let mut cut_chunks = Vec::<CutChunk>::new();
        let mut next_segment = 0;
        while let Some(segment) = group.segments.get(next_segment) {
            let region_start = segment
                .offset
                .checked_sub(1)
                .and_then(|before| covering_extent(&old_extents, before))
                .map_or(segment.offset, |extent| extent.start);
            let region_end = self.chunk_region(
                inode,
                &group.segments,
                &mut next_segment,
                &old_extents,
                region_start,
                file_end,
                &mut cut_chunks,
            )?;
            replaced.extend(
                old_extents
                    .range(region_start..region_end)
                    .map(|(start, extent)| (*start, *extent)),
            );
        }

        let mut extent_sets = Vec::<FileExtent>::with_capacity(cut_chunks.len());
        let vault = FsCore::inode_is_vault(inode);
        let mut ref_deltas =
            HashMap::<ChunkId, i64>::with_capacity(cut_chunks.len().saturating_mul(2));
        let mut pending_chunks = HashMap::<ChunkId, StagedChunk>::new();
        let mut checked_ids = HashSet::<ChunkId>::new();
        let mut dedup_hits = 0_u64;
        let mut dedup_misses = 0_u64;
        for chunk in cut_chunks {
            if let Some(old) = replaced.remove(&chunk.extent.start) {
                if old.same_mapping(&chunk.extent) {
                    continue;
                }
                let old_id = ChunkId {
                    vault,
                    hash: old.record.chunk_hash,
                };
                *ref_deltas.entry(old_id).or_insert(0) -= 1;
            }
            let id = ChunkId {
                vault,
                hash: chunk.extent.record.chunk_hash,
            };
            *ref_deltas.entry(id).or_insert(0) += 1;
            extent_sets.push(chunk.extent);
            let staged_new = self.stage_chunk_if_missing(
                txn,
                id,
                chunk.data,
                chunk.eof_tail,
                &mut checked_ids,
                &mut pending_chunks,
            )?;
            if staged_new {
                dedup_misses = dedup_misses.saturating_add(1);
            } else {
                dedup_hits = dedup_hits.saturating_add(1);
            }
        }
        let mut extent_deletes = Vec::with_capacity(replaced.len());
        for (start, old) in replaced {
            let old_id = ChunkId {
                vault,
                hash: old.record.chunk_hash,
            };
            *ref_deltas.entry(old_id).or_insert(0) -= 1;
            extent_deletes.push(start);
        }

        let new_chunks = self.materialize_pending_chunks(pending_chunks).await?;

        Ok(PreparedWritePlan {
            write_end,
            dedup_hits,
            dedup_misses,
            extent_deletes,
            extent_sets,
            ref_deltas,
            new_chunks,
        })
    }

    /// Cuts the region that starts at `region_start` into chunks, pushes them
    /// onto `out` and returns the end of the region. Every extent of
    /// `old_extents` that starts inside the region is replaced by the chunks;
    /// the region never ends inside an extent.
    ///
    /// The region's bytes are the writes from `segments[*next_segment..]`
    /// laid over the old extents. A write that the region reaches is merged
    /// into it, and `next_segment` is left at the first write it did not
    /// reach. The region ends at the first cut, past every merged write, that
    /// falls on the start of an old extent or on the end of the data (end of
    /// file or a hole), or at the old boundary that follows once the
    /// resynchronization budget is spent.
    #[allow(clippy::too_many_arguments)]
    fn chunk_region(
        &self,
        inode: &InodeRecord,
        segments: &[WriteOp],
        next_segment: &mut usize,
        old_extents: &BTreeMap<u64, FileExtent>,
        region_start: u64,
        file_end: u64,
        out: &mut Vec<CutChunk>,
    ) -> Result<u64> {
        let vault = FsCore::inode_is_vault(inode);
        let max_chunk = self.chunker.max_size();
        let resync_bytes = RESYNC_MAX_CHUNKS * max_chunk as u64;
        // Bytes [pos, fill) of the region, materialized ahead of the cut.
        let mut pending = Vec::<u8>::with_capacity(2 * max_chunk);
        let mut pos = region_start;
        let mut fill = region_start;
        // End of the last write merged into the region.
        let mut dirty_end = region_start;
        // Set once the resynchronization budget is spent: the region ends
        // exactly here.
        let mut stop_at = None::<u64>;
        let mut data_ended = false;
        let mut last_payload = None::<(u64, Arc<Vec<u8>>)>;
        loop {
            while !data_ended && pending.len() < max_chunk {
                let wanted = fill + (max_chunk - pending.len()) as u64;
                skip_segments_before(segments, next_segment, fill);
                if let Some(segment) = segments
                    .get(*next_segment)
                    .filter(|segment| segment.offset <= fill)
                {
                    let end = segment_end(segment);
                    let piece_end = end.min(wanted);
                    pending.extend_from_slice(
                        &segment.data
                            [(fill - segment.offset) as usize..(piece_end - segment.offset) as usize],
                    );
                    fill = piece_end;
                    dirty_end = dirty_end.max(end);
                    stop_at = None;
                    continue;
                }
                if stop_at == Some(fill) {
                    data_ended = true;
                    break;
                }
                let Some(extent) = covering_extent(old_extents, fill) else {
                    data_ended = true;
                    break;
                };
                let mut piece_end = extent.end().min(wanted);
                if let Some(segment) = segments.get(*next_segment) {
                    piece_end = piece_end.min(segment.offset);
                }
                if let Some(stop) = stop_at {
                    piece_end = piece_end.min(stop);
                }
                let payload = match &last_payload {
                    Some((start, payload)) if *start == extent.start => Arc::clone(payload),
                    _ => {
                        let payload = self.load_extent_payload(&extent, vault)?;
                        last_payload = Some((extent.start, Arc::clone(&payload)));
                        payload
                    }
                };
                pending.extend_from_slice(
                    &payload[(fill - extent.start) as usize..(piece_end - extent.start) as usize],
                );
                fill = piece_end;
            }

            if pending.is_empty() {
                return Ok(pos);
            }
            let len = self.chunker.cut(&pending);
            let data = pending[..len].to_vec();
            pending.drain(..len);
            let chunk_hash = FsCore::hash_for_inode(inode, &data);
            let eof_tail = data_ended && pending.is_empty() && pos + len as u64 == file_end;
            out.push(CutChunk {
                extent: FileExtent {
                    start: pos,
                    record: ExtentRecord {
                        chunk_hash,
                        len: len as u32,
                    },
                },
                data: Arc::new(data),
                eof_tail,
            });
            pos += len as u64;

            if pos < dirty_end {
                continue;
            }
            skip_segments_before(segments, next_segment, pos);
            if segments
                .get(*next_segment)
                .is_some_and(|segment| segment.offset <= pos)
            {
                continue;
            }
            if pending.is_empty() && data_ended {
                return Ok(pos);
            }
            if old_extents.contains_key(&pos) {
                return Ok(pos);
            }
            if stop_at.is_none() && pos - dirty_end >= resync_bytes {
                // No write is materialized past `dirty_end`, so `pos` lies in
                // old data and inside an old extent.
                let extent = covering_extent(old_extents, pos).ok_or_else(|| {
                    anyhow_errno(
                        Errno::EIO,
                        format!("write region of inode {} lost its data at {}", inode.ino, pos),
                    )
                })?;
                let stop = extent.end();
                pending.truncate((stop - pos) as usize);
                fill = fill.min(stop);
                stop_at = Some(stop);
            }
        }
    }
    pub(crate) async fn truncate_file_locked(
        &self,
        ino: u64,
        new_size: u64,
    ) -> Result<InodeRecord> {
        let mut attempt = 1;
        loop {
            let mut txn = self.meta.begin_write()?;
            let (inode, staged) = self.stage_truncate_in_txn(ino, new_size, &mut txn).await?;
            let Some(new_chunks) = staged else {
                return Ok(inode);
            };
            match self.meta.commit_write_txn(&mut txn).await {
                Ok(()) => {
                    self.publish_committed_chunk_records(&new_chunks);
                    self.invalidate_inode_cache(ino);
                    self.mark_mutation();
                    self.bump_inode_data_version(ino);
                    self.invalidate_inode_attr_best_effort(ino);
                    return Ok(inode);
                }
                Err(err) if attempt < MAX_COMMIT_ATTEMPTS && is_retryable_commit_error(&err) => {
                    tracing::warn!(ino, attempt, error = %format!("{err:#}"), "retrying truncate after metadata conflict");
                    attempt += 1;
                    tokio::task::yield_now().await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Stages a truncate of `ino` to `new_size` into `txn`. Returns `None`
    /// instead of the new chunk records when the size is already `new_size`.
    ///
    /// The inode and its extents are read through `txn` itself so that the
    /// refcount deltas are computed from exactly the state the commit is
    /// validated against.
    async fn stage_truncate_in_txn(
        &self,
        ino: u64,
        new_size: u64,
        txn: &mut verfsnext_surrealkv::Transaction,
    ) -> Result<(InodeRecord, Option<HashMap<ChunkId, NewChunk>>)> {
        let mut inode = FsCore::load_inode_in_txn(txn, ino, "truncate")?;
        self.ensure_inode_vault_access(&inode, "truncate")?;
        if inode.kind != INODE_KIND_FILE {
            return Err(anyhow_errno(
                Errno::EISDIR,
                format!("truncate inode {} is not a regular file", ino),
            ));
        }
        FsCore::ensure_inode_writable(&inode, "truncate")?;

        if new_size == inode.size {
            return Ok((inode, None));
        }

        let old_size = inode.size;
        let vault = FsCore::inode_is_vault(&inode);
        let mut extent_updates = Vec::<FileExtent>::new();
        let mut extent_deletes = Vec::<u64>::new();
        let mut ref_deltas = HashMap::<ChunkId, i64>::new();
        let mut pending_chunks = HashMap::<ChunkId, StagedChunk>::new();
        let mut checked_ids = HashSet::<ChunkId>::new();

        if new_size < old_size {
            for extent in self.load_extents_in_txn(txn, ino, new_size, u64::MAX)? {
                let old_id = ChunkId {
                    vault,
                    hash: extent.record.chunk_hash,
                };
                *ref_deltas.entry(old_id).or_insert(0) -= 1;
                if extent.start >= new_size {
                    extent_deletes.push(extent.start);
                    continue;
                }
                // The extent that holds the new last byte keeps its prefix.
                let payload = self.load_extent_payload(&extent, vault)?;
                let data = Arc::new(payload[..(new_size - extent.start) as usize].to_vec());
                let chunk_hash = FsCore::hash_for_inode(&inode, &data);
                let trimmed = FileExtent {
                    start: extent.start,
                    record: ExtentRecord {
                        chunk_hash,
                        len: data.len() as u32,
                    },
                };
                let id = ChunkId {
                    vault,
                    hash: chunk_hash,
                };
                *ref_deltas.entry(id).or_insert(0) += 1;
                extent_updates.push(trimmed);
                // The kept prefix now ends the file: an append re-chunks it.
                self.stage_chunk_if_missing(
                    txn,
                    id,
                    data,
                    true,
                    &mut checked_ids,
                    &mut pending_chunks,
                )?;
            }
        }
        let new_chunks = self.materialize_pending_chunks(pending_chunks).await?;

        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        inode.size = new_size;
        inode.mtime_sec = sec;
        inode.mtime_nsec = nsec;
        inode.ctime_sec = sec;
        inode.ctime_nsec = nsec;

        for start in extent_deletes {
            txn.delete(extent_key(ino, start))?;
        }
        for extent in extent_updates {
            txn.set(extent_key(ino, extent.start), encode_rkyv(&extent.record)?)?;
        }
        FsCore::apply_ref_deltas_in_txn(txn, &ref_deltas, &new_chunks)?;
        txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
        Ok((inode, Some(new_chunks)))
    }
    pub(crate) fn lock_type_valid(typ: u32) -> bool {
        typ == libc::F_RDLCK as u32 || typ == libc::F_WRLCK as u32 || typ == libc::F_UNLCK as u32
    }
    pub(crate) fn lock_ranges_overlap(a: &FileLockState, b: &FileLockState) -> bool {
        let a_end = a.end;
        let b_end = b.end;
        a.start <= b_end && b.start <= a_end
    }
    pub(crate) fn lock_conflict(request: &FileLockState, existing: &FileLockState) -> bool {
        if request.lock_owner == existing.lock_owner {
            return false;
        }
        if !FsCore::lock_ranges_overlap(request, existing) {
            return false;
        }
        request.typ == libc::F_WRLCK as u32 || existing.typ == libc::F_WRLCK as u32
    }

    /// Like commit_prepared_write but writes into an existing SurrealKV
    /// transaction instead of creating a new one. Allows batching multiple
    /// metadata commits into a single transaction.
    fn commit_prepared_write_in_txn(
        &self,
        ino: u64,
        inode: &mut InodeRecord,
        plan: &PreparedWritePlan,
        txn: &mut verfsnext_surrealkv::Transaction,
        mtime_sec: i64,
        mtime_nsec: u32,
    ) -> Result<()> {
        inode.size = inode.size.max(plan.write_end);
        inode.mtime_sec = mtime_sec;
        inode.mtime_nsec = mtime_nsec;
        inode.ctime_sec = mtime_sec;
        inode.ctime_nsec = mtime_nsec;

        for start in &plan.extent_deletes {
            txn.delete(extent_key(ino, *start))?;
        }
        for extent in &plan.extent_sets {
            txn.set(extent_key(ino, extent.start), encode_rkyv(&extent.record)?)?;
        }

        FsCore::apply_ref_deltas_in_txn(txn, &plan.ref_deltas, &plan.new_chunks)?;
        txn.set(inode_key(ino), encode_rkyv(inode)?)?;
        Ok(())
    }

    /// Stages all writes of one inode into the shared batch transaction and
    /// returns the chunk records it created.
    ///
    /// The inode and old extents are read through `txn`. Everything the plan
    /// depends on is therefore either part of the transaction snapshot or
    /// rewritten by it (inode key, touched extents, chunk refcounts), so a
    /// concurrent change makes the commit fail with a conflict instead of
    /// being silently overwritten.
    async fn apply_write_group_in_txn(
        &self,
        group: &WriteGroup,
        txn: &mut verfsnext_surrealkv::Transaction,
        mtime_sec: i64,
        mtime_nsec: u32,
    ) -> Result<HashMap<ChunkId, NewChunk>> {
        let mut inode = FsCore::load_inode_in_txn(txn, group.ino, "write")?;
        self.ensure_write_target_inode(group.ino, &inode)?;
        if group.segments.is_empty() {
            return Ok(HashMap::new());
        }
        let plan = self.prepare_write_plan(group, &inode, txn).await?;

        let inode_lock = self.inode_write_lock(group.ino);
        let _inode_guard = inode_lock.lock().await;
        self.commit_prepared_write_in_txn(group.ino, &mut inode, &plan, txn, mtime_sec, mtime_nsec)?;

        self.dedup_hits
            .fetch_add(plan.dedup_hits, Ordering::Relaxed);
        self.dedup_misses
            .fetch_add(plan.dedup_misses, Ordering::Relaxed);
        self.write_bytes_total
            .fetch_add(group.data_len(), Ordering::Relaxed);
        debug!(
            ino = group.ino,
            offset = group.first_offset(),
            bytes = group.data_len(),
            segments = group.segments.len(),
            dedup_hits = plan.dedup_hits,
            dedup_misses = plan.dedup_misses,
            "write completed through content-defined chunking, dedup and compression"
        );

        Ok(plan.new_chunks)
    }
}

#[async_trait]
impl WriteApply for FsCore {
    async fn apply_batch(&self, ops: Vec<WriteOp>) -> Vec<Result<()>> {
        let original_len = ops.len();
        if original_len == 0 {
            return Vec::new();
        }

        // A write is planned from the state its transaction sees, so all
        // writes of one inode are folded into a single group: two groups for
        // the same inode in one transaction would plan the same blocks twice.
        // One group per inode lets the whole batch share one commit and one
        // pack sync, whatever order the kernel sent the writes in.
        let mut groups = Vec::<WriteGroup>::new();
        let mut group_of_inode = HashMap::<u64, usize>::new();
        for (idx, op) in ops.into_iter().enumerate() {
            let position = *group_of_inode.entry(op.ino).or_insert_with(|| {
                groups.push(WriteGroup::new(op.ino));
                groups.len() - 1
            });
            groups[position].add(idx, op);
        }

        let mut out: Vec<Option<Result<()>>> = (0..original_len).map(|_| None).collect();
        self.apply_write_groups(groups, &mut out).await;

        debug!(
            dedup_hits = self.dedup_hits.load(Ordering::Relaxed),
            dedup_misses = self.dedup_misses.load(Ordering::Relaxed),
            "write batch applied"
        );
        out.into_iter()
            .map(|item| item.unwrap_or_else(|| Err(anyhow!("missing write result for queued op"))))
            .collect()
    }
}

impl FsCore {
    /// Stages and commits write groups (one per inode) in a single metadata
    /// transaction, filling `out` for every op.
    ///
    /// A group that fails to stage is reported as failed and the remaining
    /// groups are re-staged on a fresh transaction, so a write is acknowledged
    /// only when the transaction that contains it has committed. Commits that
    /// lose a race with a concurrent transaction are re-planned and retried.
    async fn apply_write_groups(
        &self,
        mut pending: Vec<WriteGroup>,
        out: &mut [Option<Result<()>>],
    ) {
        let mut attempt = 1;
        while !pending.is_empty() {
            // Namespace, snapshot and vault mutations hold the write side of this
            // gate. Holding the read side from begin to commit keeps them from
            // committing inside the transaction, which otherwise conflicts with
            // every retry while snapshot operations run back to back.
            let _mutation_guard = self.write_lock.read().await;
            let mut txn = match self.meta.begin_write() {
                Ok(txn) => txn,
                Err(err) => {
                    FsCore::fail_write_groups(&pending, &err, out);
                    return;
                }
            };

            // Compute the timestamp once per attempt and reuse it for all
            // inode updates. Avoids redundant clock_gettime syscalls.
            let (mtime_sec, mtime_nsec) = system_time_to_parts(SystemTime::now());
            let mut new_records = HashMap::<ChunkId, NewChunk>::new();
            let mut failed_group = None;
            for (position, group) in pending.iter().enumerate() {
                match self
                    .apply_write_group_in_txn(group, &mut txn, mtime_sec, mtime_nsec)
                    .await
                {
                    Ok(records) => new_records.extend(records),
                    Err(err) => {
                        failed_group = Some((position, err));
                        break;
                    }
                }
            }

            if let Some((position, err)) = failed_group {
                txn.rollback();
                let group = pending.remove(position);
                tracing::error!(
                    ino = group.ino,
                    offset = group.first_offset(),
                    bytes = group.data_len(),
                    segments = group.segments.len(),
                    error = %format!("{err:#}"),
                    "write group failed to stage; re-staging the rest of its batch"
                );
                let mut indices = group.indices.into_iter();
                if let Some(first) = indices.next() {
                    for idx in indices {
                        out[idx] = Some(Err(anyhow!("{err:#}")));
                    }
                    out[first] = Some(Err(err));
                }
                continue;
            }

            // No pack sync here: the metadata WAL syncs the packs before any
            // record reaches the OS (see MetaStore::open_with_pack_barrier).
            match self.meta.commit_write_txn(&mut txn).await {
                Ok(()) => {
                    // Caches are updated only after the commit so concurrent
                    // readers never observe uncommitted state.
                    self.publish_committed_chunk_records(&new_records);
                    for group in &pending {
                        self.invalidate_inode_cache(group.ino);
                        self.bump_inode_data_version(group.ino);
                        self.mark_mutation();
                        self.invalidate_inode_attr_best_effort(group.ino);
                        for idx in &group.indices {
                            out[*idx] = Some(Ok(()));
                        }
                    }
                    return;
                }
                Err(err) if attempt < MAX_COMMIT_ATTEMPTS && is_retryable_commit_error(&err) => {
                    tracing::warn!(attempt, error = %format!("{err:#}"), "retrying write batch after metadata conflict");
                    attempt += 1;
                    tokio::task::yield_now().await;
                }
                Err(err) => {
                    tracing::error!(error = %format!("{err:#}"), "write batch commit failed");
                    FsCore::fail_write_groups(&pending, &err, out);
                    return;
                }
            }
        }
    }

    fn fail_write_groups(
        groups: &[WriteGroup],
        err: &anyhow::Error,
        out: &mut [Option<Result<()>>],
    ) {
        for group in groups {
            for idx in &group.indices {
                out[*idx] = Some(Err(anyhow!("{err:#}")));
            }
        }
    }
}
