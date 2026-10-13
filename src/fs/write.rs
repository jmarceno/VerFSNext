use super::*;
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

struct PreparedWritePlan {
    write_end: u64,
    dedup_hits: u64,
    dedup_misses: u64,
    extent_updates: Vec<(u64, [u8; 16])>,
    ref_deltas: HashMap<[u8; 16], i64>,
    new_chunk_records: HashMap<[u8; 16], ChunkRecord>,
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
    async fn prepare_write_plan(
        &self,
        group: &WriteGroup,
        inode: &InodeRecord,
        txn: &verfsnext_surrealkv::Transaction,
    ) -> Result<PreparedWritePlan> {
        let block_size = BLOCK_SIZE as u64;
        let mut write_end = 0_u64;
        // Blocks touched by the group in ascending order, each with the
        // segments that overlap it. Segments are sorted and disjoint, so two
        // segments can only share the last block of the first one.
        let mut touched_blocks = Vec::<(u64, Vec<usize>)>::new();
        for (segment_idx, segment) in group.segments.iter().enumerate() {
            let segment_end = segment
                .offset
                .checked_add(segment.data.len() as u64)
                .ok_or_else(|| anyhow_errno(Errno::EOVERFLOW, "write offset overflow"))?;
            write_end = write_end.max(segment_end);
            for block_idx in segment.offset / block_size..=(segment_end - 1) / block_size {
                match touched_blocks.last_mut() {
                    Some((last, segments)) if *last == block_idx => segments.push(segment_idx),
                    _ => touched_blocks.push((block_idx, vec![segment_idx])),
                }
            }
        }

        // Fast path: files with size == 0 have no extents. Skip the KV range scan.
        // This applies to newly created files and files truncated to zero.
        let mut old_extents = HashMap::<u64, ExtentRecord>::new();
        if inode.size != 0 {
            for segment in &group.segments {
                let start_block = segment.offset / block_size;
                let end_block = (segment.offset + segment.data.len() as u64 - 1) / block_size;
                let start_key = extent_key(group.ino, start_block);
                let end_key = end_block
                    .checked_add(1)
                    .map(|end| extent_key(group.ino, end))
                    .unwrap_or_else(|| prefix_end(&extent_prefix(group.ino)));

                let mut iter = txn.range(start_key, end_key)?;
                let mut valid = iter.seek_first()?;
                while valid {
                    let key = iter.key().user_key();
                    if let Some(block_idx) = FsCore::extent_block_idx_from_key(key) {
                        if block_idx >= start_block && block_idx <= end_block {
                            let val = iter.value()?;
                            let extent: ExtentRecord = decode_rkyv(&val)?;
                            old_extents.insert(block_idx, extent);
                        }
                    }
                    valid = iter.next()?;
                }
            }
        }

        self.prefetch_chunk_meta(old_extents.values().map(|e| e.chunk_hash))?;

        let mut extent_updates = Vec::<(u64, [u8; 16])>::with_capacity(touched_blocks.len());
        let mut ref_deltas =
            HashMap::<[u8; 16], i64>::with_capacity(touched_blocks.len().saturating_mul(2));
        let mut pending_chunks = HashMap::<[u8; 16], Vec<u8>>::new();
        let mut checked_hashes = HashSet::<[u8; 16]>::new();
        let mut dedup_hits = 0_u64;
        let mut dedup_misses = 0_u64;

        for (block_idx, segment_indices) in touched_blocks {
            let block_start = block_idx * block_size;
            let block_end = block_start + block_size;

            // Fast path: a segment that covers the whole block replaces it, so
            // the old block is never read and the write slice is hashed as is.
            let full_cover = match segment_indices.as_slice() {
                [only] => {
                    let segment = &group.segments[*only];
                    let segment_end = segment.offset + segment.data.len() as u64;
                    (segment.offset <= block_start && segment_end >= block_end).then(|| {
                        let src_start = (block_start - segment.offset) as usize;
                        &segment.data[src_start..src_start + BLOCK_SIZE]
                    })
                }
                _ => None,
            };
            let (new_hash, block_data) = if let Some(write_slice) = full_cover {
                let hash = FsCore::hash_for_inode(inode, write_slice);
                (hash, write_slice.to_vec())
            } else {
                let mut block_data = if let Some(extent) = old_extents.get(&block_idx) {
                    self.read_extent_bytes(*extent, FsCore::inode_is_vault(inode))?
                } else {
                    vec![0_u8; BLOCK_SIZE]
                };
                for segment_idx in segment_indices {
                    let segment = &group.segments[segment_idx];
                    let segment_end = segment.offset + segment.data.len() as u64;
                    let copy_from = segment.offset.max(block_start);
                    let copy_until = segment_end.min(block_end);
                    let src_start = (copy_from - segment.offset) as usize;
                    let src_end = (copy_until - segment.offset) as usize;
                    let dst_start = (copy_from - block_start) as usize;
                    block_data[dst_start..dst_start + (src_end - src_start)]
                        .copy_from_slice(&segment.data[src_start..src_end]);
                }
                let hash = FsCore::hash_for_inode(inode, &block_data);
                (hash, block_data)
            };
            let old_hash = old_extents.get(&block_idx).map(|extent| extent.chunk_hash);

            if old_hash == Some(new_hash) {
                continue;
            }

            extent_updates.push((block_idx, new_hash));
            *ref_deltas.entry(new_hash).or_insert(0) += 1;
            if let Some(old_hash) = old_hash {
                *ref_deltas.entry(old_hash).or_insert(0) -= 1;
            }

            let staged_new = self.stage_chunk_if_missing(
                txn,
                new_hash,
                &block_data,
                &mut checked_hashes,
                &mut pending_chunks,
            )?;
            if staged_new {
                dedup_misses = dedup_misses.saturating_add(1);
            } else {
                dedup_hits = dedup_hits.saturating_add(1);
            }
        }

        let new_chunk_records = self
            .materialize_pending_chunks(pending_chunks, FsCore::inode_is_vault(inode))
            .await?;

        Ok(PreparedWritePlan {
            write_end,
            dedup_hits,
            dedup_misses,
            extent_updates,
            ref_deltas,
            new_chunk_records,
        })
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
            let Some(new_chunk_records) = staged else {
                return Ok(inode);
            };
            match self.meta.commit_write_txn(&mut txn).await {
                Ok(()) => {
                    for (hash, record) in &new_chunk_records {
                        self.chunk_meta_cache.insert(*hash, *record);
                    }
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
    ) -> Result<(InodeRecord, Option<HashMap<[u8; 16], ChunkRecord>>)> {
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
        let mut extent_updates = Vec::<(u64, [u8; 16])>::new();
        let mut extent_deletes = Vec::<Vec<u8>>::new();
        let mut ref_deltas = HashMap::<[u8; 16], i64>::new();
        let mut pending_chunks = HashMap::<[u8; 16], Vec<u8>>::new();
        let mut checked_hashes = HashSet::<[u8; 16]>::new();

        if new_size < old_size {
            let block_size = BLOCK_SIZE as u64;
            let has_boundary = new_size > 0 && !new_size.is_multiple_of(block_size);
            let boundary_block = new_size / block_size;
            let keep_block_start = if has_boundary {
                boundary_block + 1
            } else {
                new_size.div_ceil(block_size)
            };

            let extents = {
                let prefix = extent_prefix(ino);
                let end = prefix_end(&prefix);
                let mut out = Vec::<(u64, [u8; 16], Vec<u8>)>::new();
                for pair in scan_range_pairs(txn, prefix, end)? {
                    let (key, value) = pair?;
                    let Some(block_idx) = FsCore::extent_block_idx_from_key(&key) else {
                        continue;
                    };
                    let extent: ExtentRecord = decode_rkyv(&value)?;
                    out.push((block_idx, extent.chunk_hash, key));
                }
                out
            };

            self.prefetch_chunk_meta(extents.iter().map(|(_, hash, _)| *hash))?;

            for (block_idx, chunk_hash, key) in extents {
                if has_boundary && block_idx == boundary_block {
                    let mut block_data = self.read_extent_bytes(
                        ExtentRecord { chunk_hash },
                        FsCore::inode_is_vault(&inode),
                    )?;
                    let offset_in_block = (new_size % block_size) as usize;
                    block_data[offset_in_block..].fill(0);
                    let new_hash = FsCore::hash_for_inode(&inode, &block_data);
                    if new_hash != chunk_hash {
                        extent_updates.push((block_idx, new_hash));
                        *ref_deltas.entry(chunk_hash).or_insert(0) -= 1;
                        *ref_deltas.entry(new_hash).or_insert(0) += 1;
                        self.stage_chunk_if_missing(
                            txn,
                            new_hash,
                            &block_data,
                            &mut checked_hashes,
                            &mut pending_chunks,
                        )?;
                    }
                    continue;
                }

                if block_idx >= keep_block_start {
                    extent_deletes.push(key);
                    *ref_deltas.entry(chunk_hash).or_insert(0) -= 1;
                }
            }
        }
        let new_chunk_records = self
            .materialize_pending_chunks(pending_chunks, FsCore::inode_is_vault(&inode))
            .await?;

        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        inode.size = new_size;
        inode.mtime_sec = sec;
        inode.mtime_nsec = nsec;
        inode.ctime_sec = sec;
        inode.ctime_nsec = nsec;

        for key in extent_deletes {
            txn.delete(key)?;
        }
        for (block_idx, hash) in extent_updates {
            txn.set(
                extent_key(ino, block_idx),
                encode_rkyv(&ExtentRecord { chunk_hash: hash })?,
            )?;
        }
        FsCore::apply_ref_deltas_in_txn(txn, &ref_deltas, &new_chunk_records)?;
        txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
        Ok((inode, Some(new_chunk_records)))
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

        for (block_idx, hash) in plan.extent_updates.iter().copied() {
            let extent = ExtentRecord { chunk_hash: hash };
            txn.set(extent_key(ino, block_idx), encode_rkyv(&extent)?)?;
        }

        FsCore::apply_ref_deltas_in_txn(txn, &plan.ref_deltas, &plan.new_chunk_records)?;
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
    ) -> Result<HashMap<[u8; 16], ChunkRecord>> {
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
            "write completed through streaming dedup/compress pipeline"
        );

        Ok(plan.new_chunk_records)
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
            let mut new_records = HashMap::<[u8; 16], ChunkRecord>::new();
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
                    for (hash, record) in &new_records {
                        self.chunk_meta_cache.insert(*hash, *record);
                    }
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
