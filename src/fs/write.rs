use super::*;
use crate::fs::FsCore;
use futures::stream::{FuturesUnordered, StreamExt};

struct PreparedWritePlan {
    write_end: u64,
    cdc_chunk_count: usize,
    dedup_hits: u64,
    dedup_misses: u64,
    extent_updates: Vec<(u64, [u8; 16])>,
    ref_deltas: HashMap<[u8; 16], i64>,
    new_chunk_records: HashMap<[u8; 16], ChunkRecord>,
}

impl FsCore {
    pub(crate) async fn apply_single_write(&self, op: WriteOp) -> Result<()> {
        let prepared_version = self.inode_data_version(op.ino).await;
        let inode_snapshot = self.load_inode_or_errno(op.ino, "write")?;
        self.ensure_write_target_inode(op.ino, &inode_snapshot)?;
        if op.data.is_empty() {
            return Ok(());
        }
        let mut plan = self.prepare_write_plan(&op, &inode_snapshot).await?;

        let _mutation_guard = self.write_lock.read().await;
        let inode_lock = self.inode_write_lock(op.ino).await;
        let _inode_guard = inode_lock.lock().await;

        let mut inode = self.load_inode_or_errno(op.ino, "write")?;
        self.ensure_write_target_inode(op.ino, &inode)?;
        let current_version = self.inode_data_version(op.ino).await;
        if current_version != prepared_version {
            // The inode data changed while this write was being prepared (e.g., truncate).
            // Re-prepare against the latest state while we hold the inode lock.
            plan = self.prepare_write_plan(&op, &inode).await?;
        }

        self.commit_prepared_write(op.ino, &mut inode, &plan)
            .await?;
        self.bump_inode_data_version(op.ino).await;
        self.mark_mutation();

        self.dedup_hits
            .fetch_add(plan.dedup_hits, Ordering::Relaxed);
        self.dedup_misses
            .fetch_add(plan.dedup_misses, Ordering::Relaxed);
        self.write_bytes_total
            .fetch_add(op.data.len() as u64, Ordering::Relaxed);
        debug!(
            ino = op.ino,
            offset = op.offset,
            bytes = op.data.len(),
            cdc_chunks = plan.cdc_chunk_count,
            dedup_hits = plan.dedup_hits,
            dedup_misses = plan.dedup_misses,
            "write completed through streaming dedup/compress pipeline"
        );
        Ok(())
    }
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
        op: &WriteOp,
        inode: &InodeRecord,
    ) -> Result<PreparedWritePlan> {
        let write_end = op
            .offset
            .checked_add(op.data.len() as u64)
            .ok_or_else(|| anyhow_errno(Errno::EOVERFLOW, "write offset overflow"))?;
        let cdc_chunk_count = self.ultracdc_chunk_count(&op.data);
        let start_block = op.offset / BLOCK_SIZE as u64;
        let end_block = (write_end - 1) / BLOCK_SIZE as u64;

        let old_extents = self.meta.read_txn(|txn| {
            let mut map = HashMap::<u64, ExtentRecord>::new();
            let start_key = extent_key(op.ino, start_block);
            let end_key = end_block
                .checked_add(1)
                .map(|end| extent_key(op.ino, end))
                .unwrap_or_else(|| prefix_end(&extent_prefix(op.ino)));

            let mut iter = txn.range(start_key, end_key)?;
            let mut valid = iter.seek_first()?;
            while valid {
                let key = iter.key().user_key();
                if let Some(block_idx) = FsCore::extent_block_idx_from_key(key) {
                    if block_idx >= start_block && block_idx <= end_block {
                        let val = iter.value()?;
                        let extent: ExtentRecord = decode_rkyv(&val)?;
                        map.insert(block_idx, extent);
                    }
                }
                valid = iter.next()?;
            }
            Ok(map)
        })?;

        self.prefetch_chunk_meta(old_extents.values().map(|e| e.chunk_hash))?;

        let mut extent_updates = Vec::<(u64, [u8; 16])>::new();
        let mut ref_deltas = HashMap::<[u8; 16], i64>::new();
        let mut pending_chunks = HashMap::<[u8; 16], Vec<u8>>::new();
        let mut checked_hashes = HashSet::<[u8; 16]>::new();
        let mut dedup_hits = 0_u64;
        let mut dedup_misses = 0_u64;

        for block_idx in start_block..=end_block {
            let mut block_data = if let Some(extent) = old_extents.get(&block_idx) {
                self.read_extent_bytes(extent.clone(), FsCore::inode_is_vault(inode))?
            } else {
                vec![0_u8; BLOCK_SIZE]
            };

            let block_start = block_idx * BLOCK_SIZE as u64;
            let copy_from = op.offset.max(block_start);
            let copy_until = write_end.min(block_start + BLOCK_SIZE as u64);
            let src_start = (copy_from - op.offset) as usize;
            let src_end = (copy_until - op.offset) as usize;
            let dst_start = (copy_from - block_start) as usize;
            let dst_end = dst_start + (src_end - src_start);
            block_data[dst_start..dst_end].copy_from_slice(&op.data[src_start..src_end]);

            let new_hash = FsCore::hash_for_inode(inode, &block_data);
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
            cdc_chunk_count,
            dedup_hits,
            dedup_misses,
            extent_updates,
            ref_deltas,
            new_chunk_records,
        })
    }
    async fn commit_prepared_write(
        &self,
        ino: u64,
        inode: &mut InodeRecord,
        plan: &PreparedWritePlan,
    ) -> Result<()> {
        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        inode.size = inode.size.max(plan.write_end);
        inode.mtime_sec = sec;
        inode.mtime_nsec = nsec;
        inode.ctime_sec = sec;
        inode.ctime_nsec = nsec;

        self.meta
            .write_txn(|txn| {
                for (block_idx, hash) in plan.extent_updates.iter().copied() {
                    let extent = ExtentRecord { chunk_hash: hash };
                    txn.set(extent_key(ino, block_idx), encode_rkyv(&extent)?)?;
                }

                FsCore::apply_ref_deltas_in_txn(txn, &plan.ref_deltas, &plan.new_chunk_records)?;
                txn.set(inode_key(ino), encode_rkyv(inode)?)?;
                Ok(())
            })
            .await
    }
    pub(crate) async fn truncate_file_locked(
        &self,
        ino: u64,
        new_size: u64,
    ) -> Result<InodeRecord> {
        let mut inode = self.load_inode_or_errno(ino, "truncate")?;
        self.ensure_inode_vault_access(&inode, "truncate")?;
        if inode.kind != INODE_KIND_FILE {
            return Err(anyhow_errno(
                Errno::EISDIR,
                format!("truncate inode {} is not a regular file", ino),
            ));
        }
        FsCore::ensure_inode_writable(&inode, "truncate")?;

        if new_size == inode.size {
            return Ok(inode);
        }

        let old_size = inode.size;
        let mut extent_updates = Vec::<(u64, [u8; 16])>::new();
        let mut extent_deletes = Vec::<Vec<u8>>::new();
        let mut ref_deltas = HashMap::<[u8; 16], i64>::new();
        let mut pending_chunks = HashMap::<[u8; 16], Vec<u8>>::new();
        let mut checked_hashes = HashSet::<[u8; 16]>::new();

        if new_size < old_size {
            let block_size = BLOCK_SIZE as u64;
            let has_boundary = new_size > 0 && (new_size % block_size) != 0;
            let boundary_block = new_size / block_size;
            let keep_block_start = if has_boundary {
                boundary_block + 1
            } else {
                new_size.div_ceil(block_size)
            };

            let extents = self.meta.read_txn(|txn| {
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
                Ok(out)
            })?;

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

        self.meta
            .write_txn(|txn| {
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
                Ok(())
            })
            .await?;
        self.mark_mutation();
        self.bump_inode_data_version(ino).await;

        Ok(inode)
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
}

#[async_trait]
impl WriteApply for FsCore {
    async fn apply_batch(&self, ops: Vec<WriteOp>) -> Vec<Result<()>> {
        let original_len = ops.len();
        if original_len == 0 {
            return Vec::new();
        }

        let mut groups: Vec<(WriteOp, Vec<usize>)> = Vec::new();
        for (idx, op) in ops.into_iter().enumerate() {
            if let Some((group_op, group_indices)) = groups.last_mut() {
                if can_coalesce_write(group_op, &op) {
                    merge_write_op(group_op, op);
                    group_indices.push(idx);
                    continue;
                }
            }
            groups.push((op, vec![idx]));
        }

        let mut by_inode = HashMap::<u64, Vec<(WriteOp, Vec<usize>)>>::new();
        for (group_op, indices) in groups {
            by_inode
                .entry(group_op.ino)
                .or_default()
                .push((group_op, indices));
        }

        let mut out: Vec<Option<Result<()>>> = (0..original_len).map(|_| None).collect();
        let mut pending = FuturesUnordered::new();
        for inode_groups in by_inode.into_values() {
            pending.push(async move {
                let mut inode_results = Vec::with_capacity(inode_groups.len());
                for (group_op, indices) in inode_groups {
                    let group_result = self.apply_single_write(group_op).await;
                    inode_results.push((indices, group_result));
                }
                inode_results
            });
        }

        while let Some(inode_results) = pending.next().await {
            for (indices, group_result) in inode_results {
                for idx in indices {
                    out[idx] = Some(match &group_result {
                        Ok(()) => Ok(()),
                        Err(err) => Err(anyhow!(err.to_string())),
                    });
                }
            }
        }

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
