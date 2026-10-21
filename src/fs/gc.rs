use super::*;
use crate::fs::FsCore;
use crate::fs::chunk::StoredPayload;

enum RewriteStep {
    Rewritten(u64),
    Paused,
    None,
}

impl VerFs {
    pub async fn run_offline_gc_rebuild_discard(
        &self,
        run_rewrite_phase: bool,
    ) -> Result<OfflineGcReport> {
        let mut report = self.core.rebuild_discard_from_pack_state_offline().await?;
        report.rewrite_phase_requested = run_rewrite_phase;
        if run_rewrite_phase {
            let (rewrite_cycles, packs_rewritten) = self
                .core
                .run_gc_rewrite_phase_only_offline_until_stable()
                .await?;
            report.rewrite_cycles = rewrite_cycles;
            report.packs_rewritten = packs_rewritten;
        }
        Ok(report)
    }
}

impl FsCore {
    pub(crate) fn mark_activity(&self) {
        self.last_activity_ms.store(now_millis(), Ordering::Relaxed);
    }
    pub(crate) fn mark_mutation(&self) {
        let now = now_millis();
        self.last_mutation_ms.store(now, Ordering::Relaxed);
        self.last_activity_ms.store(now, Ordering::Relaxed);
    }
    pub(crate) fn gc_idle_window_open(&self) -> bool {
        let now = now_millis();
        let last_mutation = self.last_mutation_ms.load(Ordering::Relaxed);
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let last_event = last_mutation.max(last_activity);
        if now.saturating_sub(last_event) < self.config.gc_idle_min_ms {
            return false;
        }

        if let Ok(guard) = self.write_lock.try_write() {
            drop(guard);
            return true;
        }

        false
    }
    pub(crate) async fn maybe_run_gc(&self) -> Result<()> {
        if !self.gc_idle_window_open() {
            return Ok(());
        }

        let _gc_guard = self.gc_lock.lock().await;
        if !self.gc_idle_window_open() {
            return Ok(());
        }
        self.promote_inline_chunks().await?;
        self.run_gc_cycle().await
    }
    pub(crate) async fn run_gc_cycle(&self) -> Result<()> {
        let discard_path = self
            .config
            .packs_dir()
            .join(&self.config.gc_discard_filename);
        let current_discard_len = ensure_file(&discard_path)?;
        let current_checkpoint = self
            .meta
            .get_u64_sys("gc.discard_checkpoint")
            .unwrap_or(current_discard_len);
        if current_checkpoint == 0 {
            self.meta
                .write_txn(|txn| {
                    txn.set(
                        sys_key("gc.discard_checkpoint"),
                        current_discard_len.to_le_bytes().to_vec(),
                    )?;
                    Ok(())
                })
                .await?;
        }

        let phase = self.meta.get_u64_sys("gc.phase").unwrap_or(0);

        if phase == 0 {
            // == SCAN PHASE ==
            let (candidates, next_cursor) = self.collect_zero_ref_candidates()?;

            if !candidates.is_empty() {
                self.delete_zero_ref_candidates(&candidates).await?;
                for candidate in candidates.iter() {
                    self.chunk_meta_cache.invalidate(&candidate.id);
                    self.chunk_data_cache.invalidate(&candidate.id);
                }

                let epoch = self.next_gc_epoch().await?;
                let discard_records = candidates
                    .iter()
                    .filter(|candidate| candidate.pack_id != INLINE_PACK_ID)
                    .map(|candidate| DiscardRecord {
                        epoch_id: epoch,
                        pack_id: candidate.pack_id,
                        chunk_hash128: candidate.id.hash,
                        block_size_bytes: candidate.block_size_bytes,
                    })
                    .collect::<Vec<_>>();
                let appended_checkpoint = append_records(&discard_path, &discard_records)?;
                self.meta
                    .write_txn(|txn| {
                        txn.set(
                            sys_key("gc.discard_checkpoint"),
                            appended_checkpoint.to_le_bytes().to_vec(),
                        )?;
                        Ok(())
                    })
                    .await?;
            }

            self.meta
                .write_txn(|txn| {
                    if let Some(cursor) = &next_cursor {
                        txn.set(sys_key("gc.scan_cursor"), cursor.clone())?;
                    } else {
                        txn.delete(sys_key("gc.scan_cursor"))?;
                        txn.set(sys_key("gc.phase"), 1_u64.to_le_bytes().to_vec())?;
                    }
                    Ok(())
                })
                .await?;

            return Ok(());
        }

        // == REWRITE PHASE ==
        let checkpoint = self
            .meta
            .get_u64_sys("gc.discard_checkpoint")
            .unwrap_or(current_checkpoint.max(current_discard_len));
        let parsed = read_records(&discard_path, checkpoint)?;
        if parsed.records.is_empty() {
            self.switch_to_scan_phase().await?;
            return Ok(());
        }

        let mut by_pack = HashMap::<u64, HashMap<[u8; 16], DiscardRecord>>::new();
        for record in parsed.records {
            by_pack
                .entry(record.pack_id)
                .or_default()
                .insert(record.chunk_hash128, record);
        }

        let outcome = self.rewrite_one_eligible_pack(&by_pack, true).await?;
        let RewriteStep::Rewritten(rewritten_pack_id) = outcome else {
            if matches!(outcome, RewriteStep::Paused) {
                return Ok(());
            }
            self.switch_to_scan_phase().await?;
            return Ok(());
        };

        {
            let current = read_records(&discard_path, checkpoint)?;
            let kept = current
                .records
                .into_iter()
                .filter(|record| record.pack_id != rewritten_pack_id)
                .collect::<Vec<_>>();
            let new_len = rewrite_records(&discard_path, &kept)?;
            self.meta
                .write_txn(|txn| {
                    txn.set(
                        sys_key("gc.discard_checkpoint"),
                        new_len.to_le_bytes().to_vec(),
                    )?;
                    Ok(())
                })
                .await?;
        }

        Ok(())
    }
    pub(crate) async fn rebuild_discard_from_pack_state_offline(&self) -> Result<OfflineGcReport> {
        let _gc_guard = self.gc_lock.lock().await;

        let zero_ref_records_deleted = self.delete_all_zero_ref_records_offline().await?;

        let discard_path = self
            .config
            .packs_dir()
            .join(&self.config.gc_discard_filename);
        let mut checkpoint = rewrite_records(&discard_path, &[])?;
        let epoch = self.next_gc_epoch().await?;
        let mut report = OfflineGcReport {
            discard_checkpoint_bytes: checkpoint,
            zero_ref_records_deleted,
            ..OfflineGcReport::default()
        };

        for pack_id in self.packs.pack_ids()? {
            let entries = self.packs.read_index_entries(pack_id)?;
            report.packs_scanned = report.packs_scanned.saturating_add(1);
            report.pack_index_entries_scanned = report
                .pack_index_entries_scanned
                .saturating_add(entries.len() as u64);

            if entries.is_empty() {
                continue;
            }

            let mut pack_records = Vec::new();
            let mut seen_dead_hashes = HashSet::<[u8; 16]>::new();
            self.meta.read_txn(|txn| {
                for (hash, index_entry) in entries.iter() {
                    // Pack copies are indexed by hash alone: the copy is live
                    // when a record of either domain still points at this pack.
                    let mut is_live = false;
                    for id in [ChunkId::plain(*hash), ChunkId::vault(*hash)] {
                        let Some(raw) = txn.get(chunk_key(&id))? else {
                            continue;
                        };
                        let chunk: ChunkRecord = decode_rkyv(&raw)?;
                        if chunk.refcount > 0 && chunk.pack_id == pack_id {
                            is_live = true;
                            break;
                        }
                    }

                    if is_live {
                        report.live_entries_found = report.live_entries_found.saturating_add(1);
                        continue;
                    }

                    report.dead_entries_found = report.dead_entries_found.saturating_add(1);
                    if !seen_dead_hashes.insert(*hash) {
                        report.duplicate_dead_entries_collapsed =
                            report.duplicate_dead_entries_collapsed.saturating_add(1);
                        continue;
                    }

                    pack_records.push(DiscardRecord {
                        epoch_id: epoch,
                        pack_id,
                        chunk_hash128: *hash,
                        block_size_bytes: index_entry.compressed_len,
                    });
                }
                Ok(())
            })?;

            if pack_records.is_empty() {
                continue;
            }

            checkpoint = append_records(&discard_path, &pack_records)?;
            report.discard_records_written = report
                .discard_records_written
                .saturating_add(pack_records.len() as u64);
            report.discard_checkpoint_bytes = checkpoint;
        }

        self.meta
            .write_txn(|txn| {
                txn.set(
                    sys_key("gc.discard_checkpoint"),
                    checkpoint.to_le_bytes().to_vec(),
                )?;
                txn.delete(sys_key("gc.scan_cursor"))?;
                txn.set(sys_key("gc.phase"), 1_u64.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await?;

        Ok(report)
    }
    /// Deletes every chunk record whose refcount is 0, so the following
    /// discard rebuild and pack rewrite reclaim those chunks too. This is the
    /// work of the online scan phase, which the offline command skips. It is
    /// safe only because the daemon is stopped (the metadata store lock rules
    /// out a concurrent writer that could dedup against these chunks); each
    /// record is re-checked inside the deleting transaction.
    async fn delete_all_zero_ref_records_offline(&self) -> Result<u64> {
        const BATCH: usize = 50_000;
        let prefix = vec![crate::types::KEY_PREFIX_CHUNK];
        let end = prefix_end(&prefix);
        let mut start = prefix;
        let mut deleted = 0_u64;
        loop {
            let (ids, next_start) = self.meta.read_txn(|txn| {
                let (pairs, has_more) = scan_range_pairs_limited(txn, start.clone(), end.clone(), BATCH)?;
                let next_start = match (has_more, pairs.last()) {
                    (true, Some((key, _))) => {
                        let mut next = key.clone();
                        next.push(0);
                        Some(next)
                    }
                    _ => None,
                };
                let mut ids = Vec::new();
                for (key, value) in pairs {
                    let Some(id) = decode_chunk_key(&key) else {
                        continue;
                    };
                    let chunk: ChunkRecord = decode_rkyv(&value)?;
                    if chunk.refcount == 0 {
                        ids.push(id);
                    }
                }
                Ok((ids, next_start))
            })?;

            if !ids.is_empty() {
                let mut batch_deleted = 0_u64;
                self.meta
                    .write_txn(|txn| {
                        batch_deleted = 0;
                        for id in ids.iter() {
                            let key = chunk_key(id);
                            if let Some(raw) = txn.get(key.clone())? {
                                let chunk: ChunkRecord = decode_rkyv(&raw)?;
                                if chunk.refcount == 0 {
                                    if (chunk.flags & CHUNK_FLAG_INLINE) != 0 {
                                        txn.delete(inline_chunk_key(id))?;
                                    }
                                    txn.delete(key)?;
                                    batch_deleted += 1;
                                }
                            }
                        }
                        Ok(())
                    })
                    .await?;
                for id in ids.iter() {
                    self.chunk_meta_cache.invalidate(id);
                    self.chunk_data_cache.invalidate(id);
                }
                deleted += batch_deleted;
            }

            match next_start {
                Some(next) => start = next,
                None => return Ok(deleted),
            }
        }
    }
    pub(crate) async fn run_gc_rewrite_phase_only_offline_until_stable(
        &self,
    ) -> Result<(u64, u64)> {
        let _gc_guard = self.gc_lock.lock().await;
        self.meta
            .write_txn(|txn| {
                txn.set(sys_key("gc.phase"), 1_u64.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await?;

        let mut cycles = 0_u64;
        let mut packs_rewritten = 0_u64;
        loop {
            cycles = cycles.saturating_add(1);
            if self.run_gc_rewrite_phase_once_offline().await? {
                packs_rewritten = packs_rewritten.saturating_add(1);
                continue;
            }
            break;
        }

        Ok((cycles, packs_rewritten))
    }
    async fn run_gc_rewrite_phase_once_offline(&self) -> Result<bool> {
        let discard_path = self
            .config
            .packs_dir()
            .join(&self.config.gc_discard_filename);
        let current_discard_len = ensure_file(&discard_path)?;
        let checkpoint = self
            .meta
            .get_u64_sys("gc.discard_checkpoint")
            .unwrap_or(current_discard_len);
        let parsed = read_records(&discard_path, checkpoint)?;
        if parsed.records.is_empty() {
            self.switch_to_scan_phase().await?;
            return Ok(false);
        }

        let mut by_pack = HashMap::<u64, HashMap<[u8; 16], DiscardRecord>>::new();
        for record in parsed.records {
            by_pack
                .entry(record.pack_id)
                .or_default()
                .insert(record.chunk_hash128, record);
        }

        let outcome = self.rewrite_one_eligible_pack(&by_pack, false).await?;
        let RewriteStep::Rewritten(rewritten_pack_id) = outcome else {
            if matches!(outcome, RewriteStep::Paused) {
                return Ok(false);
            }
            self.switch_to_scan_phase().await?;
            return Ok(false);
        };

        {
            let current = read_records(&discard_path, checkpoint)?;
            let kept = current
                .records
                .into_iter()
                .filter(|record| record.pack_id != rewritten_pack_id)
                .collect::<Vec<_>>();
            let new_len = rewrite_records(&discard_path, &kept)?;
            self.meta
                .write_txn(|txn| {
                    txn.set(
                        sys_key("gc.discard_checkpoint"),
                        new_len.to_le_bytes().to_vec(),
                    )?;
                    Ok(())
                })
                .await?;
        }
        Ok(true)
    }
    /// One pack per call, the active pack included: it is compacted in place
    /// and keeps growing to its size limit.
    async fn rewrite_one_eligible_pack(
        &self,
        by_pack: &HashMap<u64, HashMap<[u8; 16], DiscardRecord>>,
        pause_when_active: bool,
    ) -> Result<RewriteStep> {
        for (pack_id, dead_records) in by_pack {
            if !self.pack_reclaim_eligible(*pack_id, dead_records)? {
                continue;
            }
            if pause_when_active && !self.gc_idle_window_open() {
                return Ok(RewriteStep::Paused);
            }
            self.rewrite_pack_durably(*pack_id).await?;
            return Ok(RewriteStep::Rewritten(*pack_id));
        }
        Ok(RewriteStep::None)
    }
    fn pack_reclaim_eligible(
        &self,
        pack_id: u64,
        dead_records: &HashMap<[u8; 16], DiscardRecord>,
    ) -> Result<bool> {
        let reclaim_bytes = dead_records
            .values()
            .map(|record| record.block_size_bytes as u64)
            .sum::<u64>();
        let pack_size = self.packs.pack_size(pack_id)?;
        if pack_size == 0 {
            return Ok(false);
        }
        let reclaim_percent = (reclaim_bytes as f64 / pack_size as f64) * 100.0;
        let meets_bytes = reclaim_bytes >= self.config.gc_pack_rewrite_min_reclaim_bytes;
        let meets_percent = reclaim_percent >= self.config.gc_pack_rewrite_min_reclaim_percent;
        Ok(meets_bytes || meets_percent)
    }
    /// Drops the dead records of `pack_id`.
    ///
    /// In-flight writes are drained first, so every record already in the
    /// pack has its metadata committed before the live hashes are read; a
    /// write that appended to this pack just before it rotated is covered the
    /// same way. For the active pack this also fixes the cutoff below which
    /// records are filtered; later appends are carried over unchanged.
    ///
    /// Every metadata commit up to now (unlinks, truncates, the scan-phase
    /// deletes) is made durable before the copy: otherwise a power loss could
    /// roll metadata back to a state that still references chunks the
    /// rewritten pack no longer holds. Copying a pack is long disk work, so it
    /// runs off the async workers, and readers keep using the current pack
    /// until the short swap at the end.
    async fn rewrite_pack_durably(&self, pack_id: u64) -> Result<()> {
        let cutoff = {
            let _guard = self.write_lock.write().await;
            self.packs.active_pack_cutoff(pack_id)?
        };
        tokio::task::block_in_place(|| {
            self.meta.flush_wal(true)?;
            let live_hashes = self.live_hashes_for_pack(pack_id)?;
            self.packs
                .rewrite_pack_with_live_hashes(pack_id, &live_hashes, cutoff)
        })
    }
    pub(crate) async fn switch_to_scan_phase(&self) -> Result<()> {
        self.meta
            .write_txn(|txn| {
                txn.set(sys_key("gc.phase"), 0_u64.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await
    }
    pub(crate) fn collect_zero_ref_candidates(
        &self,
    ) -> Result<(Vec<ZeroRefCandidate>, Option<Vec<u8>>)> {
        self.meta.read_txn(|txn| {
            let mut out = Vec::new();
            let prefix = vec![crate::types::KEY_PREFIX_CHUNK];
            let end = prefix_end(&prefix);

            let mut start = prefix.clone();
            if let Ok(Some(cursor)) = txn.get(sys_key("gc.scan_cursor")) {
                if !cursor.is_empty() {
                    start = cursor;
                }
            }

            let mut next_cursor = None;
            let (pairs, has_more) = scan_range_pairs_limited(txn, start, end, 50_000)?;

            for (key, value) in pairs {
                if has_more {
                    let mut next = key.clone();
                    next.push(0);
                    next_cursor = Some(next);
                }

                let Some(id) = decode_chunk_key(&key) else {
                    continue;
                };
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                if chunk.refcount == 0 {
                    out.push(ZeroRefCandidate {
                        id,
                        pack_id: chunk.pack_id,
                        block_size_bytes: chunk.compressed_len,
                    });
                }
            }
            Ok((out, next_cursor))
        })
    }
    pub(crate) async fn delete_zero_ref_candidates(
        &self,
        candidates: &[ZeroRefCandidate],
    ) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        let ids = candidates
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>();
        self.meta
            .write_txn(|txn| {
                for id in ids.iter() {
                    let key = chunk_key(id);
                    if let Some(raw) = txn.get(key.clone())? {
                        let chunk: ChunkRecord = decode_rkyv(&raw)?;
                        if chunk.refcount == 0 {
                            if (chunk.flags & CHUNK_FLAG_INLINE) != 0 {
                                txn.delete(inline_chunk_key(id))?;
                            }
                            txn.delete(key)?;
                        }
                    }
                }
                Ok(())
            })
            .await
    }
    pub(crate) async fn next_gc_epoch(&self) -> Result<u64> {
        let current = self.meta.get_u64_sys("gc.epoch").unwrap_or(0);
        let next = current.saturating_add(1);
        self.meta
            .write_txn(|txn| {
                txn.set(sys_key("gc.epoch"), next.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await?;
        Ok(next)
    }
    pub(crate) async fn persist_active_pack_id_if_needed(&self) -> Result<()> {
        let active_pack_id = self.packs.active_pack_id();
        let last_persisted = self.last_persisted_active_pack_id.load(Ordering::Relaxed);
        if active_pack_id == last_persisted {
            return Ok(());
        }
        self.meta
            .write_txn(|txn| {
                txn.set(
                    sys_key("active_pack_id"),
                    active_pack_id.to_le_bytes().to_vec(),
                )?;
                Ok(())
            })
            .await?;
        self.last_persisted_active_pack_id
            .store(active_pack_id, Ordering::Relaxed);
        Ok(())
    }
    pub(crate) async fn persist_pack_crc32_read_error_count_if_needed(&self) -> Result<()> {
        let current = self.pack_crc32_read_error_count.load(Ordering::Relaxed);
        let last_persisted = self
            .last_persisted_pack_crc32_read_error_count
            .load(Ordering::Relaxed);
        if current == last_persisted {
            return Ok(());
        }

        self.meta
            .write_txn(|txn| {
                txn.set(
                    sys_key(crate::migration::pack_index_crc32::SYS_PACK_CRC32_READ_ERRORS),
                    current.to_le_bytes().to_vec(),
                )?;
                Ok(())
            })
            .await?;
        self.last_persisted_pack_crc32_read_error_count
            .store(current, Ordering::Relaxed);
        Ok(())
    }
    /// Moves inline chunks that survived since the previous pass into the
    /// active pack: they are no longer the open end of a file being written.
    /// Chunks created since the previous pass stay inline for now. Each batch
    /// is appended to the pack and committed under the read side of the
    /// write lock, like a write, so GC's pack cutoff stays valid; a record
    /// that changed meanwhile is left alone and its pack copy becomes garbage.
    pub(crate) async fn promote_inline_chunks(&self) -> Result<()> {
        const BATCH: usize = 256;
        let young = std::mem::take(&mut *self.young_inline_chunks.lock());
        let prefix = vec![KEY_PREFIX_INLINE_CHUNK];
        let end = prefix_end(&prefix);
        let mut start = prefix;
        let mut promoted = 0_u64;
        loop {
            let (batch, next_start) = self.meta.read_txn(|txn| {
                let (pairs, has_more) =
                    scan_range_pairs_limited(txn, start.clone(), end.clone(), BATCH)?;
                let next_start = match (has_more, pairs.last()) {
                    (true, Some((key, _))) => {
                        let mut next = key.clone();
                        next.push(0);
                        Some(next)
                    }
                    _ => None,
                };
                let mut batch = Vec::new();
                for (key, payload) in pairs {
                    let Some(id) = decode_inline_chunk_key(&key) else {
                        return Err(anyhow_errno(
                            Errno::EIO,
                            format!("malformed inline chunk key {key:?}"),
                        ));
                    };
                    if young.contains(&id) {
                        continue;
                    }
                    let Some(raw) = txn.get(chunk_key(&id))? else {
                        return Err(anyhow_errno(
                            Errno::EIO,
                            format!("inline payload of chunk {:x?} has no chunk record", id.hash),
                        ));
                    };
                    let chunk: ChunkRecord = decode_rkyv(&raw)?;
                    // Zero-ref inline chunks are deleted by the scan phase.
                    if chunk.refcount == 0 {
                        continue;
                    }
                    if !FsCore::chunk_record_matches_domain(&id, &chunk) {
                        tracing::error!(
                            chunk_hash = ?id.hash,
                            chunk_vault = id.vault,
                            "inline chunk record does not belong to its domain key; leaving it for the records check"
                        );
                        continue;
                    }
                    batch.push((id, chunk, payload));
                }
                Ok((batch, next_start))
            })?;

            if !batch.is_empty() {
                let _mutation_guard = self.write_lock.read().await;
                let mut moved = Vec::with_capacity(batch.len());
                for (id, inline, payload) in batch {
                    // Vault payloads are already encrypted and are moved as
                    // they are; plain payloads are compressed on the way.
                    let (codec, stored, payload_crc32) = if id.vault {
                        let payload_crc32 = crc32c::crc32c(&payload);
                        (inline.codec, Arc::new(payload), payload_crc32)
                    } else {
                        let compressed = StoredPayload::new(
                            &Arc::new(payload),
                            None,
                            self.config.zstd_compression_level,
                        )?;
                        (compressed.codec, compressed.bytes, compressed.crc32)
                    };
                    let pack_id = tokio::task::block_in_place(|| {
                        self.packs.append_chunk_with_crc32(
                            id.hash,
                            codec,
                            inline.uncompressed_len,
                            &stored,
                            payload_crc32,
                        )
                    })?;
                    moved.push((
                        id,
                        inline,
                        ChunkRecord {
                            refcount: 0,
                            pack_id,
                            codec,
                            flags: inline.flags & !CHUNK_FLAG_INLINE,
                            nonce: inline.nonce,
                            uncompressed_len: inline.uncompressed_len,
                            compressed_len: stored.len() as u32,
                        },
                    ));
                }
                let mut committed = 0_u64;
                self.meta
                    .write_txn(|txn| {
                        committed = 0;
                        for (id, inline, packed) in &moved {
                            let key = chunk_key(id);
                            let Some(raw) = txn.get(key.clone())? else {
                                continue;
                            };
                            let current: ChunkRecord = decode_rkyv(&raw)?;
                            if (current.flags & CHUNK_FLAG_INLINE) == 0
                                || current.nonce != inline.nonce
                                || current.compressed_len != inline.compressed_len
                            {
                                continue;
                            }
                            txn.set(
                                key,
                                encode_rkyv(&ChunkRecord {
                                    refcount: current.refcount,
                                    ..*packed
                                })?,
                            )?;
                            txn.delete(inline_chunk_key(id))?;
                            committed += 1;
                        }
                        Ok(())
                    })
                    .await?;
                for (id, _, _) in &moved {
                    self.chunk_meta_cache.invalidate(id);
                }
                promoted += committed;
            }

            match next_start {
                Some(next) => start = next,
                None => break,
            }
        }
        if promoted > 0 {
            tracing::info!(promoted, "moved inline chunks to the active pack");
        }
        Ok(())
    }
    /// Persists the chunks found damaged on read as `CHUNK_FLAG_CORRUPT`, so
    /// the next write of their content stores a fresh copy even after a
    /// restart. A mark is written only while the chunk is still in the
    /// in-memory set: a write that repointed it meanwhile removed it.
    pub(crate) async fn persist_corrupt_chunk_marks(&self) -> Result<()> {
        let pending = self.corrupt_chunks.lock().iter().copied().collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }
        self.meta
            .write_txn(|txn| {
                for id in &pending {
                    if !self.corrupt_chunks.lock().contains(id) {
                        continue;
                    }
                    let key = chunk_key(id);
                    let Some(raw) = txn.get(key.clone())? else {
                        continue;
                    };
                    let mut chunk: ChunkRecord = decode_rkyv(&raw)?;
                    if (chunk.flags & CHUNK_FLAG_CORRUPT) != 0 {
                        continue;
                    }
                    chunk.flags |= CHUNK_FLAG_CORRUPT;
                    txn.set(key, encode_rkyv(&chunk)?)?;
                }
                Ok(())
            })
            .await?;
        let mut corrupt = self.corrupt_chunks.lock();
        for id in &pending {
            corrupt.remove(id);
            self.chunk_meta_cache.invalidate(id);
        }
        Ok(())
    }
    /// Hashes whose chunk record still points at `pack_id`, whatever their
    /// refcount or domain. Pack copies are indexed by hash alone, so a hash
    /// that is live in either domain keeps every indexed copy.
    pub(crate) fn live_hashes_for_pack(&self, pack_id: u64) -> Result<HashSet<[u8; 16]>> {
        self.meta.read_txn(|txn| {
            let mut out = HashSet::new();
            let prefix = vec![crate::types::KEY_PREFIX_CHUNK];
            let end = prefix_end(&prefix);
            for pair in scan_range_pairs(txn, prefix, end)? {
                let (key, value) = pair?;
                let Some(id) = decode_chunk_key(&key) else {
                    continue;
                };
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                // Zero-ref records are kept: a writer can still dedup against a
                // chunk until the scan phase deletes its record, so only chunks
                // without metadata are safe to drop.
                if chunk.pack_id != pack_id {
                    continue;
                }
                out.insert(id.hash);
            }
            Ok(out)
        })
    }
}
