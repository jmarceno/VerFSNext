use super::*;
use crate::fs::FsCore;

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

        if let Ok(guard) = self.write_lock.try_lock() {
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
                    self.chunk_meta_cache.invalidate(&candidate.hash);
                    self.chunk_data_cache.invalidate(&candidate.hash);
                }

                let epoch = self.next_gc_epoch().await?;
                let discard_records = candidates
                    .iter()
                    .map(|candidate| DiscardRecord {
                        epoch_id: epoch,
                        pack_id: candidate.pack_id,
                        chunk_hash128: candidate.hash,
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
                    if let Some(cursor) = next_cursor {
                        txn.set(sys_key("gc.scan_cursor"), cursor)?;
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

        let active_pack_id = self.packs.active_pack_id();
        let mut rewritten_pack: Option<u64> = None;

        for (pack_id, dead_records) in by_pack.iter() {
            if *pack_id == active_pack_id {
                continue;
            }
            let reclaim_bytes = dead_records
                .values()
                .map(|record| record.block_size_bytes as u64)
                .sum::<u64>();

            let pack_size = self.packs.pack_size(*pack_id)?;
            if pack_size == 0 {
                continue;
            }
            let reclaim_percent = (reclaim_bytes as f64 / pack_size as f64) * 100.0;
            let meets_bytes = reclaim_bytes >= self.config.gc_pack_rewrite_min_reclaim_bytes;
            let meets_percent = reclaim_percent >= self.config.gc_pack_rewrite_min_reclaim_percent;
            if meets_bytes || meets_percent {
                let live_hashes = self.live_hashes_for_pack(*pack_id)?;
                if !self.gc_idle_window_open() {
                    return Ok(());
                }
                self.packs
                    .rewrite_pack_with_live_hashes(*pack_id, &live_hashes)?;
                rewritten_pack = Some(*pack_id);
                break;
            }
        }

        if let Some(rewritten_pack_id) = rewritten_pack {
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
        } else {
            self.switch_to_scan_phase().await?;
        }

        Ok(())
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

                if key.len() != 17 {
                    continue;
                }
                let mut hash = [0_u8; 16];
                hash.copy_from_slice(&key[1..17]);
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                if chunk.refcount == 0 {
                    out.push(ZeroRefCandidate {
                        hash,
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
        let hashes = candidates
            .iter()
            .map(|entry| entry.hash)
            .collect::<Vec<_>>();
        self.meta
            .write_txn(|txn| {
                for hash in hashes.iter() {
                    let key = chunk_key(hash);
                    if let Some(raw) = txn.get(key.clone())? {
                        let chunk: ChunkRecord = decode_rkyv(&raw)?;
                        if chunk.refcount == 0 {
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
    pub(crate) fn live_hashes_for_pack(&self, pack_id: u64) -> Result<HashSet<[u8; 16]>> {
        self.meta.read_txn(|txn| {
            let mut out = HashSet::new();
            let prefix = vec![crate::types::KEY_PREFIX_CHUNK];
            let end = prefix_end(&prefix);
            for pair in scan_range_pairs(txn, prefix, end)? {
                let (key, value) = pair?;
                if key.len() != 17 {
                    continue;
                }
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                if chunk.pack_id != pack_id || chunk.refcount == 0 {
                    continue;
                }
                let mut hash = [0_u8; 16];
                hash.copy_from_slice(&key[1..17]);
                out.insert(hash);
            }
            Ok(out)
        })
    }
}
