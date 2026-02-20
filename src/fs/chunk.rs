use super::*;
use crate::fs::FsCore;

impl FsCore {
    pub(crate) fn ultracdc_chunk_count(&self, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }

        const CHUNK_FEED_BUFFER_SIZE: usize = 64 * 1024;

        let mut chunker = UltraStreamChunker::new(
            self.config.ultracdc_min_size_bytes,
            self.config.ultracdc_avg_size_bytes,
            self.config.ultracdc_max_size_bytes,
        );
        let mut count = 0_usize;
        for piece in data.chunks(CHUNK_FEED_BUFFER_SIZE) {
            count = count.saturating_add(chunker.feed(piece).len());
        }
        count.saturating_add(chunker.finish().len())
    }
    pub(crate) fn load_chunk_record(&self, hash: [u8; 16]) -> Result<ChunkRecord> {
        if let Some(cached) = self.chunk_meta_cache.get(&hash) {
            self.chunk_meta_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }
        self.chunk_meta_cache_misses.fetch_add(1, Ordering::Relaxed);
        let chunk = self.meta.read_txn(|txn| {
            let Some(raw) = txn.get(chunk_key(&hash))? else {
                return Err(anyhow_errno(
                    Errno::EIO,
                    format!("missing chunk metadata for hash {:x?}", hash),
                ));
            };
            let chunk: ChunkRecord = decode_rkyv(&raw)?;
            Ok(chunk)
        })?;
        self.chunk_meta_cache.insert(hash, chunk.clone());
        Ok(chunk)
    }
    pub(crate) fn prefetch_chunk_meta(
        &self,
        hashes: impl IntoIterator<Item = [u8; 16]>,
    ) -> Result<()> {
        let missing_hashes = hashes
            .into_iter()
            .filter(|h| !self.chunk_meta_cache.contains_key(h))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        if !missing_hashes.is_empty() {
            let chunks = self.meta.read_txn(|txn| {
                let mut chunks = Vec::new();
                for hash in &missing_hashes {
                    if let Some(raw) = txn.get(chunk_key(hash))? {
                        let chunk: ChunkRecord = decode_rkyv(&raw)?;
                        chunks.push((*hash, chunk));
                    }
                }
                Ok(chunks)
            })?;
            for (hash, chunk) in chunks {
                self.chunk_meta_cache.insert(hash, chunk);
            }
        }
        Ok(())
    }
    pub(crate) fn read_extent_bytes(
        &self,
        extent: ExtentRecord,
        vault_encrypted: bool,
    ) -> Result<Vec<u8>> {
        if let Some(cached) = self.chunk_data_cache.get(&extent.chunk_hash) {
            self.chunk_data_cache_hits.fetch_add(1, Ordering::Relaxed);
            let mut payload = cached.as_ref().clone();
            if payload.len() < BLOCK_SIZE {
                payload.resize(BLOCK_SIZE, 0);
            }
            if payload.len() > BLOCK_SIZE {
                payload.truncate(BLOCK_SIZE);
            }
            return Ok(payload);
        }
        self.chunk_data_cache_misses.fetch_add(1, Ordering::Relaxed);

        let chunk = self.load_chunk_record(extent.chunk_hash)?;
        let mut payload = if (chunk.flags & CHUNK_FLAG_ENCRYPTED) != 0 {
            if !vault_encrypted {
                return Err(anyhow_errno(
                    Errno::EIO,
                    "encrypted chunk referenced by non-vault inode",
                ));
            }
            let encrypted = self.packs.read_chunk_payload(
                chunk.pack_id,
                extent.chunk_hash,
                chunk.codec,
                chunk.uncompressed_len,
                chunk.compressed_len,
            )?;
            let folder_key = self.current_vault_key()?;
            let compressed = decrypt_chunk_payload(&folder_key, &chunk.nonce, &encrypted)?;
            crate::data::compress::decompress_chunk(
                chunk.codec,
                &compressed,
                chunk.uncompressed_len,
            )?
        } else {
            self.packs.read_chunk(
                chunk.pack_id,
                extent.chunk_hash,
                chunk.codec,
                chunk.uncompressed_len,
                chunk.compressed_len,
            )?
        };
        self.chunk_data_cache
            .insert(extent.chunk_hash, Arc::new(payload.clone()));
        if payload.len() < BLOCK_SIZE {
            payload.resize(BLOCK_SIZE, 0);
        }
        if payload.len() > BLOCK_SIZE {
            payload.truncate(BLOCK_SIZE);
        }
        Ok(payload)
    }
    pub(crate) fn stage_chunk_if_missing(
        &self,
        chunk_hash: [u8; 16],
        data: &[u8],
        checked_hashes: &mut HashSet<[u8; 16]>,
        pending_chunks: &mut HashMap<[u8; 16], Vec<u8>>,
    ) -> Result<bool> {
        if !checked_hashes.insert(chunk_hash) {
            return Ok(false);
        }

        let exists = self
            .meta
            .read_txn(|txn| Ok(txn.get(chunk_key(&chunk_hash))?.is_some()))?;
        if exists {
            return Ok(false);
        }

        pending_chunks.insert(chunk_hash, data.to_vec());
        Ok(true)
    }
    pub(crate) async fn materialize_pending_chunks(
        &self,
        pending_chunks: HashMap<[u8; 16], Vec<u8>>,
        encrypt_for_vault: bool,
    ) -> Result<HashMap<[u8; 16], ChunkRecord>> {
        if pending_chunks.is_empty() {
            return Ok(HashMap::new());
        }

        let pending = pending_chunks
            .into_iter()
            .map(|(hash, data)| PendingChunk { hash, data })
            .collect::<Vec<_>>();
        let zstd_level = self.config.zstd_compression_level;
        let ready = tokio::task::spawn_blocking(move || compress_parallel(pending, zstd_level))
            .await
            .map_err(|e| anyhow!("compression task failed: {}", e))??;
        let mut out = HashMap::with_capacity(ready.len());
        for ready_chunk in ready {
            let (payload, disk_compressed_len, nonce, flags) = if encrypt_for_vault {
                let folder_key = self.current_vault_key()?;
                let (ciphertext, nonce) =
                    encrypt_chunk_payload(&folder_key, &ready_chunk.chunk.compressed)?;
                let clen = ciphertext.len() as u32;
                (ciphertext, clen, nonce, CHUNK_FLAG_ENCRYPTED)
            } else {
                (
                    ready_chunk.chunk.compressed.clone(),
                    ready_chunk.chunk.compressed_len,
                    [0_u8; 24],
                    0,
                )
            };
            let pack_id = self.packs.append_chunk(
                ready_chunk.hash,
                ready_chunk.chunk.codec,
                ready_chunk.chunk.uncompressed_len,
                &payload,
            )?;
            let chunk = ChunkRecord {
                refcount: 0,
                pack_id,
                codec: ready_chunk.chunk.codec,
                flags,
                nonce,
                uncompressed_len: ready_chunk.chunk.uncompressed_len,
                compressed_len: disk_compressed_len,
            };
            self.chunk_meta_cache
                .insert(ready_chunk.hash, chunk.clone());
            out.insert(ready_chunk.hash, chunk);
        }

        Ok(out)
    }
    pub(crate) fn apply_ref_deltas_in_txn(
        txn: &mut surrealkv::Transaction,
        ref_deltas: &HashMap<[u8; 16], i64>,
        new_chunk_records: &HashMap<[u8; 16], ChunkRecord>,
    ) -> Result<()> {
        for (hash, delta) in ref_deltas.iter() {
            let hash = *hash;
            let delta = *delta;
            if delta == 0 {
                continue;
            }

            let key = chunk_key(&hash);
            if let Some(raw) = txn.get(key.clone())? {
                let mut chunk: ChunkRecord = decode_rkyv(&raw)?;
                let next_ref = chunk.refcount as i64 + delta;
                chunk.refcount = next_ref.max(0) as u64;
                txn.set(key, encode_rkyv(&chunk)?)?;
                continue;
            }

            if delta <= 0 {
                continue;
            }

            let Some(mut chunk) = new_chunk_records.get(&hash).cloned() else {
                return Err(anyhow_errno(
                    Errno::EIO,
                    format!("missing staged chunk metadata for hash {:x?}", hash),
                ));
            };
            chunk.refcount = delta as u64;
            txn.set(key, encode_rkyv(&chunk)?)?;
        }
        Ok(())
    }
    pub(crate) fn extent_block_idx_from_key(key: &[u8]) -> Option<u64> {
        if key.len() != 17 || key[0] != crate::types::KEY_PREFIX_EXTENT {
            return None;
        }
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&key[9..17]);
        Some(u64::from_be_bytes(bytes))
    }
    pub(crate) fn decrement_chunk_refcounts_in_txn(
        txn: &mut surrealkv::Transaction,
        hashes: &[[u8; 16]],
    ) -> Result<()> {
        if hashes.is_empty() {
            return Ok(());
        }

        let mut ref_deltas = HashMap::<[u8; 16], i64>::with_capacity(hashes.len());
        for hash in hashes {
            *ref_deltas.entry(*hash).or_insert(0) -= 1;
        }

        FsCore::apply_ref_deltas_in_txn(txn, &ref_deltas, &HashMap::new())
    }
}
