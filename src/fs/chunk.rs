use super::*;
use crate::fs::FsCore;

impl FsCore {
    pub(crate) fn validate_pack_payload_crc32(
        &self,
        pack_id: u64,
        chunk_hash: [u8; 16],
        expected_crc32: u32,
        payload: &[u8],
    ) {
        let actual_crc32 = crc32c::crc32c(payload);
        if actual_crc32 == expected_crc32 {
            return;
        }

        let total = self
            .pack_crc32_read_error_count
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        tracing::error!(
            pack_id,
            chunk_hash = ?chunk_hash,
            expected_crc32,
            actual_crc32,
            total_crc32_errors = total,
            "pack payload crc32 mismatch detected during read"
        );
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
        self.chunk_meta_cache.insert(hash, chunk);
        Ok(chunk)
    }
    /// Returns the plaintext of chunk `hash`, from the chunk cache or the
    /// pack store.
    pub(crate) fn load_chunk_payload(
        &self,
        hash: [u8; 16],
        vault_encrypted: bool,
    ) -> Result<Arc<Vec<u8>>> {
        if let Some(cached) = self.chunk_data_cache.get(&hash) {
            self.chunk_data_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }
        self.chunk_data_cache_misses.fetch_add(1, Ordering::Relaxed);
        // The pack read and decompression block, so they run off the async
        // workers.
        let payload = Arc::new(tokio::task::block_in_place(|| {
            self.read_chunk_from_pack(hash, vault_encrypted)
        })?);
        self.chunk_data_cache.insert(hash, Arc::clone(&payload));
        Ok(payload)
    }

    /// Returns the plaintext of every chunk in `hashes`. Chunks missing from
    /// the cache are read and decompressed in parallel, so a read that spans
    /// many small chunks does not pay for them one after another.
    pub(crate) fn load_chunk_payloads(
        &self,
        hashes: &[[u8; 16]],
        vault_encrypted: bool,
    ) -> Result<HashMap<[u8; 16], Arc<Vec<u8>>>> {
        let mut out = HashMap::with_capacity(hashes.len());
        let mut missing = Vec::new();
        for hash in hashes {
            if out.contains_key(hash) || missing.contains(hash) {
                continue;
            }
            match self.chunk_data_cache.get(hash) {
                Some(cached) => {
                    self.chunk_data_cache_hits.fetch_add(1, Ordering::Relaxed);
                    out.insert(*hash, cached);
                }
                None => missing.push(*hash),
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        self.chunk_data_cache_misses
            .fetch_add(missing.len() as u64, Ordering::Relaxed);
        let loaded = tokio::task::block_in_place(|| {
            missing
                .par_iter()
                .map(|hash| {
                    self.read_chunk_from_pack(*hash, vault_encrypted)
                        .map(|payload| (*hash, Arc::new(payload)))
                })
                .collect::<Result<Vec<_>>>()
        })?;
        for (hash, payload) in loaded {
            self.chunk_data_cache.insert(hash, Arc::clone(&payload));
            out.insert(hash, payload);
        }
        Ok(out)
    }

    /// Reads, authenticates and decompresses chunk `hash` from its pack.
    /// Blocks on the disk.
    fn read_chunk_from_pack(&self, hash: [u8; 16], vault_encrypted: bool) -> Result<Vec<u8>> {
        let chunk = self.load_chunk_record(hash)?;
        let folder_key = if (chunk.flags & CHUNK_FLAG_ENCRYPTED) != 0 {
            if !vault_encrypted {
                return Err(anyhow_errno(
                    Errno::EIO,
                    "encrypted chunk referenced by non-vault inode",
                ));
            }
            Some(self.current_vault_key()?)
        } else {
            None
        };
        // A copy is accepted only if it authenticates with the record's nonce
        // (vault) and decompresses to the recorded length, which is how the
        // pack store tells duplicate copies of one hash apart.
        self.packs.read_chunk_with(
            chunk.pack_id,
            hash,
            chunk.codec,
            chunk.uncompressed_len,
            chunk.compressed_len,
            |stored, index| {
                self.validate_pack_payload_crc32(chunk.pack_id, hash, index.payload_crc32, stored);
                match &folder_key {
                    Some(key) => {
                        let compressed = decrypt_chunk_payload(key, &chunk.nonce, stored)?;
                        crate::data::compress::decompress_chunk(
                            chunk.codec,
                            &compressed,
                            chunk.uncompressed_len,
                        )
                    }
                    None => crate::data::compress::decompress_chunk(
                        chunk.codec,
                        stored,
                        chunk.uncompressed_len,
                    ),
                }
            },
        )
    }

    /// Returns the plaintext of `extent`, checked against the extent length.
    pub(crate) fn load_extent_payload(
        &self,
        extent: &FileExtent,
        vault_encrypted: bool,
    ) -> Result<Arc<Vec<u8>>> {
        let payload = self.load_chunk_payload(extent.record.chunk_hash, vault_encrypted)?;
        FsCore::check_extent_payload_len(extent, &payload)?;
        Ok(payload)
    }

    pub(crate) fn check_extent_payload_len(extent: &FileExtent, payload: &[u8]) -> Result<()> {
        if payload.len() != extent.record.len as usize {
            tracing::error!(
                start = extent.start,
                extent_len = extent.record.len,
                payload_len = payload.len(),
                chunk_hash = ?extent.record.chunk_hash,
                "chunk length does not match the extent that maps it"
            );
            return Err(anyhow_errno(
                Errno::EIO,
                format!(
                    "chunk {:x?} holds {} bytes but its extent at {} maps {}",
                    extent.record.chunk_hash,
                    payload.len(),
                    extent.start,
                    extent.record.len
                ),
            ));
        }
        Ok(())
    }

    /// Appends bytes `[offset, read_end)` of `inode` to `buf` and returns how
    /// many it appended. `extents` must hold every extent of the file that
    /// overlaps the range, sorted by offset; bytes no extent covers are
    /// holes and read as zeros.
    pub(crate) fn read_extents_into(
        &self,
        inode: &InodeRecord,
        extents: &[FileExtent],
        offset: u64,
        read_end: u64,
        buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let first = extents.partition_point(|extent| extent.end() <= offset);
        let overlapping = &extents[first..];
        let overlapping =
            &overlapping[..overlapping.partition_point(|extent| extent.start < read_end)];
        let hashes = overlapping
            .iter()
            .map(|extent| extent.record.chunk_hash)
            .collect::<Vec<_>>();
        let payloads = self.load_chunk_payloads(&hashes, FsCore::inode_is_vault(inode))?;

        let initial_len = buf.len();
        buf.reserve((read_end - offset) as usize);
        let mut cursor = offset;
        for extent in overlapping {
            let payload = payloads.get(&extent.record.chunk_hash).ok_or_else(|| {
                anyhow_errno(
                    Errno::EIO,
                    format!("chunk {:x?} was not loaded", extent.record.chunk_hash),
                )
            })?;
            FsCore::check_extent_payload_len(extent, payload)?;
            let from = extent.start.max(cursor);
            buf.resize(buf.len() + (from - cursor) as usize, 0);
            let to = extent.end().min(read_end);
            buf.extend_from_slice(
                &payload[(from - extent.start) as usize..(to - extent.start) as usize],
            );
            cursor = to;
        }
        buf.resize(buf.len() + (read_end - cursor) as usize, 0);
        Ok(buf.len() - initial_len)
    }

    /// Extents of `ino` that overlap `[from, to)`, sorted by offset.
    ///
    /// Extents are keyed by their first byte, so the extent covering `from`
    /// starts at most `max_extent_len - 1` bytes earlier.
    pub(crate) fn load_extents_in_txn(
        &self,
        txn: &verfsnext_surrealkv::Transaction,
        ino: u64,
        from: u64,
        to: u64,
    ) -> Result<Vec<FileExtent>> {
        let mut out = Vec::new();
        if from >= to {
            return Ok(out);
        }
        let scan_from = from.saturating_sub(self.max_extent_len.saturating_sub(1));
        let mut iter = txn.range(extent_key(ino, scan_from), extent_key(ino, to))?;
        let mut valid = iter.seek_first()?;
        while valid {
            let key = iter.key().user_key();
            let start = FsCore::extent_start_from_key(key).ok_or_else(|| {
                anyhow_errno(Errno::EIO, format!("malformed extent key {:x?}", key))
            })?;
            let record: ExtentRecord = decode_rkyv(&iter.value()?)?;
            let extent = FileExtent { start, record };
            if extent.end() > from {
                out.push(extent);
            }
            valid = iter.next()?;
        }
        Ok(out)
    }

    /// Every extent of `ino`, sorted by offset.
    pub(crate) fn load_all_extents_in_txn(
        txn: &verfsnext_surrealkv::Transaction,
        ino: u64,
    ) -> Result<Vec<FileExtent>> {
        let prefix = extent_prefix(ino);
        let end = prefix_end(&prefix);
        let mut out = Vec::new();
        for pair in scan_range_pairs(txn, prefix, end)? {
            let (key, value) = pair?;
            let start = FsCore::extent_start_from_key(&key).ok_or_else(|| {
                anyhow_errno(Errno::EIO, format!("malformed extent key {:x?}", key))
            })?;
            let record: ExtentRecord = decode_rkyv(&value)?;
            out.push(FileExtent { start, record });
        }
        Ok(out)
    }
    /// Decides whether `chunk_hash` must be stored as a new chunk.
    ///
    /// The decision is taken against `txn`, the transaction that will commit
    /// the refcount change. A chunk that GC deletes, or that another writer
    /// creates, after `txn` started then makes the commit fail with a write
    /// conflict instead of leaving an extent that points at missing metadata
    /// or a second record for the same hash. The in-memory chunk metadata
    /// cache is never used for this decision because it can hold entries for
    /// chunks GC has already deleted.
    pub(crate) fn stage_chunk_if_missing(
        &self,
        txn: &verfsnext_surrealkv::Transaction,
        chunk_hash: [u8; 16],
        data: Arc<Vec<u8>>,
        checked_hashes: &mut HashSet<[u8; 16]>,
        pending_chunks: &mut HashMap<[u8; 16], Arc<Vec<u8>>>,
    ) -> Result<bool> {
        if !checked_hashes.insert(chunk_hash) {
            return Ok(false);
        }

        self.chunk_data_cache.insert(chunk_hash, Arc::clone(&data));
        if txn.get(chunk_key(&chunk_hash))?.is_some() {
            return Ok(false);
        }

        pending_chunks.insert(chunk_hash, data);
        Ok(true)
    }
    pub(crate) async fn materialize_pending_chunks(
        &self,
        pending_chunks: HashMap<[u8; 16], Arc<Vec<u8>>>,
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
            let chunk = if encrypt_for_vault {
                let folder_key = self.current_vault_key()?;
                let (ciphertext, nonce) =
                    encrypt_chunk_payload(&folder_key, &ready_chunk.chunk.compressed)?;
                let disk_compressed_len = ciphertext.len() as u32;
                let payload_crc32 = crc32c::crc32c(&ciphertext);
                let pack_id = self.packs.append_chunk_with_crc32(
                    ready_chunk.hash,
                    ready_chunk.chunk.codec,
                    ready_chunk.chunk.uncompressed_len,
                    &ciphertext,
                    payload_crc32,
                )?;
                ChunkRecord {
                    refcount: 0,
                    pack_id,
                    codec: ready_chunk.chunk.codec,
                    flags: CHUNK_FLAG_ENCRYPTED,
                    nonce,
                    uncompressed_len: ready_chunk.chunk.uncompressed_len,
                    compressed_len: disk_compressed_len,
                }
            } else {
                let pack_id = self.packs.append_chunk_with_crc32(
                    ready_chunk.hash,
                    ready_chunk.chunk.codec,
                    ready_chunk.chunk.uncompressed_len,
                    &ready_chunk.chunk.compressed,
                    ready_chunk.chunk.payload_crc32,
                )?;
                ChunkRecord {
                    refcount: 0,
                    pack_id,
                    codec: ready_chunk.chunk.codec,
                    flags: 0,
                    nonce: [0_u8; 24],
                    uncompressed_len: ready_chunk.chunk.uncompressed_len,
                    compressed_len: ready_chunk.chunk.compressed_len,
                }
            };
            out.insert(ready_chunk.hash, chunk);
        }

        Ok(out)
    }
    pub(crate) fn apply_ref_deltas_in_txn(
        txn: &mut verfsnext_surrealkv::Transaction,
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
                if next_ref < 0 {
                    tracing::error!(
                        chunk_hash = ?hash,
                        refcount = chunk.refcount,
                        delta,
                        "chunk refcount underflow: metadata refcounts are inconsistent, clamping to zero"
                    );
                }
                chunk.refcount = next_ref.max(0) as u64;
                txn.set(key, encode_rkyv(&chunk)?)?;
                continue;
            }

            if delta < 0 {
                tracing::error!(
                    chunk_hash = ?hash,
                    delta,
                    "refcount decrement for a chunk without metadata: metadata refcounts are inconsistent"
                );
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
    pub(crate) fn extent_start_from_key(key: &[u8]) -> Option<u64> {
        if key.len() != 17 || key[0] != crate::types::KEY_PREFIX_EXTENT {
            return None;
        }
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&key[9..17]);
        Some(u64::from_be_bytes(bytes))
    }
    pub(crate) fn decrement_chunk_refcounts_in_txn(
        txn: &mut verfsnext_surrealkv::Transaction,
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
