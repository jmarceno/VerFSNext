use super::*;
use crate::fs::FsCore;

/// A chunk staged by a write, before it is stored.
pub(crate) struct StagedChunk {
    pub(crate) data: Arc<Vec<u8>>,
    /// Provisional end-of-file chunk, stored inline in metadata.
    pub(crate) inline: bool,
}

/// A freshly stored chunk: its record, and the payload to write into
/// metadata when it is inline.
pub(crate) struct NewChunk {
    pub(crate) record: ChunkRecord,
    pub(crate) inline_payload: Option<Vec<u8>>,
}

/// Chunks per job handed to the thread pool. Every pool worker that wakes
/// spins for a while before it sleeps again, so spreading the few chunks of
/// one FUSE request over all cores burns far more CPU than the chunks need
/// (it used to cost about ten CPU-seconds per GiB read). Smaller sets run on
/// the calling thread.
const CHUNKS_PER_PARALLEL_JOB: usize = 8;

/// Maps `f` over `items` in order, in parallel only when there is more than
/// one job's worth of chunks.
fn map_chunks<T, R>(
    items: &[T],
    f: impl Fn(&T) -> Result<R> + Sync + Send,
) -> Result<Vec<R>>
where
    T: Sync,
    R: Send,
{
    if items.len() <= CHUNKS_PER_PARALLEL_JOB {
        return items.iter().map(f).collect();
    }
    items
        .par_iter()
        .with_min_len(CHUNKS_PER_PARALLEL_JOB)
        .map(f)
        .collect()
}

/// The bytes of one chunk as they go into a pack record.
pub(crate) struct StoredPayload {
    pub(crate) codec: u8,
    /// Compressed payload, the chunk itself when compression would not
    /// shrink it (shared, not copied), or the vault ciphertext.
    pub(crate) bytes: Arc<Vec<u8>>,
    nonce: [u8; 24],
    uncompressed_len: u32,
    pub(crate) crc32: u32,
}

impl StoredPayload {
    /// Compresses `data` and, with a vault `key`, encrypts the result.
    pub(crate) fn new(
        data: &Arc<Vec<u8>>,
        key: Option<&[u8; 32]>,
        zstd_level: i32,
    ) -> Result<Self> {
        let uncompressed_len = u32::try_from(data.len())
            .map_err(|_| anyhow!("chunk too large for u32 length: {}", data.len()))?;
        let (codec, bytes) = match compress_payload(data, zstd_level)? {
            Some(compressed) => (CODEC_ZSTD, Arc::new(compressed)),
            None => (CODEC_RAW, Arc::clone(data)),
        };
        let (bytes, nonce) = match key {
            Some(key) => {
                let (ciphertext, nonce) = encrypt_chunk_payload(key, &bytes)?;
                (Arc::new(ciphertext), nonce)
            }
            None => (bytes, [0_u8; 24]),
        };
        Ok(Self {
            codec,
            crc32: crc32c::crc32c(&bytes),
            bytes,
            nonce,
            uncompressed_len,
        })
    }
}

impl FsCore {
    /// Fails with EIO when a stored payload does not match the CRC32 its
    /// pack index recorded for it, so a damaged copy is never decoded.
    pub(crate) fn validate_pack_payload_crc32(
        &self,
        pack_id: u64,
        chunk_hash: [u8; 16],
        expected_crc32: u32,
        payload: &[u8],
    ) -> Result<()> {
        let actual_crc32 = crc32c::crc32c(payload);
        if actual_crc32 == expected_crc32 {
            return Ok(());
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
        Err(anyhow_errno(
            Errno::EIO,
            format!(
                "stored copy of chunk {:x?} in pack {} fails its crc32 (expected {}, got {})",
                chunk_hash, pack_id, expected_crc32, actual_crc32
            ),
        ))
    }
    pub(crate) fn load_chunk_record(&self, id: ChunkId) -> Result<ChunkRecord> {
        if let Some(cached) = self.chunk_meta_cache.get(&id) {
            self.chunk_meta_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }
        self.chunk_meta_cache_misses.fetch_add(1, Ordering::Relaxed);
        let ticket = self.chunk_meta_cache.ticket(&id);
        let chunk = self.meta.read_txn(|txn| {
            let Some(raw) = txn.get(chunk_key(&id))? else {
                return Err(anyhow_errno(
                    Errno::EIO,
                    format!("missing chunk metadata for hash {:x?}", id.hash),
                ));
            };
            let chunk: ChunkRecord = decode_rkyv(&raw)?;
            Ok(chunk)
        })?;
        self.chunk_meta_cache.insert_if_current(id, chunk, ticket);
        Ok(chunk)
    }
    /// Returns the plaintext of chunk `id`, from the chunk cache or the
    /// pack store.
    pub(crate) fn load_chunk_payload(&self, id: ChunkId) -> Result<Arc<Vec<u8>>> {
        if let Some(cached) = self.chunk_data_cache.get(&id) {
            self.chunk_data_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }
        self.chunk_data_cache_misses.fetch_add(1, Ordering::Relaxed);
        let epoch = self.vault_cache_epoch();
        // The pack read and decompression block, so they run off the async
        // workers.
        let payload = Arc::new(tokio::task::block_in_place(|| {
            self.read_chunk_from_pack(id)
        })?);
        self.cache_chunk_payload(id, Arc::clone(&payload), epoch);
        Ok(payload)
    }

    /// Returns the plaintext of every chunk in `ids`. Chunks missing from
    /// the cache are read and decompressed in parallel when there are
    /// enough of them, so a read that spans many small chunks does not pay
    /// for them one after another.
    pub(crate) fn load_chunk_payloads(
        &self,
        ids: &[ChunkId],
    ) -> Result<HashMap<ChunkId, Arc<Vec<u8>>>> {
        let mut out = HashMap::with_capacity(ids.len());
        let mut missing = Vec::new();
        for id in ids {
            if out.contains_key(id) || missing.contains(id) {
                continue;
            }
            match self.chunk_data_cache.get(id) {
                Some(cached) => {
                    self.chunk_data_cache_hits.fetch_add(1, Ordering::Relaxed);
                    out.insert(*id, cached);
                }
                None => missing.push(*id),
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        self.chunk_data_cache_misses
            .fetch_add(missing.len() as u64, Ordering::Relaxed);
        let epoch = self.vault_cache_epoch();
        let loaded = tokio::task::block_in_place(|| {
            map_chunks(&missing, |id| {
                self.read_chunk_from_pack(*id)
                    .map(|payload| (*id, Arc::new(payload)))
            })
        })?;
        for (id, payload) in loaded {
            self.cache_chunk_payload(id, Arc::clone(&payload), epoch);
            out.insert(id, payload);
        }
        Ok(out)
    }

    /// Loads the chunks of `inode` that hold `[from, to)` into the chunk
    /// cache, one after another, ahead of a sequential reader. Blocks on the
    /// disk. Copies are verified exactly as for a read; a failure is
    /// returned for the caller to log, and the reader then gets the same
    /// error from its own read.
    pub(crate) fn read_ahead(&self, inode: &InodeRecord, from: u64, to: u64) -> Result<()> {
        let extents = self
            .meta
            .read_txn(|txn| self.load_extents_in_txn(txn, inode.ino, from, to))?;
        let vault = FsCore::inode_is_vault(inode);
        for extent in extents {
            let id = ChunkId {
                vault,
                hash: extent.record.chunk_hash,
            };
            if self.chunk_data_cache.contains_key(&id) {
                continue;
            }
            self.chunk_data_cache_misses.fetch_add(1, Ordering::Relaxed);
            let epoch = self.vault_cache_epoch();
            let payload = Arc::new(self.read_chunk_from_pack(id)?);
            FsCore::check_extent_payload_len(&extent, &payload)?;
            self.cache_chunk_payload(id, payload, epoch);
        }
        Ok(())
    }

    /// Reads, authenticates, decompresses and verifies chunk `id` from its
    /// pack. Blocks on the disk.
    ///
    /// A copy is accepted only if its payload matches the index CRC32, it
    /// authenticates with the record's nonce (vault), it decompresses to the
    /// recorded length and its plaintext hashes to `id.hash`. When every copy
    /// is rejected and at least one was damaged (CRC or content hash), the
    /// chunk is marked corrupt so the next write of the same content stores
    /// a fresh copy instead of deduplicating against the damaged one.
    fn read_chunk_from_pack(&self, id: ChunkId) -> Result<Vec<u8>> {
        let mut chunk = self.load_chunk_record(id)?;
        if !FsCore::chunk_record_matches_domain(&id, &chunk) {
            return Err(anyhow_errno(
                Errno::EIO,
                format!(
                    "chunk {:x?} record does not belong to its {} domain key",
                    id.hash,
                    if id.vault { "vault" } else { "plain" }
                ),
            ));
        }
        if (chunk.flags & CHUNK_FLAG_INLINE) != 0 {
            // Record and payload are read together: a promotion may have
            // moved the chunk to a pack since the record was cached.
            let (current, inline_payload) = self.meta.read_txn(|txn| {
                let Some(raw) = txn.get(chunk_key(&id))? else {
                    return Err(anyhow_errno(
                        Errno::EIO,
                        format!("missing chunk metadata for hash {:x?}", id.hash),
                    ));
                };
                let current: ChunkRecord = decode_rkyv(&raw)?;
                if (current.flags & CHUNK_FLAG_INLINE) == 0 {
                    return Ok((current, None));
                }
                let payload = txn.get(inline_chunk_key(&id))?.ok_or_else(|| {
                    anyhow_errno(
                        Errno::EIO,
                        format!("missing inline payload of chunk {:x?}", id.hash),
                    )
                })?;
                Ok((current, Some(payload)))
            })?;
            chunk = current;
            if let Some(stored) = inline_payload {
                return self.decode_inline_chunk(id, &chunk, stored);
            }
        }
        let folder_key = if id.vault {
            Some(self.current_vault_key()?)
        } else {
            None
        };
        let mut damaged = false;
        let result = self.packs.read_chunk_with(
            chunk.pack_id,
            id.hash,
            chunk.codec,
            chunk.uncompressed_len,
            chunk.compressed_len,
            |stored, index| {
                if let Err(err) = self.validate_pack_payload_crc32(
                    chunk.pack_id,
                    id.hash,
                    index.payload_crc32,
                    &stored,
                ) {
                    damaged = true;
                    return Err(err);
                }
                let plaintext = match &folder_key {
                    Some(key) => {
                        let compressed = decrypt_chunk_payload(key, &chunk.nonce, &stored)?;
                        crate::data::compress::decompress_chunk(
                            chunk.codec,
                            compressed,
                            chunk.uncompressed_len,
                        )?
                    }
                    None => crate::data::compress::decompress_chunk(
                        chunk.codec,
                        stored,
                        chunk.uncompressed_len,
                    )?,
                };
                if FsCore::chunk_content_hash(id.vault, &plaintext) != id.hash {
                    damaged = true;
                    tracing::error!(
                        pack_id = chunk.pack_id,
                        chunk_hash = ?id.hash,
                        offset = index.offset,
                        "chunk plaintext does not match its content hash"
                    );
                    return Err(anyhow_errno(
                        Errno::EIO,
                        format!(
                            "stored copy of chunk {:x?} in pack {} decodes to different content",
                            id.hash, chunk.pack_id
                        ),
                    ));
                }
                Ok(plaintext)
            },
        );
        match result {
            Ok(plaintext) => Ok(plaintext),
            Err(err) => {
                if damaged {
                    self.corrupt_chunks.lock().insert(id);
                    tracing::error!(
                        pack_id = chunk.pack_id,
                        chunk_hash = ?id.hash,
                        "no stored copy of chunk is intact; it is marked corrupt and the next write of the same content will store it again"
                    );
                }
                Err(anyhow_errno(Errno::EIO, format!("{err:#}")))
            }
        }
    }

    /// Decodes the payload of an inline chunk and verifies its content hash.
    fn decode_inline_chunk(
        &self,
        id: ChunkId,
        chunk: &ChunkRecord,
        stored: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let plaintext = if id.vault {
            let key = self.current_vault_key()?;
            decrypt_chunk_payload(&key, &chunk.nonce, &stored)?
        } else {
            stored
        };
        let plaintext =
            crate::data::compress::decompress_chunk(chunk.codec, plaintext, chunk.uncompressed_len)?;
        if FsCore::chunk_content_hash(id.vault, &plaintext) != id.hash {
            self.corrupt_chunks.lock().insert(id);
            tracing::error!(
                chunk_hash = ?id.hash,
                "inline chunk does not match its content hash; it is marked corrupt and the next write of the same content will store it again"
            );
            return Err(anyhow_errno(
                Errno::EIO,
                format!("inline chunk {:x?} decodes to different content", id.hash),
            ));
        }
        Ok(plaintext)
    }

    /// Returns the plaintext of `extent`, checked against the extent length.
    pub(crate) fn load_extent_payload(
        &self,
        extent: &FileExtent,
        vault_encrypted: bool,
    ) -> Result<Arc<Vec<u8>>> {
        let id = ChunkId {
            vault: vault_encrypted,
            hash: extent.record.chunk_hash,
        };
        let payload = self.load_chunk_payload(id)?;
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
        let vault = FsCore::inode_is_vault(inode);
        let ids = overlapping
            .iter()
            .map(|extent| ChunkId {
                vault,
                hash: extent.record.chunk_hash,
            })
            .collect::<Vec<_>>();
        let payloads = self.load_chunk_payloads(&ids)?;

        let initial_len = buf.len();
        buf.reserve((read_end - offset) as usize);
        let mut cursor = offset;
        for extent in overlapping {
            let id = ChunkId {
                vault,
                hash: extent.record.chunk_hash,
            };
            let payload = payloads.get(&id).ok_or_else(|| {
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
    /// Decides whether `id` must be stored as a new chunk.
    ///
    /// The decision is taken against `txn`, the transaction that will commit
    /// the refcount change. A chunk that GC deletes, or that another writer
    /// creates, after `txn` started then makes the commit fail with a write
    /// conflict instead of leaving an extent that points at missing metadata
    /// or a second record for the same hash. The in-memory chunk metadata
    /// cache is never used for this decision because it can hold entries for
    /// chunks GC has already deleted.
    ///
    /// A chunk whose stored copy is known to be damaged is stored again; the
    /// commit then repoints its record at the fresh copy, which repairs every
    /// file that references it.
    ///
    /// `inline` marks the provisional end-of-file chunk of a write, which is
    /// stored in metadata instead of a pack when it is new.
    pub(crate) fn stage_chunk_if_missing(
        &self,
        txn: &verfsnext_surrealkv::Transaction,
        id: ChunkId,
        data: Arc<Vec<u8>>,
        inline: bool,
        checked_ids: &mut HashSet<ChunkId>,
        pending_chunks: &mut HashMap<ChunkId, StagedChunk>,
    ) -> Result<bool> {
        if !checked_ids.insert(id) {
            return Ok(false);
        }

        // Writes and truncates stage chunks under the read side of the
        // mutation gate, which a vault lock takes exclusively, so this needs
        // no vault cache epoch check.
        self.chunk_data_cache.insert(id, Arc::clone(&data));
        if let Some(raw) = txn.get(chunk_key(&id))? {
            let existing: ChunkRecord = decode_rkyv(&raw)?;
            let damaged = !FsCore::chunk_record_matches_domain(&id, &existing)
                || (existing.flags & CHUNK_FLAG_CORRUPT) != 0
                || self.corrupt_chunks.lock().contains(&id);
            if !damaged {
                return Ok(false);
            }
            tracing::warn!(
                chunk_hash = ?id.hash,
                chunk_vault = id.vault,
                "storing a fresh copy of a chunk whose stored copy is damaged or in the wrong domain"
            );
        }

        pending_chunks.insert(id, StagedChunk { data, inline });
        Ok(true)
    }
    /// Stores the staged chunks: pack chunks are compressed (vault: then
    /// encrypted) in parallel and appended to the active pack with one
    /// vectored write; inline chunks keep their bytes (vault: encrypted) for
    /// the commit to write into metadata. The domain of each chunk comes from
    /// its [`ChunkId`], so a staged vault chunk is always encrypted.
    pub(crate) async fn materialize_pending_chunks(
        &self,
        pending_chunks: HashMap<ChunkId, StagedChunk>,
    ) -> Result<HashMap<ChunkId, NewChunk>> {
        if pending_chunks.is_empty() {
            return Ok(HashMap::new());
        }
        let folder_key = if pending_chunks.keys().any(|id| id.vault) {
            Some(self.current_vault_key()?)
        } else {
            None
        };
        let vault_key = || {
            folder_key
                .as_ref()
                .ok_or_else(|| anyhow!("vault chunk staged without a vault key"))
        };

        let mut out = HashMap::with_capacity(pending_chunks.len());
        let mut packed = Vec::with_capacity(pending_chunks.len());
        for (id, staged) in pending_chunks {
            if !staged.inline {
                packed.push((id, staged.data));
                continue;
            }
            let uncompressed_len = u32::try_from(staged.data.len())
                .map_err(|_| anyhow!("chunk too large for u32 length: {}", staged.data.len()))?;
            let (payload, nonce, flags) = if id.vault {
                let (ciphertext, nonce) = encrypt_chunk_payload(vault_key()?, &staged.data)?;
                (ciphertext, nonce, CHUNK_FLAG_INLINE | CHUNK_FLAG_ENCRYPTED)
            } else {
                (staged.data.as_ref().clone(), [0_u8; 24], CHUNK_FLAG_INLINE)
            };
            out.insert(
                id,
                NewChunk {
                    record: ChunkRecord {
                        refcount: 0,
                        pack_id: INLINE_PACK_ID,
                        codec: CODEC_RAW,
                        flags,
                        nonce,
                        uncompressed_len,
                        compressed_len: payload.len() as u32,
                    },
                    inline_payload: Some(payload),
                },
            );
        }
        if packed.is_empty() {
            return Ok(out);
        }

        // Compression, encryption and the pack write block, so they run off
        // the async workers.
        let zstd_level = self.config.zstd_compression_level;
        let (stored, pack_ids) = tokio::task::block_in_place(|| -> Result<_> {
            let stored = map_chunks(&packed, |(id, data)| {
                let key = if id.vault { Some(vault_key()?) } else { None };
                StoredPayload::new(data, key, zstd_level)
            })?;
            let appends = packed
                .iter()
                .zip(&stored)
                .map(|((id, _), payload)| ChunkAppend {
                    hash: id.hash,
                    codec: payload.codec,
                    uncompressed_len: payload.uncompressed_len,
                    payload: &payload.bytes,
                    payload_crc32: payload.crc32,
                })
                .collect::<Vec<_>>();
            let pack_ids = self.packs.append_chunks(&appends)?;
            Ok((stored, pack_ids))
        })?;

        for (((id, _), payload), pack_id) in packed.into_iter().zip(stored).zip(pack_ids) {
            out.insert(
                id,
                NewChunk {
                    record: ChunkRecord {
                        refcount: 0,
                        pack_id,
                        codec: payload.codec,
                        flags: if id.vault { CHUNK_FLAG_ENCRYPTED } else { 0 },
                        nonce: payload.nonce,
                        uncompressed_len: payload.uncompressed_len,
                        compressed_len: payload.bytes.len() as u32,
                    },
                    inline_payload: None,
                },
            );
        }

        Ok(out)
    }
    /// Applies refcount deltas and stores the records of freshly written
    /// chunks. A fresh chunk whose hash already has a record replaces a
    /// damaged copy: the record keeps its refcount and is repointed at the
    /// new copy. An inline chunk whose refcount drops to zero is deleted
    /// with its payload right away: it has no pack copy for GC to reclaim.
    pub(crate) fn apply_ref_deltas_in_txn(
        txn: &mut verfsnext_surrealkv::Transaction,
        ref_deltas: &HashMap<ChunkId, i64>,
        new_chunks: &HashMap<ChunkId, NewChunk>,
    ) -> Result<()> {
        let ids = ref_deltas
            .iter()
            .filter(|(_, delta)| **delta != 0)
            .map(|(id, _)| *id)
            .chain(new_chunks.keys().copied())
            .collect::<HashSet<_>>();
        for id in ids {
            let delta = ref_deltas.get(&id).copied().unwrap_or(0);
            let fresh = new_chunks.get(&id);

            let key = chunk_key(&id);
            if let Some(raw) = txn.get(key.clone())? {
                let existing: ChunkRecord = decode_rkyv(&raw)?;
                let mut chunk = match fresh {
                    Some(fresh) => {
                        tracing::warn!(
                            chunk_hash = ?id.hash,
                            chunk_vault = id.vault,
                            old_pack_id = existing.pack_id,
                            new_pack_id = fresh.record.pack_id,
                            "repointing damaged chunk record at a fresh copy"
                        );
                        if (existing.flags & CHUNK_FLAG_INLINE) != 0 {
                            txn.delete(inline_chunk_key(&id))?;
                        }
                        if let Some(payload) = &fresh.inline_payload {
                            txn.set(inline_chunk_key(&id), payload.clone())?;
                        }
                        ChunkRecord {
                            refcount: existing.refcount,
                            ..fresh.record
                        }
                    }
                    None => existing,
                };
                let next_ref = chunk.refcount as i64 + delta;
                if next_ref < 0 {
                    tracing::error!(
                        chunk_hash = ?id.hash,
                        chunk_vault = id.vault,
                        refcount = chunk.refcount,
                        delta,
                        "chunk refcount underflow: metadata refcounts are inconsistent, clamping to zero"
                    );
                }
                chunk.refcount = next_ref.max(0) as u64;
                if chunk.refcount == 0 && (chunk.flags & CHUNK_FLAG_INLINE) != 0 {
                    txn.delete(key)?;
                    txn.delete(inline_chunk_key(&id))?;
                    continue;
                }
                txn.set(key, encode_rkyv(&chunk)?)?;
                continue;
            }

            if delta < 0 {
                tracing::error!(
                    chunk_hash = ?id.hash,
                    chunk_vault = id.vault,
                    delta,
                    "refcount decrement for a chunk without metadata: metadata refcounts are inconsistent"
                );
                continue;
            }

            let Some(fresh) = fresh else {
                return Err(anyhow_errno(
                    Errno::EIO,
                    format!("missing staged chunk metadata for hash {:x?}", id.hash),
                ));
            };
            if delta == 0 && fresh.inline_payload.is_some() {
                // Staged but no longer referenced by this commit.
                continue;
            }
            if let Some(payload) = &fresh.inline_payload {
                txn.set(inline_chunk_key(&id), payload.clone())?;
            }
            let mut chunk = fresh.record;
            chunk.refcount = delta as u64;
            txn.set(key, encode_rkyv(&chunk)?)?;
        }
        Ok(())
    }
    /// Updates the in-memory state after the chunk records written by a
    /// commit became visible: a damaged chunk that got a fresh copy is no
    /// longer corrupt. The metadata cache entry is dropped rather than
    /// filled, since the committed refcount is not known here.
    pub(crate) fn publish_committed_chunk_records(&self, new_chunks: &HashMap<ChunkId, NewChunk>) {
        if new_chunks.is_empty() {
            return;
        }
        let mut corrupt = self.corrupt_chunks.lock();
        let mut young_inline = self.young_inline_chunks.lock();
        for (id, chunk) in new_chunks {
            self.chunk_meta_cache.invalidate(id);
            corrupt.remove(id);
            if chunk.inline_payload.is_some() {
                young_inline.insert(*id);
            }
        }
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
        ids: &[ChunkId],
    ) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let mut ref_deltas = HashMap::<ChunkId, i64>::with_capacity(ids.len());
        for id in ids {
            *ref_deltas.entry(*id).or_insert(0) -= 1;
        }

        FsCore::apply_ref_deltas_in_txn(txn, &ref_deltas, &HashMap::new())
    }
}
