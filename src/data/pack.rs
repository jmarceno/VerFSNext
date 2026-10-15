use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use moka::sync::Cache;
use parking_lot::{Mutex, RwLock};
use rkyv::{Archive, Deserialize, Serialize};

use crate::permissions::{ensure_dir, set_file_mode};

const RECORD_MAGIC: [u8; 4] = *b"VPK2";
const RECORD_RESERVED: [u8; 3] = [0_u8; 3];

#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy)]
#[rkyv(bytecheck())]
struct PackRecordHeader {
    magic: [u8; 4],
    hash: [u8; 16],
    codec: u8,
    reserved: [u8; 3],
    uncompressed_len: u32,
    compressed_len: u32,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy)]
#[rkyv(bytecheck())]
struct PackIndexRecord {
    hash: [u8; 16],
    offset: u64,
    codec: u8,
    reserved: [u8; 3],
    uncompressed_len: u32,
    compressed_len: u32,
    payload_crc32: u32,
}

const RECORD_HEADER_LEN: usize = size_of::<ArchivedPackRecordHeader>();
const RECORD_HEADER_LEN_U64: u64 = RECORD_HEADER_LEN as u64;
const INDEX_ENTRY_LEN: usize = size_of::<ArchivedPackIndexRecord>();
const INDEX_ENTRY_LEN_U64: u64 = INDEX_ENTRY_LEN as u64;
const PACK_FILE_CACHE_CAPACITY: u64 = 128;

#[repr(C, align(16))]
struct AlignedBytes<const N: usize> {
    data: [u8; N],
}

impl<const N: usize> AlignedBytes<N> {
    fn zeroed() -> Self {
        Self { data: [0_u8; N] }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackIndexEntry {
    pub offset: u64,
    pub codec: u8,
    pub uncompressed_len: u32,
    pub compressed_len: u32,
    pub payload_crc32: u32,
}

struct ActivePack {
    pack_id: u64,
    file: File,
    index_file: File,
    size_bytes: u64,
    /// Buffer of index entries not yet written to disk.
    /// Flushed to index_file during sync() or rotate_active_pack().
    /// Readers find them in `index_map`, which gets every entry on append.
    pending_index: Vec<([u8; 16], PackIndexEntry)>,
    /// Lookup map of this pack, including entries not yet flushed.
    index_map: SharedIndexMap,
}

/// Every indexed copy of each hash in one pack, oldest first. The newest copy
/// is the one readers try first.
#[derive(Default)]
struct PackIndexMap {
    copies: HashMap<[u8; 16], Vec<PackIndexEntry>>,
    len: usize,
}

type SharedIndexMap = Arc<RwLock<PackIndexMap>>;

impl PackIndexMap {
    fn insert(&mut self, hash: [u8; 16], entry: PackIndexEntry) {
        self.copies.entry(hash).or_default().push(entry);
        self.len += 1;
    }

    fn from_entries(entries: impl IntoIterator<Item = ([u8; 16], PackIndexEntry)>) -> Self {
        let mut map = Self::default();
        for (hash, entry) in entries {
            map.insert(hash, entry);
        }
        map
    }

    /// Copies of `hash`, newest first.
    fn copies_newest_first(&self, hash: &[u8; 16]) -> Vec<PackIndexEntry> {
        self.copies
            .get(hash)
            .map(|copies| copies.iter().rev().copied().collect())
            .unwrap_or_default()
    }

    /// Makes `offset` the copy tried first for `hash`.
    fn prefer(&mut self, hash: &[u8; 16], offset: u64) {
        if let Some(copies) = self.copies.get_mut(hash) {
            if let Some(position) = copies.iter().position(|entry| entry.offset == offset) {
                let entry = copies.remove(position);
                copies.push(entry);
            }
        }
    }
}

/// Lookup maps held in memory: the active pack's (always, it holds entries
/// not yet on disk) and a bounded LRU of sealed packs' maps, loaded from the
/// index file in one read when a pack is first needed.
struct IndexMaps {
    active_pack_id: u64,
    active: SharedIndexMap,
    /// Least recently used first.
    sealed: Vec<(u64, SharedIndexMap)>,
    sealed_entries: usize,
}

pub struct PackStore {
    packs_dir: PathBuf,
    active: Mutex<ActivePack>,
    /// Per pack: held shared by readers for the whole index-lookup-plus-
    /// payload-read sequence and exclusively while GC swaps a rewritten pack
    /// into place, so a reader never pairs an offset from one pack generation
    /// with the file of another. Readers of other packs never wait on a swap.
    swap_locks: Mutex<HashMap<u64, Arc<RwLock<()>>>>,
    /// Set when a pack swap failed after its first destructive step. The
    /// active-pack handles may then point at a file that is no longer the
    /// canonical pack, so every later append and sync fails until a restart
    /// recovers the pack and its index from disk.
    swap_failure: Mutex<Option<String>>,
    index_maps: Mutex<IndexMaps>,
    /// Serializes loading sealed maps so concurrent readers of one pack load
    /// its index once.
    index_map_load: Mutex<()>,
    /// Upper bound for the entries of the sealed-pack maps kept in memory;
    /// the most recently used map is always kept.
    index_map_capacity_entries: usize,
    pack_file_cache: Cache<u64, Arc<File>>,
    max_pack_size_bytes: u64,
    /// Number of records appended since open. Incremented under the
    /// active-pack lock once a record is written.
    append_watermark: AtomicU64,
    /// Highest `append_watermark` known to be on disk.
    durable_watermark: AtomicU64,
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|handle| handle.sync_all())
        .with_context(|| format!("failed to sync directory {}", dir.display()))
}

impl PackStore {
    pub fn open(
        packs_dir: &Path,
        active_pack_id: u64,
        index_cache_capacity: u64,
        max_pack_size_bytes: u64,
    ) -> Result<Self> {
        ensure_dir(packs_dir)?;

        for subdir in ["A", "B", "C", "D", "E", "F"] {
            let subdir_path = packs_dir.join(subdir);
            ensure_dir(&subdir_path)?;
        }
        // Subdirectories and pack files created here must survive a power
        // loss before metadata may reference chunks in them.
        sync_dir(packs_dir)?;

        let existing_pack_ids = Self::list_existing_pack_ids(packs_dir)?;
        let resolved_active_pack_id = existing_pack_ids
            .iter()
            .copied()
            .max()
            .map(|existing_max| existing_max.max(active_pack_id))
            .unwrap_or(active_pack_id);

        Self::repair_active_pack_tail(packs_dir, resolved_active_pack_id)?;
        let mut all_pack_ids = existing_pack_ids;
        all_pack_ids.insert(resolved_active_pack_id);
        // Before any handle is opened: a rebuild replaces the index file.
        for pack_id in all_pack_ids.iter().copied() {
            Self::ensure_index_covers_pack(packs_dir, pack_id)?;
        }

        let path = Self::pack_path_impl(packs_dir, resolved_active_pack_id);
        let index_path = Self::index_path_impl(packs_dir, resolved_active_pack_id);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open active pack file {}", path.display()))?;
        set_file_mode(&path)?;
        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&index_path)
            .with_context(|| format!("failed to open pack index file {}", index_path.display()))?;
        set_file_mode(&index_path)?;
        sync_dir(path.parent().context("pack path has no parent directory")?)?;
        let size_bytes = file
            .metadata()
            .with_context(|| format!("failed to stat active pack file {}", path.display()))?
            .len();
        let active_map: SharedIndexMap = Arc::new(RwLock::new(PackIndexMap::from_entries(
            Self::read_index_entries_at(&index_path)?,
        )));

        let store = Self {
            packs_dir: packs_dir.to_path_buf(),
            swap_locks: Mutex::new(HashMap::new()),
            swap_failure: Mutex::new(None),
            append_watermark: AtomicU64::new(0),
            durable_watermark: AtomicU64::new(0),
            active: Mutex::new(ActivePack {
                pack_id: resolved_active_pack_id,
                file,
                index_file,
                size_bytes,
                pending_index: Vec::new(),
                index_map: Arc::clone(&active_map),
            }),
            index_maps: Mutex::new(IndexMaps {
                active_pack_id: resolved_active_pack_id,
                active: active_map,
                sealed: Vec::new(),
                sealed_entries: 0,
            }),
            index_map_load: Mutex::new(()),
            index_map_capacity_entries: usize::try_from(index_cache_capacity)
                .context("pack index cache capacity does not fit in memory")?,
            pack_file_cache: Cache::builder()
                .max_capacity(PACK_FILE_CACHE_CAPACITY)
                .build(),
            max_pack_size_bytes,
        };

        Ok(store)
    }

    /// Fails once a pack swap broke the active-pack handles.
    fn ensure_no_swap_failure(&self) -> Result<()> {
        match self.swap_failure.lock().as_ref() {
            Some(reason) => bail!(
                "pack store stopped accepting writes after a failed pack swap ({reason}); restart the daemon to recover the pack from disk"
            ),
            None => Ok(()),
        }
    }

    pub fn append_chunk_with_crc32(
        &self,
        chunk_hash: [u8; 16],
        codec: u8,
        uncompressed_len: u32,
        compressed_data: &[u8],
        payload_crc32: u32,
    ) -> Result<u64> {
        let mut active = self.active.lock();
        self.ensure_no_swap_failure()?;
        self.make_room(&mut active, Self::record_len(compressed_data.len()))?;
        Self::append_record_locked(
            &mut active,
            chunk_hash,
            codec,
            uncompressed_len,
            compressed_data,
            payload_crc32,
        )?;
        self.append_watermark.fetch_add(1, Ordering::Release);
        Ok(active.pack_id)
    }

    /// Appends several stored copies of one chunk, in order, to a single pack
    /// and returns its id. The copies are kept together because a chunk record
    /// names one pack; when they do not fit together in the remaining space,
    /// the pack is rotated first and the new pack may exceed the size target.
    pub fn append_chunk_copies(
        &self,
        chunk_hash: [u8; 16],
        copies: &[(PackIndexEntry, Vec<u8>)],
    ) -> Result<u64> {
        let group_len = copies
            .iter()
            .map(|(_, payload)| Self::record_len(payload.len()))
            .sum::<u64>();
        let mut active = self.active.lock();
        self.ensure_no_swap_failure()?;
        self.make_room(&mut active, group_len)?;
        for (entry, payload) in copies {
            Self::append_record_locked(
                &mut active,
                chunk_hash,
                entry.codec,
                entry.uncompressed_len,
                payload,
                entry.payload_crc32,
            )?;
            self.append_watermark.fetch_add(1, Ordering::Release);
        }
        Ok(active.pack_id)
    }

    fn append_record_locked(
        active: &mut ActivePack,
        chunk_hash: [u8; 16],
        codec: u8,
        uncompressed_len: u32,
        compressed_data: &[u8],
        payload_crc32: u32,
    ) -> Result<()> {
        if compressed_data.len() > u32::MAX as usize {
            bail!("compressed chunk too large: {}", compressed_data.len());
        }
        let record_len = Self::record_len(compressed_data.len());
        let next_size = active
            .size_bytes
            .checked_add(record_len)
            .context("active pack size overflow")?;
        let record_offset = active.size_bytes;
        let compressed_len = compressed_data.len() as u32;
        let header = PackRecordHeader {
            magic: RECORD_MAGIC,
            hash: chunk_hash,
            codec,
            reserved: RECORD_RESERVED,
            uncompressed_len,
            compressed_len,
        };
        let header_bytes = Self::encode_pack_header(&header)?;

        // One write per record: content-defined chunks are small, so the
        // syscall count matters more than the copy of the payload.
        let mut record = Vec::with_capacity(record_len as usize);
        record.extend_from_slice(&header_bytes);
        record.extend_from_slice(compressed_data);
        let file = &mut active.file;
        if let Err(err) = file.write_all(&record) {
            // A partial record (e.g. ENOSPC) would shift every later record
            // away from the offset recorded in its index entry, so cut the
            // pack back to the last complete record before reporting.
            if let Err(rollback_err) = file.set_len(record_offset) {
                active.size_bytes = file
                    .metadata()
                    .context("failed to stat active pack after failed rollback")?
                    .len();
                bail!(
                    "failed to append pack record ({err}) and failed to truncate the partial record ({rollback_err}); pack {} now has a torn record at offset {}",
                    active.pack_id,
                    record_offset
                );
            }
            return Err(err).context("failed to append pack record");
        }

        let index = PackIndexEntry {
            offset: record_offset,
            codec,
            uncompressed_len,
            compressed_len,
            payload_crc32,
        };
        // Buffer the index entry instead of writing it immediately.
        // Index entries are flushed to disk in a single write during sync().
        // The lookup map is updated immediately so reads are still correct.
        active.pending_index.push((chunk_hash, index));
        active.index_map.write().insert(chunk_hash, index);
        active.size_bytes = next_size;
        Ok(())
    }

    /// Reads chunk `expected_hash` from `pack_id` and returns what `accept`
    /// produces from its stored payload.
    ///
    /// A pack can hold several copies of one hash: a commit retry appends the
    /// chunk again, a repair stores a fresh copy of a damaged chunk, and older
    /// versions appended chunks that already existed. Copies are not
    /// interchangeable when they differ (vault ciphertext is sealed with a
    /// random nonce, and only the nonce of one copy is in the chunk record),
    /// so `accept` decides: it must fail for a copy that does not belong to
    /// the record, e.g. when AEAD authentication or the CRC fails. Copies are
    /// tried from newest to oldest, including entries not yet flushed to the
    /// index file; an accepted older copy is tried first from then on.
    pub fn read_chunk_with<T>(
        &self,
        pack_id: u64,
        expected_hash: [u8; 16],
        expected_codec: u8,
        expected_uncompressed_len: u32,
        expected_compressed_len: u32,
        mut accept: impl FnMut(&[u8], &PackIndexEntry) -> Result<T>,
    ) -> Result<T> {
        let swap_lock = self.swap_lock(pack_id);
        let _swap_guard = swap_lock.read();
        let map = self.index_map(pack_id)?;
        let copies = map.read().copies_newest_first(&expected_hash);
        let Some(primary) = copies.first().copied() else {
            bail!(
                "missing pack index entry for pack {} hash {:x?}",
                pack_id,
                expected_hash
            );
        };
        let mut primary_err = None;
        for entry in &copies {
            let result = self
                .read_indexed_copy(
                    pack_id,
                    expected_hash,
                    entry,
                    expected_codec,
                    expected_uncompressed_len,
                    expected_compressed_len,
                )
                .and_then(|payload| accept(&payload, entry));
            match result {
                Ok(value) => {
                    if let Some(rejected) = primary_err {
                        tracing::error!(
                            pack_id,
                            chunk_hash = ?expected_hash,
                            rejected_offset = primary.offset,
                            accepted_offset = entry.offset,
                            rejected_reason = %format!("{rejected:#}"),
                            "newest pack copy of chunk does not match its record; resolved to an older duplicate copy"
                        );
                        map.write().prefer(&expected_hash, entry.offset);
                    }
                    return Ok(value);
                }
                Err(err) => {
                    if primary_err.is_none() {
                        primary_err = Some(err);
                    } else {
                        tracing::warn!(
                            pack_id,
                            chunk_hash = ?expected_hash,
                            offset = entry.offset,
                            error = %format!("{err:#}"),
                            "duplicate chunk copy rejected"
                        );
                    }
                }
            }
        }

        let primary_err = primary_err.context("chunk copies were tried without a result")?;
        Err(primary_err.context(format!(
            "no stored copy of chunk {:x?} in pack {} is usable ({} other indexed copies tried)",
            expected_hash,
            pack_id,
            copies.len() - 1
        )))
    }

    /// Reads every indexed copy of `hash` in `pack_id`, oldest first, without
    /// interpreting the payloads. Used to move chunks between packs while the
    /// vault may be locked, so that the copy matching the chunk record is
    /// carried along whichever one it is.
    pub fn read_indexed_copies(
        &self,
        pack_id: u64,
        hash: [u8; 16],
    ) -> Result<Vec<(PackIndexEntry, Vec<u8>)>> {
        let swap_lock = self.swap_lock(pack_id);
        let _swap_guard = swap_lock.read();
        let mut entries = self.index_map(pack_id)?.read().copies_newest_first(&hash);
        entries.reverse();
        if entries.is_empty() {
            bail!(
                "missing pack index entry for pack {} hash {:x?}",
                pack_id,
                hash
            );
        }
        let mut copies = Vec::with_capacity(entries.len());
        for entry in entries {
            let payload = self.read_indexed_copy(
                pack_id,
                hash,
                &entry,
                entry.codec,
                entry.uncompressed_len,
                entry.compressed_len,
            )?;
            copies.push((entry, payload));
        }
        Ok(copies)
    }

    /// The swap lock of `pack_id`.
    fn swap_lock(&self, pack_id: u64) -> Arc<RwLock<()>> {
        Arc::clone(self.swap_locks.lock().entry(pack_id).or_default())
    }

    /// The lookup map of `pack_id`. Callers hold that pack's swap lock, so the
    /// map matches the pack file they read. A sealed pack's map is loaded from
    /// its index file in one read and kept in a bounded LRU.
    fn index_map(&self, pack_id: u64) -> Result<SharedIndexMap> {
        if let Some(map) = self.cached_index_map(pack_id) {
            return Ok(map);
        }
        let _load_guard = self.index_map_load.lock();
        if let Some(map) = self.cached_index_map(pack_id) {
            return Ok(map);
        }
        let map: SharedIndexMap = Arc::new(RwLock::new(PackIndexMap::from_entries(
            Self::load_index_file(&self.index_path(pack_id))?,
        )));
        self.insert_sealed_index_map(pack_id, Arc::clone(&map));
        Ok(map)
    }

    fn cached_index_map(&self, pack_id: u64) -> Option<SharedIndexMap> {
        let mut maps = self.index_maps.lock();
        if maps.active_pack_id == pack_id {
            return Some(Arc::clone(&maps.active));
        }
        let position = maps.sealed.iter().position(|(id, _)| *id == pack_id)?;
        let entry = maps.sealed.remove(position);
        let map = Arc::clone(&entry.1);
        maps.sealed.push(entry);
        Some(map)
    }

    /// Adds (or replaces) the map of a sealed pack as the most recently used
    /// one and evicts the least recently used maps above the capacity.
    fn insert_sealed_index_map(&self, pack_id: u64, map: SharedIndexMap) {
        let entries = map.read().len;
        let mut maps = self.index_maps.lock();
        if let Some(position) = maps.sealed.iter().position(|(id, _)| *id == pack_id) {
            let (_, old) = maps.sealed.remove(position);
            maps.sealed_entries -= old.read().len;
        }
        maps.sealed.push((pack_id, map));
        maps.sealed_entries += entries;
        while maps.sealed.len() > 1 && maps.sealed_entries > self.index_map_capacity_entries {
            let (_, evicted) = maps.sealed.remove(0);
            maps.sealed_entries -= evicted.read().len;
        }
    }

    /// Reads a whole index file in one read and decodes its entries in order.
    fn load_index_file(path: &Path) -> Result<Vec<([u8; 16], PackIndexEntry)>> {
        let raw = match std::fs::read(path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to read pack index {}", path.display()));
            }
        };
        if raw.len() % INDEX_ENTRY_LEN != 0 {
            bail!(
                "pack index {} has a torn entry ({} bytes)",
                path.display(),
                raw.len()
            );
        }
        let mut out = Vec::with_capacity(raw.len() / INDEX_ENTRY_LEN);
        let mut aligned = AlignedBytes::<INDEX_ENTRY_LEN>::zeroed();
        for record in raw.chunks_exact(INDEX_ENTRY_LEN) {
            aligned.as_mut_slice().copy_from_slice(record);
            out.push(Self::decode_index_record(aligned.as_slice())?);
        }
        Ok(out)
    }

    /// Reads the payload of the record `index` points at after checking that
    /// the index entry and the record header describe the expected chunk.
    fn read_indexed_copy(
        &self,
        pack_id: u64,
        expected_hash: [u8; 16],
        index: &PackIndexEntry,
        expected_codec: u8,
        expected_uncompressed_len: u32,
        expected_compressed_len: u32,
    ) -> Result<Vec<u8>> {
        if index.codec != expected_codec {
            bail!(
                "pack index codec mismatch for pack {} hash {:x?}: expected {}, got {}",
                pack_id,
                expected_hash,
                expected_codec,
                index.codec
            );
        }

        if index.uncompressed_len != expected_uncompressed_len {
            bail!(
                "pack index uncompressed length mismatch for pack {} hash {:x?}: expected {}, got {}",
                pack_id,
                expected_hash,
                expected_uncompressed_len,
                index.uncompressed_len
            );
        }
        if index.compressed_len != expected_compressed_len {
            bail!(
                "pack index compressed length mismatch for pack {} hash {:x?}: expected {}, got {}",
                pack_id,
                expected_hash,
                expected_compressed_len,
                index.compressed_len
            );
        }

        let path = self.pack_path(pack_id);
        let file = self.open_pack_file(pack_id)?;

        let mut raw_header = AlignedBytes::<RECORD_HEADER_LEN>::zeroed();
        file.read_exact_at(raw_header.as_mut_slice(), index.offset)
            .with_context(|| {
                format!("failed to read pack record header from {}", path.display())
            })?;
        let header = Self::decode_pack_header(raw_header.as_slice())?;

        if header.magic != RECORD_MAGIC {
            bail!("pack record magic mismatch for {}", path.display());
        }

        if header.hash != expected_hash {
            bail!("pack record hash mismatch for {}", path.display());
        }

        if header.codec != index.codec {
            bail!(
                "pack record codec mismatch for {}: index {}, header {}",
                path.display(),
                index.codec,
                header.codec
            );
        }

        if header.uncompressed_len != expected_uncompressed_len {
            bail!(
                "pack record uncompressed length mismatch for {}: expected {}, got {}",
                path.display(),
                expected_uncompressed_len,
                header.uncompressed_len
            );
        }

        if header.compressed_len != index.compressed_len {
            bail!(
                "pack record compressed length mismatch for {}: index {}, header {}",
                path.display(),
                index.compressed_len,
                header.compressed_len
            );
        }

        let payload_offset = index
            .offset
            .checked_add(RECORD_HEADER_LEN_U64)
            .context("pack record offset overflow while reading payload")?;
        let mut payload = vec![0_u8; header.compressed_len as usize];
        file.read_exact_at(&mut payload, payload_offset)
            .with_context(|| format!("failed to read pack payload from {}", path.display()))?;

        Ok(payload)
    }

    /// On-disk size of a record holding `payload_len` payload bytes.
    fn record_len(payload_len: usize) -> u64 {
        RECORD_HEADER_LEN_U64 + payload_len as u64
    }

    fn make_room(&self, active: &mut ActivePack, bytes: u64) -> Result<()> {
        while active.size_bytes > 0 {
            let next_size = active
                .size_bytes
                .checked_add(bytes)
                .context("active pack size overflow")?;
            if next_size <= self.max_pack_size_bytes {
                break;
            }
            self.rotate_active_pack(active)?;
        }
        Ok(())
    }

    /// Makes everything appended so far durable.
    ///
    /// Only the index flush runs under the active-pack lock. The fsync goes
    /// through duplicated handles so appends and reads continue while the disk
    /// works; data appended before this call is covered either way, and a
    /// rotation syncs the pack it retires itself.
    pub fn sync(&self, full: bool) -> Result<()> {
        let (watermark, file, index_file) = {
            let mut active = self.active.lock();
            self.ensure_no_swap_failure()?;
            Self::flush_pending_index(&mut active)?;
            (
                self.append_watermark.load(Ordering::Acquire),
                active
                    .file
                    .try_clone()
                    .context("failed to duplicate active pack handle for sync")?,
                active
                    .index_file
                    .try_clone()
                    .context("failed to duplicate active pack index handle for sync")?,
            )
        };
        if full {
            file.sync_all().context("failed to sync_all active pack")?;
            index_file
                .sync_all()
                .context("failed to sync_all active pack index")?;
        } else {
            file.sync_data().context("failed to sync_data active pack")?;
            index_file
                .sync_data()
                .context("failed to sync_data active pack index")?;
        }
        self.durable_watermark.fetch_max(watermark, Ordering::AcqRel);
        Ok(())
    }

    /// Watermark of the newest appended record.
    pub fn append_watermark(&self) -> u64 {
        self.append_watermark.load(Ordering::Acquire)
    }

    /// Highest append watermark known to be on disk.
    pub fn durable_watermark(&self) -> u64 {
        self.durable_watermark.load(Ordering::Acquire)
    }

    /// Makes every record up to `watermark` durable, syncing only when one is
    /// not yet known durable.
    pub fn sync_to(&self, watermark: u64) -> Result<()> {
        if self.durable_watermark() >= watermark {
            return Ok(());
        }
        self.sync(false)
    }

    /// Write all buffered index entries to the index file in a single batch.
    fn flush_pending_index(active: &mut ActivePack) -> Result<()> {
        if active.pending_index.is_empty() {
            return Ok(());
        }
        // Pre-encode all entries into one contiguous buffer.
        let mut buf = Vec::with_capacity(active.pending_index.len() * INDEX_ENTRY_LEN);
        for (hash, entry) in &active.pending_index {
            let record = PackIndexRecord {
                hash: *hash,
                offset: entry.offset,
                codec: entry.codec,
                reserved: RECORD_RESERVED,
                uncompressed_len: entry.uncompressed_len,
                compressed_len: entry.compressed_len,
                payload_crc32: entry.payload_crc32,
            };
            let raw = Self::encode_index_record(&record)?;
            buf.extend_from_slice(&raw);
        }
        let index_len = active
            .index_file
            .metadata()
            .context("failed to stat active pack index")?
            .len();
        if let Err(err) = active.index_file.write_all(&buf) {
            // Keep the index aligned to whole entries; the pending entries stay
            // buffered and are written again by the next flush.
            active
                .index_file
                .set_len(index_len)
                .with_context(|| format!("failed to roll back partial index write ({err})"))?;
            return Err(err).context("failed to flush buffered index entries");
        }
        active.pending_index.clear();
        Ok(())
    }

    pub fn active_pack_id(&self) -> u64 {
        self.active.lock().pack_id
    }

    pub fn pack_ids(&self) -> Result<Vec<u64>> {
        Ok(Self::list_existing_pack_ids(&self.packs_dir)?
            .into_iter()
            .collect())
    }

    pub fn read_index_entries(&self, pack_id: u64) -> Result<Vec<([u8; 16], PackIndexEntry)>> {
        Self::read_index_entries_at(&self.index_path(pack_id))
    }

    /// Restores the append invariants of the active pack after an unclean stop.
    ///
    /// A record cut short at the end of the pack was never committed, because
    /// payloads are synced before metadata points at them, so it is truncated.
    /// Left in place, the next append would land after the fragment and every
    /// later record would be unreachable for the sequential scanners (GC
    /// rewrite, index rebuild). An index that no longer matches the repaired
    /// pack is rebuilt by `ensure_index_covers_pack`.
    fn repair_active_pack_tail(packs_dir: &Path, pack_id: u64) -> Result<()> {
        let path = Self::pack_path_impl(packs_dir, pack_id);
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to open active pack {}", path.display()));
            }
        };
        let pack_len = file
            .metadata()
            .with_context(|| format!("failed to stat active pack {}", path.display()))?
            .len();

        let mut valid_end = 0_u64;
        while pack_len - valid_end >= RECORD_HEADER_LEN_U64 {
            let mut raw_header = AlignedBytes::<RECORD_HEADER_LEN>::zeroed();
            file.read_exact_at(raw_header.as_mut_slice(), valid_end)
                .with_context(|| format!("failed to scan active pack {}", path.display()))?;
            let header = Self::decode_pack_header(raw_header.as_slice())?;
            if header.magic != RECORD_MAGIC {
                bail!(
                    "corrupt pack record magic at offset {} in {}",
                    valid_end,
                    path.display()
                );
            }
            let record_end = valid_end
                .checked_add(RECORD_HEADER_LEN_U64)
                .and_then(|v| v.checked_add(header.compressed_len as u64))
                .context("pack offset overflow while scanning active pack")?;
            if record_end > pack_len {
                break;
            }
            valid_end = record_end;
        }

        if valid_end < pack_len {
            tracing::warn!(
                pack = %path.display(),
                pack_len,
                valid_end,
                "truncating incomplete record at the end of the active pack"
            );
            file.set_len(valid_end)
                .with_context(|| format!("failed to truncate active pack {}", path.display()))?;
            file.sync_all()
                .with_context(|| format!("failed to sync active pack {}", path.display()))?;
        }

        Ok(())
    }

    fn read_index_entries_at(path: &Path) -> Result<Vec<([u8; 16], PackIndexEntry)>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)
            .with_context(|| format!("failed to open pack index file {}", path.display()))?;
        let mut cursor = 0_u64;
        let mut out = Vec::new();

        loop {
            let mut raw = AlignedBytes::<INDEX_ENTRY_LEN>::zeroed();
            match file.read_exact_at(raw.as_mut_slice(), cursor) {
                Ok(()) => {
                    out.push(Self::decode_index_record(raw.as_slice())?);
                    cursor = cursor
                        .checked_add(INDEX_ENTRY_LEN_U64)
                        .context("pack index cursor overflow")?;
                }
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to scan pack index {}", path.display()));
                }
            }
        }

        Ok(out)
    }

    fn list_existing_pack_ids(packs_dir: &Path) -> Result<BTreeSet<u64>> {
        let mut out = BTreeSet::new();
        for subdir in ["A", "B", "C", "D", "E", "F"] {
            let subdir_path = packs_dir.join(subdir);
            let entries = match std::fs::read_dir(&subdir_path) {
                Ok(entries) => entries,
                Err(err) if err.kind() == ErrorKind::NotFound => continue,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "failed to read packs subdirectory while listing pack ids {}",
                            subdir_path.display()
                        )
                    });
                }
            };
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if !path.is_file() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if !name.starts_with("pack-") || !name.ends_with(".vpk") {
                    continue;
                }
                let number = &name[5..name.len().saturating_sub(4)];
                if let Ok(pack_id) = number.parse::<u64>() {
                    out.insert(pack_id);
                }
            }
        }
        Ok(out)
    }

    fn rotate_active_pack(&self, active: &mut ActivePack) -> Result<()> {
        // Flush buffered index entries before rotating to a new pack.
        Self::flush_pending_index(active)?;
        active
            .file
            .sync_data()
            .context("failed to sync current active pack before rotation")?;
        active
            .index_file
            .sync_data()
            .context("failed to sync current active pack index before rotation")?;

        let mut next_pack_id = active
            .pack_id
            .checked_add(1)
            .context("active pack id overflow during rotation")?;
        loop {
            let next_path = self.pack_path(next_pack_id);
            let next_index_path = self.index_path(next_pack_id);
            Self::ensure_index_covers_pack(&self.packs_dir, next_pack_id)?;
            let next_file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&next_path)
                .with_context(|| {
                    format!("failed to open next pack file {}", next_path.display())
                })?;
            set_file_mode(&next_path)?;
            let next_index_file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&next_index_path)
                .with_context(|| {
                    format!(
                        "failed to open next pack index file {}",
                        next_index_path.display()
                    )
                })?;
            set_file_mode(&next_index_path)?;
            sync_dir(
                next_path
                    .parent()
                    .context("pack path has no parent directory")?,
            )?;
            let next_size = next_file
                .metadata()
                .with_context(|| format!("failed to stat next pack file {}", next_path.display()))?
                .len();

            if next_size >= self.max_pack_size_bytes && next_size > 0 {
                next_pack_id = next_pack_id
                    .checked_add(1)
                    .context("active pack id overflow while skipping full packs")?;
                continue;
            }

            let next_map: SharedIndexMap = Arc::new(RwLock::new(PackIndexMap::from_entries(
                Self::load_index_file(&next_index_path)?,
            )));
            let retired_pack_id = active.pack_id;
            let retired_map = std::mem::replace(&mut active.index_map, Arc::clone(&next_map));
            {
                let mut maps = self.index_maps.lock();
                maps.active_pack_id = next_pack_id;
                maps.active = next_map;
            }
            self.insert_sealed_index_map(retired_pack_id, retired_map);
            active.pack_id = next_pack_id;
            active.file = next_file;
            active.index_file = next_index_file;
            active.size_bytes = next_size;
            active.pending_index = Vec::new();
            return Ok(());
        }
    }

    pub fn pack_size(&self, pack_id: u64) -> Result<u64> {
        let path = self.pack_path(pack_id);
        let size = match std::fs::metadata(&path) {
            Ok(meta) => meta.len(),
            Err(err) if err.kind() == ErrorKind::NotFound => 0,
            Err(err) => {
                return Err(err).with_context(|| format!("failed to stat pack {}", path.display()));
            }
        };
        Ok(size)
    }

    /// Returns the size of the active pack when it is `pack_id`, after writing
    /// its buffered index entries, so a rewrite can copy everything below that
    /// offset without the append lock.
    ///
    /// Callers that append under the filesystem write lock must hold that lock
    /// across this call: every record below the returned offset then has its
    /// metadata committed (or never will), so the live hashes read afterwards
    /// cover it.
    pub fn active_pack_cutoff(&self, pack_id: u64) -> Result<Option<u64>> {
        let mut active = self.active.lock();
        self.ensure_no_swap_failure()?;
        if active.pack_id != pack_id {
            return Ok(None);
        }
        Self::flush_pending_index(&mut active)?;
        Ok(Some(active.size_bytes))
    }

    /// Rewrites `pack_id` keeping only records of `live_hashes`.
    ///
    /// `cutoff` is the active pack's size from `active_pack_cutoff`, or None
    /// for a pack that was not active then. Records below it are filtered;
    /// records appended at or after it are carried over unchanged, because
    /// their metadata may still be committing. The active pack is compacted in
    /// place: appends wait only while that tail is copied and the files are
    /// swapped, then continue on the same pack id, so it still grows to its
    /// size limit.
    ///
    /// Every copy of a live hash that the pack index references is kept in its
    /// original order, so readers resolve exactly the same copy afterwards
    /// (duplicate copies of one hash can differ, e.g. vault ciphertext sealed
    /// with different nonces). Records the index does not reference were never
    /// readable and are dropped. Each kept payload is verified against its
    /// index CRC32: a damaged copy is dropped only when an intact copy of the
    /// same hash is kept, otherwise the rewrite aborts instead of carrying
    /// damaged data forward under a fresh checksum.
    ///
    /// The long copy runs before the swap lock is taken, so reads of this pack
    /// continue against the current file. The swap is crash-safe: the old
    /// index is removed before the new pack is renamed into place, so a crash
    /// between the renames leaves a pack without an index, which startup
    /// rebuilds from the pack itself. The active pack's replacement handles
    /// are opened before the swap; if any swap step fails, the store refuses
    /// further appends and syncs instead of writing through handles that may
    /// be detached from the canonical pack.
    pub fn rewrite_pack_with_live_hashes(
        &self,
        pack_id: u64,
        live_hashes: &HashSet<[u8; 16]>,
        cutoff: Option<u64>,
    ) -> Result<()> {
        self.ensure_no_swap_failure()?;
        let src_pack_path = self.pack_path(pack_id);
        if !src_pack_path.exists() {
            return Ok(());
        }
        let copy_end = match cutoff {
            Some(cutoff) => cutoff,
            None => self.pack_size(pack_id)?,
        };
        let src_idx_path = self.index_path(pack_id);
        let src_entries = self
            .read_index_entries(pack_id)?
            .into_iter()
            .filter(|(_, entry)| entry.offset < copy_end)
            .collect::<Vec<_>>();
        let indexed_by_offset = src_entries
            .iter()
            .map(|(hash, entry)| (entry.offset, (*hash, *entry)))
            .collect::<HashMap<_, _>>();
        let indexed_hashes = src_entries
            .iter()
            .map(|(hash, _)| *hash)
            .collect::<HashSet<_>>();

        let tmp_pack_path = src_pack_path.with_extension("vpk.rewrite");
        let tmp_idx_path = src_idx_path.with_extension("idx.rewrite");
        let src_file = File::open(&src_pack_path)
            .with_context(|| format!("failed to open source pack {}", src_pack_path.display()))?;
        let mut dst_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_pack_path)
            .with_context(|| format!("failed to open temp pack {}", tmp_pack_path.display()))?;
        set_file_mode(&tmp_pack_path)?;
        let mut dst_index = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_idx_path)
            .with_context(|| {
                format!("failed to open temp pack index {}", tmp_idx_path.display())
            })?;
        set_file_mode(&tmp_idx_path)?;

        let mut cursor = 0_u64;
        let mut new_offset = 0_u64;
        let mut kept_offsets = HashSet::<u64>::new();
        // Copies that fail their CRC32. One is dropped only when an intact
        // copy of the same hash is kept (a write stored the content again
        // after the damage was found); otherwise the rewrite is refused.
        let mut damaged = Vec::<([u8; 16], u64)>::new();
        // Entries of the rewritten index, in order, for its lookup map.
        let mut new_entries = Vec::<([u8; 16], PackIndexEntry)>::new();
        while cursor < copy_end {
            let mut raw_header = AlignedBytes::<RECORD_HEADER_LEN>::zeroed();
            match src_file.read_exact_at(raw_header.as_mut_slice(), cursor) {
                Ok(()) => {
                    let header = Self::decode_pack_header(raw_header.as_slice())?;
                    if header.magic != RECORD_MAGIC {
                        bail!(
                            "corrupt pack record magic at offset {} in {}",
                            cursor,
                            src_pack_path.display()
                        );
                    }

                    let hash = header.hash;
                    let codec = header.codec;
                    let ulen = header.uncompressed_len;
                    let clen = header.compressed_len;

                    let record_len = RECORD_HEADER_LEN_U64
                        .checked_add(clen as u64)
                        .context("record length overflow while rewriting pack")?;

                    let indexed = indexed_by_offset
                        .get(&cursor)
                        .filter(|_| live_hashes.contains(&hash))
                        .copied();
                    if let Some((indexed_hash, entry)) = indexed {
                        if indexed_hash != hash
                            || entry.codec != codec
                            || entry.uncompressed_len != ulen
                            || entry.compressed_len != clen
                        {
                            bail!(
                                "pack index entry at offset {} in {} does not match the record header",
                                cursor,
                                src_pack_path.display()
                            );
                        }
                        let mut payload = vec![0_u8; clen as usize];
                        src_file
                            .read_exact_at(&mut payload, cursor + RECORD_HEADER_LEN_U64)
                            .with_context(|| {
                                format!(
                                    "failed to read pack payload while rewriting {}",
                                    src_pack_path.display()
                                )
                            })?;
                        let payload_crc32 = crc32c::crc32c(&payload);
                        if payload_crc32 != entry.payload_crc32 {
                            tracing::error!(
                                pack_id,
                                chunk_hash = ?hash,
                                offset = cursor,
                                index_crc32 = entry.payload_crc32,
                                payload_crc32,
                                "damaged chunk copy found while rewriting pack"
                            );
                            damaged.push((hash, cursor));
                            cursor = cursor
                                .checked_add(record_len)
                                .context("source offset overflow during pack rewrite")?;
                            continue;
                        }

                        dst_file.write_all(raw_header.as_slice()).with_context(|| {
                            format!(
                                "failed writing pack header while rewriting {}",
                                src_pack_path.display()
                            )
                        })?;
                        dst_file.write_all(&payload).with_context(|| {
                            format!(
                                "failed writing pack payload while rewriting {}",
                                src_pack_path.display()
                            )
                        })?;
                        let kept_entry = PackIndexEntry {
                            offset: new_offset,
                            codec,
                            uncompressed_len: ulen,
                            compressed_len: clen,
                            payload_crc32,
                        };
                        Self::append_index_record(&mut dst_index, hash, kept_entry)?;
                        new_entries.push((hash, kept_entry));
                        kept_offsets.insert(cursor);
                        new_offset = new_offset
                            .checked_add(record_len)
                            .context("destination offset overflow during pack rewrite")?;
                    }

                    cursor = cursor
                        .checked_add(record_len)
                        .context("source offset overflow during pack rewrite")?;
                }
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "failed reading source pack while rewriting {}",
                            src_pack_path.display()
                        )
                    });
                }
            }
        }

        let damaged_offsets = damaged.iter().map(|(_, offset)| *offset).collect::<HashSet<_>>();
        for (hash, entry) in src_entries.iter() {
            if live_hashes.contains(hash)
                && !kept_offsets.contains(&entry.offset)
                && !damaged_offsets.contains(&entry.offset)
            {
                bail!(
                    "pack index entry for live chunk {:x?} at offset {} does not point at a record in {}; refusing to rewrite pack",
                    hash,
                    entry.offset,
                    src_pack_path.display()
                );
            }
        }

        // Appends wait from here until the swap is done, so nothing reaches
        // the old file after its tail is copied.
        let mut active = Some(self.active.lock()).filter(|active| active.pack_id == pack_id);
        let tail_end = match active.as_mut() {
            Some(active) => {
                Self::flush_pending_index(active)?;
                active.size_bytes
            }
            None => self.pack_size(pack_id)?,
        };
        let tail_entries = if tail_end > copy_end {
            self.copy_pack_tail(
                &src_file,
                &mut dst_file,
                &mut dst_index,
                pack_id,
                copy_end,
                tail_end,
                new_offset,
            )?
        } else {
            Vec::new()
        };
        let tail_hashes = tail_entries
            .iter()
            .map(|(hash, _)| *hash)
            .collect::<HashSet<_>>();
        if let Some(hash) = live_hashes
            .iter()
            .find(|hash| !indexed_hashes.contains(*hash) && !tail_hashes.contains(*hash))
        {
            bail!(
                "live chunk {:x?} of pack {} has no entry in the pack index; refusing to rewrite the pack, which would drop it",
                hash,
                pack_id
            );
        }
        let intact_hashes = src_entries
            .iter()
            .filter(|(_, entry)| kept_offsets.contains(&entry.offset))
            .map(|(hash, _)| *hash)
            .chain(tail_entries.iter().map(|(hash, _)| *hash))
            .collect::<HashSet<_>>();
        if let Some((hash, offset)) = damaged
            .iter()
            .find(|(hash, _)| !intact_hashes.contains(hash))
        {
            bail!(
                "pack payload crc32 mismatch for live chunk {:x?} at offset {} in {} and no intact copy of it; refusing to rewrite pack",
                hash,
                offset,
                src_pack_path.display()
            );
        }
        let new_size = new_offset
            .checked_add(tail_end.saturating_sub(copy_end))
            .context("destination size overflow during pack rewrite")?;

        dst_file.sync_all().with_context(|| {
            format!("failed to sync rewritten pack {}", tmp_pack_path.display())
        })?;
        dst_index.sync_all().with_context(|| {
            format!(
                "failed to sync rewritten pack index {}",
                tmp_idx_path.display()
            )
        })?;
        drop(dst_file);
        drop(dst_index);

        let pack_dir = src_pack_path
            .parent()
            .context("pack path has no parent directory")?;
        // The handles that continue appending to a compacted active pack are
        // opened on the new files before anything is replaced; they follow
        // the files through the renames below.
        let replacement_handles = match active.as_ref() {
            Some(_) => Some((
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&tmp_pack_path)
                    .with_context(|| {
                        format!("failed to open compacted pack {}", tmp_pack_path.display())
                    })?,
                OpenOptions::new()
                    .read(true)
                    .append(true)
                    .open(&tmp_idx_path)
                    .with_context(|| {
                        format!(
                            "failed to open compacted pack index {}",
                            tmp_idx_path.display()
                        )
                    })?,
            )),
            None => None,
        };
        // The lookup map of the new generation is built before readers are
        // blocked, so the swap itself stays short.
        let new_map: SharedIndexMap = Arc::new(RwLock::new(PackIndexMap::from_entries(
            new_entries.into_iter().chain(tail_entries.iter().copied()),
        )));
        let swap_lock = self.swap_lock(pack_id);
        let _swap_guard = swap_lock.write();
        let swap_result = Self::swap_rewritten_pack(
            pack_dir,
            &src_pack_path,
            &src_idx_path,
            &tmp_pack_path,
            &tmp_idx_path,
        );
        if let Err(err) = swap_result {
            // The old index is already gone and the pack may already be
            // replaced, so the active handles can no longer be trusted.
            let reason = format!("{err:#}");
            tracing::error!(
                pack_id,
                error = %reason,
                "pack swap failed after its first destructive step; refusing further pack writes"
            );
            *self.swap_failure.lock() = Some(reason);
            return Err(err);
        }

        match (active.as_mut(), replacement_handles) {
            (Some(active), Some((file, index_file))) => {
                active.file = file;
                active.index_file = index_file;
                active.size_bytes = new_size;
                active.index_map = Arc::clone(&new_map);
                self.index_maps.lock().active = new_map;
            }
            _ => self.insert_sealed_index_map(pack_id, new_map),
        }
        self.pack_file_cache.invalidate(&pack_id);
        tracing::info!(
            pack_id,
            active = active.is_some(),
            source_bytes = tail_end,
            kept_bytes = new_size,
            carried_tail_bytes = tail_end - copy_end,
            "rewrote pack keeping live records"
        );
        Ok(())
    }

    /// Replaces a pack and its index with their rewritten versions: removes
    /// the old index, renames the pack, renames the index, and syncs the
    /// directory after each step, so a crash leaves at most a pack without
    /// an index, which startup rebuilds.
    fn swap_rewritten_pack(
        pack_dir: &Path,
        src_pack_path: &Path,
        src_idx_path: &Path,
        tmp_pack_path: &Path,
        tmp_idx_path: &Path,
    ) -> Result<()> {
        match std::fs::remove_file(src_idx_path) {
            Ok(()) => {}
            Err(err) if err.kind() == ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to remove old pack index {}", src_idx_path.display())
                });
            }
        }
        sync_dir(pack_dir)?;
        std::fs::rename(tmp_pack_path, src_pack_path).with_context(|| {
            format!(
                "failed to atomically replace pack {} with {}",
                src_pack_path.display(),
                tmp_pack_path.display()
            )
        })?;
        sync_dir(pack_dir)?;
        std::fs::rename(tmp_idx_path, src_idx_path).with_context(|| {
            format!(
                "failed to atomically replace pack index {} with {}",
                src_idx_path.display(),
                tmp_idx_path.display()
            )
        })?;
        sync_dir(pack_dir)
    }

    /// Copies the records in `[from, to)` of a pack being rewritten unchanged
    /// to `dst_offset` of the new pack and appends their index entries,
    /// shifted to the new offsets. Returns the shifted index entries.
    #[allow(clippy::too_many_arguments)]
    fn copy_pack_tail(
        &self,
        src_file: &File,
        dst_file: &mut File,
        dst_index: &mut File,
        pack_id: u64,
        from: u64,
        to: u64,
        dst_offset: u64,
    ) -> Result<Vec<([u8; 16], PackIndexEntry)>> {
        let mut buf = vec![0_u8; 8 * 1024 * 1024];
        let mut cursor = from;
        while cursor < to {
            let len = (to - cursor).min(buf.len() as u64) as usize;
            src_file
                .read_exact_at(&mut buf[..len], cursor)
                .with_context(|| format!("failed to read tail of pack {pack_id} while rewriting"))?;
            dst_file
                .write_all(&buf[..len])
                .with_context(|| format!("failed to write tail of pack {pack_id} while rewriting"))?;
            cursor += len as u64;
        }

        let tail_entries = self
            .read_index_entries(pack_id)?
            .into_iter()
            .filter(|(_, entry)| entry.offset >= from && entry.offset < to)
            .map(|(hash, entry)| {
                (
                    hash,
                    PackIndexEntry {
                        offset: entry.offset - from + dst_offset,
                        ..entry
                    },
                )
            })
            .collect::<Vec<_>>();
        for (hash, entry) in &tail_entries {
            Self::append_index_record(dst_index, *hash, *entry)?;
        }
        Ok(tail_entries)
    }

    fn open_pack_file(&self, pack_id: u64) -> Result<Arc<File>> {
        if let Some(file) = self.pack_file_cache.get(&pack_id) {
            return Ok(file);
        }

        let path = self.pack_path(pack_id);
        let file = Arc::new(
            File::open(&path)
                .with_context(|| format!("failed to open pack file {}", path.display()))?,
        );
        self.pack_file_cache.insert(pack_id, Arc::clone(&file));
        Ok(file)
    }

    fn pack_path(&self, pack_id: u64) -> PathBuf {
        Self::pack_path_impl(&self.packs_dir, pack_id)
    }

    fn index_path(&self, pack_id: u64) -> PathBuf {
        Self::index_path_impl(&self.packs_dir, pack_id)
    }

    fn pack_path_impl(base: &Path, pack_id: u64) -> PathBuf {
        let subdir = match pack_id % 6 {
            0 => "A",
            1 => "B",
            2 => "C",
            3 => "D",
            4 => "E",
            5 => "F",
            _ => unreachable!(),
        };
        base.join(subdir).join(format!("pack-{pack_id:020}.vpk"))
    }

    fn index_path_impl(base: &Path, pack_id: u64) -> PathBuf {
        let subdir = match pack_id % 6 {
            0 => "A",
            1 => "B",
            2 => "C",
            3 => "D",
            4 => "E",
            5 => "F",
            _ => unreachable!(),
        };
        base.join(subdir).join(format!("pack-{pack_id:020}.idx"))
    }

    pub fn verify_pack_headers(&self, pack_id: u64) -> Result<()> {
        let path = self.pack_path(pack_id);
        if !path.exists() {
            return Ok(());
        }

        let file = File::open(&path)
            .with_context(|| format!("failed to open pack file {}", path.display()))?;
        let mut cursor = 0_u64;

        loop {
            let mut raw_header = AlignedBytes::<RECORD_HEADER_LEN>::zeroed();
            match file.read_exact_at(raw_header.as_mut_slice(), cursor) {
                Ok(()) => {
                    let header = Self::decode_pack_header(raw_header.as_slice())?;
                    if header.magic != RECORD_MAGIC {
                        bail!(
                            "corrupt pack record magic at offset {} in {}",
                            cursor,
                            path.display()
                        );
                    }
                    let compressed_len = header.compressed_len as u64;
                    cursor = cursor
                        .checked_add(RECORD_HEADER_LEN_U64)
                        .and_then(|v| v.checked_add(compressed_len))
                        .context("pack offset overflow while validating")?;
                }
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to validate pack headers for {}", path.display())
                    });
                }
            }
        }

        Ok(())
    }

    /// Makes sure the index of `pack_id` describes every record of the pack,
    /// rebuilding it from the pack when it does not.
    ///
    /// Records are appended and indexed in pack order, and GC rewrites and
    /// migrations keep that order, so a complete index is a run of entries
    /// that starts at offset 0, where each entry starts where the previous
    /// record ends and the last one ends at the end of the pack. Anything else
    /// (missing or torn index, an index cut short by an interrupted rebuild,
    /// entries past the end of the pack, complete records written after the
    /// last index flush) means records exist that readers and GC cannot see.
    /// Only the index file is read, so this is cheap enough for every start.
    fn ensure_index_covers_pack(packs_dir: &Path, pack_id: u64) -> Result<()> {
        let pack_path = Self::pack_path_impl(packs_dir, pack_id);
        let pack_len = match std::fs::metadata(&pack_path) {
            Ok(meta) => meta.len(),
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(()),
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to stat pack {}", pack_path.display()));
            }
        };
        let index_path = Self::index_path_impl(packs_dir, pack_id);
        let index_len = match std::fs::metadata(&index_path) {
            Ok(meta) => meta.len(),
            Err(err) if err.kind() == ErrorKind::NotFound => 0,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to stat pack index {}", index_path.display())
                });
            }
        };
        let mut covered_end = 0_u64;
        let mut contiguous = index_len.is_multiple_of(INDEX_ENTRY_LEN_U64);
        if contiguous {
            for (_, entry) in Self::read_index_entries_at(&index_path)? {
                if entry.offset != covered_end {
                    contiguous = false;
                    break;
                }
                covered_end = entry
                    .offset
                    .checked_add(RECORD_HEADER_LEN_U64)
                    .and_then(|v| v.checked_add(entry.compressed_len as u64))
                    .context("pack index entry end overflows")?;
            }
        }
        if contiguous && covered_end == pack_len {
            return Ok(());
        }
        tracing::warn!(
            pack = %pack_path.display(),
            pack_len,
            index_len,
            covered_end,
            contiguous,
            "pack index does not cover the pack; rebuilding it from the pack"
        );
        Self::rebuild_index(packs_dir, pack_id)
    }

    /// Rebuilds the index of `pack_id` from the pack's record headers.
    ///
    /// The new index is written and synced under a temporary name and then
    /// renamed over the old one, so an interruption leaves either the old
    /// index or the complete new one, never a prefix. Payload CRC32s are
    /// computed from the stored bytes; damage that happened before is still
    /// caught on read by the content-hash check.
    fn rebuild_index(packs_dir: &Path, pack_id: u64) -> Result<()> {
        let pack_path = Self::pack_path_impl(packs_dir, pack_id);
        let index_path = Self::index_path_impl(packs_dir, pack_id);
        let tmp_index_path = index_path.with_extension("idx.rebuild");
        let pack = File::open(&pack_path)
            .with_context(|| format!("failed to open pack file {}", pack_path.display()))?;
        let mut index = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_index_path)
            .with_context(|| {
                format!("failed to create rebuilt pack index {}", tmp_index_path.display())
            })?;
        set_file_mode(&tmp_index_path)?;

        let mut cursor = 0_u64;
        loop {
            let mut raw_header = AlignedBytes::<RECORD_HEADER_LEN>::zeroed();
            match pack.read_exact_at(raw_header.as_mut_slice(), cursor) {
                Ok(()) => {
                    let header = Self::decode_pack_header(raw_header.as_slice())?;
                    if header.magic != RECORD_MAGIC {
                        bail!(
                            "corrupt pack record magic while rebuilding index at offset {} in {}",
                            cursor,
                            pack_path.display()
                        );
                    }

                    let payload_offset = cursor
                        .checked_add(RECORD_HEADER_LEN_U64)
                        .context("pack offset overflow while rebuilding index payload")?;
                    let mut payload = vec![0_u8; header.compressed_len as usize];
                    pack.read_exact_at(&mut payload, payload_offset)
                        .with_context(|| {
                            format!(
                                "failed reading pack payload at offset {} while rebuilding index {}",
                                cursor,
                                pack_path.display()
                            )
                        })?;
                    let entry = PackIndexEntry {
                        offset: cursor,
                        codec: header.codec,
                        uncompressed_len: header.uncompressed_len,
                        compressed_len: header.compressed_len,
                        payload_crc32: crc32c::crc32c(&payload),
                    };
                    Self::append_index_record(&mut index, header.hash, entry)?;

                    cursor = payload_offset
                        .checked_add(entry.compressed_len as u64)
                        .context("pack offset overflow while rebuilding index")?;
                }
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "failed while rebuilding index from pack {}",
                            pack_path.display()
                        )
                    });
                }
            }
        }

        index.sync_all().with_context(|| {
            format!("failed to sync rebuilt index {}", tmp_index_path.display())
        })?;
        drop(index);
        std::fs::rename(&tmp_index_path, &index_path).with_context(|| {
            format!(
                "failed to replace pack index {} with rebuilt {}",
                index_path.display(),
                tmp_index_path.display()
            )
        })?;
        sync_dir(
            index_path
                .parent()
                .context("pack index path has no parent directory")?,
        )?;
        tracing::info!(pack = %pack_path.display(), records_bytes = cursor, "rebuilt pack index");
        Ok(())
    }

    fn append_index_record(
        index_file: &mut File,
        hash: [u8; 16],
        entry: PackIndexEntry,
    ) -> Result<()> {
        let record = PackIndexRecord {
            hash,
            offset: entry.offset,
            codec: entry.codec,
            reserved: RECORD_RESERVED,
            uncompressed_len: entry.uncompressed_len,
            compressed_len: entry.compressed_len,
            payload_crc32: entry.payload_crc32,
        };
        let raw = Self::encode_index_record(&record)?;
        index_file
            .write_all(&raw)
            .context("failed to append pack index record")
    }

    fn decode_index_record(raw: &[u8]) -> Result<([u8; 16], PackIndexEntry)> {
        let archived = rkyv::access::<ArchivedPackIndexRecord, rkyv::rancor::Error>(raw)
            .context("failed to decode pack index record")?;
        if archived.reserved != RECORD_RESERVED {
            bail!("pack index record reserved bytes are non-zero");
        }
        Ok((
            archived.hash,
            PackIndexEntry {
                offset: archived.offset.to_native(),
                codec: archived.codec,
                uncompressed_len: archived.uncompressed_len.to_native(),
                compressed_len: archived.compressed_len.to_native(),
                payload_crc32: archived.payload_crc32.to_native(),
            },
        ))
    }

    fn decode_pack_header(raw: &[u8]) -> Result<PackRecordHeader> {
        let archived = rkyv::access::<ArchivedPackRecordHeader, rkyv::rancor::Error>(raw)
            .context("failed to decode pack record header")?;
        if archived.reserved != RECORD_RESERVED {
            bail!("pack record header reserved bytes are non-zero");
        }
        Ok(PackRecordHeader {
            magic: archived.magic,
            hash: archived.hash,
            codec: archived.codec,
            reserved: archived.reserved,
            uncompressed_len: archived.uncompressed_len.to_native(),
            compressed_len: archived.compressed_len.to_native(),
        })
    }

    fn encode_pack_header(header: &PackRecordHeader) -> Result<rkyv::util::AlignedVec> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(header)
            .context("failed to encode pack record header")?;
        if bytes.len() != RECORD_HEADER_LEN {
            bail!(
                "invalid pack record header size: expected {}, got {}",
                RECORD_HEADER_LEN,
                bytes.len()
            );
        }
        Ok(bytes)
    }

    fn encode_index_record(record: &PackIndexRecord) -> Result<rkyv::util::AlignedVec> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(record)
            .context("failed to encode pack index record")?;
        if bytes.len() != INDEX_ENTRY_LEN {
            bail!(
                "invalid pack index record size: expected {}, got {}",
                INDEX_ENTRY_LEN,
                bytes.len()
            );
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::data::compress::CODEC_RAW;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(suffix: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch")
                .as_nanos();
            let path = std::env::temp_dir().join(format!(
                "verfsnext-pack-{suffix}-{}-{nanos}",
                std::process::id()
            ));
            std::fs::create_dir_all(&path).expect("failed to create temporary directory");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn archived_pack_record_sizes_are_stable() {
        let header = PackRecordHeader {
            magic: RECORD_MAGIC,
            hash: [1_u8; 16],
            codec: CODEC_RAW,
            reserved: RECORD_RESERVED,
            uncompressed_len: 1024,
            compressed_len: 1000,
        };
        let index = PackIndexRecord {
            hash: [2_u8; 16],
            offset: 1234,
            codec: CODEC_RAW,
            reserved: RECORD_RESERVED,
            uncompressed_len: 2048,
            compressed_len: 1200,
            payload_crc32: 0xAABBCCDD,
        };

        let header_bytes =
            PackStore::encode_pack_header(&header).expect("header should encode with rkyv");
        let index_bytes =
            PackStore::encode_index_record(&index).expect("index should encode with rkyv");

        assert_eq!(header_bytes.len(), RECORD_HEADER_LEN);
        assert_eq!(index_bytes.len(), INDEX_ENTRY_LEN);
    }

    #[test]
    fn pack_store_roundtrips_payload_and_writes_rkyv_index_records() {
        let tmp = TempDir::new("roundtrip");
        let store =
            PackStore::open(&tmp.path, 0, 1024, 64 * 1024 * 1024).expect("pack store should open");

        let hash = [7_u8; 16];
        let payload = b"verfs-pack-payload".to_vec();
        let pack_id = store
            .append_chunk_with_crc32(
                hash,
                CODEC_RAW,
                payload.len() as u32,
                &payload,
                crc32c::crc32c(&payload),
            )
            .expect("chunk append should succeed");

        let read_back = store
            .read_chunk_with(
                pack_id,
                hash,
                CODEC_RAW,
                payload.len() as u32,
                payload.len() as u32,
                |stored, _| Ok(stored.to_vec()),
            )
            .expect("chunk read should succeed");
        assert_eq!(read_back, payload);
        store.sync(false).expect("pack sync should succeed");

        store
            .verify_pack_headers(pack_id)
            .expect("pack header verification should succeed");

        let index_path = PackStore::index_path_impl(&tmp.path, pack_id);
        let index_len = std::fs::metadata(index_path)
            .expect("index metadata should be readable")
            .len();
        assert_eq!(index_len, INDEX_ENTRY_LEN_U64);
    }
}
