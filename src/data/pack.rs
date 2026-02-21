use std::collections::{BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use moka::sync::Cache;
use parking_lot::Mutex;
use rkyv::{Archive, Deserialize, Serialize};

use crate::data::compress::decompress_chunk;
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
}

const RECORD_HEADER_LEN: usize = size_of::<ArchivedPackRecordHeader>();
const RECORD_HEADER_LEN_U64: u64 = RECORD_HEADER_LEN as u64;
const INDEX_ENTRY_LEN: usize = size_of::<ArchivedPackIndexRecord>();
const INDEX_ENTRY_LEN_U64: u64 = INDEX_ENTRY_LEN as u64;

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
}

struct ActivePack {
    pack_id: u64,
    file: File,
    index_file: File,
    size_bytes: u64,
}

pub struct PackStore {
    packs_dir: PathBuf,
    active: Mutex<ActivePack>,
    index_cache: Cache<(u64, [u8; 16]), PackIndexEntry>,
    max_pack_size_bytes: u64,
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

        let existing_pack_ids = Self::list_existing_pack_ids(packs_dir)?;
        let resolved_active_pack_id = existing_pack_ids
            .iter()
            .copied()
            .max()
            .map(|existing_max| existing_max.max(active_pack_id))
            .unwrap_or(active_pack_id);

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
        let size_bytes = file
            .metadata()
            .with_context(|| format!("failed to stat active pack file {}", path.display()))?
            .len();

        let store = Self {
            packs_dir: packs_dir.to_path_buf(),
            active: Mutex::new(ActivePack {
                pack_id: resolved_active_pack_id,
                file,
                index_file,
                size_bytes,
            }),
            index_cache: Cache::builder().max_capacity(index_cache_capacity).build(),
            max_pack_size_bytes,
        };

        let mut all_pack_ids = existing_pack_ids;
        all_pack_ids.insert(resolved_active_pack_id);
        for pack_id in all_pack_ids {
            store.rebuild_index_if_missing(pack_id)?;
            store.prime_index_cache(pack_id)?;
        }

        Ok(store)
    }

    pub fn append_chunk(
        &self,
        chunk_hash: [u8; 16],
        codec: u8,
        uncompressed_len: u32,
        compressed_data: &[u8],
    ) -> Result<u64> {
        if compressed_data.len() > u32::MAX as usize {
            bail!("compressed chunk too large: {}", compressed_data.len());
        }
        let record_len = RECORD_HEADER_LEN_U64
            .checked_add(compressed_data.len() as u64)
            .context("pack record length overflow")?;
        let mut active = self.active.lock();
        while active.size_bytes > 0 {
            let next_size = active
                .size_bytes
                .checked_add(record_len)
                .context("active pack size overflow")?;
            if next_size <= self.max_pack_size_bytes {
                break;
            }
            self.rotate_active_pack(&mut active)?;
        }
        let next_size = active
            .size_bytes
            .checked_add(record_len)
            .context("active pack size overflow")?;
        let file = &mut active.file;

        let record_offset = file
            .seek(SeekFrom::End(0))
            .context("failed to seek to pack end")?;
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

        file.write_all(&header_bytes)
            .context("failed to write pack record header")?;
        file.write_all(compressed_data)
            .context("failed to write pack record payload")?;

        let index = PackIndexEntry {
            offset: record_offset,
            codec,
            uncompressed_len,
            compressed_len,
        };
        Self::append_index_record(&mut active.index_file, chunk_hash, index)?;

        self.index_cache.insert((active.pack_id, chunk_hash), index);
        active.size_bytes = next_size;

        Ok(active.pack_id)
    }

    pub fn read_chunk(
        &self,
        pack_id: u64,
        expected_hash: [u8; 16],
        expected_codec: u8,
        expected_uncompressed_len: u32,
        expected_compressed_len: u32,
    ) -> Result<Vec<u8>> {
        let payload = self.read_chunk_payload(
            pack_id,
            expected_hash,
            expected_codec,
            expected_uncompressed_len,
            expected_compressed_len,
        )?;
        decompress_chunk(expected_codec, &payload, expected_uncompressed_len)
    }

    pub fn read_chunk_payload(
        &self,
        pack_id: u64,
        expected_hash: [u8; 16],
        expected_codec: u8,
        expected_uncompressed_len: u32,
        expected_compressed_len: u32,
    ) -> Result<Vec<u8>> {
        let index = self
            .lookup_index_entry(pack_id, expected_hash)?
            .with_context(|| {
                format!(
                    "missing pack index entry for pack {} hash {:x?}",
                    pack_id, expected_hash
                )
            })?;

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
        let file = File::open(&path)
            .with_context(|| format!("failed to open pack file {}", path.display()))?;

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

    pub fn sync(&self, full: bool) -> Result<()> {
        let active = self.active.lock();
        if full {
            active
                .file
                .sync_all()
                .context("failed to sync_all active pack")?;
            active
                .index_file
                .sync_all()
                .context("failed to sync_all active pack index")
        } else {
            active
                .file
                .sync_data()
                .context("failed to sync_data active pack")?;
            active
                .index_file
                .sync_data()
                .context("failed to sync_data active pack index")
        }
    }

    pub fn active_pack_id(&self) -> u64 {
        self.active.lock().pack_id
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
            let next_size = next_file
                .metadata()
                .with_context(|| format!("failed to stat next pack file {}", next_path.display()))?
                .len();

            self.rebuild_index_if_missing(next_pack_id)?;
            self.prime_index_cache(next_pack_id)?;

            if next_size >= self.max_pack_size_bytes && next_size > 0 {
                next_pack_id = next_pack_id
                    .checked_add(1)
                    .context("active pack id overflow while skipping full packs")?;
                continue;
            }

            active.pack_id = next_pack_id;
            active.file = next_file;
            active.index_file = next_index_file;
            active.size_bytes = next_size;
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

    pub fn rewrite_pack_with_live_hashes(
        &self,
        pack_id: u64,
        live_hashes: &HashSet<[u8; 16]>,
    ) -> Result<()> {
        if pack_id == self.active_pack_id() {
            return Ok(());
        }

        let src_pack_path = self.pack_path(pack_id);
        if !src_pack_path.exists() {
            return Ok(());
        }
        let src_idx_path = self.index_path(pack_id);

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
        let mut seen = HashSet::<[u8; 16]>::new();
        loop {
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

                    if live_hashes.contains(&hash) && seen.insert(hash) {
                        let mut payload = vec![0_u8; clen as usize];
                        src_file
                            .read_exact_at(&mut payload, cursor + RECORD_HEADER_LEN_U64)
                            .with_context(|| {
                                format!(
                                    "failed to read pack payload while rewriting {}",
                                    src_pack_path.display()
                                )
                            })?;

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
                        Self::append_index_record(
                            &mut dst_index,
                            hash,
                            PackIndexEntry {
                                offset: new_offset,
                                codec,
                                uncompressed_len: ulen,
                                compressed_len: clen,
                            },
                        )?;
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

        std::fs::rename(&tmp_pack_path, &src_pack_path).with_context(|| {
            format!(
                "failed to atomically replace pack {} with {}",
                src_pack_path.display(),
                tmp_pack_path.display()
            )
        })?;
        std::fs::rename(&tmp_idx_path, &src_idx_path).with_context(|| {
            format!(
                "failed to atomically replace pack index {} with {}",
                src_idx_path.display(),
                tmp_idx_path.display()
            )
        })?;

        let _ = self
            .index_cache
            .invalidate_entries_if(move |(cached_pack_id, _), _| *cached_pack_id == pack_id);
        self.prime_index_cache(pack_id)?;
        Ok(())
    }

    fn lookup_index_entry(
        &self,
        pack_id: u64,
        expected_hash: [u8; 16],
    ) -> Result<Option<PackIndexEntry>> {
        if let Some(entry) = self.index_cache.get(&(pack_id, expected_hash)) {
            return Ok(Some(entry));
        }

        let path = self.index_path(pack_id);
        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(&path)
            .with_context(|| format!("failed to open pack index file {}", path.display()))?;
        let mut cursor = 0_u64;
        let mut found = None;
        loop {
            let mut raw = AlignedBytes::<INDEX_ENTRY_LEN>::zeroed();
            match file.read_exact_at(raw.as_mut_slice(), cursor) {
                Ok(()) => {
                    let (hash, entry) = Self::decode_index_record(raw.as_slice())?;
                    if hash == expected_hash {
                        found = Some(entry);
                    }
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

        if let Some(entry) = found {
            self.index_cache.insert((pack_id, expected_hash), entry);
            return Ok(Some(entry));
        }
        Ok(None)
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

    fn rebuild_index_if_missing(&self, pack_id: u64) -> Result<()> {
        let pack_path = self.pack_path(pack_id);
        if !pack_path.exists() {
            return Ok(());
        }
        let index_path = self.index_path(pack_id);
        let index_missing_or_empty = match std::fs::metadata(&index_path) {
            Ok(meta) => meta.len() == 0,
            Err(err) if err.kind() == ErrorKind::NotFound => true,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to stat pack index {}", index_path.display())
                });
            }
        };
        if !index_missing_or_empty {
            return Ok(());
        }

        let pack = File::open(&pack_path)
            .with_context(|| format!("failed to open pack file {}", pack_path.display()))?;
        let mut index = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&index_path)
            .with_context(|| format!("failed to rebuild pack index {}", index_path.display()))?;
        set_file_mode(&index_path)?;

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

                    let entry = PackIndexEntry {
                        offset: cursor,
                        codec: header.codec,
                        uncompressed_len: header.uncompressed_len,
                        compressed_len: header.compressed_len,
                    };
                    Self::append_index_record(&mut index, header.hash, entry)?;

                    cursor = cursor
                        .checked_add(RECORD_HEADER_LEN_U64)
                        .and_then(|v| v.checked_add(entry.compressed_len as u64))
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

        index
            .sync_all()
            .with_context(|| format!("failed to sync rebuilt index {}", index_path.display()))?;
        Ok(())
    }

    fn prime_index_cache(&self, pack_id: u64) -> Result<()> {
        let index_path = self.index_path(pack_id);
        if !index_path.exists() {
            return Ok(());
        }

        let file = File::open(&index_path)
            .with_context(|| format!("failed to open pack index file {}", index_path.display()))?;
        let mut cursor = 0_u64;
        loop {
            let mut raw = AlignedBytes::<INDEX_ENTRY_LEN>::zeroed();
            match file.read_exact_at(raw.as_mut_slice(), cursor) {
                Ok(()) => {
                    let (hash, entry) = Self::decode_index_record(raw.as_slice())?;
                    self.index_cache.insert((pack_id, hash), entry);
                    cursor = cursor
                        .checked_add(INDEX_ENTRY_LEN_U64)
                        .context("pack index cursor overflow while priming cache")?;
                }
                Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to prime index from {}", index_path.display())
                    });
                }
            }
        }
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
            .append_chunk(hash, CODEC_RAW, payload.len() as u32, &payload)
            .expect("chunk append should succeed");

        let read_back = store
            .read_chunk_payload(
                pack_id,
                hash,
                CODEC_RAW,
                payload.len() as u32,
                payload.len() as u32,
            )
            .expect("chunk read should succeed");
        assert_eq!(read_back, payload);

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
