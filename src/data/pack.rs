use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use moka::sync::Cache;
use parking_lot::Mutex;

use crate::data::compress::decompress_chunk;

const RECORD_MAGIC: [u8; 4] = *b"VPK2";
const RECORD_HEADER_LEN: u64 = 32;
const INDEX_ENTRY_LEN: usize = 36;

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
}

pub struct PackStore {
    packs_dir: PathBuf,
    active: Mutex<ActivePack>,
    index_cache: Cache<(u64, [u8; 16]), PackIndexEntry>,
}

impl PackStore {
    pub fn open(packs_dir: &Path, active_pack_id: u64, index_cache_capacity: u64) -> Result<Self> {
        std::fs::create_dir_all(packs_dir).with_context(|| {
            format!(
                "failed to create packs directory {}",
                packs_dir.to_string_lossy()
            )
        })?;

        let path = Self::pack_path_impl(packs_dir, active_pack_id);
        let index_path = Self::index_path_impl(packs_dir, active_pack_id);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open active pack file {}", path.display()))?;
        let index_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&index_path)
            .with_context(|| format!("failed to open pack index file {}", index_path.display()))?;

        let store = Self {
            packs_dir: packs_dir.to_path_buf(),
            active: Mutex::new(ActivePack {
                pack_id: active_pack_id,
                file,
                index_file,
            }),
            index_cache: Cache::builder().max_capacity(index_cache_capacity).build(),
        };

        store.rebuild_index_if_missing(active_pack_id)?;
        store.prime_index_cache(active_pack_id)?;

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

        let mut active = self.active.lock();
        let file = &mut active.file;

        let record_offset = file
            .seek(SeekFrom::End(0))
            .context("failed to seek to pack end")?;
        let compressed_len = compressed_data.len() as u32;

        file.write_all(&RECORD_MAGIC)
            .context("failed to write pack record magic")?;
        file.write_all(&chunk_hash)
            .context("failed to write pack record hash")?;
        file.write_all(&[codec])
            .context("failed to write pack record codec")?;
        file.write_all(&[0_u8; 3])
            .context("failed to write pack record reserved bytes")?;
        file.write_all(&uncompressed_len.to_le_bytes())
            .context("failed to write pack record uncompressed len")?;
        file.write_all(&compressed_len.to_le_bytes())
            .context("failed to write pack record compressed len")?;
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

        Ok(active.pack_id)
    }

    pub fn read_chunk(
        &self,
        pack_id: u64,
        expected_hash: [u8; 16],
        expected_uncompressed_len: u32,
    ) -> Result<Vec<u8>> {
        let index = self
            .lookup_index_entry(pack_id, expected_hash)?
            .with_context(|| {
                format!(
                    "missing pack index entry for pack {} hash {:x?}",
                    pack_id, expected_hash
                )
            })?;

        if index.uncompressed_len != expected_uncompressed_len {
            bail!(
                "pack index uncompressed length mismatch for pack {} hash {:x?}: expected {}, got {}",
                pack_id,
                expected_hash,
                expected_uncompressed_len,
                index.uncompressed_len
            );
        }

        let path = self.pack_path(pack_id);
        let file = File::open(&path)
            .with_context(|| format!("failed to open pack file {}", path.display()))?;

        let mut header = [0_u8; RECORD_HEADER_LEN as usize];
        file.read_exact_at(&mut header, index.offset)
            .with_context(|| {
                format!("failed to read pack record header from {}", path.display())
            })?;

        if header[..4] != RECORD_MAGIC {
            bail!("pack record magic mismatch for {}", path.display());
        }

        let mut stored_hash = [0_u8; 16];
        stored_hash.copy_from_slice(&header[4..20]);
        if stored_hash != expected_hash {
            bail!("pack record hash mismatch for {}", path.display());
        }

        let stored_codec = header[20];
        if stored_codec != index.codec {
            bail!(
                "pack record codec mismatch for {}: index {}, header {}",
                path.display(),
                index.codec,
                stored_codec
            );
        }

        let mut ulen_bytes = [0_u8; 4];
        ulen_bytes.copy_from_slice(&header[24..28]);
        let stored_ulen = u32::from_le_bytes(ulen_bytes);
        if stored_ulen != expected_uncompressed_len {
            bail!(
                "pack record uncompressed length mismatch for {}: expected {}, got {}",
                path.display(),
                expected_uncompressed_len,
                stored_ulen
            );
        }

        let mut clen_bytes = [0_u8; 4];
        clen_bytes.copy_from_slice(&header[28..32]);
        let stored_clen = u32::from_le_bytes(clen_bytes);
        if stored_clen != index.compressed_len {
            bail!(
                "pack record compressed length mismatch for {}: index {}, header {}",
                path.display(),
                index.compressed_len,
                stored_clen
            );
        }

        let payload_offset = index
            .offset
            .checked_add(RECORD_HEADER_LEN)
            .context("pack record offset overflow while reading payload")?;
        let mut payload = vec![0_u8; stored_clen as usize];
        file.read_exact_at(&mut payload, payload_offset)
            .with_context(|| format!("failed to read pack payload from {}", path.display()))?;

        decompress_chunk(index.codec, &payload, expected_uncompressed_len)
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
        let mut dst_index = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_idx_path)
            .with_context(|| {
                format!("failed to open temp pack index {}", tmp_idx_path.display())
            })?;

        let mut cursor = 0_u64;
        let mut new_offset = 0_u64;
        let mut seen = HashSet::<[u8; 16]>::new();
        loop {
            let mut header = [0_u8; RECORD_HEADER_LEN as usize];
            match src_file.read_exact_at(&mut header, cursor) {
                Ok(()) => {
                    if header[..4] != RECORD_MAGIC {
                        bail!(
                            "corrupt pack record magic at offset {} in {}",
                            cursor,
                            src_pack_path.display()
                        );
                    }

                    let mut hash = [0_u8; 16];
                    hash.copy_from_slice(&header[4..20]);
                    let codec = header[20];
                    let mut ulen_bytes = [0_u8; 4];
                    ulen_bytes.copy_from_slice(&header[24..28]);
                    let mut clen_bytes = [0_u8; 4];
                    clen_bytes.copy_from_slice(&header[28..32]);
                    let ulen = u32::from_le_bytes(ulen_bytes);
                    let clen = u32::from_le_bytes(clen_bytes);

                    let record_len = RECORD_HEADER_LEN
                        .checked_add(clen as u64)
                        .context("record length overflow while rewriting pack")?;

                    if live_hashes.contains(&hash) && seen.insert(hash) {
                        let mut payload = vec![0_u8; clen as usize];
                        src_file
                            .read_exact_at(&mut payload, cursor + RECORD_HEADER_LEN)
                            .with_context(|| {
                                format!(
                                    "failed to read pack payload while rewriting {}",
                                    src_pack_path.display()
                                )
                            })?;

                        dst_file.write_all(&header).with_context(|| {
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
            let mut raw = [0_u8; INDEX_ENTRY_LEN];
            match file.read_exact_at(&mut raw, cursor) {
                Ok(()) => {
                    let (hash, entry) = Self::decode_index_record(&raw)?;
                    if hash == expected_hash {
                        found = Some(entry);
                    }
                    cursor = cursor
                        .checked_add(INDEX_ENTRY_LEN as u64)
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
        base.join(format!("pack-{pack_id:020}.vpk"))
    }

    fn index_path_impl(base: &Path, pack_id: u64) -> PathBuf {
        base.join(format!("pack-{pack_id:020}.idx"))
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
            let mut header = [0_u8; RECORD_HEADER_LEN as usize];
            match file.read_exact_at(&mut header, cursor) {
                Ok(()) => {
                    if header[..4] != RECORD_MAGIC {
                        bail!(
                            "corrupt pack record magic at offset {} in {}",
                            cursor,
                            path.display()
                        );
                    }
                    let mut clen_bytes = [0_u8; 4];
                    clen_bytes.copy_from_slice(&header[28..32]);
                    let compressed_len = u32::from_le_bytes(clen_bytes) as u64;
                    cursor = cursor
                        .checked_add(RECORD_HEADER_LEN)
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

        let mut cursor = 0_u64;
        loop {
            let mut header = [0_u8; RECORD_HEADER_LEN as usize];
            match pack.read_exact_at(&mut header, cursor) {
                Ok(()) => {
                    if header[..4] != RECORD_MAGIC {
                        bail!(
                            "corrupt pack record magic while rebuilding index at offset {} in {}",
                            cursor,
                            pack_path.display()
                        );
                    }

                    let mut hash = [0_u8; 16];
                    hash.copy_from_slice(&header[4..20]);
                    let codec = header[20];
                    let mut ulen_bytes = [0_u8; 4];
                    ulen_bytes.copy_from_slice(&header[24..28]);
                    let mut clen_bytes = [0_u8; 4];
                    clen_bytes.copy_from_slice(&header[28..32]);

                    let entry = PackIndexEntry {
                        offset: cursor,
                        codec,
                        uncompressed_len: u32::from_le_bytes(ulen_bytes),
                        compressed_len: u32::from_le_bytes(clen_bytes),
                    };
                    Self::append_index_record(&mut index, hash, entry)?;

                    cursor = cursor
                        .checked_add(RECORD_HEADER_LEN)
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
            let mut raw = [0_u8; INDEX_ENTRY_LEN];
            match file.read_exact_at(&mut raw, cursor) {
                Ok(()) => {
                    let (hash, entry) = Self::decode_index_record(&raw)?;
                    self.index_cache.insert((pack_id, hash), entry);
                    cursor = cursor
                        .checked_add(INDEX_ENTRY_LEN as u64)
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
        let mut raw = [0_u8; INDEX_ENTRY_LEN];
        raw[..16].copy_from_slice(&hash);
        raw[16..24].copy_from_slice(&entry.offset.to_le_bytes());
        raw[24] = entry.codec;
        raw[28..32].copy_from_slice(&entry.uncompressed_len.to_le_bytes());
        raw[32..36].copy_from_slice(&entry.compressed_len.to_le_bytes());
        index_file
            .write_all(&raw)
            .context("failed to append pack index record")
    }

    fn decode_index_record(raw: &[u8; INDEX_ENTRY_LEN]) -> Result<([u8; 16], PackIndexEntry)> {
        let mut hash = [0_u8; 16];
        hash.copy_from_slice(&raw[..16]);

        let mut offset_bytes = [0_u8; 8];
        offset_bytes.copy_from_slice(&raw[16..24]);

        let codec = raw[24];
        let mut ulen_bytes = [0_u8; 4];
        ulen_bytes.copy_from_slice(&raw[28..32]);
        let mut clen_bytes = [0_u8; 4];
        clen_bytes.copy_from_slice(&raw[32..36]);

        Ok((
            hash,
            PackIndexEntry {
                offset: u64::from_le_bytes(offset_bytes),
                codec,
                uncompressed_len: u32::from_le_bytes(ulen_bytes),
                compressed_len: u32::from_le_bytes(clen_bytes),
            },
        ))
    }
}
