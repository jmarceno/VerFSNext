use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rkyv::{Archive, Deserialize, Serialize};
use tracing::info;

use crate::config::Config;
use crate::meta::MetaStore;
use crate::permissions::set_file_mode;
use crate::types::sys_key;

pub const SYS_PACK_INDEX_CRC32_MIGRATION_V1: &str = "pack_index_crc32_migration_v1";
pub const SYS_PACK_CRC32_READ_ERRORS: &str = "pack_crc32_read_errors";

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
struct PackIndexRecordV1 {
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
const INDEX_ENTRY_LEN_V1: usize = size_of::<ArchivedPackIndexRecordV1>();

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

pub async fn ensure_pack_index_crc32_compat(config: &Config, meta: &MetaStore) -> Result<()> {
    let (already_migrated, has_crc_counter) = meta.read_txn(|txn| {
        let migrated = match txn.get(sys_key(SYS_PACK_INDEX_CRC32_MIGRATION_V1))? {
            Some(raw) => {
                if raw.len() != 8 {
                    bail!(
                        "invalid SYS:{} length {}, expected 8",
                        SYS_PACK_INDEX_CRC32_MIGRATION_V1,
                        raw.len()
                    );
                }
                let mut bytes = [0_u8; 8];
                bytes.copy_from_slice(&raw);
                u64::from_le_bytes(bytes) >= 1
            }
            None => false,
        };
        let has_crc_counter = txn.get(sys_key(SYS_PACK_CRC32_READ_ERRORS))?.is_some();
        Ok((migrated, has_crc_counter))
    })?;

    if !already_migrated {
        let pack_paths = list_pack_files(&config.packs_dir())?;
        info!(
            pack_count = pack_paths.len(),
            "running pack-index CRC32 migration (rewriting .idx files)"
        );
        for pack_path in &pack_paths {
            rewrite_index_with_crc32(pack_path)?;
        }
        info!(
            pack_count = pack_paths.len(),
            "pack-index CRC32 migration finished"
        );
    }

    if !already_migrated || !has_crc_counter {
        meta.write_txn(|txn| {
            if txn.get(sys_key(SYS_PACK_CRC32_READ_ERRORS))?.is_none() {
                txn.set(
                    sys_key(SYS_PACK_CRC32_READ_ERRORS),
                    0_u64.to_le_bytes().to_vec(),
                )?;
            }
            txn.set(
                sys_key(SYS_PACK_INDEX_CRC32_MIGRATION_V1),
                1_u64.to_le_bytes().to_vec(),
            )?;
            Ok(())
        })
        .await?;
    }

    Ok(())
}

fn list_pack_files(packs_dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for subdir in ["A", "B", "C", "D", "E", "F"] {
        let subdir_path = packs_dir.join(subdir);
        if !subdir_path.exists() {
            continue;
        }
        let entries = std::fs::read_dir(&subdir_path).with_context(|| {
            format!(
                "failed to read packs subdirectory {}",
                subdir_path.display()
            )
        })?;
        for entry in entries {
            let entry = entry.with_context(|| {
                format!(
                    "failed reading entry from packs subdirectory {}",
                    subdir_path.display()
                )
            })?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if name.starts_with("pack-") && name.ends_with(".vpk") {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn rewrite_index_with_crc32(pack_path: &Path) -> Result<()> {
    let pack_file = File::open(pack_path)
        .with_context(|| format!("failed to open pack file {}", pack_path.display()))?;
    let index_path = pack_path.with_extension("idx");
    let tmp_index_path = pack_path.with_extension("idx.crc32migrate");
    let mut index_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_index_path)
        .with_context(|| {
            format!(
                "failed to open temporary pack index file {}",
                tmp_index_path.display()
            )
        })?;
    set_file_mode(&tmp_index_path)?;

    let mut cursor = 0_u64;
    loop {
        let mut raw_header = AlignedBytes::<RECORD_HEADER_LEN>::zeroed();
        match pack_file.read_exact_at(raw_header.as_mut_slice(), cursor) {
            Ok(()) => {
                let header = decode_pack_header(raw_header.as_slice())?;
                if header.magic != RECORD_MAGIC {
                    bail!(
                        "corrupt pack record magic at offset {} in {} during CRC32 migration",
                        cursor,
                        pack_path.display()
                    );
                }

                let payload_offset = cursor
                    .checked_add(RECORD_HEADER_LEN_U64)
                    .context("pack offset overflow while migrating index payload")?;
                let mut payload = vec![0_u8; header.compressed_len as usize];
                pack_file
                    .read_exact_at(&mut payload, payload_offset)
                    .with_context(|| {
                        format!(
                            "failed to read pack payload from {} during CRC32 migration",
                            pack_path.display()
                        )
                    })?;

                let record = PackIndexRecordV1 {
                    hash: header.hash,
                    offset: cursor,
                    codec: header.codec,
                    reserved: RECORD_RESERVED,
                    uncompressed_len: header.uncompressed_len,
                    compressed_len: header.compressed_len,
                    payload_crc32: crc32c::crc32c(&payload),
                };
                let raw = encode_index_record_v1(&record)?;
                index_file.write_all(&raw).with_context(|| {
                    format!(
                        "failed to write migrated pack index record {}",
                        tmp_index_path.display()
                    )
                })?;

                cursor = payload_offset
                    .checked_add(header.compressed_len as u64)
                    .context("pack offset overflow while migrating index")?;
            }
            Err(err) if err.kind() == ErrorKind::UnexpectedEof => break,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to read pack header from {} during CRC32 migration",
                        pack_path.display()
                    )
                });
            }
        }
    }

    index_file.sync_all().with_context(|| {
        format!(
            "failed to sync migrated pack index file {}",
            tmp_index_path.display()
        )
    })?;
    drop(index_file);
    std::fs::rename(&tmp_index_path, &index_path).with_context(|| {
        format!(
            "failed to replace pack index {} with {} during CRC32 migration",
            index_path.display(),
            tmp_index_path.display()
        )
    })?;
    Ok(())
}

fn decode_pack_header(raw: &[u8]) -> Result<PackRecordHeader> {
    let archived = rkyv::access::<ArchivedPackRecordHeader, rkyv::rancor::Error>(raw)
        .context("failed to decode pack record header during CRC32 migration")?;
    if archived.reserved != RECORD_RESERVED {
        bail!("pack record header reserved bytes are non-zero during CRC32 migration");
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

fn encode_index_record_v1(record: &PackIndexRecordV1) -> Result<rkyv::util::AlignedVec> {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(record)
        .context("failed to encode migrated pack index record")?;
    if bytes.len() != INDEX_ENTRY_LEN_V1 {
        bail!(
            "invalid migrated pack index record size: expected {}, got {}",
            INDEX_ENTRY_LEN_V1,
            bytes.len()
        );
    }
    Ok(bytes)
}
