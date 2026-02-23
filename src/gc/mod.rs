use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::permissions::set_file_mode;

pub const DISCARD_MAGIC: [u8; 8] = *b"VFDISC01";
pub const DISCARD_VERSION: u16 = 1;
pub const DISCARD_HEADER_LEN: u64 = 16;
pub const DISCARD_RECORD_LEN: u16 = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiscardRecord {
    pub epoch_id: u64,
    pub pack_id: u64,
    pub chunk_hash128: [u8; 16],
    pub block_size_bytes: u32,
}

#[derive(Debug, Default)]
pub struct DiscardReadResult {
    pub records: Vec<DiscardRecord>,
    pub invalid_records: u64,
}

pub fn append_records(path: &Path, records: &[DiscardRecord]) -> Result<u64> {
    let mut file = open_for_append(path)?;

    for record in records {
        let encoded = encode_record(record)?;
        file.write_all(&encoded)
            .with_context(|| format!("failed writing discard record to {}", path.display()))?;
    }

    file.sync_data()
        .with_context(|| format!("failed syncing discard file {}", path.display()))?;

    let end = file
        .seek(SeekFrom::End(0))
        .with_context(|| format!("failed to read discard length for {}", path.display()))?;
    Ok(end)
}

pub fn read_records(path: &Path, checkpoint: u64) -> Result<DiscardReadResult> {
    if !path.exists() {
        return Ok(DiscardReadResult::default());
    }

    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("failed to open discard file {}", path.display()))?;
    let file_len = file
        .seek(SeekFrom::End(0))
        .with_context(|| format!("failed to stat discard file {}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to seek discard file {}", path.display()))?;

    if file_len == 0 {
        return Ok(DiscardReadResult::default());
    }

    validate_header(&mut file, path)?;

    let mut out = DiscardReadResult::default();
    let upper = checkpoint.min(file_len);
    if upper <= DISCARD_HEADER_LEN {
        return Ok(out);
    }

    let mut cursor = DISCARD_HEADER_LEN;
    while cursor + DISCARD_RECORD_LEN as u64 <= upper {
        file.seek(SeekFrom::Start(cursor))
            .with_context(|| format!("failed to seek discard record in {}", path.display()))?;

        let mut raw = vec![0_u8; DISCARD_RECORD_LEN as usize];
        file.read_exact(&mut raw)
            .with_context(|| format!("failed to read discard record from {}", path.display()))?;

        match decode_record(&raw) {
            Ok(record) => out.records.push(record),
            Err(_) => {
                out.invalid_records = out.invalid_records.saturating_add(1);
            }
        }

        cursor = cursor.saturating_add(DISCARD_RECORD_LEN as u64);
    }

    Ok(out)
}

pub fn rewrite_records(path: &Path, records: &[DiscardRecord]) -> Result<u64> {
    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)
        .with_context(|| format!("failed to open discard temp file {}", tmp_path.display()))?;
    set_file_mode(&tmp_path)?;

    write_header(&mut file)?;
    for record in records {
        file.write_all(&encode_record(record)?)
            .with_context(|| format!("failed to write discard temp file {}", tmp_path.display()))?;
    }

    file.sync_all()
        .with_context(|| format!("failed to sync discard temp file {}", tmp_path.display()))?;
    drop(file);

    std::fs::rename(&tmp_path, path).with_context(|| {
        format!(
            "failed to atomically replace discard file {} with {}",
            path.display(),
            tmp_path.display()
        )
    })?;

    let len = std::fs::metadata(path)
        .with_context(|| format!("failed to stat discard file {}", path.display()))?
        .len();
    Ok(len)
}

pub fn ensure_file(path: &Path) -> Result<u64> {
    let mut file = open_for_append(path)?;
    let len = file
        .seek(SeekFrom::End(0))
        .with_context(|| format!("failed to stat discard file {}", path.display()))?;
    Ok(len)
}

fn open_for_append(path: &Path) -> Result<std::fs::File> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open discard file {}", path.display()))?;
    set_file_mode(path)?;

    ensure_header(&mut file, path)?;
    Ok(file)
}

fn ensure_header(file: &mut std::fs::File, path: &Path) -> Result<()> {
    let len = file
        .seek(SeekFrom::End(0))
        .with_context(|| format!("failed to seek discard file {}", path.display()))?;
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to seek discard file {}", path.display()))?;

    if len == 0 {
        write_header(file)?;
        file.sync_data()
            .with_context(|| format!("failed to sync discard header {}", path.display()))?;
        return Ok(());
    }

    validate_header(file, path)
}

fn write_header(file: &mut std::fs::File) -> Result<()> {
    let flags = 0_u16;
    let header_crc = crc32c::crc32c(&flags.to_le_bytes());

    file.write_all(&DISCARD_MAGIC)
        .context("failed to write discard header magic")?;
    file.write_all(&DISCARD_VERSION.to_le_bytes())
        .context("failed to write discard header version")?;
    file.write_all(&flags.to_le_bytes())
        .context("failed to write discard header flags")?;
    file.write_all(&header_crc.to_le_bytes())
        .context("failed to write discard header crc")?;
    Ok(())
}

fn validate_header(file: &mut std::fs::File, path: &Path) -> Result<()> {
    file.seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to seek discard header in {}", path.display()))?;

    let mut header = [0_u8; DISCARD_HEADER_LEN as usize];
    file.read_exact(&mut header)
        .with_context(|| format!("failed to read discard header {}", path.display()))?;

    if header[..8] != DISCARD_MAGIC {
        bail!("invalid discard magic in {}", path.display());
    }

    let mut version_bytes = [0_u8; 2];
    version_bytes.copy_from_slice(&header[8..10]);
    let version = u16::from_le_bytes(version_bytes);
    if version != DISCARD_VERSION {
        bail!(
            "unsupported discard version {} in {}",
            version,
            path.display()
        );
    }

    let mut flags_bytes = [0_u8; 2];
    flags_bytes.copy_from_slice(&header[10..12]);

    let mut crc_bytes = [0_u8; 4];
    crc_bytes.copy_from_slice(&header[12..16]);
    let stored_crc = u32::from_le_bytes(crc_bytes);
    let computed_crc = crc32c::crc32c(&flags_bytes);
    if stored_crc != computed_crc {
        bail!("discard header CRC mismatch in {}", path.display());
    }

    Ok(())
}

fn encode_record(record: &DiscardRecord) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(DISCARD_RECORD_LEN as usize);
    out.extend_from_slice(&DISCARD_RECORD_LEN.to_le_bytes());
    out.push(1); // record_version
    out.push(0); // record_flags
    out.extend_from_slice(&record.epoch_id.to_le_bytes());
    out.extend_from_slice(&record.pack_id.to_le_bytes());
    out.extend_from_slice(&record.chunk_hash128);
    out.extend_from_slice(&record.block_size_bytes.to_le_bytes());
    out.extend_from_slice(&0_u32.to_le_bytes()); // reserved

    let crc_start = 4;
    let crc = crc32c::crc32c(&out[crc_start..]);
    out.extend_from_slice(&crc.to_le_bytes());

    if out.len() != DISCARD_RECORD_LEN as usize {
        bail!(
            "discard record encoded length mismatch: expected {}, got {}",
            DISCARD_RECORD_LEN,
            out.len()
        );
    }

    Ok(out)
}

fn decode_record(raw: &[u8]) -> Result<DiscardRecord> {
    if raw.len() != DISCARD_RECORD_LEN as usize {
        bail!(
            "discard record length mismatch: expected {}, got {}",
            DISCARD_RECORD_LEN,
            raw.len()
        );
    }

    let mut len_bytes = [0_u8; 2];
    len_bytes.copy_from_slice(&raw[0..2]);
    if u16::from_le_bytes(len_bytes) != DISCARD_RECORD_LEN {
        bail!("discard record declared length mismatch");
    }

    if raw[2] != 1 {
        bail!("unsupported discard record version {}", raw[2]);
    }

    let mut crc_bytes = [0_u8; 4];
    crc_bytes.copy_from_slice(&raw[44..48]);
    let stored_crc = u32::from_le_bytes(crc_bytes);
    let computed_crc = crc32c::crc32c(&raw[4..44]);
    if stored_crc != computed_crc {
        bail!("discard record CRC mismatch");
    }

    let mut epoch_bytes = [0_u8; 8];
    epoch_bytes.copy_from_slice(&raw[4..12]);
    let mut pack_bytes = [0_u8; 8];
    pack_bytes.copy_from_slice(&raw[12..20]);
    let mut hash = [0_u8; 16];
    hash.copy_from_slice(&raw[20..36]);
    let mut block_size_bytes = [0_u8; 4];
    block_size_bytes.copy_from_slice(&raw[36..40]);

    Ok(DiscardRecord {
        epoch_id: u64::from_le_bytes(epoch_bytes),
        pack_id: u64::from_le_bytes(pack_bytes),
        chunk_hash128: hash,
        block_size_bytes: u32::from_le_bytes(block_size_bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_path() -> std::path::PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("verfsnext-discard-test-{stamp}.bin"))
    }

    #[test]
    fn append_read_and_rewrite_roundtrip() {
        let path = unique_path();
        let records = vec![
            DiscardRecord {
                epoch_id: 1,
                pack_id: 7,
                chunk_hash128: [1_u8; 16],
                block_size_bytes: 1024,
            },
            DiscardRecord {
                epoch_id: 2,
                pack_id: 9,
                chunk_hash128: [2_u8; 16],
                block_size_bytes: 2048,
            },
        ];

        let end = append_records(&path, &records).expect("append should succeed");
        let parsed = read_records(&path, end).expect("read should succeed");
        assert_eq!(parsed.invalid_records, 0);
        assert_eq!(parsed.records, records);

        let rewritten = vec![records[1]];
        let new_end = rewrite_records(&path, &rewritten).expect("rewrite should succeed");
        let reparsed = read_records(&path, new_end).expect("re-read should succeed");
        assert_eq!(reparsed.invalid_records, 0);
        assert_eq!(reparsed.records, rewritten);

        let _ = std::fs::remove_file(path);
    }
}
