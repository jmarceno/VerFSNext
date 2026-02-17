use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;

const RECORD_MAGIC: [u8; 4] = *b"VPK1";
const RECORD_HEADER_LEN: u64 = 4 + 16 + 4;

struct ActivePack {
    pack_id: u64,
    file: File,
}

pub struct PackStore {
    packs_dir: PathBuf,
    active: Mutex<ActivePack>,
}

impl PackStore {
    pub fn open(packs_dir: &Path, active_pack_id: u64) -> Result<Self> {
        std::fs::create_dir_all(packs_dir).with_context(|| {
            format!(
                "failed to create packs directory {}",
                packs_dir.to_string_lossy()
            )
        })?;
        let path = Self::pack_path_impl(packs_dir, active_pack_id);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open active pack file {}", path.display()))?;

        Ok(Self {
            packs_dir: packs_dir.to_path_buf(),
            active: Mutex::new(ActivePack {
                pack_id: active_pack_id,
                file,
            }),
        })
    }

    pub fn append_chunk(&self, chunk_hash: [u8; 16], data: &[u8]) -> Result<(u64, u64, u32)> {
        if data.len() > u32::MAX as usize {
            bail!("chunk too large: {}", data.len());
        }

        let mut active = self.active.lock();
        let file = &mut active.file;

        let record_start = file
            .seek(SeekFrom::End(0))
            .context("failed to seek to pack end")?;

        file.write_all(&RECORD_MAGIC)
            .context("failed to write pack record magic")?;
        file.write_all(&chunk_hash)
            .context("failed to write pack record hash")?;
        file.write_all(&(data.len() as u32).to_le_bytes())
            .context("failed to write pack record len")?;
        file.write_all(data)
            .context("failed to write pack record payload")?;

        let payload_offset = record_start + RECORD_HEADER_LEN;
        Ok((active.pack_id, payload_offset, data.len() as u32))
    }

    pub fn read_chunk(
        &self,
        pack_id: u64,
        payload_offset: u64,
        expected_hash: [u8; 16],
        expected_len: u32,
    ) -> Result<Vec<u8>> {
        let path = self.pack_path(pack_id);
        let file = File::open(&path)
            .with_context(|| format!("failed to open pack file {}", path.display()))?;

        let mut header = [0_u8; RECORD_HEADER_LEN as usize];
        let header_offset = payload_offset
            .checked_sub(RECORD_HEADER_LEN)
            .context("invalid payload offset while reading pack record")?;
        file.read_exact_at(&mut header, header_offset)
            .with_context(|| format!("failed to read pack record header from {}", path.display()))?;

        if header[..4] != RECORD_MAGIC {
            bail!("pack record magic mismatch for {}", path.display());
        }

        let mut stored_hash = [0_u8; 16];
        stored_hash.copy_from_slice(&header[4..20]);
        if stored_hash != expected_hash {
            bail!("pack record hash mismatch for {}", path.display());
        }

        let mut len_bytes = [0_u8; 4];
        len_bytes.copy_from_slice(&header[20..24]);
        let stored_len = u32::from_le_bytes(len_bytes);
        if stored_len != expected_len {
            bail!(
                "pack record len mismatch for {}: expected {}, got {}",
                path.display(),
                expected_len,
                stored_len
            );
        }

        let mut out = vec![0_u8; stored_len as usize];
        file.read_exact_at(&mut out, payload_offset)
            .with_context(|| format!("failed to read pack payload from {}", path.display()))?;
        Ok(out)
    }

    pub fn sync(&self, full: bool) -> Result<()> {
        let active = self.active.lock();
        if full {
            active
                .file
                .sync_all()
                .context("failed to sync_all active pack")
        } else {
            active
                .file
                .sync_data()
                .context("failed to sync_data active pack")
        }
    }

    fn pack_path(&self, pack_id: u64) -> PathBuf {
        Self::pack_path_impl(&self.packs_dir, pack_id)
    }

    fn pack_path_impl(base: &Path, pack_id: u64) -> PathBuf {
        base.join(format!("pack-{pack_id:020}.vpk"))
    }

    pub fn verify_pack_headers(&self, pack_id: u64) -> Result<()> {
        let path = self.pack_path(pack_id);
        let file = File::open(&path)
            .with_context(|| format!("failed to open pack file {}", path.display()))?;
        let mut cursor = 0_u64;

        loop {
            let mut header = [0_u8; RECORD_HEADER_LEN as usize];
            match file.read_exact_at(&mut header, cursor) {
                Ok(()) => {
                    if header[..4] != RECORD_MAGIC {
                        bail!("corrupt pack record magic at offset {} in {}", cursor, path.display());
                    }
                    let mut len_bytes = [0_u8; 4];
                    len_bytes.copy_from_slice(&header[20..24]);
                    let payload_len = u32::from_le_bytes(len_bytes) as u64;
                    cursor = cursor
                        .checked_add(RECORD_HEADER_LEN)
                        .and_then(|v| v.checked_add(payload_len))
                        .context("pack offset overflow while validating")?;
                }
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to validate pack headers for {}", path.display())
                    });
                }
            }
        }

        Ok(())
    }
}
