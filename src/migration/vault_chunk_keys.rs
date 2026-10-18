//! One-time migration: move existing vault chunk metadata into the
//! domain-tagged key space.
//!
//! The vault content hash of `V` is the plain content hash of `0xA7 || V`
//! (the domain byte is a byte prefix to the same xxh3 stream). Older versions
//! keyed every chunk record by its hash alone, so a vault chunk and a plain
//! chunk could share one record; the second writer deduplicated onto the first
//! and its bytes were never stored.
//!
//! Chunk records now live in one key space per domain ([`ChunkId`]). Records
//! written by older versions sit at the plain key (`C` + hash); vault records
//! are recognized by `CHUNK_FLAG_ENCRYPTED` and moved to the vault key (`C` +
//! `0xA7` + hash), together with their inline payload when they have one.
//!
//! The move is a metadata-only operation: it never decrypts a payload, so it
//! runs while the vault is locked. Batches are idempotent (already tagged keys
//! are skipped), and the marker is committed only after every record moved.
//! Files whose bytes were lost to the old collision stay unreadable; the
//! records check reports them.

use anyhow::{bail, Context, Result};
use tracing::{error, info, warn};

use crate::fs::utils::scan_range_pairs_limited;
use crate::meta::MetaStore;
use crate::types::{
    chunk_key, decode_chunk_key, decode_rkyv, inline_chunk_key, prefix_end, sys_key, ChunkId,
    ChunkRecord, KEY_PREFIX_CHUNK,
};
use crate::vault::{CHUNK_FLAG_ENCRYPTED, CHUNK_FLAG_INLINE, SYS_VAULT_WRAP};

pub const SYS_VAULT_CHUNK_KEYS_MIGRATION_V1: &str = "vault_chunk_keys_migration_v1";

/// Records moved per metadata transaction, bounding the transaction size on
/// large vaults.
const BATCH: usize = 10_000;

pub async fn ensure_vault_chunk_keys_migrated(meta: &MetaStore) -> Result<()> {
    if migration_done(meta)? {
        return Ok(());
    }

    if meta.get_sys(SYS_VAULT_WRAP)?.is_none() {
        // No vault has ever been created in this store, so no vault chunk
        // record exists. Skip the full chunk scan.
        mark_done(meta).await?;
        return Ok(());
    }

    let moved = move_vault_chunk_records(meta).await?;
    mark_done(meta).await?;
    info!(
        moved_records = moved,
        "moved vault chunk records into the domain-tagged key space"
    );
    warn!(
        "run `verfsnext check` once after this upgrade: it lists any file whose bytes the old vault/plain hash alias made unreachable"
    );
    Ok(())
}

fn migration_done(meta: &MetaStore) -> Result<bool> {
    let Some(raw) = meta.get_sys(SYS_VAULT_CHUNK_KEYS_MIGRATION_V1)? else {
        return Ok(false);
    };
    if raw.len() != 8 {
        bail!(
            "invalid SYS:{} length {}, expected 8",
            SYS_VAULT_CHUNK_KEYS_MIGRATION_V1,
            raw.len()
        );
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&raw);
    Ok(u64::from_le_bytes(bytes) >= 1)
}

async fn mark_done(meta: &MetaStore) -> Result<()> {
    meta.write_txn(|txn| {
        txn.set(
            sys_key(SYS_VAULT_CHUNK_KEYS_MIGRATION_V1),
            1_u64.to_le_bytes().to_vec(),
        )?;
        Ok(())
    })
    .await
}

async fn move_vault_chunk_records(meta: &MetaStore) -> Result<u64> {
    let prefix = vec![KEY_PREFIX_CHUNK];
    let end = prefix_end(&prefix);
    let mut start = prefix;
    let mut moved = 0_u64;
    loop {
        let (batch, next_start) = meta.read_txn(|txn| {
            let (pairs, has_more) =
                scan_range_pairs_limited(txn, start.clone(), end.clone(), BATCH)?;
            let next_start = match (has_more, pairs.last()) {
                (true, Some((key, _))) => {
                    let mut next = key.clone();
                    next.push(0);
                    Some(next)
                }
                _ => None,
            };
            let mut batch = Vec::new();
            for (key, value) in pairs {
                // Tagged keys are records a previous run already moved.
                let Some(id) = decode_chunk_key(&key) else {
                    continue;
                };
                if id.vault {
                    continue;
                }
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                if (chunk.flags & CHUNK_FLAG_ENCRYPTED) == 0 {
                    continue;
                }
                batch.push((key, ChunkId::vault(id.hash)));
            }
            Ok((batch, next_start))
        })?;

        if !batch.is_empty() {
            meta.write_txn(|txn| {
                for (old_key, id) in &batch {
                    let Some(value) = txn.get(old_key.clone())? else {
                        continue;
                    };
                    let chunk: ChunkRecord = decode_rkyv(&value)?;
                    if (chunk.flags & CHUNK_FLAG_ENCRYPTED) == 0 {
                        continue;
                    }
                    let new_key = chunk_key(id);
                    if txn.get(new_key.clone())?.is_some() {
                        bail!(
                            "vault chunk {:x?} has records under both the plain and the vault key; refusing to overwrite either",
                            id.hash
                        );
                    }
                    txn.set(new_key, value)?;
                    txn.delete(old_key.clone())?;

                    if (chunk.flags & CHUNK_FLAG_INLINE) != 0 {
                        let old_payload_key = inline_chunk_key(&ChunkId::plain(id.hash));
                        match txn.get(old_payload_key.clone())? {
                            Some(payload) => {
                                txn.set(inline_chunk_key(id), payload)?;
                                txn.delete(old_payload_key)?;
                            }
                            None => error!(
                                chunk_hash = ?id.hash,
                                "inline vault chunk record has no payload to move; reads of it will fail until the file is rewritten or the records check reports it"
                            ),
                        }
                    }
                }
                Ok(())
            })
            .await
            .with_context(|| {
                format!(
                    "failed to move a batch of {} vault chunk records into the domain-tagged key space",
                    batch.len()
                )
            })?;
            moved = moved.saturating_add(batch.len() as u64);
        }

        match next_start {
            Some(next) => start = next,
            None => return Ok(moved),
        }
    }
}
