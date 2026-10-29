//! One-time migration: upgrade inode records to storage schema v2.
//!
//! Schema v2 appends the birth time and the device id that statx and
//! block/character device nodes need: `btime_sec`, `btime_nsec` and `rdev`.
//! Records written before it are shorter and cannot be decoded by the current
//! [`InodeRecord`], so every inode record is rewritten in place. Snapshot
//! inodes live in the same key space (`I` + inode number) and are upgraded by
//! the same scan.
//!
//! The birth time of an inode created before the schema existed is unknown.
//! Like the filesystems that gained birth time later, the migration uses the
//! inode's ctime as the best available approximation. `rdev` stays zero
//! because device nodes could not be created before the schema existed.
//!
//! The rewrite is batched (one transaction per batch of inodes) and idempotent
//! per record: records that already decode as the v2 layout are skipped, so a
//! crash in the middle of the scan resumes cleanly and a rerun is a no-op. The
//! marker is written only after every inode record was visited.

use std::mem::size_of;

use anyhow::{bail, Result};
use rkyv::{Archive, Deserialize, Serialize};
use tracing::info;

use crate::fs::utils::scan_range_pairs_limited;
use crate::meta::MetaStore;
use crate::types::{
    decode_rkyv, dirent_key, encode_rkyv, inode_key, prefix_end, sys_key, InodeRecord,
    KEY_PREFIX_INODE, ROOT_INODE, SNAPSHOTS_DIR_NAME,
};

pub const SYS_INODE_SCHEMA_V2_MIGRATION_V1: &str = "inode_schema_v2_migration_v1";

/// Inode records rewritten per metadata transaction, bounding the transaction
/// size on large data directories.
const BATCH: usize = 10_000;

/// The pre-v2 on-disk inode layout. Kept only to read what older versions
/// wrote; it must stay a byte-for-byte description of the old archive.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, Copy)]
#[rkyv(bytecheck())]
struct InodeRecordV1 {
    ino: u64,
    parent: u64,
    kind: u8,
    perm: u16,
    uid: u32,
    gid: u32,
    nlink: u32,
    size: u64,
    atime_sec: i64,
    atime_nsec: u32,
    mtime_sec: i64,
    mtime_nsec: u32,
    ctime_sec: i64,
    ctime_nsec: u32,
    generation: u64,
    flags: u32,
}

/// Length of a schema v1 inode record as stored in the metadata store. The
/// archives of both schemas are plain structs, so their length is an exact
/// discriminator between a record upgraded by an earlier run and one still in
/// the old layout (needed to resume safely after a crash mid-migration).
fn schema_v1_record_len() -> usize {
    size_of::<ArchivedInodeRecordV1>()
}

fn schema_v2_record_len() -> usize {
    size_of::<<InodeRecord as Archive>::Archived>()
}

pub async fn ensure_inode_schema_v2_migrated(meta: &MetaStore) -> Result<()> {
    if migration_done(meta)? {
        return Ok(());
    }

    let migrated = upgrade_inode_records(meta).await?;
    ensure_snapshots_inode_readonly(meta).await?;
    mark_done(meta).await?;
    info!(
        upgraded_inodes = migrated,
        "upgraded inode records to storage schema v2 (birth time = ctime, rdev = 0)"
    );
    Ok(())
}

fn migration_done(meta: &MetaStore) -> Result<bool> {
    let Some(raw) = meta.get_sys(SYS_INODE_SCHEMA_V2_MIGRATION_V1)? else {
        return Ok(false);
    };
    if raw.len() != 8 {
        bail!(
            "invalid SYS:{} length {}, expected 8",
            SYS_INODE_SCHEMA_V2_MIGRATION_V1,
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
            sys_key(SYS_INODE_SCHEMA_V2_MIGRATION_V1),
            1_u64.to_le_bytes().to_vec(),
        )?;
        Ok(())
    })
    .await
}

/// Rewrites every pre-v2 inode record. Returns the number of records upgraded
/// on this run (already-upgraded records are counted as zero, so a resumed run
/// only reports the work it actually did).
async fn upgrade_inode_records(meta: &MetaStore) -> Result<u64> {
    let prefix = vec![KEY_PREFIX_INODE];
    let end = prefix_end(&prefix);
    let mut start = prefix;
    let mut upgraded = 0_u64;
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
            let v1_len = schema_v1_record_len();
            let v2_len = schema_v2_record_len();
            let mut batch = Vec::new();
            for (key, value) in pairs {
                if key.len() != 9 {
                    continue;
                }
                let ino = u64::from_be_bytes(key[1..9].try_into().map_err(|_| {
                    anyhow::anyhow!("malformed inode key {key:?}")
                })?);
                if value.len() == v2_len {
                    // Written by this or a later version, already upgraded.
                    let inode: InodeRecord = decode_rkyv(&value)?;
                    if inode.ino != ino {
                        bail!(
                            "inode key {key:?} holds an inode record for inode {}",
                            inode.ino
                        );
                    }
                } else if value.len() == v1_len {
                    batch.push(key);
                } else {
                    bail!(
                        "inode key {key:?} has {} bytes, which is neither the schema v1 ({v1_len}) nor the schema v2 ({v2_len}) record length",
                        value.len()
                    );
                }
            }
            Ok((batch, next_start))
        })?;

        if !batch.is_empty() {
            let migrated = batch.len() as u64;
            meta.write_txn(|txn| {
                for key in &batch {
                    let Some(raw) = txn.get(key.clone())? else {
                        continue;
                    };
                    if raw.len() != schema_v1_record_len() {
                        // A concurrent writer upgraded it after the scan.
                        continue;
                    }
                    let old: InodeRecordV1 = decode_rkyv(&raw)?;
                    let expected_ino = u64::from_be_bytes(
                        key[1..9]
                            .try_into()
                            .map_err(|_| anyhow::anyhow!("malformed inode key {key:?}"))?,
                    );
                    if old.ino != expected_ino {
                        bail!(
                            "inode key {key:?} holds a schema v1 record for inode {}",
                            old.ino
                        );
                    }
                    let upgraded = InodeRecord {
                        ino: old.ino,
                        parent: old.parent,
                        kind: old.kind,
                        perm: old.perm,
                        uid: old.uid,
                        gid: old.gid,
                        nlink: old.nlink,
                        size: old.size,
                        atime_sec: old.atime_sec,
                        atime_nsec: old.atime_nsec,
                        mtime_sec: old.mtime_sec,
                        mtime_nsec: old.mtime_nsec,
                        ctime_sec: old.ctime_sec,
                        ctime_nsec: old.ctime_nsec,
                        generation: old.generation,
                        flags: old.flags,
                        // Birth time was not recorded before schema v2.
                        btime_sec: old.ctime_sec,
                        btime_nsec: old.ctime_nsec,
                        // Device nodes could not exist before schema v2.
                        rdev: 0,
                    };
                    txn.set(key.clone(), encode_rkyv(&upgraded)?)?;
                }
                Ok(())
            })
            .await?;
            upgraded = upgraded.saturating_add(migrated);
        }

        match next_start {
            Some(next) => start = next,
            None => return Ok(upgraded),
        }
    }
}

/// The `.snapshots` directory is a read-only view: the flag must stay set.
/// Older versions repaired it on every open; the repair now runs once here,
/// because reading an inode record before the schema upgrade is impossible.
async fn ensure_snapshots_inode_readonly(meta: &MetaStore) -> Result<()> {
    meta.write_txn(|txn| {
        let Some(raw_dirent) = txn.get(dirent_key(ROOT_INODE, SNAPSHOTS_DIR_NAME.as_bytes()))?
        else {
            return Ok(());
        };
        let dirent: crate::types::DirentRecord = decode_rkyv(&raw_dirent)?;
        let Some(raw_inode) = txn.get(inode_key(dirent.ino))? else {
            bail!(
                "{} exists in the root directory but its inode {} is missing",
                SNAPSHOTS_DIR_NAME,
                dirent.ino
            );
        };
        let mut inode: InodeRecord = decode_rkyv(&raw_inode)?;
        if (inode.flags & crate::types::INODE_FLAG_READONLY) == 0 {
            inode.flags |= crate::types::INODE_FLAG_READONLY;
            txn.set(inode_key(inode.ino), encode_rkyv(&inode)?)?;
        }
        Ok(())
    })
    .await
}
