use std::collections::HashMap;
use std::time::SystemTime;

use anyhow::{anyhow, Context, Result};
use nix::errno::Errno;
use surrealkv::LSMIterator;

use crate::meta::MetaStore;
use crate::types::{
    chunk_key, decode_dirent_name, decode_rkyv, decode_snapshot_name, dirent_key, dirent_prefix,
    encode_rkyv, extent_prefix, inode_key, prefix_end, snapshot_key, snapshot_prefix,
    symlink_target_key, sys_key, system_time_to_parts, xattr_key, xattr_prefix, ChunkRecord,
    DirentRecord, ExtentRecord, InodeRecord, SnapshotRecord, INODE_FLAG_READONLY, INODE_KIND_DIR,
    INODE_KIND_FILE, INODE_KIND_SYMLINK, ROOT_INODE, SNAPSHOTS_DIR_NAME,
};

pub struct SnapshotManager<'a> {
    meta: &'a MetaStore,
}

impl<'a> SnapshotManager<'a> {
    pub fn new(meta: &'a MetaStore) -> Self {
        Self { meta }
    }

    pub fn list(&self) -> Result<Vec<String>> {
        self.meta.read_txn(|txn| {
            let prefix = snapshot_prefix();
            let end = prefix_end(&prefix);
            let mut names = Vec::new();
            for (key, _) in scan_range_pairs(txn, prefix, end)? {
                let Some(raw_name) = decode_snapshot_name(&key) else {
                    continue;
                };
                names.push(String::from_utf8_lossy(raw_name).to_string());
            }
            names.sort_unstable();
            Ok(names)
        })
    }

    pub async fn create(&self, name: &str) -> Result<()> {
        validate_snapshot_name(name)?;

        self.meta
            .write_txn(|txn| {
                let snapshots_dir = load_snapshots_dir_ino(txn)?;
                if txn.get(snapshot_key(name.as_bytes()))?.is_some() {
                    return Err(anyhow_errno(
                        Errno::EEXIST,
                        format!("snapshot '{name}' already exists"),
                    ));
                }
                if txn
                    .get(dirent_key(snapshots_dir, name.as_bytes()))?
                    .is_some()
                {
                    return Err(anyhow_errno(
                        Errno::EEXIST,
                        format!("snapshot directory '{name}' already exists"),
                    ));
                }

                let root = load_inode(txn, ROOT_INODE, "snapshot create root")?;
                if root.kind != INODE_KIND_DIR {
                    return Err(anyhow_errno(
                        Errno::EIO,
                        "root inode kind is not directory during snapshot create",
                    ));
                }

                let mut next_inode = read_next_inode(txn)?;
                let mut ref_deltas = HashMap::<[u8; 16], i64>::new();

                let snap_root_ino = allocate_inode(&mut next_inode)?;
                let mut snap_root = root.clone();
                snap_root.ino = snap_root_ino;
                snap_root.parent = snapshots_dir;
                snap_root.flags |= INODE_FLAG_READONLY;
                txn.set(inode_key(snap_root_ino), encode_rkyv(&snap_root)?)?;
                txn.set(
                    dirent_key(snapshots_dir, name.as_bytes()),
                    encode_rkyv(&DirentRecord {
                        ino: snap_root_ino,
                        kind: INODE_KIND_DIR,
                    })?,
                )?;

                clone_dir_children(
                    txn,
                    ROOT_INODE,
                    snap_root_ino,
                    &mut next_inode,
                    &mut ref_deltas,
                    true,
                )?;

                apply_ref_deltas_in_txn(txn, &ref_deltas)?;

                let now = SystemTime::now();
                let (sec, nsec) = system_time_to_parts(now);
                let snapshot_record = SnapshotRecord {
                    root_ino: snap_root_ino,
                    created_at_sec: sec,
                    created_at_nsec: nsec,
                };
                txn.set(
                    snapshot_key(name.as_bytes()),
                    encode_rkyv(&snapshot_record)?,
                )?;
                txn.set(sys_key("next_inode"), next_inode.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        validate_snapshot_name(name)?;

        self.meta
            .write_txn(|txn| {
                let snapshots_dir = load_snapshots_dir_ino(txn)?;
                let Some(snapshot_dirent_raw) =
                    txn.get(dirent_key(snapshots_dir, name.as_bytes()))?
                else {
                    return Err(anyhow_errno(
                        Errno::ENOENT,
                        format!("snapshot '{name}' does not exist"),
                    ));
                };
                let snapshot_dirent: DirentRecord = decode_rkyv(&snapshot_dirent_raw)?;

                let mut ref_deltas = HashMap::<[u8; 16], i64>::new();
                remove_snapshot_subtree(txn, snapshot_dirent.ino, &mut ref_deltas)?;
                apply_ref_deltas_in_txn(txn, &ref_deltas)?;

                txn.delete(dirent_key(snapshots_dir, name.as_bytes()))?;
                txn.delete(snapshot_key(name.as_bytes()))?;
                Ok(())
            })
            .await
    }
}

fn validate_snapshot_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(anyhow_errno(
            Errno::EINVAL,
            "snapshot name must not be empty",
        ));
    }
    if name == "." || name == ".." {
        return Err(anyhow_errno(
            Errno::EINVAL,
            "snapshot name cannot be . or ..",
        ));
    }
    if name.contains('/') {
        return Err(anyhow_errno(
            Errno::EINVAL,
            "snapshot name cannot include '/'",
        ));
    }
    Ok(())
}

fn load_snapshots_dir_ino(txn: &surrealkv::Transaction) -> Result<u64> {
    let Some(raw) = txn.get(dirent_key(ROOT_INODE, SNAPSHOTS_DIR_NAME.as_bytes()))? else {
        return Err(anyhow_errno(
            Errno::EIO,
            "missing /.snapshots bootstrap directory",
        ));
    };
    let dirent: DirentRecord = decode_rkyv(&raw)?;
    Ok(dirent.ino)
}

fn read_next_inode(txn: &surrealkv::Transaction) -> Result<u64> {
    let raw = txn
        .get(sys_key("next_inode"))?
        .context("missing SYS:next_inode")?;
    if raw.len() != 8 {
        return Err(anyhow!("invalid SYS:next_inode length {}", raw.len()));
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&raw);
    Ok(u64::from_le_bytes(bytes))
}

fn allocate_inode(next_inode: &mut u64) -> Result<u64> {
    let ino = *next_inode;
    if ino == u64::MAX {
        return Err(anyhow!("inode allocator exhausted"));
    }
    *next_inode = next_inode.saturating_add(1);
    Ok(ino)
}

fn load_inode(txn: &surrealkv::Transaction, ino: u64, ctx: &str) -> Result<InodeRecord> {
    let Some(raw) = txn.get(inode_key(ino))? else {
        return Err(anyhow_errno(
            Errno::ENOENT,
            format!("{ctx}: inode {ino} not found"),
        ));
    };
    decode_rkyv(&raw)
}

fn clone_dir_children(
    txn: &mut surrealkv::Transaction,
    src_dir_ino: u64,
    dst_dir_ino: u64,
    next_inode: &mut u64,
    ref_deltas: &mut HashMap<[u8; 16], i64>,
    src_is_root: bool,
) -> Result<()> {
    let entries = list_dirents(txn, src_dir_ino)?;
    for (name_bytes, dirent) in entries {
        if src_is_root && name_bytes == SNAPSHOTS_DIR_NAME.as_bytes() {
            continue;
        }

        let src_inode = load_inode(txn, dirent.ino, "snapshot clone source")?;
        let dst_ino = allocate_inode(next_inode)?;
        let mut dst_inode = src_inode.clone();
        dst_inode.ino = dst_ino;
        dst_inode.parent = dst_dir_ino;
        dst_inode.flags |= INODE_FLAG_READONLY;

        txn.set(inode_key(dst_ino), encode_rkyv(&dst_inode)?)?;
        txn.set(
            dirent_key(dst_dir_ino, &name_bytes),
            encode_rkyv(&DirentRecord {
                ino: dst_ino,
                kind: src_inode.kind,
            })?,
        )?;

        clone_xattrs(txn, src_inode.ino, dst_ino)?;

        match src_inode.kind {
            INODE_KIND_DIR => {
                clone_dir_children(txn, src_inode.ino, dst_ino, next_inode, ref_deltas, false)?;
            }
            INODE_KIND_FILE => {
                clone_file_extents(txn, src_inode.ino, dst_ino, ref_deltas)?;
            }
            INODE_KIND_SYMLINK => {
                if let Some(target) = txn.get(symlink_target_key(src_inode.ino))? {
                    txn.set(symlink_target_key(dst_ino), target)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn list_dirents(
    txn: &surrealkv::Transaction,
    dir_ino: u64,
) -> Result<Vec<(Vec<u8>, DirentRecord)>> {
    let mut out = Vec::new();
    let prefix = dirent_prefix(dir_ino);
    let end = prefix_end(&prefix);
    for (key, value) in scan_range_pairs(txn, prefix, end)? {
        let Some(name_bytes) = decode_dirent_name(&key) else {
            continue;
        };
        let dirent: DirentRecord = decode_rkyv(&value)?;
        out.push((name_bytes.to_vec(), dirent));
    }
    Ok(out)
}

fn clone_xattrs(txn: &mut surrealkv::Transaction, src_ino: u64, dst_ino: u64) -> Result<()> {
    let prefix = xattr_prefix(src_ino);
    let end = prefix_end(&prefix);
    let pairs = scan_range_pairs(txn, prefix, end)?;
    for (key, value) in pairs {
        let Some(name) = crate::types::decode_xattr_name(&key) else {
            continue;
        };
        txn.set(xattr_key(dst_ino, name), value)?;
    }
    Ok(())
}

fn clone_file_extents(
    txn: &mut surrealkv::Transaction,
    src_ino: u64,
    dst_ino: u64,
    ref_deltas: &mut HashMap<[u8; 16], i64>,
) -> Result<()> {
    let prefix = extent_prefix(src_ino);
    let end = prefix_end(&prefix);
    for (key, value) in scan_range_pairs(txn, prefix, end)? {
        let mut dst_key = extent_prefix(dst_ino);
        dst_key.extend_from_slice(&key[9..]);
        txn.set(dst_key, value.clone())?;
        let extent: ExtentRecord = decode_rkyv(&value)?;
        *ref_deltas.entry(extent.chunk_hash).or_insert(0) += 1;
    }
    Ok(())
}

fn remove_snapshot_subtree(
    txn: &mut surrealkv::Transaction,
    ino: u64,
    ref_deltas: &mut HashMap<[u8; 16], i64>,
) -> Result<()> {
    let inode = load_inode(txn, ino, "snapshot delete")?;

    if inode.kind == INODE_KIND_DIR {
        let children = list_dirents(txn, ino)?;
        for (name, dirent) in children {
            remove_snapshot_subtree(txn, dirent.ino, ref_deltas)?;
            txn.delete(dirent_key(ino, &name))?;
        }
    }

    let xattr_prefix_bytes = xattr_prefix(ino);
    let xattr_end = prefix_end(&xattr_prefix_bytes);
    for (key, _) in scan_range_pairs(txn, xattr_prefix_bytes, xattr_end)? {
        txn.delete(key)?;
    }

    match inode.kind {
        INODE_KIND_FILE => {
            let ext_prefix = extent_prefix(ino);
            let ext_end = prefix_end(&ext_prefix);
            for (key, value) in scan_range_pairs(txn, ext_prefix, ext_end)? {
                let extent: ExtentRecord = decode_rkyv(&value)?;
                *ref_deltas.entry(extent.chunk_hash).or_insert(0) -= 1;
                txn.delete(key)?;
            }
        }
        INODE_KIND_SYMLINK => {
            txn.delete(symlink_target_key(ino))?;
        }
        _ => {}
    }

    txn.delete(inode_key(ino))?;
    Ok(())
}

fn apply_ref_deltas_in_txn(
    txn: &mut surrealkv::Transaction,
    ref_deltas: &HashMap<[u8; 16], i64>,
) -> Result<()> {
    for (hash, delta) in ref_deltas {
        if *delta == 0 {
            continue;
        }
        let key = chunk_key(hash);
        let Some(raw) = txn.get(key.clone())? else {
            if *delta < 0 {
                continue;
            }
            return Err(anyhow_errno(
                Errno::EIO,
                format!(
                    "missing chunk metadata while applying snapshot delta for {:x?}",
                    hash
                ),
            ));
        };

        let mut chunk: ChunkRecord = decode_rkyv(&raw)?;
        let next_ref = chunk.refcount as i64 + *delta;
        if next_ref < 0 {
            return Err(anyhow_errno(
                Errno::EIO,
                format!(
                    "negative chunk refcount while applying snapshot delta for {:x?}",
                    hash
                ),
            ));
        }
        chunk.refcount = next_ref as u64;
        txn.set(key, encode_rkyv(&chunk)?)?;
    }
    Ok(())
}

fn scan_range_pairs(
    txn: &surrealkv::Transaction,
    start: Vec<u8>,
    end: Vec<u8>,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut iter = txn.range(start, end)?;
    let mut valid = iter.seek_first()?;
    while valid {
        out.push((iter.key().user_key().to_vec(), iter.value()?));
        valid = iter.next()?;
    }
    Ok(out)
}

fn anyhow_errno(message_errno: Errno, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(message_errno).context(message.into())
}
