use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};
use nix::unistd::{getgid, getuid};
use surrealkv::{Mode, Tree, TreeBuilder};

use crate::types::{
    decode_rkyv, dirent_key, encode_rkyv, inode_key, sys_key, system_time_to_parts, DirentRecord,
    InodeRecord, INODE_FLAG_READONLY, INODE_KIND_DIR, ROOT_INODE, SNAPSHOTS_DIR_NAME,
};

pub struct MetaStore {
    tree: Tree,
}

impl MetaStore {
    pub async fn open(path: &Path) -> Result<Self> {
        let tree = TreeBuilder::new()
            .with_path(path.to_path_buf())
            .with_max_memtable_size(32 * 1024 * 1024)
            .with_flush_on_close(true)
            .build()
            .context("failed to open SurrealKV tree")?;
        let store = Self { tree };
        store.bootstrap().await?;
        Ok(store)
    }

    async fn bootstrap(&self) -> Result<()> {
        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        let root = InodeRecord {
            ino: ROOT_INODE,
            parent: ROOT_INODE,
            kind: INODE_KIND_DIR,
            perm: 0o755,
            uid: getuid().as_raw(),
            gid: getgid().as_raw(),
            nlink: 2,
            size: 0,
            atime_sec: sec,
            atime_nsec: nsec,
            mtime_sec: sec,
            mtime_nsec: nsec,
            ctime_sec: sec,
            ctime_nsec: nsec,
            generation: 1,
            flags: 0,
        };

        self.write_txn(|txn| {
            if txn.get(inode_key(ROOT_INODE))?.is_none() {
                txn.set(inode_key(ROOT_INODE), encode_rkyv(&root)?)?;
            }
            if txn.get(sys_key("next_inode"))?.is_none() {
                txn.set(sys_key("next_inode"), 2_u64.to_le_bytes().to_vec())?;
            }
            if txn.get(sys_key("active_pack_id"))?.is_none() {
                txn.set(sys_key("active_pack_id"), 1_u64.to_le_bytes().to_vec())?;
            }
            if txn.get(sys_key("gc.discard_checkpoint"))?.is_none() {
                txn.set(
                    sys_key("gc.discard_checkpoint"),
                    0_u64.to_le_bytes().to_vec(),
                )?;
            }
            if txn.get(sys_key("gc.epoch"))?.is_none() {
                txn.set(sys_key("gc.epoch"), 0_u64.to_le_bytes().to_vec())?;
            }

            let snapshots_name = SNAPSHOTS_DIR_NAME.as_bytes();
            let snapshots_entry_key = dirent_key(ROOT_INODE, snapshots_name);
            let mut next_inode = {
                let raw = txn
                    .get(sys_key("next_inode"))?
                    .context("missing SYS:next_inode after bootstrap initialization")?;
                if raw.len() != 8 {
                    anyhow::bail!("invalid SYS:next_inode length {}", raw.len());
                }
                let mut bytes = [0_u8; 8];
                bytes.copy_from_slice(&raw);
                u64::from_le_bytes(bytes)
            };

            if txn.get(snapshots_entry_key.clone())?.is_none() {
                let mut root_inode: InodeRecord = decode_rkyv(
                    &txn.get(inode_key(ROOT_INODE))?
                        .context("missing root inode during snapshots bootstrap")?,
                )?;
                let snapshots_ino = next_inode;
                next_inode = next_inode.saturating_add(1);

                let snapshots_inode = InodeRecord {
                    ino: snapshots_ino,
                    parent: ROOT_INODE,
                    kind: INODE_KIND_DIR,
                    perm: 0o755,
                    uid: getuid().as_raw(),
                    gid: getgid().as_raw(),
                    nlink: 2,
                    size: 0,
                    atime_sec: sec,
                    atime_nsec: nsec,
                    mtime_sec: sec,
                    mtime_nsec: nsec,
                    ctime_sec: sec,
                    ctime_nsec: nsec,
                    generation: 1,
                    flags: INODE_FLAG_READONLY,
                };

                root_inode.nlink = root_inode.nlink.saturating_add(1);
                root_inode.mtime_sec = sec;
                root_inode.mtime_nsec = nsec;
                root_inode.ctime_sec = sec;
                root_inode.ctime_nsec = nsec;

                txn.set(inode_key(ROOT_INODE), encode_rkyv(&root_inode)?)?;
                txn.set(inode_key(snapshots_ino), encode_rkyv(&snapshots_inode)?)?;
                txn.set(
                    snapshots_entry_key,
                    encode_rkyv(&DirentRecord {
                        ino: snapshots_ino,
                        kind: INODE_KIND_DIR,
                    })?,
                )?;
                txn.set(sys_key("next_inode"), next_inode.to_le_bytes().to_vec())?;
            } else if let Some(raw_dirent) = txn.get(snapshots_entry_key)? {
                let dirent: DirentRecord = decode_rkyv(&raw_dirent)?;
                if let Some(raw_inode) = txn.get(inode_key(dirent.ino))? {
                    let mut inode: InodeRecord = decode_rkyv(&raw_inode)?;
                    if (inode.flags & INODE_FLAG_READONLY) == 0 {
                        inode.flags |= INODE_FLAG_READONLY;
                        txn.set(inode_key(inode.ino), encode_rkyv(&inode)?)?;
                    }
                }
            }
            Ok(())
        })
        .await
    }

    pub fn read_txn<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&surrealkv::Transaction) -> Result<R>,
    {
        let txn = self
            .tree
            .begin_with_mode(Mode::ReadOnly)
            .context("failed to start readonly transaction")?;
        f(&txn)
    }

    pub async fn write_txn<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut surrealkv::Transaction) -> Result<()>,
    {
        let mut txn = self
            .tree
            .begin()
            .context("failed to start write transaction")?;
        f(&mut txn)?;
        txn.commit().await.context("failed to commit transaction")
    }

    pub fn get_inode(&self, ino: u64) -> Result<Option<InodeRecord>> {
        self.read_txn(|txn| {
            let Some(raw) = txn.get(inode_key(ino))? else {
                return Ok(None);
            };
            let inode: InodeRecord = decode_rkyv(&raw)?;
            Ok(Some(inode))
        })
    }

    pub fn get_u64_sys(&self, name: &str) -> Result<u64> {
        self.read_txn(|txn| {
            let key = sys_key(name);
            let raw = txn
                .get(key)?
                .with_context(|| format!("missing system key SYS:{}", name))?;
            if raw.len() != 8 {
                anyhow::bail!("invalid SYS:{} length {}, expected 8", name, raw.len());
            }
            let mut bytes = [0_u8; 8];
            bytes.copy_from_slice(&raw);
            Ok(u64::from_le_bytes(bytes))
        })
    }

    pub fn flush_wal(&self, sync: bool) -> Result<()> {
        self.tree
            .flush_wal(sync)
            .context("failed to flush SurrealKV WAL")
    }

    pub async fn close(&self) -> Result<()> {
        self.tree.close().await.context("failed to close SurrealKV")
    }
}
