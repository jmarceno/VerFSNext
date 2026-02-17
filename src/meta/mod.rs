use std::path::Path;
use std::time::SystemTime;

use anyhow::{Context, Result};
use nix::unistd::{getgid, getuid};
use surrealkv::{Mode, Tree, TreeBuilder};

use crate::types::{
    INODE_KIND_DIR, InodeRecord, ROOT_INODE, decode_rkyv, encode_rkyv, inode_key, sys_key,
    system_time_to_parts,
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
