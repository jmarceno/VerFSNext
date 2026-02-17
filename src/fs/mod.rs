use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use async_fusex::error::{AsyncFusexError, AsyncFusexResult};
use async_fusex::fs_util::{
    CreateParam, FileAttr, INum, RenameParam, SetAttrParam, StatFsParam, build_error_result_from_errno,
};
use async_fusex::{DirEntry, FileType, VirtualFs};
use async_trait::async_trait;
use nix::errno::Errno;
use nix::sys::stat::SFlag;
use nix::sys::statvfs;
use surrealkv::LSMIterator;
use tokio::sync::Mutex;
use xxhash_rust::xxh3::xxh3_128;

use crate::config::Config;
use crate::data::pack::PackStore;
use crate::meta::MetaStore;
use crate::sync::{SyncService, SyncTarget};
use crate::types::{
    BLOCK_SIZE, ChunkRecord, DirentRecord, ExtentRecord, INODE_KIND_DIR, INODE_KIND_FILE,
    INODE_KIND_SYMLINK, InodeRecord, chunk_key, decode_dirent_name, decode_rkyv, dirent_key,
    dirent_prefix, encode_rkyv, extent_key, extent_prefix, inode_key, parts_to_system_time,
    prefix_end, system_time_to_parts,
};
use crate::write::batcher::{WriteApply, WriteBatcher, WriteOp};

const ATTR_TTL: Duration = Duration::from_secs(1);

pub struct VerFs {
    core: Arc<FsCore>,
    batcher: WriteBatcher,
    sync_service: SyncService,
    shutdown_started: AtomicBool,
}

struct FsCore {
    config: Config,
    meta: MetaStore,
    packs: PackStore,
    next_handle: AtomicU64,
    write_lock: Mutex<()>,
}

impl VerFs {
    pub async fn new(config: Config) -> Result<Self> {
        config.ensure_dirs()?;

        let meta = MetaStore::open(&config.metadata_dir()).await?;
        let active_pack_id = meta.get_u64_sys("active_pack_id")?;
        let packs = PackStore::open(&config.packs_dir(), active_pack_id)?;
        packs.verify_pack_headers(active_pack_id)?;

        let core = Arc::new(FsCore {
            config: config.clone(),
            meta,
            packs,
            next_handle: AtomicU64::new(1),
            write_lock: Mutex::new(()),
        });

        let write_sink: Arc<dyn WriteApply> = core.clone();
        let sync_target: Arc<dyn SyncTarget> = core.clone();

        let batcher = WriteBatcher::new(
            write_sink,
            config.batch_max_blocks,
            Duration::from_millis(config.batch_flush_interval_ms),
            config.batch_max_blocks.saturating_mul(2).max(64),
        );
        let sync_service = SyncService::start(
            sync_target,
            Duration::from_millis(config.sync_interval_ms),
        );

        Ok(Self {
            core,
            batcher,
            sync_service,
            shutdown_started: AtomicBool::new(false),
        })
    }

    pub async fn graceful_shutdown(&self) -> Result<()> {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.batcher.shutdown().await?;
        self.sync_service.shutdown_with_full_sync().await?;
        self.core.meta.close().await?;
        Ok(())
    }
}

impl FsCore {
    fn new_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed)
    }

    fn file_attr_from_inode(&self, inode: &InodeRecord) -> FileAttr {
        FileAttr {
            ino: inode.ino,
            size: inode.size,
            blocks: inode.size.div_ceil(512),
            atime: parts_to_system_time(inode.atime_sec, inode.atime_nsec),
            mtime: parts_to_system_time(inode.mtime_sec, inode.mtime_nsec),
            ctime: parts_to_system_time(inode.ctime_sec, inode.ctime_nsec),
            kind: sflag_for_kind(inode.kind),
            perm: inode.perm,
            nlink: inode.nlink,
            uid: inode.uid,
            gid: inode.gid,
            rdev: 0,
            version: inode.generation,
        }
    }

    fn load_inode_or_errno(&self, ino: u64, context: &'static str) -> Result<InodeRecord> {
        self.meta
            .get_inode(ino)?
            .ok_or_else(|| anyhow_errno(Errno::ENOENT, format!("{}: inode {} not found", context, ino)))
    }

    fn lookup_dirent(&self, parent: u64, name: &str) -> Result<Option<DirentRecord>> {
        self.meta.read_txn(|txn| {
            let Some(raw) = txn.get(dirent_key(parent, name.as_bytes()))? else {
                return Ok(None);
            };
            let dirent: DirentRecord = decode_rkyv(&raw)?;
            Ok(Some(dirent))
        })
    }

    fn read_block_bytes(&self, ino: u64, block_idx: u64) -> Result<Vec<u8>> {
        let maybe_extent = self.meta.read_txn(|txn| {
            let Some(raw) = txn.get(extent_key(ino, block_idx))? else {
                return Ok(None);
            };
            let extent: ExtentRecord = decode_rkyv(&raw)?;
            Ok(Some(extent))
        })?;

        let Some(extent) = maybe_extent else {
            return Ok(vec![0_u8; BLOCK_SIZE]);
        };

        let chunk = self.meta.read_txn(|txn| {
            let Some(raw) = txn.get(chunk_key(&extent.chunk_hash))? else {
                return Err(anyhow_errno(
                    Errno::EIO,
                    format!(
                        "missing chunk metadata for hash {:x?} while reading inode {}",
                        extent.chunk_hash, ino
                    ),
                ));
            };
            let chunk: ChunkRecord = decode_rkyv(&raw)?;
            Ok(chunk)
        })?;

        let mut payload = self
            .packs
            .read_chunk(chunk.pack_id, chunk.offset, extent.chunk_hash, chunk.len)?;
        if payload.len() < BLOCK_SIZE {
            payload.resize(BLOCK_SIZE, 0);
        }
        if payload.len() > BLOCK_SIZE {
            payload.truncate(BLOCK_SIZE);
        }
        Ok(payload)
    }

    async fn apply_single_write(&self, op: WriteOp) -> Result<()> {
        let _guard = self.write_lock.lock().await;

        let mut inode = self.load_inode_or_errno(op.ino, "write")?;
        if inode.kind != INODE_KIND_FILE {
            return Err(anyhow_errno(
                Errno::EISDIR,
                format!("write target inode {} is not a regular file", op.ino),
            ));
        }

        let write_end = op
            .offset
            .checked_add(op.data.len() as u64)
            .ok_or_else(|| anyhow_errno(Errno::EOVERFLOW, "write offset overflow"))?;

        if op.data.is_empty() {
            return Ok(());
        }

        let start_block = op.offset / BLOCK_SIZE as u64;
        let end_block = (write_end - 1) / BLOCK_SIZE as u64;

        let old_extents = self.meta.read_txn(|txn| {
            let mut map = HashMap::<u64, ExtentRecord>::new();
            for block_idx in start_block..=end_block {
                if let Some(raw) = txn.get(extent_key(op.ino, block_idx))? {
                    map.insert(block_idx, decode_rkyv(&raw)?);
                }
            }
            Ok(map)
        })?;

        let mut extent_updates = Vec::<(u64, [u8; 16])>::new();
        let mut ref_deltas = HashMap::<[u8; 16], i64>::new();
        let mut new_chunk_records = HashMap::<[u8; 16], ChunkRecord>::new();
        let mut checked_hashes = HashSet::<[u8; 16]>::new();

        for block_idx in start_block..=end_block {
            let mut block_data = if old_extents.contains_key(&block_idx) {
                self.read_block_bytes(op.ino, block_idx)?
            } else {
                vec![0_u8; BLOCK_SIZE]
            };

            let block_start = block_idx * BLOCK_SIZE as u64;
            let copy_from = op.offset.max(block_start);
            let copy_until = write_end.min(block_start + BLOCK_SIZE as u64);
            let src_start = (copy_from - op.offset) as usize;
            let src_end = (copy_until - op.offset) as usize;
            let dst_start = (copy_from - block_start) as usize;
            let dst_end = dst_start + (src_end - src_start);
            block_data[dst_start..dst_end].copy_from_slice(&op.data[src_start..src_end]);

            let new_hash = xxh3_128(&block_data).to_le_bytes();
            let old_hash = old_extents.get(&block_idx).map(|extent| extent.chunk_hash);

            if old_hash == Some(new_hash) {
                continue;
            }

            extent_updates.push((block_idx, new_hash));
            *ref_deltas.entry(new_hash).or_insert(0) += 1;
            if let Some(old_hash) = old_hash {
                *ref_deltas.entry(old_hash).or_insert(0) -= 1;
            }

            if checked_hashes.insert(new_hash) {
                let exists = self.meta.read_txn(|txn| {
                    Ok(txn.get(chunk_key(&new_hash))?.is_some())
                })?;
                if !exists {
                    let (pack_id, offset, len) = self.packs.append_chunk(new_hash, &block_data)?;
                    new_chunk_records.insert(
                        new_hash,
                        ChunkRecord {
                            refcount: 0,
                            pack_id,
                            offset,
                            len,
                        },
                    );
                }
            }
        }

        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        inode.size = inode.size.max(write_end);
        inode.mtime_sec = sec;
        inode.mtime_nsec = nsec;
        inode.ctime_sec = sec;
        inode.ctime_nsec = nsec;

        self.meta
            .write_txn(|txn| {
                for (block_idx, hash) in extent_updates.iter().copied() {
                    let extent = ExtentRecord { chunk_hash: hash };
                    txn.set(extent_key(op.ino, block_idx), encode_rkyv(&extent)?)?;
                }

                for (hash, delta) in ref_deltas.iter() {
                    let hash = *hash;
                    let delta = *delta;
                    let key = chunk_key(&hash);
                    if let Some(raw) = txn.get(key.clone())? {
                        let mut chunk: ChunkRecord = decode_rkyv(&raw)?;
                        let next_ref = chunk.refcount as i64 + delta;
                        if next_ref <= 0 {
                            txn.delete(key)?;
                        } else {
                            chunk.refcount = next_ref as u64;
                            txn.set(key, encode_rkyv(&chunk)?)?;
                        }
                        continue;
                    }

                    if delta <= 0 {
                        continue;
                    }

                    let Some(mut chunk) = new_chunk_records.get(&hash).cloned() else {
                        return Err(anyhow_errno(
                            Errno::EIO,
                            format!("missing staged chunk metadata for hash {:x?}", hash),
                        ));
                    };
                    chunk.refcount = delta as u64;
                    txn.set(key, encode_rkyv(&chunk)?)?;
                }

                txn.set(inode_key(op.ino), encode_rkyv(&inode)?)?;
                Ok(())
            })
            .await
    }

    fn decrement_chunk_refcounts_in_txn(
        txn: &mut surrealkv::Transaction,
        hashes: &[[u8; 16]],
    ) -> Result<()> {
        for hash in hashes {
            let key = chunk_key(hash);
            let Some(raw) = txn.get(key.clone())? else {
                continue;
            };
            let mut chunk: ChunkRecord = decode_rkyv(&raw)?;
            if chunk.refcount <= 1 {
                txn.delete(key)?;
            } else {
                chunk.refcount -= 1;
                txn.set(key, encode_rkyv(&chunk)?)?;
            }
        }
        Ok(())
    }

    fn ensure_dirent_target(
        txn: &surrealkv::Transaction,
        parent: u64,
        name: &str,
        context: &'static str,
    ) -> Result<DirentRecord> {
        let Some(raw) = txn.get(dirent_key(parent, name.as_bytes()))? else {
            return Err(anyhow_errno(
                Errno::ENOENT,
                format!("{}: {} does not exist under inode {}", context, name, parent),
            ));
        };
        decode_rkyv(&raw)
    }

    fn create_inode_in_txn(
        txn: &mut surrealkv::Transaction,
        parent: INum,
        name: &str,
        kind: u8,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<InodeRecord> {
        let Some(parent_raw) = txn.get(inode_key(parent))? else {
            return Err(anyhow_errno(
                Errno::ENOENT,
                format!("create: parent inode {} does not exist", parent),
            ));
        };
        let mut parent_inode: InodeRecord = decode_rkyv(&parent_raw)?;
        if parent_inode.kind != INODE_KIND_DIR {
            return Err(anyhow_errno(
                Errno::ENOTDIR,
                format!("create: parent inode {} is not a directory", parent),
            ));
        }

        if txn.get(dirent_key(parent, name.as_bytes()))?.is_some() {
            return Err(anyhow_errno(
                Errno::EEXIST,
                format!("create: {} already exists", name),
            ));
        }

        let Some(next_inode_raw) = txn.get(crate::types::sys_key("next_inode"))? else {
            return Err(anyhow_errno(Errno::EIO, "missing SYS:next_inode"));
        };
        if next_inode_raw.len() != 8 {
            return Err(anyhow_errno(Errno::EIO, "invalid SYS:next_inode encoding"));
        }
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&next_inode_raw);
        let ino = u64::from_le_bytes(bytes);

        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        let inode = InodeRecord {
            ino,
            parent,
            kind,
            perm: (mode & 0o7777) as u16,
            uid,
            gid,
            nlink: if kind == INODE_KIND_DIR { 2 } else { 1 },
            size: 0,
            atime_sec: sec,
            atime_nsec: nsec,
            mtime_sec: sec,
            mtime_nsec: nsec,
            ctime_sec: sec,
            ctime_nsec: nsec,
            generation: 1,
        };

        txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
        txn.set(
            dirent_key(parent, name.as_bytes()),
            encode_rkyv(&DirentRecord { ino, kind })?,
        )?;
        txn.set(crate::types::sys_key("next_inode"), (ino + 1).to_le_bytes().to_vec())?;

        if kind == INODE_KIND_DIR {
            parent_inode.nlink = parent_inode.nlink.saturating_add(1);
            parent_inode.mtime_sec = sec;
            parent_inode.mtime_nsec = nsec;
            parent_inode.ctime_sec = sec;
            parent_inode.ctime_nsec = nsec;
            txn.set(inode_key(parent), encode_rkyv(&parent_inode)?)?;
        }

        Ok(inode)
    }

    fn inode_kind_to_file_type(kind: u8) -> FileType {
        match kind {
            INODE_KIND_DIR => FileType::Dir,
            INODE_KIND_FILE => FileType::File,
            INODE_KIND_SYMLINK => FileType::Symlink,
            _ => FileType::File,
        }
    }
}

#[async_trait]
impl VirtualFs for VerFs {
    async fn init(&self) -> AsyncFusexResult<()> {
        Ok(())
    }

    async fn destroy(&self) -> AsyncFusexResult<()> {
        self.graceful_shutdown().await.map_err(map_anyhow_to_fuse)
    }

    async fn lookup(
        &self,
        _uid: u32,
        _gid: u32,
        parent: INum,
        name: &str,
    ) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        let ino = if name == "." {
            parent
        } else if name == ".." {
            let parent_inode = self
                .core
                .meta
                .get_inode(parent)
                .map_err(map_anyhow_to_fuse)?
                .ok_or_else(|| {
                    AsyncFusexError::from(anyhow_errno(
                        Errno::ENOENT,
                        format!("lookup: inode {} not found", parent),
                    ))
                })?;
            parent_inode.parent
        } else {
            let Some(dirent) = self.core.lookup_dirent(parent, name).map_err(map_anyhow_to_fuse)? else {
                return build_error_result_from_errno(
                    Errno::ENOENT,
                    format!("lookup: {} not found under inode {}", name, parent),
                );
            };
            dirent.ino
        };

        let inode = self
            .core
            .meta
            .get_inode(ino)
            .map_err(map_anyhow_to_fuse)?
            .ok_or_else(|| {
                AsyncFusexError::from(anyhow_errno(
                    Errno::ENOENT,
                    format!("lookup: inode {} not found", ino),
                ))
            })?;

        let attr = self.core.file_attr_from_inode(&inode);
        Ok((ATTR_TTL, attr, inode.generation))
    }

    async fn forget(&self, _ino: u64, _nlookup: u64) {}

    async fn getattr(&self, ino: u64) -> AsyncFusexResult<(Duration, FileAttr)> {
        let inode = self
            .core
            .meta
            .get_inode(ino)
            .map_err(map_anyhow_to_fuse)?
            .ok_or_else(|| AsyncFusexError::from(anyhow_errno(Errno::ENOENT, "getattr: inode not found")))?;

        Ok((ATTR_TTL, self.core.file_attr_from_inode(&inode)))
    }

    async fn setattr(
        &self,
        _uid: u32,
        _gid: u32,
        ino: u64,
        param: SetAttrParam,
    ) -> AsyncFusexResult<(Duration, FileAttr)> {
        let _guard = self.core.write_lock.lock().await;
        let mut inode = self
            .core
            .load_inode_or_errno(ino, "setattr")
            .map_err(map_anyhow_to_fuse)?;

        if let Some(mode) = param.mode {
            inode.perm = (mode & 0o7777) as u16;
        }
        if let Some(uid) = param.u_id {
            inode.uid = uid;
        }
        if let Some(gid) = param.g_id {
            inode.gid = gid;
        }
        if let Some(size) = param.size {
            inode.size = size;
        }
        if let Some(atime) = param.a_time {
            let (sec, nsec) = system_time_to_parts(atime);
            inode.atime_sec = sec;
            inode.atime_nsec = nsec;
        }
        if let Some(mtime) = param.m_time {
            let (sec, nsec) = system_time_to_parts(mtime);
            inode.mtime_sec = sec;
            inode.mtime_nsec = nsec;
        }

        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        inode.ctime_sec = sec;
        inode.ctime_nsec = nsec;

        self.core
            .meta
            .write_txn(|txn| {
                txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;

        Ok((ATTR_TTL, self.core.file_attr_from_inode(&inode)))
    }

    async fn readlink(&self, _ino: u64) -> AsyncFusexResult<Vec<u8>> {
        build_error_result_from_errno(Errno::ENOSYS, "readlink is not implemented in phase 1".to_owned())
    }

    async fn mknod(&self, param: CreateParam) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        let create_kind = if param.node_type == SFlag::S_IFDIR {
            INODE_KIND_DIR
        } else {
            INODE_KIND_FILE
        };

        let mut created: Option<InodeRecord> = None;
        self.core
            .meta
            .write_txn(|txn| {
                let inode = FsCore::create_inode_in_txn(
                    txn,
                    param.parent,
                    &param.name,
                    create_kind,
                    param.mode,
                    param.uid,
                    param.gid,
                )?;
                created = Some(inode);
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;

        let inode = created.ok_or_else(|| AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing created inode")))?;
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
        ))
    }

    async fn mkdir(&self, param: CreateParam) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        let mut created: Option<InodeRecord> = None;
        self.core
            .meta
            .write_txn(|txn| {
                let inode = FsCore::create_inode_in_txn(
                    txn,
                    param.parent,
                    &param.name,
                    INODE_KIND_DIR,
                    param.mode,
                    param.uid,
                    param.gid,
                )?;
                created = Some(inode);
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;

        let inode = created.ok_or_else(|| AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing created directory")))?;
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
        ))
    }

    async fn unlink(
        &self,
        _uid: u32,
        _gid: u32,
        parent: INum,
        name: &str,
    ) -> AsyncFusexResult<()> {
        let _guard = self.core.write_lock.lock().await;

        self.core
            .meta
            .write_txn(|txn| {
                let dirent = FsCore::ensure_dirent_target(txn, parent, name, "unlink")?;
                let Some(inode_raw) = txn.get(inode_key(dirent.ino))? else {
                    return Err(anyhow_errno(Errno::ENOENT, "unlink target inode not found"));
                };
                let inode: InodeRecord = decode_rkyv(&inode_raw)?;

                if inode.kind == INODE_KIND_DIR {
                    return Err(anyhow_errno(Errno::EISDIR, "cannot unlink directory"));
                }

                let extent_prefix_bytes = extent_prefix(dirent.ino);
                let extent_end = prefix_end(&extent_prefix_bytes);
                let mut extent_keys = Vec::new();
                let mut hashes = Vec::new();
                for (key, value) in scan_range_pairs(txn, extent_prefix_bytes, extent_end)? {
                    let extent: ExtentRecord = decode_rkyv(&value)?;
                    extent_keys.push(key);
                    hashes.push(extent.chunk_hash);
                }

                for key in extent_keys {
                    txn.delete(key)?;
                }
                FsCore::decrement_chunk_refcounts_in_txn(txn, &hashes)?;

                txn.delete(dirent_key(parent, name.as_bytes()))?;
                txn.delete(inode_key(dirent.ino))?;
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)
    }

    async fn rmdir(
        &self,
        _uid: u32,
        _gid: u32,
        parent: INum,
        dir_name: &str,
    ) -> AsyncFusexResult<Option<INum>> {
        let _guard = self.core.write_lock.lock().await;

        self.core
            .meta
            .write_txn(|txn| {
                let dirent = FsCore::ensure_dirent_target(txn, parent, dir_name, "rmdir")?;
                let Some(raw) = txn.get(inode_key(dirent.ino))? else {
                    return Err(anyhow_errno(Errno::ENOENT, "rmdir target inode not found"));
                };
                let target_inode: InodeRecord = decode_rkyv(&raw)?;
                if target_inode.kind != INODE_KIND_DIR {
                    return Err(anyhow_errno(Errno::ENOTDIR, "rmdir target is not a directory"));
                }

                let prefix = dirent_prefix(dirent.ino);
                let end = prefix_end(&prefix);
                let mut iter = txn.range(prefix, end)?;
                let not_empty = iter.seek_first()?;
                drop(iter);
                if not_empty {
                    return Err(anyhow_errno(Errno::ENOTEMPTY, "directory not empty"));
                }

                txn.delete(dirent_key(parent, dir_name.as_bytes()))?;
                txn.delete(inode_key(dirent.ino))?;

                if let Some(parent_raw) = txn.get(inode_key(parent))? {
                    let mut parent_inode: InodeRecord = decode_rkyv(&parent_raw)?;
                    parent_inode.nlink = parent_inode.nlink.saturating_sub(1);
                    let now = SystemTime::now();
                    let (sec, nsec) = system_time_to_parts(now);
                    parent_inode.mtime_sec = sec;
                    parent_inode.mtime_nsec = nsec;
                    parent_inode.ctime_sec = sec;
                    parent_inode.ctime_nsec = nsec;
                    txn.set(inode_key(parent), encode_rkyv(&parent_inode)?)?;
                }

                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;

        Ok(None)
    }

    async fn symlink(
        &self,
        _uid: u32,
        _gid: u32,
        _parent: INum,
        _name: &str,
        _target_path: &Path,
    ) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        build_error_result_from_errno(Errno::ENOSYS, "symlink is not implemented in phase 1".to_owned())
    }

    async fn rename(&self, _uid: u32, _gid: u32, param: RenameParam) -> AsyncFusexResult<()> {
        let _guard = self.core.write_lock.lock().await;

        self.core
            .meta
            .write_txn(|txn| {
                let source = FsCore::ensure_dirent_target(
                    txn,
                    param.old_parent,
                    &param.old_name,
                    "rename source",
                )?;
                let Some(source_inode_raw) = txn.get(inode_key(source.ino))? else {
                    return Err(anyhow_errno(Errno::ENOENT, "rename source inode not found"));
                };
                let mut source_inode: InodeRecord = decode_rkyv(&source_inode_raw)?;

                if txn
                    .get(dirent_key(param.new_parent, param.new_name.as_bytes()))?
                    .is_some()
                {
                    return Err(anyhow_errno(
                        Errno::EEXIST,
                        "rename target exists; overwrite support is deferred",
                    ));
                }

                txn.delete(dirent_key(param.old_parent, param.old_name.as_bytes()))?;
                txn.set(
                    dirent_key(param.new_parent, param.new_name.as_bytes()),
                    encode_rkyv(&DirentRecord {
                        ino: source.ino,
                        kind: source.kind,
                    })?,
                )?;

                if param.old_parent != param.new_parent {
                    source_inode.parent = param.new_parent;
                    txn.set(inode_key(source.ino), encode_rkyv(&source_inode)?)?;
                }

                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)
    }

    async fn open(&self, _uid: u32, _gid: u32, ino: u64, _flags: u32) -> AsyncFusexResult<u64> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "open")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind == INODE_KIND_DIR {
            return build_error_result_from_errno(Errno::EISDIR, "open called on directory".to_owned());
        }
        Ok(self.core.new_handle())
    }

    async fn read(
        &self,
        ino: u64,
        offset: u64,
        size: u32,
        buf: &mut Vec<u8>,
    ) -> AsyncFusexResult<usize> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "read")
            .map_err(map_anyhow_to_fuse)?;

        if inode.kind != INODE_KIND_FILE {
            return build_error_result_from_errno(Errno::EISDIR, "read called on non-file inode".to_owned());
        }

        if size == 0 || offset >= inode.size {
            return Ok(0);
        }

        let read_end = inode.size.min(offset.saturating_add(size as u64));
        let start_block = offset / BLOCK_SIZE as u64;
        let end_block = (read_end - 1) / BLOCK_SIZE as u64;

        let (extents, chunks) = self
            .core
            .meta
            .read_txn(|txn| {
                let mut extents = HashMap::<u64, ExtentRecord>::new();
                let mut hashes = HashSet::<[u8; 16]>::new();

                for block_idx in start_block..=end_block {
                    if let Some(raw) = txn.get(extent_key(ino, block_idx))? {
                        let extent: ExtentRecord = decode_rkyv(&raw)?;
                        hashes.insert(extent.chunk_hash);
                        extents.insert(block_idx, extent);
                    }
                }

                let mut chunks = HashMap::<[u8; 16], ChunkRecord>::new();
                for hash in hashes {
                    if let Some(raw) = txn.get(chunk_key(&hash))? {
                        chunks.insert(hash, decode_rkyv(&raw)?);
                    }
                }

                Ok((extents, chunks))
            })
            .map_err(map_anyhow_to_fuse)?;

        let mut out = Vec::with_capacity((read_end - offset) as usize);
        for block_idx in start_block..=end_block {
            let mut block = vec![0_u8; BLOCK_SIZE];
            if let Some(extent) = extents.get(&block_idx) {
                let Some(chunk) = chunks.get(&extent.chunk_hash) else {
                    return build_error_result_from_errno(
                        Errno::EIO,
                        format!("missing chunk metadata for block {}", block_idx),
                    );
                };
                let mut payload = self
                    .core
                    .packs
                    .read_chunk(chunk.pack_id, chunk.offset, extent.chunk_hash, chunk.len)
                    .map_err(map_anyhow_to_fuse)?;
                if payload.len() < BLOCK_SIZE {
                    payload.resize(BLOCK_SIZE, 0);
                }
                if payload.len() > BLOCK_SIZE {
                    payload.truncate(BLOCK_SIZE);
                }
                block = payload;
            }

            let block_start = block_idx * BLOCK_SIZE as u64;
            let from = offset.max(block_start) as usize - block_start as usize;
            let to = read_end.min(block_start + BLOCK_SIZE as u64) as usize - block_start as usize;
            out.extend_from_slice(&block[from..to]);
        }

        let read_len = out.len();
        buf.extend_from_slice(&out);
        Ok(read_len)
    }

    async fn write(&self, ino: u64, offset: i64, data: &[u8], _flags: u32) -> AsyncFusexResult<()> {
        if offset < 0 {
            return build_error_result_from_errno(Errno::EINVAL, "negative write offset".to_owned());
        }

        let touched_blocks = if data.is_empty() {
            1
        } else {
            let start = (offset as u64) / BLOCK_SIZE as u64;
            let end = ((offset as u64) + (data.len() as u64) - 1) / BLOCK_SIZE as u64;
            (end - start + 1) as usize
        };

        let op = WriteOp {
            ino,
            offset: offset as u64,
            data: data.to_vec(),
        };

        self.batcher
            .enqueue(op, touched_blocks)
            .await
            .map_err(map_anyhow_to_fuse)
    }

    async fn flush(&self, _ino: u64, _lock_owner: u64) -> AsyncFusexResult<()> {
        Ok(())
    }

    async fn release(
        &self,
        _ino: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> AsyncFusexResult<()> {
        Ok(())
    }

    async fn fsync(&self, _ino: u64, datasync: bool) -> AsyncFusexResult<()> {
        self.core
            .sync_cycle(!datasync)
            .await
            .map_err(map_anyhow_to_fuse)
    }

    async fn opendir(&self, _uid: u32, _gid: u32, ino: u64, _flags: u32) -> AsyncFusexResult<u64> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "opendir")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind != INODE_KIND_DIR {
            return build_error_result_from_errno(Errno::ENOTDIR, "opendir called on non-directory".to_owned());
        }
        Ok(self.core.new_handle())
    }

    async fn readdir(
        &self,
        _uid: u32,
        _gid: u32,
        ino: u64,
        _fh: u64,
        _offset: i64,
    ) -> AsyncFusexResult<Vec<DirEntry>> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "readdir")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind != INODE_KIND_DIR {
            return build_error_result_from_errno(Errno::ENOTDIR, "readdir called on non-directory".to_owned());
        }

        let mut entries = Vec::new();
        entries.push(DirEntry::new(ino, ".".to_owned(), FileType::Dir));
        entries.push(DirEntry::new(
            inode.parent,
            "..".to_owned(),
            FileType::Dir,
        ));

        let dir_entries = self
            .core
            .meta
            .read_txn(|txn| {
                let prefix = dirent_prefix(ino);
                let end = prefix_end(&prefix);
                let mut out = Vec::new();
                for (key, value) in scan_range_pairs(txn, prefix, end)? {
                    let Some(name_bytes) = decode_dirent_name(&key) else {
                        continue;
                    };
                    let name = String::from_utf8_lossy(name_bytes).to_string();
                    let dirent: DirentRecord = decode_rkyv(&value)?;
                    out.push((name, dirent.ino, dirent.kind));
                }
                Ok(out)
            })
            .map_err(map_anyhow_to_fuse)?;

        for (name, child_ino, kind) in dir_entries {
            entries.push(DirEntry::new(
                child_ino,
                name,
                FsCore::inode_kind_to_file_type(kind),
            ));
        }

        Ok(entries)
    }

    async fn releasedir(&self, _ino: u64, _fh: u64, _flags: u32) -> AsyncFusexResult<()> {
        Ok(())
    }

    async fn fsyncdir(&self, _ino: u64, _fh: u64, datasync: bool) -> AsyncFusexResult<()> {
        self.core
            .sync_cycle(!datasync)
            .await
            .map_err(map_anyhow_to_fuse)
    }

    async fn statfs(&self, _uid: u32, _gid: u32, _ino: u64) -> AsyncFusexResult<StatFsParam> {
        let vfs = statvfs::statvfs(&self.core.config.data_dir).map_err(|err| {
            AsyncFusexError::from(anyhow::Error::new(err).context("statfs failed on data dir"))
        })?;

        Ok(StatFsParam {
            blocks: vfs.blocks(),
            bfree: vfs.blocks_free(),
            bavail: vfs.blocks_available(),
            files: vfs.files(),
            f_free: vfs.files_free(),
            bsize: vfs.block_size() as u32,
            namelen: vfs.name_max() as u32,
            frsize: vfs.fragment_size() as u32,
        })
    }

    async fn create(
        &self,
        uid: u32,
        gid: u32,
        _ino: u64,
        parent: u64,
        name: &str,
        mode: u32,
        _flags: u32,
    ) -> AsyncFusexResult<(Duration, FileAttr, u64, u64, u32)> {
        let mut created: Option<InodeRecord> = None;
        self.core
            .meta
            .write_txn(|txn| {
                let inode = FsCore::create_inode_in_txn(
                    txn,
                    parent,
                    name,
                    INODE_KIND_FILE,
                    mode,
                    uid,
                    gid,
                )?;
                created = Some(inode);
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;

        let inode = created.ok_or_else(|| AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing created file inode")))?;
        let fh = self.core.new_handle();
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
            fh,
            0,
        ))
    }
}

#[async_trait]
impl WriteApply for FsCore {
    async fn apply_batch(&self, ops: Vec<WriteOp>) -> Vec<Result<()>> {
        let mut out = Vec::with_capacity(ops.len());
        for op in ops {
            let res = self.apply_single_write(op).await;
            out.push(res);
        }
        out
    }
}

#[async_trait]
impl SyncTarget for FsCore {
    async fn sync_cycle(&self, full: bool) -> Result<()> {
        if full {
            self.packs.sync(true)?;
            self.meta.flush_wal(true)?;
        } else {
            self.packs.sync(false)?;
            self.meta.flush_wal(false)?;
        }
        Ok(())
    }
}

fn sflag_for_kind(kind: u8) -> SFlag {
    match kind {
        INODE_KIND_DIR => SFlag::S_IFDIR,
        INODE_KIND_FILE => SFlag::S_IFREG,
        INODE_KIND_SYMLINK => SFlag::S_IFLNK,
        _ => SFlag::S_IFREG,
    }
}

fn map_anyhow_to_fuse(err: anyhow::Error) -> AsyncFusexError {
    if err.root_cause().downcast_ref::<nix::Error>().is_some() {
        AsyncFusexError::from(err)
    } else {
        AsyncFusexError::from(anyhow_errno(Errno::EIO, err.to_string()))
    }
}

fn anyhow_errno(message_errno: Errno, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(message_errno).context(message.into())
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
