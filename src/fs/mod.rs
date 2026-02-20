use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_fusex::error::{AsyncFusexError, AsyncFusexResult};
use async_fusex::fs_util::{
    build_error_result_from_errno, CreateParam, FileAttr, FileLockParam, INum, RenameParam,
    SetAttrParam, StatFsParam,
};
use async_fusex::protocol::FUSE_WRITE_CACHE;
use async_fusex::{DirEntry, FileType, VirtualFs};
use async_trait::async_trait;
use moka::sync::Cache;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::libc;
use nix::sys::stat::SFlag;
use nix::sys::statvfs;
use parking_lot::RwLock;
use surrealkv::LSMIterator;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::debug;

use crate::config::Config;
use crate::data::chunker::UltraStreamChunker;
use crate::data::compress::{compress_parallel, PendingChunk};
use crate::data::hash::{hash128, hash128_with_domain};
use crate::data::pack::PackStore;
use crate::gc::{append_records, ensure_file, read_records, rewrite_records, DiscardRecord};
use crate::meta::MetaStore;
use crate::snapshot::SnapshotManager;
use crate::sync::{SyncService, SyncTarget};
use crate::types::{
    chunk_key, decode_dirent_name, decode_rkyv, decode_xattr_name, dirent_key, dirent_prefix,
    encode_rkyv, extent_key, extent_prefix, inode_key, parts_to_system_time, prefix_end,
    symlink_target_key, sys_key, system_time_to_parts, xattr_key, xattr_prefix, ChunkRecord,
    DirentRecord, ExtentRecord, InodeRecord, BLOCK_SIZE, INODE_FLAG_READONLY, INODE_FLAG_VAULT,
    INODE_FLAG_VAULT_ROOT, INODE_KIND_DIR, INODE_KIND_FILE, INODE_KIND_SYMLINK, KEY_PREFIX_CHUNK,
    KEY_PREFIX_INODE, ROOT_INODE, SNAPSHOTS_DIR_NAME, VAULT_DIR_NAME,
};
use crate::vault::{
    build_wrap_record, decrypt_chunk_payload, encrypt_chunk_payload, generate_folder_key,
    generate_key_file_material, read_key_file, resolve_create_key_path, unwrap_folder_key,
    write_key_file, VaultArgon2Params, VaultRuntime, VaultWrapRecord, CHUNK_FLAG_ENCRYPTED,
    SYS_VAULT_STATE, SYS_VAULT_WRAP, VAULT_STATE_LOCKED, VAULT_STATE_UNLOCKED,
};
use crate::write::batcher::{WriteApply, WriteBatcher, WriteOp};

const ATTR_TTL: Duration = Duration::from_secs(1);
const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const RENAME_FLAG_NOREPLACE: u32 = libc::RENAME_NOREPLACE as u32;
const RENAME_FLAG_EXCHANGE: u32 = libc::RENAME_EXCHANGE as u32;

#[derive(Clone)]
struct FileLockState {
    lock_owner: u64,
    start: u64,
    end: u64,
    typ: u32,
    pid: u32,
}

#[derive(Debug, Clone, Copy)]
struct ZeroRefCandidate {
    hash: [u8; 16],
    pack_id: u64,
    block_size_bytes: u32,
}

#[derive(Debug, Clone)]
pub struct VerFsStats {
    pub live_logical_size_bytes: u64,
    pub snapshots_logical_size_bytes: u64,
    pub all_logical_size_bytes: u64,
    pub live_referenced_uncompressed_bytes: u64,
    pub live_referenced_compressed_bytes: u64,
    pub live_unique_uncompressed_bytes: u64,
    pub live_unique_compressed_bytes: u64,
    pub stored_unique_uncompressed_bytes: u64,
    pub stored_unique_compressed_bytes: u64,
    pub metadata_size_bytes: u64,
    pub data_dir_size_bytes: u64,
    pub live_dedup_savings_bytes: u64,
    pub cache_hits: u64,
    pub cache_requests: u64,
    pub cache_hit_rate: f64,
    pub process_private_memory_bytes: u64,
    pub process_rss_bytes: u64,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
    pub uptime_secs: f64,
    pub read_throughput_bps: f64,
    pub write_throughput_bps: f64,
    pub metadata_cache_entries: u64,
    pub chunk_cache_entries: u64,
    pub approx_cache_memory_bytes: u64,
}

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
    chunk_meta_cache: Cache<[u8; 16], ChunkRecord>,
    chunk_data_cache: Cache<[u8; 16], Arc<Vec<u8>>>,
    next_handle: AtomicU64,
    last_persisted_active_pack_id: AtomicU64,
    dedup_hits: AtomicU64,
    dedup_misses: AtomicU64,
    chunk_meta_cache_hits: AtomicU64,
    chunk_meta_cache_misses: AtomicU64,
    chunk_data_cache_hits: AtomicU64,
    chunk_data_cache_misses: AtomicU64,
    read_bytes_total: AtomicU64,
    write_bytes_total: AtomicU64,
    stats_started_ms: AtomicU64,
    last_mutation_ms: AtomicU64,
    last_activity_ms: AtomicU64,
    write_lock: Mutex<()>,
    gc_lock: Mutex<()>,
    file_locks: Mutex<HashMap<u64, Vec<FileLockState>>>,
    vault: RwLock<VaultRuntime>,
}

impl VerFs {
    pub async fn new(config: Config) -> Result<Self> {
        config.ensure_dirs()?;

        let meta = MetaStore::open(&config.metadata_dir()).await?;
        let configured_active_pack_id = meta.get_u64_sys("active_pack_id")?;
        let packs = PackStore::open(
            &config.packs_dir(),
            configured_active_pack_id,
            config.pack_index_cache_capacity_entries,
            config.pack_max_size_mb.saturating_mul(1024 * 1024),
        )?;
        packs.verify_pack_headers(packs.active_pack_id())?;
        let vault_initialized = meta
            .get_sys(SYS_VAULT_WRAP)?
            .map(|raw| !raw.is_empty())
            .unwrap_or(false);
        if meta.get_sys(SYS_VAULT_STATE)?.is_none() {
            meta.write_txn(|txn| {
                txn.set(sys_key(SYS_VAULT_STATE), vec![VAULT_STATE_LOCKED])?;
                Ok(())
            })
            .await?;
        }

        let core = Arc::new(FsCore {
            config: config.clone(),
            meta,
            packs,
            chunk_meta_cache: Cache::builder()
                .max_capacity(config.metadata_cache_capacity_entries)
                .build(),
            chunk_data_cache: Cache::builder()
                .max_capacity(config.chunk_cache_capacity_mb.saturating_mul(1024 * 1024))
                .weigher(|_k, v: &Arc<Vec<u8>>| v.capacity() as u32)
                .build(),
            next_handle: AtomicU64::new(1),
            last_persisted_active_pack_id: AtomicU64::new(configured_active_pack_id),
            dedup_hits: AtomicU64::new(0),
            dedup_misses: AtomicU64::new(0),
            chunk_meta_cache_hits: AtomicU64::new(0),
            chunk_meta_cache_misses: AtomicU64::new(0),
            chunk_data_cache_hits: AtomicU64::new(0),
            chunk_data_cache_misses: AtomicU64::new(0),
            read_bytes_total: AtomicU64::new(0),
            write_bytes_total: AtomicU64::new(0),
            stats_started_ms: AtomicU64::new(now_millis()),
            last_mutation_ms: AtomicU64::new(now_millis()),
            last_activity_ms: AtomicU64::new(now_millis()),
            write_lock: Mutex::new(()),
            gc_lock: Mutex::new(()),
            file_locks: Mutex::new(HashMap::new()),
            vault: RwLock::new(VaultRuntime::new(vault_initialized)),
        });

        let write_sink: Arc<dyn WriteApply> = core.clone();
        let sync_target: Arc<dyn SyncTarget> = core.clone();

        let batcher = WriteBatcher::new(
            write_sink,
            config.batch_max_size_mb.saturating_mul(1024 * 1024),
            Duration::from_millis(config.batch_flush_interval_ms),
            2048,
        );
        let sync_service =
            SyncService::start(sync_target, Duration::from_millis(config.sync_interval_ms));

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

    pub fn list_snapshots(&self) -> Result<Vec<String>> {
        let snapshots = SnapshotManager::new(&self.core.meta);
        snapshots.list()
    }

    pub async fn create_snapshot(&self, name: &str) -> Result<()> {
        let _guard = self.core.write_lock.lock().await;
        let snapshots = SnapshotManager::new(&self.core.meta);
        snapshots.create(name).await?;
        self.core.mark_mutation();
        Ok(())
    }

    pub async fn delete_snapshot(&self, name: &str) -> Result<()> {
        let _guard = self.core.write_lock.lock().await;
        let snapshots = SnapshotManager::new(&self.core.meta);
        snapshots.delete(name).await?;
        self.core.mark_mutation();
        Ok(())
    }

    pub async fn create_vault(
        &self,
        password: &str,
        key_path: Option<&Path>,
    ) -> Result<std::path::PathBuf> {
        self.core.create_vault(password, key_path).await
    }

    pub async fn unlock_vault(&self, password: &str, key_file: &Path) -> Result<()> {
        self.core.unlock_vault(password, key_file).await
    }

    pub async fn lock_vault(&self) -> Result<()> {
        self.core.lock_vault().await
    }

    pub fn collect_stats(&self) -> Result<VerFsStats> {
        self.core.collect_stats()
    }

    pub fn is_gc_in_progress(&self) -> bool {
        self.core.gc_lock.try_lock().is_err()
    }
}

impl FsCore {
    fn new_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed)
    }

    fn vault_locked(&self) -> bool {
        !self.vault.read().unlocked()
    }

    fn inode_is_vault(inode: &InodeRecord) -> bool {
        (inode.flags & INODE_FLAG_VAULT) != 0
    }

    fn hash_for_inode(inode: &InodeRecord, data: &[u8]) -> [u8; 16] {
        if FsCore::inode_is_vault(inode) {
            hash128_with_domain(0xA7, data)
        } else {
            hash128(data)
        }
    }

    fn check_vault_parent_name_gate(&self, parent: u64, name: &str) -> Result<()> {
        if self.vault_locked() && parent == ROOT_INODE && name == VAULT_DIR_NAME {
            return Err(anyhow_errno(Errno::ENOENT, "vault is locked"));
        }
        Ok(())
    }

    fn ensure_inode_vault_access(&self, inode: &InodeRecord, context: &'static str) -> Result<()> {
        if self.vault_locked() && FsCore::inode_is_vault(inode) {
            return Err(anyhow_errno(
                Errno::ENOENT,
                format!("{context}: vault is locked"),
            ));
        }
        Ok(())
    }

    fn current_vault_key(&self) -> Result<[u8; 32]> {
        let vault = self.vault.read();
        if !vault.unlocked() {
            return Err(anyhow_errno(Errno::EACCES, "vault is locked"));
        }
        vault
            .key()
            .ok_or_else(|| anyhow_errno(Errno::EIO, "vault key is unavailable"))
    }

    async fn create_vault(
        &self,
        password: &str,
        key_path: Option<&Path>,
    ) -> Result<std::path::PathBuf> {
        if !self.config.vault_enabled {
            return Err(anyhow_errno(
                Errno::EOPNOTSUPP,
                "vault is disabled by configuration",
            ));
        }
        let _guard = self.write_lock.lock().await;

        if self.meta.get_sys(SYS_VAULT_WRAP)?.is_some() {
            return Err(anyhow_errno(Errno::EEXIST, "vault is already initialized"));
        }

        let key_material = generate_key_file_material();
        let folder_key = generate_folder_key();
        let wrap = build_wrap_record(
            password,
            &key_material,
            &folder_key,
            VaultArgon2Params {
                mem_kib: self.config.vault_argon2_mem_kib,
                iters: self.config.vault_argon2_iters,
                parallelism: self.config.vault_argon2_parallelism,
            },
        )?;
        let out_key_path = resolve_create_key_path(key_path)?;
        write_key_file(&out_key_path, &key_material)?;

        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);

        self.meta
            .write_txn(|txn| {
                if txn
                    .get(dirent_key(ROOT_INODE, VAULT_DIR_NAME.as_bytes()))?
                    .is_some()
                {
                    return Err(anyhow_errno(
                        Errno::EEXIST,
                        "vault namespace already exists",
                    ));
                }

                let Some(next_inode_raw) = txn.get(sys_key("next_inode"))? else {
                    return Err(anyhow_errno(Errno::EIO, "missing SYS:next_inode"));
                };
                if next_inode_raw.len() != 8 {
                    return Err(anyhow_errno(Errno::EIO, "invalid SYS:next_inode encoding"));
                }
                let mut next_inode_bytes = [0_u8; 8];
                next_inode_bytes.copy_from_slice(&next_inode_raw);
                let vault_ino = u64::from_le_bytes(next_inode_bytes);

                let Some(root_raw) = txn.get(inode_key(ROOT_INODE))? else {
                    return Err(anyhow_errno(Errno::EIO, "missing root inode"));
                };
                let mut root_inode: InodeRecord = decode_rkyv(&root_raw)?;
                root_inode.nlink = root_inode.nlink.saturating_add(1);
                root_inode.mtime_sec = sec;
                root_inode.mtime_nsec = nsec;
                root_inode.ctime_sec = sec;
                root_inode.ctime_nsec = nsec;
                txn.set(inode_key(ROOT_INODE), encode_rkyv(&root_inode)?)?;

                let vault_inode = InodeRecord {
                    ino: vault_ino,
                    parent: ROOT_INODE,
                    kind: INODE_KIND_DIR,
                    perm: 0o700,
                    uid: root_inode.uid,
                    gid: root_inode.gid,
                    nlink: 2,
                    size: 0,
                    atime_sec: sec,
                    atime_nsec: nsec,
                    mtime_sec: sec,
                    mtime_nsec: nsec,
                    ctime_sec: sec,
                    ctime_nsec: nsec,
                    generation: 1,
                    flags: INODE_FLAG_VAULT | INODE_FLAG_VAULT_ROOT,
                };
                txn.set(inode_key(vault_ino), encode_rkyv(&vault_inode)?)?;
                txn.set(
                    dirent_key(ROOT_INODE, VAULT_DIR_NAME.as_bytes()),
                    encode_rkyv(&DirentRecord {
                        ino: vault_ino,
                        kind: INODE_KIND_DIR,
                    })?,
                )?;
                txn.set(
                    sys_key("next_inode"),
                    (vault_ino + 1).to_le_bytes().to_vec(),
                )?;
                txn.set(sys_key(SYS_VAULT_WRAP), encode_rkyv(&wrap)?)?;
                txn.set(sys_key(SYS_VAULT_STATE), vec![VAULT_STATE_LOCKED])?;
                Ok(())
            })
            .await?;

        {
            let mut vault = self.vault.write();
            vault.set_initialized(true);
            vault.lock();
        }
        self.chunk_meta_cache.invalidate_all();
        self.chunk_data_cache.invalidate_all();
        self.mark_mutation();
        Ok(out_key_path)
    }

    async fn unlock_vault(&self, password: &str, key_file: &Path) -> Result<()> {
        if !self.config.vault_enabled {
            return Err(anyhow_errno(
                Errno::EOPNOTSUPP,
                "vault is disabled by configuration",
            ));
        }
        let _guard = self.write_lock.lock().await;

        let wrap_raw = self
            .meta
            .get_sys(SYS_VAULT_WRAP)?
            .ok_or_else(|| anyhow_errno(Errno::ENOENT, "vault is not initialized"))?;
        let wrap: VaultWrapRecord = decode_rkyv(&wrap_raw)?;
        let key_material = read_key_file(key_file)?;
        let folder_key = unwrap_folder_key(password, &key_material, &wrap)?;
        {
            let mut vault = self.vault.write();
            if vault.unlocked() {
                return Err(anyhow_errno(Errno::EALREADY, "vault is already unlocked"));
            }
            vault.unlock_with(folder_key);
            vault.set_initialized(true);
        }
        self.meta
            .write_txn(|txn| {
                txn.set(sys_key(SYS_VAULT_STATE), vec![VAULT_STATE_UNLOCKED])?;
                Ok(())
            })
            .await?;
        self.chunk_meta_cache.invalidate_all();
        self.chunk_data_cache.invalidate_all();
        Ok(())
    }

    async fn lock_vault(&self) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        {
            let mut vault = self.vault.write();
            if !vault.unlocked() {
                return Err(anyhow_errno(Errno::EALREADY, "vault is already locked"));
            }
            vault.lock();
        }
        self.meta
            .write_txn(|txn| {
                txn.set(sys_key(SYS_VAULT_STATE), vec![VAULT_STATE_LOCKED])?;
                Ok(())
            })
            .await?;
        self.chunk_meta_cache.invalidate_all();
        self.chunk_data_cache.invalidate_all();
        Ok(())
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
        self.meta.get_inode(ino)?.ok_or_else(|| {
            anyhow_errno(
                Errno::ENOENT,
                format!("{}: inode {} not found", context, ino),
            )
        })
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

    fn ultracdc_chunk_count(&self, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }
        let mut chunker = UltraStreamChunker::new(
            self.config.ultracdc_min_size_bytes,
            self.config.ultracdc_avg_size_bytes,
            self.config.ultracdc_max_size_bytes,
        );
        let mut count = 0_usize;
        for piece in data.chunks(64 * 1024) {
            count = count.saturating_add(chunker.feed(piece).len());
        }
        count.saturating_add(chunker.finish().len())
    }

    fn load_chunk_record(&self, hash: [u8; 16]) -> Result<ChunkRecord> {
        if let Some(cached) = self.chunk_meta_cache.get(&hash) {
            self.chunk_meta_cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached);
        }
        self.chunk_meta_cache_misses.fetch_add(1, Ordering::Relaxed);
        let chunk = self.meta.read_txn(|txn| {
            let Some(raw) = txn.get(chunk_key(&hash))? else {
                return Err(anyhow_errno(
                    Errno::EIO,
                    format!("missing chunk metadata for hash {:x?}", hash),
                ));
            };
            let chunk: ChunkRecord = decode_rkyv(&raw)?;
            Ok(chunk)
        })?;
        self.chunk_meta_cache.insert(hash, chunk.clone());
        Ok(chunk)
    }

    fn read_block_bytes(&self, ino: u64, block_idx: u64, vault_encrypted: bool) -> Result<Vec<u8>> {
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

        if let Some(cached) = self.chunk_data_cache.get(&extent.chunk_hash) {
            self.chunk_data_cache_hits.fetch_add(1, Ordering::Relaxed);
            let mut payload = cached.as_ref().clone();
            if payload.len() < BLOCK_SIZE {
                payload.resize(BLOCK_SIZE, 0);
            }
            if payload.len() > BLOCK_SIZE {
                payload.truncate(BLOCK_SIZE);
            }
            return Ok(payload);
        }
        self.chunk_data_cache_misses.fetch_add(1, Ordering::Relaxed);

        let chunk = self.load_chunk_record(extent.chunk_hash)?;
        let mut payload = if (chunk.flags & CHUNK_FLAG_ENCRYPTED) != 0 {
            if !vault_encrypted {
                return Err(anyhow_errno(
                    Errno::EIO,
                    "encrypted chunk referenced by non-vault inode",
                ));
            }
            let encrypted = self.packs.read_chunk_payload(
                chunk.pack_id,
                extent.chunk_hash,
                chunk.codec,
                chunk.uncompressed_len,
                chunk.compressed_len,
            )?;
            let folder_key = self.current_vault_key()?;
            let compressed = decrypt_chunk_payload(&folder_key, &chunk.nonce, &encrypted)?;
            crate::data::compress::decompress_chunk(
                chunk.codec,
                &compressed,
                chunk.uncompressed_len,
            )?
        } else {
            self.packs.read_chunk(
                chunk.pack_id,
                extent.chunk_hash,
                chunk.codec,
                chunk.uncompressed_len,
                chunk.compressed_len,
            )?
        };
        self.chunk_data_cache
            .insert(extent.chunk_hash, Arc::new(payload.clone()));
        if payload.len() < BLOCK_SIZE {
            payload.resize(BLOCK_SIZE, 0);
        }
        if payload.len() > BLOCK_SIZE {
            payload.truncate(BLOCK_SIZE);
        }
        Ok(payload)
    }

    fn stage_chunk_if_missing(
        &self,
        chunk_hash: [u8; 16],
        data: &[u8],
        checked_hashes: &mut HashSet<[u8; 16]>,
        pending_chunks: &mut HashMap<[u8; 16], Vec<u8>>,
    ) -> Result<bool> {
        if !checked_hashes.insert(chunk_hash) {
            return Ok(false);
        }

        let exists = self
            .meta
            .read_txn(|txn| Ok(txn.get(chunk_key(&chunk_hash))?.is_some()))?;
        if exists {
            return Ok(false);
        }

        pending_chunks.insert(chunk_hash, data.to_vec());
        Ok(true)
    }

    async fn materialize_pending_chunks(
        &self,
        pending_chunks: HashMap<[u8; 16], Vec<u8>>,
        encrypt_for_vault: bool,
    ) -> Result<HashMap<[u8; 16], ChunkRecord>> {
        if pending_chunks.is_empty() {
            return Ok(HashMap::new());
        }

        let pending = pending_chunks
            .into_iter()
            .map(|(hash, data)| PendingChunk { hash, data })
            .collect::<Vec<_>>();
        let zstd_level = self.config.zstd_compression_level;
        let ready = tokio::task::spawn_blocking(move || compress_parallel(pending, zstd_level))
            .await
            .map_err(|e| anyhow!("compression task failed: {}", e))??;

        let mut out = HashMap::with_capacity(ready.len());
        for ready_chunk in ready {
            let (payload, disk_compressed_len, nonce, flags) = if encrypt_for_vault {
                let folder_key = self.current_vault_key()?;
                let (ciphertext, nonce) =
                    encrypt_chunk_payload(&folder_key, &ready_chunk.chunk.compressed)?;
                let clen = ciphertext.len() as u32;
                (ciphertext, clen, nonce, CHUNK_FLAG_ENCRYPTED)
            } else {
                (
                    ready_chunk.chunk.compressed.clone(),
                    ready_chunk.chunk.compressed_len,
                    [0_u8; 24],
                    0,
                )
            };
            let pack_id = self.packs.append_chunk(
                ready_chunk.hash,
                ready_chunk.chunk.codec,
                ready_chunk.chunk.uncompressed_len,
                &payload,
            )?;
            let chunk = ChunkRecord {
                refcount: 0,
                pack_id,
                codec: ready_chunk.chunk.codec,
                flags,
                nonce,
                uncompressed_len: ready_chunk.chunk.uncompressed_len,
                compressed_len: disk_compressed_len,
            };
            self.chunk_meta_cache
                .insert(ready_chunk.hash, chunk.clone());
            out.insert(ready_chunk.hash, chunk);
        }

        Ok(out)
    }

    fn apply_ref_deltas_in_txn(
        txn: &mut surrealkv::Transaction,
        ref_deltas: &HashMap<[u8; 16], i64>,
        new_chunk_records: &HashMap<[u8; 16], ChunkRecord>,
    ) -> Result<()> {
        for (hash, delta) in ref_deltas.iter() {
            let hash = *hash;
            let delta = *delta;
            if delta == 0 {
                continue;
            }

            let key = chunk_key(&hash);
            if let Some(raw) = txn.get(key.clone())? {
                let mut chunk: ChunkRecord = decode_rkyv(&raw)?;
                let next_ref = chunk.refcount as i64 + delta;
                chunk.refcount = next_ref.max(0) as u64;
                txn.set(key, encode_rkyv(&chunk)?)?;
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
        Ok(())
    }

    fn extent_block_idx_from_key(key: &[u8]) -> Option<u64> {
        if key.len() != 17 || key[0] != crate::types::KEY_PREFIX_EXTENT {
            return None;
        }
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(&key[9..17]);
        Some(u64::from_be_bytes(bytes))
    }

    fn xattr_name_nonempty(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(anyhow_errno(Errno::EINVAL, "xattr name must not be empty"));
        }
        Ok(())
    }

    fn check_access_for_mask(inode: &InodeRecord, uid: u32, gid: u32, mask: u32) -> Result<()> {
        let mode_bits = inode.perm as u32;
        let access_mask = mask & ((libc::R_OK | libc::W_OK | libc::X_OK) as u32);
        if access_mask == 0 {
            return Ok(());
        }

        if uid == 0 {
            let exec_requested = (access_mask & libc::X_OK as u32) != 0;
            if !exec_requested {
                return Ok(());
            }
            let any_exec =
                (mode_bits & 0o100) != 0 || (mode_bits & 0o010) != 0 || (mode_bits & 0o001) != 0;
            if any_exec {
                return Ok(());
            }
            return Err(anyhow_errno(Errno::EACCES, "execute permission denied"));
        }

        let perm_triplet = if uid == inode.uid {
            (mode_bits >> 6) & 0o7
        } else if gid == inode.gid {
            (mode_bits >> 3) & 0o7
        } else {
            mode_bits & 0o7
        };

        let mut required = 0_u32;
        if (access_mask & libc::R_OK as u32) != 0 {
            required |= 0o4;
        }
        if (access_mask & libc::W_OK as u32) != 0 {
            required |= 0o2;
        }
        if (access_mask & libc::X_OK as u32) != 0 {
            required |= 0o1;
        }

        if (perm_triplet & required) == required {
            Ok(())
        } else {
            Err(anyhow_errno(Errno::EACCES, "permission denied"))
        }
    }

    fn remove_inode_payload_in_txn(
        txn: &mut surrealkv::Transaction,
        inode: &InodeRecord,
    ) -> Result<()> {
        let extent_prefix_bytes = extent_prefix(inode.ino);
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

        let xattr_prefix_bytes = xattr_prefix(inode.ino);
        let xattr_end = prefix_end(&xattr_prefix_bytes);
        for (key, _) in scan_range_pairs(txn, xattr_prefix_bytes, xattr_end)? {
            txn.delete(key)?;
        }

        if inode.kind == INODE_KIND_SYMLINK {
            txn.delete(symlink_target_key(inode.ino))?;
        }
        Ok(())
    }

    fn remove_name_from_inode_in_txn(
        txn: &mut surrealkv::Transaction,
        inode: &mut InodeRecord,
    ) -> Result<bool> {
        if inode.kind == INODE_KIND_DIR {
            FsCore::remove_inode_payload_in_txn(txn, inode)?;
            txn.delete(inode_key(inode.ino))?;
            return Ok(true);
        }

        if inode.nlink > 1 {
            inode.nlink -= 1;
            txn.set(inode_key(inode.ino), encode_rkyv(inode)?)?;
            Ok(false)
        } else {
            FsCore::remove_inode_payload_in_txn(txn, inode)?;
            txn.delete(inode_key(inode.ino))?;
            Ok(true)
        }
    }

    fn is_dir_empty(txn: &surrealkv::Transaction, ino: u64) -> Result<bool> {
        let prefix = dirent_prefix(ino);
        let end = prefix_end(&prefix);
        let mut iter = txn.range(prefix, end)?;
        let not_empty = iter.seek_first()?;
        Ok(!not_empty)
    }

    fn touch_parent_inode_in_txn(
        txn: &mut surrealkv::Transaction,
        parent_ino: u64,
        nlink_delta: i32,
        sec: i64,
        nsec: u32,
    ) -> Result<()> {
        let Some(raw) = txn.get(inode_key(parent_ino))? else {
            return Err(anyhow_errno(
                Errno::ENOENT,
                format!("rename parent inode {} not found", parent_ino),
            ));
        };
        let mut inode: InodeRecord = decode_rkyv(&raw)?;
        if inode.kind != INODE_KIND_DIR {
            return Err(anyhow_errno(
                Errno::ENOTDIR,
                format!("rename parent inode {} is not a directory", parent_ino),
            ));
        }

        if nlink_delta < 0 {
            inode.nlink = inode.nlink.saturating_sub(nlink_delta.unsigned_abs());
        } else if nlink_delta > 0 {
            inode.nlink = inode.nlink.saturating_add(nlink_delta as u32);
        }

        inode.mtime_sec = sec;
        inode.mtime_nsec = nsec;
        inode.ctime_sec = sec;
        inode.ctime_nsec = nsec;
        txn.set(inode_key(parent_ino), encode_rkyv(&inode)?)?;
        Ok(())
    }

    fn lock_type_valid(typ: u32) -> bool {
        typ == libc::F_RDLCK as u32 || typ == libc::F_WRLCK as u32 || typ == libc::F_UNLCK as u32
    }

    fn lock_ranges_overlap(a: &FileLockState, b: &FileLockState) -> bool {
        let a_end = a.end;
        let b_end = b.end;
        a.start <= b_end && b.start <= a_end
    }

    fn lock_conflict(request: &FileLockState, existing: &FileLockState) -> bool {
        if request.lock_owner == existing.lock_owner {
            return false;
        }
        if !FsCore::lock_ranges_overlap(request, existing) {
            return false;
        }
        request.typ == libc::F_WRLCK as u32 || existing.typ == libc::F_WRLCK as u32
    }

    async fn truncate_file_locked(&self, ino: u64, new_size: u64) -> Result<InodeRecord> {
        let mut inode = self.load_inode_or_errno(ino, "truncate")?;
        self.ensure_inode_vault_access(&inode, "truncate")?;
        if inode.kind != INODE_KIND_FILE {
            return Err(anyhow_errno(
                Errno::EISDIR,
                format!("truncate inode {} is not a regular file", ino),
            ));
        }
        FsCore::ensure_inode_writable(&inode, "truncate")?;

        if new_size == inode.size {
            return Ok(inode);
        }

        let old_size = inode.size;
        let mut extent_updates = Vec::<(u64, [u8; 16])>::new();
        let mut extent_deletes = Vec::<Vec<u8>>::new();
        let mut ref_deltas = HashMap::<[u8; 16], i64>::new();
        let mut pending_chunks = HashMap::<[u8; 16], Vec<u8>>::new();
        let mut checked_hashes = HashSet::<[u8; 16]>::new();

        if new_size < old_size {
            let block_size = BLOCK_SIZE as u64;
            let has_boundary = new_size > 0 && (new_size % block_size) != 0;
            let boundary_block = new_size / block_size;
            let keep_block_start = if has_boundary {
                boundary_block + 1
            } else {
                new_size.div_ceil(block_size)
            };

            let extents = self.meta.read_txn(|txn| {
                let prefix = extent_prefix(ino);
                let end = prefix_end(&prefix);
                let mut out = Vec::<(u64, [u8; 16], Vec<u8>)>::new();
                for (key, value) in scan_range_pairs(txn, prefix, end)? {
                    let Some(block_idx) = FsCore::extent_block_idx_from_key(&key) else {
                        continue;
                    };
                    let extent: ExtentRecord = decode_rkyv(&value)?;
                    out.push((block_idx, extent.chunk_hash, key));
                }
                Ok(out)
            })?;

            for (block_idx, chunk_hash, key) in extents {
                if has_boundary && block_idx == boundary_block {
                    let mut block_data =
                        self.read_block_bytes(ino, block_idx, FsCore::inode_is_vault(&inode))?;
                    let offset_in_block = (new_size % block_size) as usize;
                    block_data[offset_in_block..].fill(0);
                    let new_hash = FsCore::hash_for_inode(&inode, &block_data);
                    if new_hash != chunk_hash {
                        extent_updates.push((block_idx, new_hash));
                        *ref_deltas.entry(chunk_hash).or_insert(0) -= 1;
                        *ref_deltas.entry(new_hash).or_insert(0) += 1;
                        self.stage_chunk_if_missing(
                            new_hash,
                            &block_data,
                            &mut checked_hashes,
                            &mut pending_chunks,
                        )?;
                    }
                    continue;
                }

                if block_idx >= keep_block_start {
                    extent_deletes.push(key);
                    *ref_deltas.entry(chunk_hash).or_insert(0) -= 1;
                }
            }
        }
        let new_chunk_records = self
            .materialize_pending_chunks(pending_chunks, FsCore::inode_is_vault(&inode))
            .await?;

        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        inode.size = new_size;
        inode.mtime_sec = sec;
        inode.mtime_nsec = nsec;
        inode.ctime_sec = sec;
        inode.ctime_nsec = nsec;

        self.meta
            .write_txn(|txn| {
                for key in extent_deletes {
                    txn.delete(key)?;
                }
                for (block_idx, hash) in extent_updates {
                    txn.set(
                        extent_key(ino, block_idx),
                        encode_rkyv(&ExtentRecord { chunk_hash: hash })?,
                    )?;
                }
                FsCore::apply_ref_deltas_in_txn(txn, &ref_deltas, &new_chunk_records)?;
                txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
                Ok(())
            })
            .await?;
        self.mark_mutation();

        Ok(inode)
    }

    async fn apply_single_write(&self, op: WriteOp) -> Result<()> {
        let _guard = self.write_lock.lock().await;

        let mut inode = self.load_inode_or_errno(op.ino, "write")?;
        self.ensure_inode_vault_access(&inode, "write")?;
        if inode.kind != INODE_KIND_FILE {
            return Err(anyhow_errno(
                Errno::EISDIR,
                format!("write target inode {} is not a regular file", op.ino),
            ));
        }
        FsCore::ensure_inode_writable(&inode, "write")?;

        let write_end = op
            .offset
            .checked_add(op.data.len() as u64)
            .ok_or_else(|| anyhow_errno(Errno::EOVERFLOW, "write offset overflow"))?;

        if op.data.is_empty() {
            return Ok(());
        }

        let cdc_chunk_count = self.ultracdc_chunk_count(&op.data);
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
        let mut pending_chunks = HashMap::<[u8; 16], Vec<u8>>::new();
        let mut checked_hashes = HashSet::<[u8; 16]>::new();
        let mut dedup_hits = 0_u64;
        let mut dedup_misses = 0_u64;

        for block_idx in start_block..=end_block {
            let mut block_data = if old_extents.contains_key(&block_idx) {
                self.read_block_bytes(op.ino, block_idx, FsCore::inode_is_vault(&inode))?
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

            let new_hash = FsCore::hash_for_inode(&inode, &block_data);
            let old_hash = old_extents.get(&block_idx).map(|extent| extent.chunk_hash);

            if old_hash == Some(new_hash) {
                continue;
            }

            extent_updates.push((block_idx, new_hash));
            *ref_deltas.entry(new_hash).or_insert(0) += 1;
            if let Some(old_hash) = old_hash {
                *ref_deltas.entry(old_hash).or_insert(0) -= 1;
            }

            let staged_new = self.stage_chunk_if_missing(
                new_hash,
                &block_data,
                &mut checked_hashes,
                &mut pending_chunks,
            )?;
            if staged_new {
                dedup_misses = dedup_misses.saturating_add(1);
            } else {
                dedup_hits = dedup_hits.saturating_add(1);
            }
        }
        let new_chunk_records = self
            .materialize_pending_chunks(pending_chunks, FsCore::inode_is_vault(&inode))
            .await?;

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

                FsCore::apply_ref_deltas_in_txn(txn, &ref_deltas, &new_chunk_records)?;

                txn.set(inode_key(op.ino), encode_rkyv(&inode)?)?;
                Ok(())
            })
            .await?;
        self.mark_mutation();

        self.dedup_hits.fetch_add(dedup_hits, Ordering::Relaxed);
        self.dedup_misses.fetch_add(dedup_misses, Ordering::Relaxed);
        self.write_bytes_total
            .fetch_add(op.data.len() as u64, Ordering::Relaxed);
        debug!(
            ino = op.ino,
            offset = op.offset,
            bytes = op.data.len(),
            cdc_chunks = cdc_chunk_count,
            dedup_hits,
            dedup_misses,
            "write completed through streaming dedup/compress pipeline"
        );
        Ok(())
    }

    fn decrement_chunk_refcounts_in_txn(
        txn: &mut surrealkv::Transaction,
        hashes: &[[u8; 16]],
    ) -> Result<()> {
        if hashes.is_empty() {
            return Ok(());
        }

        let mut ref_deltas = HashMap::<[u8; 16], i64>::with_capacity(hashes.len());
        for hash in hashes {
            *ref_deltas.entry(*hash).or_insert(0) -= 1;
        }

        FsCore::apply_ref_deltas_in_txn(txn, &ref_deltas, &HashMap::new())
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
                format!(
                    "{}: {} does not exist under inode {}",
                    context, name, parent
                ),
            ));
        };
        decode_rkyv(&raw)
    }

    fn ensure_parent_dir_writable_in_txn(
        txn: &surrealkv::Transaction,
        parent: u64,
        context: &'static str,
    ) -> Result<InodeRecord> {
        let Some(parent_raw) = txn.get(inode_key(parent))? else {
            return Err(anyhow_errno(
                Errno::ENOENT,
                format!("{context}: parent inode {parent} not found"),
            ));
        };
        let parent_inode: InodeRecord = decode_rkyv(&parent_raw)?;
        if parent_inode.kind != INODE_KIND_DIR {
            return Err(anyhow_errno(
                Errno::ENOTDIR,
                format!("{context}: parent inode {parent} is not a directory"),
            ));
        }
        FsCore::ensure_inode_writable(&parent_inode, context)?;
        Ok(parent_inode)
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
        if (parent_inode.flags & INODE_FLAG_READONLY) != 0 {
            return Err(anyhow_errno(
                Errno::EROFS,
                format!("create: parent inode {} is read-only", parent),
            ));
        }
        if parent == ROOT_INODE && name == VAULT_DIR_NAME {
            return Err(anyhow_errno(
                Errno::EPERM,
                "top-level .vault is managed only by crypt control commands",
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
        let inherited_flags = if (parent_inode.flags & INODE_FLAG_VAULT) != 0 {
            INODE_FLAG_VAULT
        } else {
            0
        };
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
            flags: inherited_flags,
        };

        txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
        txn.set(
            dirent_key(parent, name.as_bytes()),
            encode_rkyv(&DirentRecord { ino, kind })?,
        )?;
        txn.set(
            crate::types::sys_key("next_inode"),
            (ino + 1).to_le_bytes().to_vec(),
        )?;

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

    fn ensure_inode_writable(inode: &InodeRecord, context: &'static str) -> Result<()> {
        if (inode.flags & INODE_FLAG_READONLY) != 0 {
            return Err(anyhow_errno(
                Errno::EROFS,
                format!("{context}: read-only inode {}", inode.ino),
            ));
        }
        Ok(())
    }

    fn mark_activity(&self) {
        self.last_activity_ms.store(now_millis(), Ordering::Relaxed);
    }

    fn mark_mutation(&self) {
        let now = now_millis();
        self.last_mutation_ms.store(now, Ordering::Relaxed);
        self.last_activity_ms.store(now, Ordering::Relaxed);
    }

    fn gc_idle_window_open(&self) -> bool {
        let now = now_millis();
        let last_mutation = self.last_mutation_ms.load(Ordering::Relaxed);
        let last_activity = self.last_activity_ms.load(Ordering::Relaxed);
        let last_event = last_mutation.max(last_activity);
        if now.saturating_sub(last_event) < self.config.gc_idle_min_ms {
            return false;
        }

        if let Ok(guard) = self.write_lock.try_lock() {
            drop(guard);
            return true;
        }

        false
    }

    fn collect_stats(&self) -> Result<VerFsStats> {
        self.chunk_meta_cache.run_pending_tasks();
        self.chunk_data_cache.run_pending_tasks();

        let (
            all_logical_size_bytes,
            live_logical_size_bytes,
            stored_unique_uncompressed_bytes,
            stored_unique_compressed_bytes,
            live_unique_uncompressed_bytes,
            live_unique_compressed_bytes,
            live_referenced_uncompressed_bytes,
            live_referenced_compressed_bytes,
        ) = self.meta.read_txn(|txn| {
            let mut all_logical_size_bytes = 0_u64;
            let inode_prefix = vec![KEY_PREFIX_INODE];
            let inode_end = prefix_end(&inode_prefix);
            for (_, value) in scan_range_pairs(txn, inode_prefix, inode_end)? {
                let inode: InodeRecord = decode_rkyv(&value)?;
                if inode.kind == INODE_KIND_FILE {
                    all_logical_size_bytes = all_logical_size_bytes.saturating_add(inode.size);
                }
            }

            let mut live_logical_size_bytes = 0_u64;
            let mut live_seen_inodes = HashSet::<u64>::new();
            let mut live_dir_stack = vec![ROOT_INODE];
            let mut live_extent_refcounts = HashMap::<[u8; 16], u64>::new();
            live_seen_inodes.insert(ROOT_INODE);

            while let Some(dir_ino) = live_dir_stack.pop() {
                let prefix = dirent_prefix(dir_ino);
                let end = prefix_end(&prefix);
                for (key, value) in scan_range_pairs(txn, prefix, end)? {
                    let Some(name) = decode_dirent_name(&key) else {
                        continue;
                    };
                    if dir_ino == ROOT_INODE && name == SNAPSHOTS_DIR_NAME.as_bytes() {
                        continue;
                    }

                    let dirent: DirentRecord = decode_rkyv(&value)?;
                    if !live_seen_inodes.insert(dirent.ino) {
                        continue;
                    }
                    let Some(child_raw) = txn.get(inode_key(dirent.ino))? else {
                        continue;
                    };
                    let child: InodeRecord = decode_rkyv(&child_raw)?;

                    match child.kind {
                        INODE_KIND_FILE => {
                            live_logical_size_bytes =
                                live_logical_size_bytes.saturating_add(child.size);
                            let ext_prefix = extent_prefix(child.ino);
                            let ext_end = prefix_end(&ext_prefix);
                            for (_, ext_value) in scan_range_pairs(txn, ext_prefix, ext_end)? {
                                let extent: ExtentRecord = decode_rkyv(&ext_value)?;
                                let counter =
                                    live_extent_refcounts.entry(extent.chunk_hash).or_insert(0);
                                *counter = counter.saturating_add(1);
                            }
                        }
                        INODE_KIND_DIR => {
                            live_dir_stack.push(child.ino);
                        }
                        _ => {}
                    }
                }
            }

            let mut stored_unique_uncompressed_bytes = 0_u64;
            let mut stored_unique_compressed_bytes = 0_u64;
            let mut live_unique_uncompressed_bytes = 0_u64;
            let mut live_unique_compressed_bytes = 0_u64;
            let mut live_referenced_uncompressed_bytes = 0_u64;
            let mut live_referenced_compressed_bytes = 0_u64;

            let chunk_prefix = vec![KEY_PREFIX_CHUNK];
            let chunk_end = prefix_end(&chunk_prefix);
            for (key, value) in scan_range_pairs(txn, chunk_prefix, chunk_end)? {
                if key.len() != 17 {
                    continue;
                }
                let mut hash = [0_u8; 16];
                hash.copy_from_slice(&key[1..17]);
                let chunk: ChunkRecord = decode_rkyv(&value)?;

                let chunk_uncompressed = chunk.uncompressed_len as u64;
                let chunk_compressed = chunk.compressed_len as u64;
                stored_unique_uncompressed_bytes =
                    stored_unique_uncompressed_bytes.saturating_add(chunk_uncompressed);
                stored_unique_compressed_bytes =
                    stored_unique_compressed_bytes.saturating_add(chunk_compressed);

                if let Some(reference_count) = live_extent_refcounts.get(&hash) {
                    live_unique_uncompressed_bytes =
                        live_unique_uncompressed_bytes.saturating_add(chunk_uncompressed);
                    live_unique_compressed_bytes =
                        live_unique_compressed_bytes.saturating_add(chunk_compressed);
                    live_referenced_uncompressed_bytes = live_referenced_uncompressed_bytes
                        .saturating_add(chunk_uncompressed.saturating_mul(*reference_count));
                    live_referenced_compressed_bytes = live_referenced_compressed_bytes
                        .saturating_add(chunk_compressed.saturating_mul(*reference_count));
                }
            }

            Ok((
                all_logical_size_bytes,
                live_logical_size_bytes,
                stored_unique_uncompressed_bytes,
                stored_unique_compressed_bytes,
                live_unique_uncompressed_bytes,
                live_unique_compressed_bytes,
                live_referenced_uncompressed_bytes,
                live_referenced_compressed_bytes,
            ))
        })?;

        let metadata_size_bytes = dir_size_recursive(&self.config.metadata_dir())?;
        let data_dir_size_bytes = dir_size_recursive(&self.config.data_dir)?;
        let snapshots_logical_size_bytes =
            all_logical_size_bytes.saturating_sub(live_logical_size_bytes);
        let live_dedup_savings_bytes =
            live_referenced_compressed_bytes.saturating_sub(live_unique_compressed_bytes);

        let cache_hits = self
            .chunk_meta_cache_hits
            .load(Ordering::Relaxed)
            .saturating_add(self.chunk_data_cache_hits.load(Ordering::Relaxed));
        let cache_requests = cache_hits
            .saturating_add(self.chunk_meta_cache_misses.load(Ordering::Relaxed))
            .saturating_add(self.chunk_data_cache_misses.load(Ordering::Relaxed));
        let cache_hit_rate = if cache_requests == 0 {
            0.0
        } else {
            cache_hits as f64 / cache_requests as f64
        };

        let metadata_cache_entries = self.chunk_meta_cache.entry_count();
        let chunk_cache_entries = self.chunk_data_cache.entry_count();
        let approx_cache_memory_bytes = metadata_cache_entries
            .saturating_mul(size_of::<([u8; 16], ChunkRecord)>() as u64)
            .saturating_add(
                chunk_cache_entries
                    .saturating_mul((size_of::<([u8; 16], Arc<Vec<u8>>)>() + BLOCK_SIZE) as u64),
            );

        let process_rss_bytes = read_process_rss_bytes().unwrap_or(0);
        let process_private_memory_bytes =
            read_process_private_memory_bytes().unwrap_or(process_rss_bytes);
        let read_bytes_total = self.read_bytes_total.load(Ordering::Relaxed);
        let write_bytes_total = self.write_bytes_total.load(Ordering::Relaxed);
        let elapsed_ms = now_millis().saturating_sub(self.stats_started_ms.load(Ordering::Relaxed));
        let uptime_secs = (elapsed_ms as f64 / 1000.0).max(0.001);
        let read_throughput_bps = read_bytes_total as f64 / uptime_secs;
        let write_throughput_bps = write_bytes_total as f64 / uptime_secs;

        Ok(VerFsStats {
            live_logical_size_bytes,
            snapshots_logical_size_bytes,
            all_logical_size_bytes,
            live_referenced_uncompressed_bytes,
            live_referenced_compressed_bytes,
            live_unique_uncompressed_bytes,
            live_unique_compressed_bytes,
            stored_unique_uncompressed_bytes,
            stored_unique_compressed_bytes,
            metadata_size_bytes,
            data_dir_size_bytes,
            live_dedup_savings_bytes,
            cache_hits,
            cache_requests,
            cache_hit_rate,
            process_private_memory_bytes,
            process_rss_bytes,
            read_bytes_total,
            write_bytes_total,
            uptime_secs,
            read_throughput_bps,
            write_throughput_bps,
            metadata_cache_entries,
            chunk_cache_entries,
            approx_cache_memory_bytes,
        })
    }

    async fn maybe_run_gc(&self) -> Result<()> {
        if !self.gc_idle_window_open() {
            return Ok(());
        }

        let _gc_guard = self.gc_lock.lock().await;
        if !self.gc_idle_window_open() {
            return Ok(());
        }
        self.run_gc_cycle().await
    }

    async fn run_gc_cycle(&self) -> Result<()> {
        let discard_path = self
            .config
            .packs_dir()
            .join(&self.config.gc_discard_filename);
        let current_discard_len = ensure_file(&discard_path)?;
        let current_checkpoint = self
            .meta
            .get_u64_sys("gc.discard_checkpoint")
            .unwrap_or(current_discard_len);
        if current_checkpoint == 0 {
            self.meta
                .write_txn(|txn| {
                    txn.set(
                        sys_key("gc.discard_checkpoint"),
                        current_discard_len.to_le_bytes().to_vec(),
                    )?;
                    Ok(())
                })
                .await?;
        }

        let phase = self.meta.get_u64_sys("gc.phase").unwrap_or(0);

        if phase == 0 {
            // == SCAN PHASE ==
            let (candidates, next_cursor) = self.collect_zero_ref_candidates()?;

            if !candidates.is_empty() {
                self.delete_zero_ref_candidates(&candidates).await?;
                for candidate in candidates.iter() {
                    self.chunk_meta_cache.invalidate(&candidate.hash);
                    self.chunk_data_cache.invalidate(&candidate.hash);
                }

                let epoch = self.next_gc_epoch().await?;
                let discard_records = candidates
                    .iter()
                    .map(|candidate| DiscardRecord {
                        epoch_id: epoch,
                        pack_id: candidate.pack_id,
                        chunk_hash128: candidate.hash,
                        block_size_bytes: candidate.block_size_bytes,
                    })
                    .collect::<Vec<_>>();
                let appended_checkpoint = append_records(&discard_path, &discard_records)?;
                self.meta
                    .write_txn(|txn| {
                        txn.set(
                            sys_key("gc.discard_checkpoint"),
                            appended_checkpoint.to_le_bytes().to_vec(),
                        )?;
                        Ok(())
                    })
                    .await?;
            }

            self.meta
                .write_txn(|txn| {
                    if let Some(cursor) = next_cursor {
                        txn.set(sys_key("gc.scan_cursor"), cursor)?;
                    } else {
                        txn.delete(sys_key("gc.scan_cursor"))?;
                        txn.set(sys_key("gc.phase"), 1_u64.to_le_bytes().to_vec())?;
                    }
                    Ok(())
                })
                .await?;

            return Ok(());
        }

        // == REWRITE PHASE ==
        let checkpoint = self
            .meta
            .get_u64_sys("gc.discard_checkpoint")
            .unwrap_or(current_checkpoint.max(current_discard_len));
        let parsed = read_records(&discard_path, checkpoint)?;
        if parsed.records.is_empty() {
            self.switch_to_scan_phase().await?;
            return Ok(());
        }

        let mut by_pack = HashMap::<u64, HashMap<[u8; 16], DiscardRecord>>::new();
        for record in parsed.records {
            by_pack
                .entry(record.pack_id)
                .or_default()
                .insert(record.chunk_hash128, record);
        }

        let active_pack_id = self.packs.active_pack_id();
        let mut rewritten_pack: Option<u64> = None;

        for (pack_id, dead_records) in by_pack.iter() {
            if *pack_id == active_pack_id {
                continue;
            }
            let reclaim_bytes = dead_records
                .values()
                .map(|record| record.block_size_bytes as u64)
                .sum::<u64>();

            let pack_size = self.packs.pack_size(*pack_id)?;
            if pack_size == 0 {
                continue;
            }
            let reclaim_percent = (reclaim_bytes as f64 / pack_size as f64) * 100.0;
            let meets_bytes = reclaim_bytes >= self.config.gc_pack_rewrite_min_reclaim_bytes;
            let meets_percent = reclaim_percent >= self.config.gc_pack_rewrite_min_reclaim_percent;
            if meets_bytes || meets_percent {
                let live_hashes = self.live_hashes_for_pack(*pack_id)?;
                if !self.gc_idle_window_open() {
                    return Ok(());
                }
                self.packs
                    .rewrite_pack_with_live_hashes(*pack_id, &live_hashes)?;
                rewritten_pack = Some(*pack_id);
                break;
            }
        }

        if let Some(rewritten_pack_id) = rewritten_pack {
            let current = read_records(&discard_path, checkpoint)?;
            let kept = current
                .records
                .into_iter()
                .filter(|record| record.pack_id != rewritten_pack_id)
                .collect::<Vec<_>>();
            let new_len = rewrite_records(&discard_path, &kept)?;
            self.meta
                .write_txn(|txn| {
                    txn.set(
                        sys_key("gc.discard_checkpoint"),
                        new_len.to_le_bytes().to_vec(),
                    )?;
                    Ok(())
                })
                .await?;
        } else {
            self.switch_to_scan_phase().await?;
        }

        Ok(())
    }

    async fn switch_to_scan_phase(&self) -> Result<()> {
        self.meta
            .write_txn(|txn| {
                txn.set(sys_key("gc.phase"), 0_u64.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await
    }

    fn collect_zero_ref_candidates(&self) -> Result<(Vec<ZeroRefCandidate>, Option<Vec<u8>>)> {
        self.meta.read_txn(|txn| {
            let mut out = Vec::new();
            let prefix = vec![crate::types::KEY_PREFIX_CHUNK];
            let end = prefix_end(&prefix);

            let mut start = prefix.clone();
            if let Ok(Some(cursor)) = txn.get(sys_key("gc.scan_cursor")) {
                if !cursor.is_empty() {
                    start = cursor;
                }
            }

            let mut next_cursor = None;
            let (pairs, has_more) = scan_range_pairs_limited(txn, start, end, 50_000)?;

            for (key, value) in pairs {
                if has_more {
                    let mut next = key.clone();
                    next.push(0);
                    next_cursor = Some(next);
                }

                if key.len() != 17 {
                    continue;
                }
                let mut hash = [0_u8; 16];
                hash.copy_from_slice(&key[1..17]);
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                if chunk.refcount == 0 {
                    out.push(ZeroRefCandidate {
                        hash,
                        pack_id: chunk.pack_id,
                        block_size_bytes: chunk.compressed_len,
                    });
                }
            }
            Ok((out, next_cursor))
        })
    }

    async fn delete_zero_ref_candidates(&self, candidates: &[ZeroRefCandidate]) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        let hashes = candidates
            .iter()
            .map(|entry| entry.hash)
            .collect::<Vec<_>>();
        self.meta
            .write_txn(|txn| {
                for hash in hashes.iter() {
                    let key = chunk_key(hash);
                    if let Some(raw) = txn.get(key.clone())? {
                        let chunk: ChunkRecord = decode_rkyv(&raw)?;
                        if chunk.refcount == 0 {
                            txn.delete(key)?;
                        }
                    }
                }
                Ok(())
            })
            .await
    }

    async fn next_gc_epoch(&self) -> Result<u64> {
        let current = self.meta.get_u64_sys("gc.epoch").unwrap_or(0);
        let next = current.saturating_add(1);
        self.meta
            .write_txn(|txn| {
                txn.set(sys_key("gc.epoch"), next.to_le_bytes().to_vec())?;
                Ok(())
            })
            .await?;
        Ok(next)
    }

    async fn persist_active_pack_id_if_needed(&self) -> Result<()> {
        let active_pack_id = self.packs.active_pack_id();
        let last_persisted = self.last_persisted_active_pack_id.load(Ordering::Relaxed);
        if active_pack_id == last_persisted {
            return Ok(());
        }
        self.meta
            .write_txn(|txn| {
                txn.set(
                    sys_key("active_pack_id"),
                    active_pack_id.to_le_bytes().to_vec(),
                )?;
                Ok(())
            })
            .await?;
        self.last_persisted_active_pack_id
            .store(active_pack_id, Ordering::Relaxed);
        Ok(())
    }

    fn live_hashes_for_pack(&self, pack_id: u64) -> Result<HashSet<[u8; 16]>> {
        self.meta.read_txn(|txn| {
            let mut out = HashSet::new();
            let prefix = vec![crate::types::KEY_PREFIX_CHUNK];
            let end = prefix_end(&prefix);
            for (key, value) in scan_range_pairs(txn, prefix, end)? {
                if key.len() != 17 {
                    continue;
                }
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                if chunk.pack_id != pack_id || chunk.refcount == 0 {
                    continue;
                }
                let mut hash = [0_u8; 16];
                hash.copy_from_slice(&key[1..17]);
                out.insert(hash);
            }
            Ok(out)
        })
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

    async fn interrupt(&self, _unique: u64) {}

    async fn lookup(
        &self,
        _uid: u32,
        _gid: u32,
        parent: INum,
        name: &str,
    ) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
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
        self.core
            .ensure_inode_vault_access(&parent_inode, "lookup")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .check_vault_parent_name_gate(parent, name)
            .map_err(map_anyhow_to_fuse)?;

        let ino = if name == "." {
            parent
        } else if name == ".." {
            parent_inode.parent
        } else {
            let Some(dirent) = self
                .core
                .lookup_dirent(parent, name)
                .map_err(map_anyhow_to_fuse)?
            else {
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
        self.core
            .ensure_inode_vault_access(&inode, "lookup")
            .map_err(map_anyhow_to_fuse)?;

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
            .ok_or_else(|| {
                AsyncFusexError::from(anyhow_errno(Errno::ENOENT, "getattr: inode not found"))
            })?;
        self.core
            .ensure_inode_vault_access(&inode, "getattr")
            .map_err(map_anyhow_to_fuse)?;

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
        let mut inode = if let Some(size) = param.size {
            self.core
                .truncate_file_locked(ino, size)
                .await
                .map_err(map_anyhow_to_fuse)?
        } else {
            self.core
                .load_inode_or_errno(ino, "setattr")
                .map_err(map_anyhow_to_fuse)?
        };
        self.core
            .ensure_inode_vault_access(&inode, "setattr")
            .map_err(map_anyhow_to_fuse)?;
        FsCore::ensure_inode_writable(&inode, "setattr").map_err(map_anyhow_to_fuse)?;

        if let Some(mode) = param.mode {
            inode.perm = (mode & 0o7777) as u16;
        }
        if let Some(uid) = param.u_id {
            inode.uid = uid;
        }
        if let Some(gid) = param.g_id {
            inode.gid = gid;
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
        self.core.mark_mutation();

        Ok((ATTR_TTL, self.core.file_attr_from_inode(&inode)))
    }

    async fn readlink(&self, ino: u64) -> AsyncFusexResult<Vec<u8>> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "readlink")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "readlink")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind != INODE_KIND_SYMLINK {
            return build_error_result_from_errno(
                Errno::EINVAL,
                "readlink target is not a symlink".to_owned(),
            );
        }

        self.core
            .meta
            .read_txn(|txn| {
                let Some(target) = txn.get(symlink_target_key(ino))? else {
                    return Err(anyhow_errno(
                        Errno::EIO,
                        format!("symlink inode {} has no target payload", ino),
                    ));
                };
                Ok(target)
            })
            .map_err(map_anyhow_to_fuse)
    }

    async fn mknod(&self, param: CreateParam) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        let parent_inode = self
            .core
            .load_inode_or_errno(param.parent, "mknod")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&parent_inode, "mknod")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .check_vault_parent_name_gate(param.parent, &param.name)
            .map_err(map_anyhow_to_fuse)?;
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
        self.core.mark_mutation();

        let inode = created.ok_or_else(|| {
            AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing created inode"))
        })?;
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
        ))
    }

    async fn mkdir(&self, param: CreateParam) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        let parent_inode = self
            .core
            .load_inode_or_errno(param.parent, "mkdir")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&parent_inode, "mkdir")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .check_vault_parent_name_gate(param.parent, &param.name)
            .map_err(map_anyhow_to_fuse)?;
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
        self.core.mark_mutation();

        let inode = created.ok_or_else(|| {
            AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing created directory"))
        })?;
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
        ))
    }

    async fn unlink(&self, _uid: u32, _gid: u32, parent: INum, name: &str) -> AsyncFusexResult<()> {
        self.core
            .check_vault_parent_name_gate(parent, name)
            .map_err(map_anyhow_to_fuse)?;
        let parent_inode = self
            .core
            .load_inode_or_errno(parent, "unlink")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&parent_inode, "unlink")
            .map_err(map_anyhow_to_fuse)?;
        let _guard = self.core.write_lock.lock().await;

        self.core
            .meta
            .write_txn(|txn| {
                let _parent_inode =
                    FsCore::ensure_parent_dir_writable_in_txn(txn, parent, "unlink")?;
                let dirent = FsCore::ensure_dirent_target(txn, parent, name, "unlink")?;
                let Some(inode_raw) = txn.get(inode_key(dirent.ino))? else {
                    return Err(anyhow_errno(Errno::ENOENT, "unlink target inode not found"));
                };
                let mut inode: InodeRecord = decode_rkyv(&inode_raw)?;
                FsCore::ensure_inode_writable(&inode, "unlink")?;

                if inode.kind == INODE_KIND_DIR {
                    return Err(anyhow_errno(Errno::EISDIR, "cannot unlink directory"));
                }

                txn.delete(dirent_key(parent, name.as_bytes()))?;
                let now = SystemTime::now();
                let (sec, nsec) = system_time_to_parts(now);
                inode.ctime_sec = sec;
                inode.ctime_nsec = nsec;
                let _removed_inode = FsCore::remove_name_from_inode_in_txn(txn, &mut inode)?;
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;
        self.core.mark_mutation();
        Ok(())
    }

    async fn rmdir(
        &self,
        _uid: u32,
        _gid: u32,
        parent: INum,
        dir_name: &str,
    ) -> AsyncFusexResult<Option<INum>> {
        self.core
            .check_vault_parent_name_gate(parent, dir_name)
            .map_err(map_anyhow_to_fuse)?;
        let parent_inode = self
            .core
            .load_inode_or_errno(parent, "rmdir")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&parent_inode, "rmdir")
            .map_err(map_anyhow_to_fuse)?;
        let _guard = self.core.write_lock.lock().await;

        self.core
            .meta
            .write_txn(|txn| {
                let _parent_inode =
                    FsCore::ensure_parent_dir_writable_in_txn(txn, parent, "rmdir")?;
                let dirent = FsCore::ensure_dirent_target(txn, parent, dir_name, "rmdir")?;
                let Some(raw) = txn.get(inode_key(dirent.ino))? else {
                    return Err(anyhow_errno(Errno::ENOENT, "rmdir target inode not found"));
                };
                let target_inode: InodeRecord = decode_rkyv(&raw)?;
                FsCore::ensure_inode_writable(&target_inode, "rmdir")?;
                if target_inode.kind != INODE_KIND_DIR {
                    return Err(anyhow_errno(
                        Errno::ENOTDIR,
                        "rmdir target is not a directory",
                    ));
                }

                if !FsCore::is_dir_empty(txn, dirent.ino)? {
                    return Err(anyhow_errno(Errno::ENOTEMPTY, "directory not empty"));
                }

                txn.delete(dirent_key(parent, dir_name.as_bytes()))?;
                FsCore::remove_inode_payload_in_txn(txn, &target_inode)?;
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
        self.core.mark_mutation();

        Ok(None)
    }

    async fn symlink(
        &self,
        uid: u32,
        gid: u32,
        parent: INum,
        name: &str,
        target_path: &Path,
    ) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        let parent_inode = self
            .core
            .load_inode_or_errno(parent, "symlink")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&parent_inode, "symlink")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .check_vault_parent_name_gate(parent, name)
            .map_err(map_anyhow_to_fuse)?;
        let target_bytes = target_path.as_os_str().as_encoded_bytes().to_vec();

        let mut created: Option<InodeRecord> = None;
        self.core
            .meta
            .write_txn(|txn| {
                let mut inode = FsCore::create_inode_in_txn(
                    txn,
                    parent,
                    name,
                    INODE_KIND_SYMLINK,
                    0o777,
                    uid,
                    gid,
                )?;
                inode.size = target_bytes.len() as u64;
                txn.set(inode_key(inode.ino), encode_rkyv(&inode)?)?;
                txn.set(symlink_target_key(inode.ino), target_bytes.clone())?;
                created = Some(inode);
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;
        self.core.mark_mutation();

        let inode = created.ok_or_else(|| {
            AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing symlink inode"))
        })?;
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
        ))
    }

    async fn rename(&self, _uid: u32, _gid: u32, param: RenameParam) -> AsyncFusexResult<()> {
        if (param.old_parent == ROOT_INODE
            && param.old_name == VAULT_DIR_NAME
            && !(param.new_parent == ROOT_INODE && param.new_name == VAULT_DIR_NAME))
            || (param.new_parent == ROOT_INODE
                && param.new_name == VAULT_DIR_NAME
                && !(param.old_parent == ROOT_INODE && param.old_name == VAULT_DIR_NAME))
        {
            return build_error_result_from_errno(
                Errno::EPERM,
                "top-level .vault cannot be renamed".to_owned(),
            );
        }
        let old_parent_inode = self
            .core
            .load_inode_or_errno(param.old_parent, "rename")
            .map_err(map_anyhow_to_fuse)?;
        let new_parent_inode = self
            .core
            .load_inode_or_errno(param.new_parent, "rename")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&old_parent_inode, "rename")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&new_parent_inode, "rename")
            .map_err(map_anyhow_to_fuse)?;
        if FsCore::inode_is_vault(&old_parent_inode) != FsCore::inode_is_vault(&new_parent_inode) {
            return build_error_result_from_errno(
                Errno::EXDEV,
                "cannot rename across vault boundary".to_owned(),
            );
        }
        let _guard = self.core.write_lock.lock().await;

        self.core
            .meta
            .write_txn(|txn| {
                if param.old_name == "."
                    || param.old_name == ".."
                    || param.new_name == "."
                    || param.new_name == ".."
                {
                    return Err(anyhow_errno(
                        Errno::EINVAL,
                        "rename does not allow dot entries",
                    ));
                }
                if param.old_parent == param.new_parent && param.old_name == param.new_name {
                    return Ok(());
                }
                let allowed_flags = RENAME_FLAG_NOREPLACE | RENAME_FLAG_EXCHANGE;
                if (param.flags & !allowed_flags) != 0 {
                    return Err(anyhow_errno(
                        Errno::EINVAL,
                        format!("unsupported rename flags {}", param.flags),
                    ));
                }

                let source = FsCore::ensure_dirent_target(
                    txn,
                    param.old_parent,
                    &param.old_name,
                    "rename source",
                )?;
                let _old_parent =
                    FsCore::ensure_parent_dir_writable_in_txn(txn, param.old_parent, "rename")?;
                let _new_parent =
                    FsCore::ensure_parent_dir_writable_in_txn(txn, param.new_parent, "rename")?;
                let Some(source_inode_raw) = txn.get(inode_key(source.ino))? else {
                    return Err(anyhow_errno(Errno::ENOENT, "rename source inode not found"));
                };
                let mut source_inode: InodeRecord = decode_rkyv(&source_inode_raw)?;
                FsCore::ensure_inode_writable(&source_inode, "rename")?;
                let source_is_dir = source_inode.kind == INODE_KIND_DIR;
                let target =
                    match txn.get(dirent_key(param.new_parent, param.new_name.as_bytes()))? {
                        Some(raw) => Some(decode_rkyv::<DirentRecord>(&raw)?),
                        None => None,
                    };

                let now = SystemTime::now();
                let (sec, nsec) = system_time_to_parts(now);
                source_inode.ctime_sec = sec;
                source_inode.ctime_nsec = nsec;

                if (param.flags & RENAME_FLAG_EXCHANGE) != 0 {
                    let Some(target) = target else {
                        return Err(anyhow_errno(
                            Errno::ENOENT,
                            "rename exchange target missing",
                        ));
                    };
                    if target.ino == source.ino {
                        return Ok(());
                    }

                    let Some(target_inode_raw) = txn.get(inode_key(target.ino))? else {
                        return Err(anyhow_errno(
                            Errno::ENOENT,
                            "rename exchange target inode missing",
                        ));
                    };
                    let mut target_inode: InodeRecord = decode_rkyv(&target_inode_raw)?;
                    FsCore::ensure_inode_writable(&target_inode, "rename")?;
                    target_inode.ctime_sec = sec;
                    target_inode.ctime_nsec = nsec;

                    txn.set(
                        dirent_key(param.old_parent, param.old_name.as_bytes()),
                        encode_rkyv(&DirentRecord {
                            ino: target.ino,
                            kind: target.kind,
                        })?,
                    )?;
                    txn.set(
                        dirent_key(param.new_parent, param.new_name.as_bytes()),
                        encode_rkyv(&DirentRecord {
                            ino: source.ino,
                            kind: source.kind,
                        })?,
                    )?;

                    if param.old_parent != param.new_parent {
                        if source_is_dir {
                            source_inode.parent = param.new_parent;
                        }
                        if target_inode.kind == INODE_KIND_DIR {
                            target_inode.parent = param.old_parent;
                        }
                    }

                    txn.set(inode_key(source.ino), encode_rkyv(&source_inode)?)?;
                    txn.set(inode_key(target.ino), encode_rkyv(&target_inode)?)?;
                    FsCore::touch_parent_inode_in_txn(txn, param.old_parent, 0, sec, nsec)?;
                    if param.old_parent != param.new_parent {
                        FsCore::touch_parent_inode_in_txn(txn, param.new_parent, 0, sec, nsec)?;
                    }
                    return Ok(());
                }

                if let Some(target) = target {
                    if (param.flags & RENAME_FLAG_NOREPLACE) != 0 {
                        return Err(anyhow_errno(Errno::EEXIST, "rename target already exists"));
                    }
                    if target.ino == source.ino {
                        return Ok(());
                    }

                    let Some(target_inode_raw) = txn.get(inode_key(target.ino))? else {
                        return Err(anyhow_errno(Errno::ENOENT, "rename target inode not found"));
                    };
                    let mut target_inode: InodeRecord = decode_rkyv(&target_inode_raw)?;
                    FsCore::ensure_inode_writable(&target_inode, "rename")?;
                    let target_is_dir = target_inode.kind == INODE_KIND_DIR;

                    if source_is_dir && !target_is_dir {
                        return Err(anyhow_errno(
                            Errno::ENOTDIR,
                            "cannot overwrite non-directory with directory",
                        ));
                    }
                    if !source_is_dir && target_is_dir {
                        return Err(anyhow_errno(
                            Errno::EISDIR,
                            "cannot overwrite directory with non-directory",
                        ));
                    }
                    if target_is_dir && !FsCore::is_dir_empty(txn, target_inode.ino)? {
                        return Err(anyhow_errno(
                            Errno::ENOTEMPTY,
                            "rename target directory is not empty",
                        ));
                    }

                    txn.delete(dirent_key(param.new_parent, param.new_name.as_bytes()))?;
                    target_inode.ctime_sec = sec;
                    target_inode.ctime_nsec = nsec;
                    let _removed_target =
                        FsCore::remove_name_from_inode_in_txn(txn, &mut target_inode)?;

                    if target_is_dir {
                        FsCore::touch_parent_inode_in_txn(txn, param.new_parent, -1, sec, nsec)?;
                    }
                } else if (param.flags & RENAME_FLAG_NOREPLACE) != 0 {
                    // no-op; destination does not exist and NOREPLACE allows that
                }

                txn.delete(dirent_key(param.old_parent, param.old_name.as_bytes()))?;
                txn.set(
                    dirent_key(param.new_parent, param.new_name.as_bytes()),
                    encode_rkyv(&DirentRecord {
                        ino: source.ino,
                        kind: source.kind,
                    })?,
                )?;

                if param.old_parent != param.new_parent && source_is_dir {
                    source_inode.parent = param.new_parent;
                }

                txn.set(inode_key(source.ino), encode_rkyv(&source_inode)?)?;
                if source_is_dir && param.old_parent != param.new_parent {
                    FsCore::touch_parent_inode_in_txn(txn, param.old_parent, -1, sec, nsec)?;
                    FsCore::touch_parent_inode_in_txn(txn, param.new_parent, 1, sec, nsec)?;
                } else {
                    FsCore::touch_parent_inode_in_txn(txn, param.old_parent, 0, sec, nsec)?;
                    if param.old_parent != param.new_parent {
                        FsCore::touch_parent_inode_in_txn(txn, param.new_parent, 0, sec, nsec)?;
                    }
                }

                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;
        self.core.mark_mutation();
        Ok(())
    }

    async fn open(&self, _uid: u32, _gid: u32, ino: u64, flags: u32) -> AsyncFusexResult<u64> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "open")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "open")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind == INODE_KIND_DIR {
            return build_error_result_from_errno(
                Errno::EISDIR,
                "open called on directory".to_owned(),
            );
        }

        let oflags = OFlag::from_bits_truncate(flags as i32);
        if oflags.contains(OFlag::O_TRUNC) {
            let _guard = self.core.write_lock.lock().await;
            let _truncated = self
                .core
                .truncate_file_locked(ino, 0)
                .await
                .map_err(map_anyhow_to_fuse)?;
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
        self.core.mark_activity();
        let inode = self
            .core
            .load_inode_or_errno(ino, "read")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "read")
            .map_err(map_anyhow_to_fuse)?;

        if inode.kind != INODE_KIND_FILE {
            return build_error_result_from_errno(
                Errno::EISDIR,
                "read called on non-file inode".to_owned(),
            );
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
                        let chunk: ChunkRecord = decode_rkyv(&raw)?;
                        chunks.insert(hash, chunk);
                    }
                }

                Ok((extents, chunks))
            })
            .map_err(map_anyhow_to_fuse)?;

        for (hash, chunk) in chunks.iter() {
            self.core.chunk_meta_cache.insert(*hash, chunk.clone());
        }

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
                let mut payload =
                    if let Some(cached) = self.core.chunk_data_cache.get(&extent.chunk_hash) {
                        self.core
                            .chunk_data_cache_hits
                            .fetch_add(1, Ordering::Relaxed);
                        cached.as_ref().clone()
                    } else {
                        self.core
                            .chunk_data_cache_misses
                            .fetch_add(1, Ordering::Relaxed);
                        let bytes = if (chunk.flags & CHUNK_FLAG_ENCRYPTED) != 0 {
                            if !FsCore::inode_is_vault(&inode) {
                                return build_error_result_from_errno(
                                    Errno::EIO,
                                    "encrypted chunk mapped outside vault inode".to_owned(),
                                );
                            }
                            let encrypted = self
                                .core
                                .packs
                                .read_chunk_payload(
                                    chunk.pack_id,
                                    extent.chunk_hash,
                                    chunk.codec,
                                    chunk.uncompressed_len,
                                    chunk.compressed_len,
                                )
                                .map_err(map_anyhow_to_fuse)?;
                            let folder_key =
                                self.core.current_vault_key().map_err(map_anyhow_to_fuse)?;
                            let compressed =
                                decrypt_chunk_payload(&folder_key, &chunk.nonce, &encrypted)
                                    .map_err(map_anyhow_to_fuse)?;
                            crate::data::compress::decompress_chunk(
                                chunk.codec,
                                &compressed,
                                chunk.uncompressed_len,
                            )
                            .map_err(map_anyhow_to_fuse)?
                        } else {
                            self.core
                                .packs
                                .read_chunk(
                                    chunk.pack_id,
                                    extent.chunk_hash,
                                    chunk.codec,
                                    chunk.uncompressed_len,
                                    chunk.compressed_len,
                                )
                                .map_err(map_anyhow_to_fuse)?
                        };
                        self.core
                            .chunk_data_cache
                            .insert(extent.chunk_hash, Arc::new(bytes.clone()));
                        bytes
                    };
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
        self.core
            .read_bytes_total
            .fetch_add(read_len as u64, Ordering::Relaxed);
        Ok(read_len)
    }

    async fn write(&self, ino: u64, offset: i64, data: &[u8], flags: u32) -> AsyncFusexResult<()> {
        self.core.mark_activity();
        let inode = self
            .core
            .load_inode_or_errno(ino, "write")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "write")
            .map_err(map_anyhow_to_fuse)?;
        if offset < 0 {
            return build_error_result_from_errno(
                Errno::EINVAL,
                "negative write offset".to_owned(),
            );
        }

        let write_bytes = data.len().max(1);

        let op = WriteOp {
            ino,
            offset: offset as u64,
            data: data.to_vec(),
        };

        if (flags & FUSE_WRITE_CACHE) != 0 {
            self.batcher
                .enqueue(op, write_bytes)
                .await
                .map_err(map_anyhow_to_fuse)
        } else {
            self.batcher
                .enqueue_and_wait(op, write_bytes)
                .await
                .map_err(map_anyhow_to_fuse)
        }
    }

    async fn flush(&self, _ino: u64, _lock_owner: u64) -> AsyncFusexResult<()> {
        self.core.mark_activity();
        if let Ok(inode) = self.core.load_inode_or_errno(_ino, "flush") {
            self.core
                .ensure_inode_vault_access(&inode, "flush")
                .map_err(map_anyhow_to_fuse)?;
        }
        self.batcher.drain().await.map_err(map_anyhow_to_fuse)
    }

    async fn release(
        &self,
        _ino: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> AsyncFusexResult<()> {
        self.core.mark_activity();
        if let Ok(inode) = self.core.load_inode_or_errno(_ino, "release") {
            self.core
                .ensure_inode_vault_access(&inode, "release")
                .map_err(map_anyhow_to_fuse)?;
        }
        self.batcher.drain().await.map_err(map_anyhow_to_fuse)
    }

    async fn fsync(&self, _ino: u64, datasync: bool) -> AsyncFusexResult<()> {
        self.core.mark_activity();
        let inode = self
            .core
            .load_inode_or_errno(_ino, "fsync")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "fsync")
            .map_err(map_anyhow_to_fuse)?;
        self.batcher.drain().await.map_err(map_anyhow_to_fuse)?;
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
        self.core
            .ensure_inode_vault_access(&inode, "opendir")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind != INODE_KIND_DIR {
            return build_error_result_from_errno(
                Errno::ENOTDIR,
                "opendir called on non-directory".to_owned(),
            );
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
        self.core
            .ensure_inode_vault_access(&inode, "readdir")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind != INODE_KIND_DIR {
            return build_error_result_from_errno(
                Errno::ENOTDIR,
                "readdir called on non-directory".to_owned(),
            );
        }

        let mut entries = Vec::new();
        entries.push(DirEntry::new(ino, ".".to_owned(), FileType::Dir));
        entries.push(DirEntry::new(inode.parent, "..".to_owned(), FileType::Dir));

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
                    if self.core.vault_locked() && ino == ROOT_INODE && name == VAULT_DIR_NAME {
                        continue;
                    }
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
        self.core.mark_activity();
        let inode = self
            .core
            .load_inode_or_errno(_ino, "fsyncdir")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "fsyncdir")
            .map_err(map_anyhow_to_fuse)?;
        self.batcher.drain().await.map_err(map_anyhow_to_fuse)?;
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
        let parent_inode = self
            .core
            .load_inode_or_errno(parent, "create")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&parent_inode, "create")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .check_vault_parent_name_gate(parent, name)
            .map_err(map_anyhow_to_fuse)?;
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
        self.core.mark_mutation();

        let inode = created.ok_or_else(|| {
            AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing created file inode"))
        })?;
        let fh = self.core.new_handle();
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
            fh,
            0,
        ))
    }

    async fn link(
        &self,
        ino: u64,
        newparent: u64,
        newname: &str,
    ) -> AsyncFusexResult<(Duration, FileAttr, u64)> {
        self.core
            .check_vault_parent_name_gate(newparent, newname)
            .map_err(map_anyhow_to_fuse)?;
        let target_inode = self
            .core
            .load_inode_or_errno(ino, "link")
            .map_err(map_anyhow_to_fuse)?;
        let parent_inode = self
            .core
            .load_inode_or_errno(newparent, "link")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&target_inode, "link")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&parent_inode, "link")
            .map_err(map_anyhow_to_fuse)?;
        if FsCore::inode_is_vault(&target_inode) != FsCore::inode_is_vault(&parent_inode) {
            return build_error_result_from_errno(
                Errno::EXDEV,
                "cannot hard-link across vault boundary".to_owned(),
            );
        }
        let _guard = self.core.write_lock.lock().await;
        let mut linked: Option<InodeRecord> = None;

        self.core
            .meta
            .write_txn(|txn| {
                let Some(target_raw) = txn.get(inode_key(ino))? else {
                    return Err(anyhow_errno(
                        Errno::ENOENT,
                        format!("link source inode {} not found", ino),
                    ));
                };
                let mut target_inode: InodeRecord = decode_rkyv(&target_raw)?;
                FsCore::ensure_inode_writable(&target_inode, "link")?;
                if target_inode.kind == INODE_KIND_DIR {
                    return Err(anyhow_errno(
                        Errno::EPERM,
                        "hard links to directories are forbidden",
                    ));
                }

                let Some(parent_raw) = txn.get(inode_key(newparent))? else {
                    return Err(anyhow_errno(
                        Errno::ENOENT,
                        format!("link parent inode {} not found", newparent),
                    ));
                };
                let parent_inode: InodeRecord = decode_rkyv(&parent_raw)?;
                if parent_inode.kind != INODE_KIND_DIR {
                    return Err(anyhow_errno(
                        Errno::ENOTDIR,
                        "link parent is not a directory",
                    ));
                }
                FsCore::ensure_inode_writable(&parent_inode, "link")?;
                if txn
                    .get(dirent_key(newparent, newname.as_bytes()))?
                    .is_some()
                {
                    return Err(anyhow_errno(
                        Errno::EEXIST,
                        "link destination already exists",
                    ));
                }

                txn.set(
                    dirent_key(newparent, newname.as_bytes()),
                    encode_rkyv(&DirentRecord {
                        ino,
                        kind: target_inode.kind,
                    })?,
                )?;

                target_inode.nlink = target_inode.nlink.saturating_add(1);
                let now = SystemTime::now();
                let (sec, nsec) = system_time_to_parts(now);
                target_inode.ctime_sec = sec;
                target_inode.ctime_nsec = nsec;
                txn.set(inode_key(ino), encode_rkyv(&target_inode)?)?;
                linked = Some(target_inode);
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;
        self.core.mark_mutation();

        let inode = linked.ok_or_else(|| {
            AsyncFusexError::from(anyhow_errno(Errno::EIO, "missing linked inode"))
        })?;
        Ok((
            ATTR_TTL,
            self.core.file_attr_from_inode(&inode),
            inode.generation,
        ))
    }

    async fn setxattr(
        &self,
        ino: u64,
        name: &str,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> AsyncFusexResult<()> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "setxattr")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "setxattr")
            .map_err(map_anyhow_to_fuse)?;
        if position != 0 {
            return build_error_result_from_errno(
                Errno::EINVAL,
                "xattr position must be zero".to_owned(),
            );
        }
        FsCore::xattr_name_nonempty(name).map_err(map_anyhow_to_fuse)?;

        let create = (flags & libc::XATTR_CREATE as u32) != 0;
        let replace = (flags & libc::XATTR_REPLACE as u32) != 0;
        let known = libc::XATTR_CREATE as u32 | libc::XATTR_REPLACE as u32;
        if (flags & !known) != 0 {
            return build_error_result_from_errno(Errno::EINVAL, "unknown xattr flags".to_owned());
        }
        if create && replace {
            return build_error_result_from_errno(Errno::EINVAL, "invalid xattr flags".to_owned());
        }

        let _guard = self.core.write_lock.lock().await;
        self.core
            .meta
            .write_txn(|txn| {
                let Some(inode_raw) = txn.get(inode_key(ino))? else {
                    return Err(anyhow_errno(
                        Errno::ENOENT,
                        format!("setxattr inode {} not found", ino),
                    ));
                };
                let mut inode: InodeRecord = decode_rkyv(&inode_raw)?;
                FsCore::ensure_inode_writable(&inode, "setxattr")?;
                let key = xattr_key(ino, name.as_bytes());
                let exists = txn.get(key.clone())?.is_some();
                if create && exists {
                    return Err(anyhow_errno(
                        Errno::EEXIST,
                        format!("xattr {} already exists", name),
                    ));
                }
                if replace && !exists {
                    return Err(anyhow_errno(
                        Errno::ENODATA,
                        format!("xattr {} not found", name),
                    ));
                }
                txn.set(key, value.to_vec())?;

                let now = SystemTime::now();
                let (sec, nsec) = system_time_to_parts(now);
                inode.ctime_sec = sec;
                inode.ctime_nsec = nsec;
                txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;
        self.core.mark_mutation();
        Ok(())
    }

    async fn getxattr(&self, ino: u64, name: &str, size: u32) -> AsyncFusexResult<Vec<u8>> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "getxattr")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "getxattr")
            .map_err(map_anyhow_to_fuse)?;
        FsCore::xattr_name_nonempty(name).map_err(map_anyhow_to_fuse)?;
        let value = self
            .core
            .meta
            .read_txn(|txn| {
                if txn.get(inode_key(ino))?.is_none() {
                    return Err(anyhow_errno(
                        Errno::ENOENT,
                        format!("getxattr inode {} not found", ino),
                    ));
                }
                let Some(value) = txn.get(xattr_key(ino, name.as_bytes()))? else {
                    return Err(anyhow_errno(
                        Errno::ENODATA,
                        format!("xattr {} not found", name),
                    ));
                };
                Ok(value)
            })
            .map_err(map_anyhow_to_fuse)?;

        if size != 0 && value.len() > size as usize {
            return build_error_result_from_errno(
                Errno::ERANGE,
                "xattr buffer too small".to_owned(),
            );
        }
        Ok(value)
    }

    async fn listxattr(&self, ino: u64, size: u32) -> AsyncFusexResult<Vec<u8>> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "listxattr")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "listxattr")
            .map_err(map_anyhow_to_fuse)?;
        let mut buffer = self
            .core
            .meta
            .read_txn(|txn| {
                if txn.get(inode_key(ino))?.is_none() {
                    return Err(anyhow_errno(
                        Errno::ENOENT,
                        format!("listxattr inode {} not found", ino),
                    ));
                }

                let prefix = xattr_prefix(ino);
                let end = prefix_end(&prefix);
                let mut names = Vec::<Vec<u8>>::new();
                for (key, _) in scan_range_pairs(txn, prefix, end)? {
                    let Some(name) = decode_xattr_name(&key) else {
                        continue;
                    };
                    names.push(name.to_vec());
                }
                names.sort_unstable();
                names.dedup();
                let mut out = Vec::new();
                for name in names {
                    out.extend_from_slice(&name);
                    out.push(0);
                }
                Ok(out)
            })
            .map_err(map_anyhow_to_fuse)?;

        if size != 0 && buffer.len() > size as usize {
            return build_error_result_from_errno(
                Errno::ERANGE,
                "xattr list buffer too small".to_owned(),
            );
        }
        if size == 0 {
            return Ok(buffer);
        }
        buffer.shrink_to_fit();
        Ok(buffer)
    }

    async fn removexattr(&self, ino: u64, name: &str) -> AsyncFusexResult<()> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "removexattr")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "removexattr")
            .map_err(map_anyhow_to_fuse)?;
        FsCore::xattr_name_nonempty(name).map_err(map_anyhow_to_fuse)?;
        let _guard = self.core.write_lock.lock().await;
        self.core
            .meta
            .write_txn(|txn| {
                let Some(inode_raw) = txn.get(inode_key(ino))? else {
                    return Err(anyhow_errno(
                        Errno::ENOENT,
                        format!("removexattr inode {} not found", ino),
                    ));
                };
                let mut inode: InodeRecord = decode_rkyv(&inode_raw)?;
                FsCore::ensure_inode_writable(&inode, "removexattr")?;
                let key = xattr_key(ino, name.as_bytes());
                if txn.get(key.clone())?.is_none() {
                    return Err(anyhow_errno(
                        Errno::ENODATA,
                        format!("xattr {} not found", name),
                    ));
                }
                txn.delete(key)?;
                let now = SystemTime::now();
                let (sec, nsec) = system_time_to_parts(now);
                inode.ctime_sec = sec;
                inode.ctime_nsec = nsec;
                txn.set(inode_key(ino), encode_rkyv(&inode)?)?;
                Ok(())
            })
            .await
            .map_err(map_anyhow_to_fuse)?;
        self.core.mark_mutation();
        Ok(())
    }

    async fn access(&self, uid: u32, gid: u32, ino: u64, mask: u32) -> AsyncFusexResult<()> {
        let inode = self
            .core
            .load_inode_or_errno(ino, "access")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "access")
            .map_err(map_anyhow_to_fuse)?;
        if mask == libc::F_OK as u32 {
            return Ok(());
        }
        FsCore::check_access_for_mask(&inode, uid, gid, mask).map_err(map_anyhow_to_fuse)
    }

    async fn getlk(
        &self,
        _uid: u32,
        _gid: u32,
        ino: u64,
        lk_param: FileLockParam,
    ) -> AsyncFusexResult<FileLockParam> {
        if !FsCore::lock_type_valid(lk_param.typ) {
            return build_error_result_from_errno(Errno::EINVAL, "invalid lock type".to_owned());
        }
        let inode = self
            .core
            .load_inode_or_errno(ino, "getlk")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "getlk")
            .map_err(map_anyhow_to_fuse)?;

        let request = FileLockState {
            lock_owner: lk_param.lock_owner,
            start: lk_param.start,
            end: lk_param.end,
            typ: lk_param.typ,
            pid: lk_param.pid,
        };

        let locks = self.core.file_locks.lock().await;
        if let Some(existing) = locks.get(&ino).and_then(|list| {
            list.iter()
                .find(|entry| FsCore::lock_conflict(&request, entry))
                .cloned()
        }) {
            return Ok(FileLockParam {
                fh: lk_param.fh,
                lock_owner: lk_param.lock_owner,
                start: existing.start,
                end: existing.end,
                typ: existing.typ,
                pid: existing.pid,
            });
        }

        Ok(FileLockParam {
            fh: lk_param.fh,
            lock_owner: lk_param.lock_owner,
            start: lk_param.start,
            end: lk_param.end,
            typ: libc::F_UNLCK as u32,
            pid: 0,
        })
    }

    async fn setlk(
        &self,
        _uid: u32,
        _gid: u32,
        ino: u64,
        lk_param: FileLockParam,
        sleep_wait: bool,
    ) -> AsyncFusexResult<()> {
        if !FsCore::lock_type_valid(lk_param.typ) {
            return build_error_result_from_errno(Errno::EINVAL, "invalid lock type".to_owned());
        }
        let inode = self
            .core
            .load_inode_or_errno(ino, "setlk")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "setlk")
            .map_err(map_anyhow_to_fuse)?;

        let request = FileLockState {
            lock_owner: lk_param.lock_owner,
            start: lk_param.start,
            end: lk_param.end,
            typ: lk_param.typ,
            pid: lk_param.pid,
        };

        loop {
            let mut locks = self.core.file_locks.lock().await;
            let entry = locks.entry(ino).or_default();
            let conflict = entry
                .iter()
                .any(|existing| FsCore::lock_conflict(&request, existing));
            if conflict {
                if !sleep_wait {
                    return build_error_result_from_errno(
                        Errno::EAGAIN,
                        "file lock conflict".to_owned(),
                    );
                }
            } else {
                entry.retain(|existing| {
                    !(existing.lock_owner == request.lock_owner
                        && FsCore::lock_ranges_overlap(existing, &request))
                });
                if request.typ != libc::F_UNLCK as u32 {
                    entry.push(request.clone());
                }
                if entry.is_empty() {
                    locks.remove(&ino);
                }
                return Ok(());
            }
            drop(locks);
            sleep(LOCK_RETRY_INTERVAL).await;
        }
    }

    async fn bmap(
        &self,
        _uid: u32,
        _gid: u32,
        ino: u64,
        blocksize: u32,
        _idx: u64,
    ) -> AsyncFusexResult<()> {
        if blocksize == 0 {
            return build_error_result_from_errno(
                Errno::EINVAL,
                "bmap blocksize must be > 0".to_owned(),
            );
        }
        let inode = self
            .core
            .load_inode_or_errno(ino, "bmap")
            .map_err(map_anyhow_to_fuse)?;
        self.core
            .ensure_inode_vault_access(&inode, "bmap")
            .map_err(map_anyhow_to_fuse)?;
        if inode.kind != INODE_KIND_FILE {
            return build_error_result_from_errno(
                Errno::EINVAL,
                "bmap requires regular file".to_owned(),
            );
        }
        Ok(())
    }
}

#[async_trait]
impl WriteApply for FsCore {
    async fn apply_batch(&self, ops: Vec<WriteOp>) -> Vec<Result<()>> {
        let original_len = ops.len();
        if original_len == 0 {
            return Vec::new();
        }

        let mut groups: Vec<(WriteOp, Vec<usize>)> = Vec::new();
        for (idx, op) in ops.into_iter().enumerate() {
            if let Some((group_op, group_indices)) = groups.last_mut() {
                if can_coalesce_write(group_op, &op) {
                    merge_write_op(group_op, op);
                    group_indices.push(idx);
                    continue;
                }
            }
            groups.push((op, vec![idx]));
        }

        let mut out: Vec<Option<Result<()>>> = (0..original_len).map(|_| None).collect();
        for (group_op, indices) in groups {
            let group_result = self.apply_single_write(group_op).await;
            for idx in indices {
                out[idx] = Some(match &group_result {
                    Ok(()) => Ok(()),
                    Err(err) => Err(anyhow!(err.to_string())),
                });
            }
        }

        debug!(
            dedup_hits = self.dedup_hits.load(Ordering::Relaxed),
            dedup_misses = self.dedup_misses.load(Ordering::Relaxed),
            "write batch applied"
        );
        out.into_iter()
            .map(|item| item.unwrap_or_else(|| Err(anyhow!("missing write result for queued op"))))
            .collect()
    }
}

#[async_trait]
impl SyncTarget for FsCore {
    async fn sync_cycle(&self, full: bool) -> Result<()> {
        self.persist_active_pack_id_if_needed().await?;
        if full {
            self.packs.sync(true)?;
            self.meta.flush_wal(true)?;
        } else {
            self.packs.sync(false)?;
            self.meta.flush_wal(false)?;
            self.maybe_run_gc().await?;
        }
        Ok(())
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
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
    } else if let Some(io_err) = err.root_cause().downcast_ref::<std::io::Error>() {
        match io_err.raw_os_error() {
            Some(raw_errno) => {
                AsyncFusexError::from(anyhow_errno(Errno::from_i32(raw_errno), err.to_string()))
            }
            None => AsyncFusexError::from(anyhow_errno(Errno::EIO, err.to_string())),
        }
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

fn scan_range_pairs_limited(
    txn: &surrealkv::Transaction,
    start: Vec<u8>,
    end: Vec<u8>,
    limit: usize,
) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)> {
    let mut out = Vec::new();
    let mut iter = txn.range(start, end)?;
    let mut valid = iter.seek_first()?;
    while valid {
        out.push((iter.key().user_key().to_vec(), iter.value()?));
        if out.len() >= limit {
            return Ok((out, true));
        }
        valid = iter.next()?;
    }
    Ok((out, false))
}

fn dir_size_recursive(path: &Path) -> Result<u64> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(anyhow::Error::new(err)),
    };

    if metadata.is_file() || metadata.file_type().is_symlink() {
        return Ok(metadata.len());
    }

    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        total = total.saturating_add(dir_size_recursive(&entry.path())?);
    }
    Ok(total)
}

fn read_process_rss_bytes() -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    for line in status.lines() {
        if let Some(bytes) = parse_proc_kib_line(line, "VmRSS:") {
            return Ok(bytes?);
        }
    }
    Ok(0)
}

fn read_process_private_memory_bytes() -> Result<u64> {
    let smaps_rollup = std::fs::read_to_string("/proc/self/smaps_rollup")?;
    let mut private_clean = 0_u64;
    let mut private_dirty = 0_u64;

    for line in smaps_rollup.lines() {
        if let Some(bytes) = parse_proc_kib_line(line, "Private_Clean:") {
            private_clean = bytes?;
            continue;
        }
        if let Some(bytes) = parse_proc_kib_line(line, "Private_Dirty:") {
            private_dirty = bytes?;
        }
    }

    Ok(private_clean.saturating_add(private_dirty))
}

fn parse_proc_kib_line(line: &str, key: &str) -> Option<Result<u64>> {
    if !line.starts_with(key) {
        return None;
    }
    Some(
        line.strip_prefix(key)
            .ok_or_else(|| anyhow!("malformed proc line for {key}"))
            .and_then(|rest| {
                let kb = rest
                    .split_whitespace()
                    .next()
                    .ok_or_else(|| anyhow!("missing value for {key}"))?
                    .parse::<u64>()?;
                Ok(kb.saturating_mul(1024))
            }),
    )
}

fn can_coalesce_write(existing: &WriteOp, incoming: &WriteOp) -> bool {
    if existing.ino != incoming.ino {
        return false;
    }
    let existing_end = existing.offset.saturating_add(existing.data.len() as u64);
    let incoming_end = incoming.offset.saturating_add(incoming.data.len() as u64);
    incoming.offset <= existing_end && incoming_end >= existing.offset
        || incoming.offset == existing_end
}

fn merge_write_op(existing: &mut WriteOp, incoming: WriteOp) {
    let existing_start = existing.offset;
    let existing_end = existing.offset.saturating_add(existing.data.len() as u64);
    let incoming_start = incoming.offset;
    let incoming_end = incoming.offset.saturating_add(incoming.data.len() as u64);

    let merged_start = existing_start.min(incoming_start);
    let merged_end = existing_end.max(incoming_end);
    let merged_len = merged_end.saturating_sub(merged_start) as usize;

    if merged_start == existing_start && merged_len == existing.data.len() {
        let dst_start = (incoming_start.saturating_sub(merged_start)) as usize;
        let dst_end = dst_start.saturating_add(incoming.data.len());
        existing.data[dst_start..dst_end].copy_from_slice(&incoming.data);
        return;
    }

    let mut merged = vec![0_u8; merged_len];
    let existing_dst_start = (existing_start.saturating_sub(merged_start)) as usize;
    let existing_dst_end = existing_dst_start.saturating_add(existing.data.len());
    merged[existing_dst_start..existing_dst_end].copy_from_slice(&existing.data);

    let incoming_dst_start = (incoming_start.saturating_sub(merged_start)) as usize;
    let incoming_dst_end = incoming_dst_start.saturating_add(incoming.data.len());
    merged[incoming_dst_start..incoming_dst_end].copy_from_slice(&incoming.data);

    existing.offset = merged_start;
    existing.data = merged;
}
