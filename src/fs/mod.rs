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
    KEY_PREFIX_INODE, PERM_SYMLINK_DEFAULT, PERM_VAULT_DIRECTORY, ROOT_INODE, SNAPSHOTS_DIR_NAME,
    VAULT_DIR_NAME,
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

pub mod chunk;
pub mod fuse;
pub mod gc;
pub mod inode;
pub mod utils;
pub mod vault;
pub mod write;

pub(crate) use utils::*;

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
            for pair in scan_range_pairs(txn, inode_prefix, inode_end)? {
                let (_, value) = pair?;
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
                for pair in scan_range_pairs(txn, prefix, end)? {
                    let (key, value) = pair?;
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
                            for pair in scan_range_pairs(txn, ext_prefix, ext_end)? {
                                let (_, ext_value) = pair?;
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
            for pair in scan_range_pairs(txn, chunk_prefix, chunk_end)? {
                let (key, value) = pair?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::types::ROOT_INODE;
    use anyhow::anyhow;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    fn temp_dir_manual() -> PathBuf {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();
        let mut path = std::env::temp_dir();
        path.push(format!("verfs_bench_{}_{}", pid, t));
        path
    }

    #[tokio::test]
    async fn bench_overwrite_performance() -> anyhow::Result<()> {
        let root = temp_dir_manual();
        let data_dir = root.join("data");
        let meta_dir = root.join("meta");
        let packs_dir = data_dir.join("packs");
        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(&meta_dir)?;
        fs::create_dir_all(&packs_dir)?;

        let config = Config {
            mount_point: root.join("mnt"),
            data_dir: data_dir.clone(),
            sync_interval_ms: 1000,
            batch_max_size_mb: 10,
            batch_flush_interval_ms: 100,
            metadata_cache_capacity_entries: 10000,
            chunk_cache_capacity_mb: 100,
            pack_index_cache_capacity_entries: 10000,
            pack_max_size_mb: 10,
            zstd_compression_level: 1,
            ultracdc_min_size_bytes: 2048,
            ultracdc_avg_size_bytes: 4096,
            ultracdc_max_size_bytes: 8192,
            fuse_max_write_bytes: 1024 * 1024,
            fuse_direct_io: false,
            fuse_fsname: "verfs_bench".to_string(),
            fuse_subtype: "verfs".to_string(),
            gc_idle_min_ms: 1000,
            gc_pack_rewrite_min_reclaim_bytes: 1024 * 1024,
            gc_pack_rewrite_min_reclaim_percent: 50.0,
            gc_discard_filename: "discard.log".to_string(),
            vault_enabled: false,
            vault_argon2_mem_kib: 16,
            vault_argon2_iters: 1,
            vault_argon2_parallelism: 1,
        };

        let fs = VerFs::new(config).await?;

        let (_ttl, attr, _gen, _fh, _) = fs
            .create(1000, 1000, ROOT_INODE, ROOT_INODE, "bench_file", 0o644, 0)
            .await
            .map_err(|e| anyhow!("create failed: {:?}", e))?;
        let ino = attr.ino;

        let size = 50 * 1024 * 1024;
        let data = vec![0xAA_u8; size];

        let chunk_size = 1024 * 1024;
        for offset in (0..size).step_by(chunk_size) {
            let len = std::cmp::min(chunk_size, size - offset);
            fs.write(ino, offset as i64, &data[offset..offset + len], 0)
                .await
                .map_err(|e| anyhow!("write failed: {:?}", e))?;
        }

        fs.batcher
            .drain()
            .await
            .map_err(|e| anyhow!("drain failed: {:?}", e))?;
        fs.core.sync_cycle(true).await?;

        fs.core.chunk_meta_cache.invalidate_all();
        fs.core.chunk_data_cache.invalidate_all();

        println!("Starting overwrite benchmark...");
        let start = Instant::now();

        for offset in (0..size).step_by(chunk_size) {
            let len = std::cmp::min(chunk_size, size - offset);
            fs.write(ino, offset as i64, &data[offset..offset + len], 0)
                .await
                .map_err(|e| anyhow!("overwrite failed: {:?}", e))?;
        }
        fs.batcher
            .drain()
            .await
            .map_err(|e| anyhow!("drain overwrite failed: {:?}", e))?;

        let duration = start.elapsed();
        println!("Overwrite took: {:?}", duration);

        fs.graceful_shutdown().await?;
        fs::remove_dir_all(&root)?;

        Ok(())
    }
}
