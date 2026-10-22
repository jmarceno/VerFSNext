use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::size_of;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use verfsnext_async_fusex::error::{AsyncFusexError, AsyncFusexResult};
use verfsnext_async_fusex::session::SessionNotifier;
use verfsnext_async_fusex::fs_util::{
    build_error_result_from_errno, CreateParam, FileAttr, FileLockParam, INum, RenameParam,
    SetAttrParam, StatFsParam,
};
use verfsnext_async_fusex::{DirEntry, FileType, VirtualFs};
use async_trait::async_trait;
use moka::sync::Cache;
use nix::errno::Errno;
use nix::fcntl::OFlag;
use nix::libc;
use nix::sys::stat::SFlag;
use nix::sys::statvfs;
use parking_lot::{Mutex as ParkingMutex, RwLock as ParkingRwLock};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use verfsnext_surrealkv::LSMIterator;
use tokio::sync::{Mutex, RwLock as AsyncRwLock};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use fill_cache::FillCache;
use crate::data::chunker::UltraCdc;
use crate::data::compress::{compress_payload, CODEC_RAW, CODEC_ZSTD};
use crate::data::hash::{hash128, hash128_with_domain};
use crate::data::pack::{ChunkAppend, PackStore};
use crate::gc::{append_records, ensure_file, read_records, rewrite_records, DiscardRecord};
use crate::meta::{is_retryable_commit_error, MetaStore, MAX_COMMIT_ATTEMPTS};
use crate::migration::pack_index_crc32::{
    ensure_pack_index_crc32_compat, SYS_PACK_CRC32_READ_ERRORS,
};
use crate::migration::pack_size::ensure_pack_size_metadata_compat;
use crate::migration::vault_chunk_keys::ensure_vault_chunk_keys_migrated;
use crate::permissions::normalize_data_tree;
use crate::snapshot::{records_in_txn, SnapshotManager, SnapshotUsage};
use crate::sync::{SyncService, SyncTarget};
use crate::types::{
    chunk_key, decode_chunk_key, decode_dirent_name, decode_inline_chunk_key, decode_rkyv,
    decode_xattr_name, dirent_key, dirent_prefix, encode_rkyv, extent_key, extent_prefix,
    inode_key, inline_chunk_key, parts_to_system_time, prefix_end, symlink_target_key, sys_key,
    system_time_to_parts, xattr_key, xattr_prefix, ChunkId, ChunkRecord, DirentRecord,
    ExtentRecord, InodeRecord, INODE_FLAG_READONLY, INODE_FLAG_VAULT, INODE_FLAG_VAULT_ROOT,
    INODE_KIND_DIR, INODE_KIND_FILE, INODE_KIND_SYMLINK, INLINE_PACK_ID, KEY_PREFIX_CHUNK,
    KEY_PREFIX_INLINE_CHUNK, MODE_PERM_MASK, PERM_SYMLINK_DEFAULT, PERM_VAULT_DIRECTORY,
    ROOT_INODE, SNAPSHOTS_DIR_NAME, SYS_MAX_EXTENT_LEN, VAULT_DIR_NAME, VAULT_DOMAIN_BYTE,
};
use crate::vault::{
    build_wrap_record, decrypt_chunk_payload, encrypt_chunk_payload, generate_folder_key,
    unwrap_folder_key, VaultArgon2Params, VaultRuntime, VaultWrapRecord, CHUNK_FLAG_CORRUPT, CHUNK_FLAG_ENCRYPTED,
    CHUNK_FLAG_INLINE,
    SYS_VAULT_STATE, SYS_VAULT_WRAP, VAULT_STATE_LOCKED, VAULT_STATE_UNLOCKED,
};
use crate::write::batcher::{WriteApply, WriteBatcher, WriteOp};

const LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(10);
const RENAME_FLAG_NOREPLACE: u32 = libc::RENAME_NOREPLACE;
const RENAME_FLAG_EXCHANGE: u32 = libc::RENAME_EXCHANGE;
/// A kept statistics scan answers display requests until it is this old; the
/// first request after that starts a background refresh.
const STATS_REFRESH_MIN_AGE_MS: u64 = 30_000;
/// File in the data directory that keeps the last completed scan across
/// daemon restarts.
const STATS_SNAPSHOT_FILE: &str = "stats-snapshot.json";

pub mod chunk;
mod fill_cache;
pub mod fuse;
pub mod gc;
mod health;
pub mod inode;
pub mod utils;
pub mod vault;
pub mod write;

pub use health::{AffectedFile, RecordsCheckReport};

pub(crate) use utils::*;

#[derive(Clone)]
struct FileLockState {
    lock_owner: u64,
    start: u64,
    end: u64,
    typ: u32,
    pid: u32,
}

#[derive(Clone)]
struct CachedDirEntry {
    ino: u64,
    name: String,
    file_type: FileType,
}

#[derive(Clone, Hash, Eq, PartialEq)]
struct DirentCacheKey {
    parent: u64,
    name: Vec<u8>,
}

struct DirHandleState {
    ino: u64,
    snapshot: Option<Vec<CachedDirEntry>>,
}

/// One extent of a file: `record.len` bytes of chunk `record.chunk_hash`
/// starting at byte `start`. A file's extents never overlap; bytes no extent
/// covers are holes and read as zeros.
#[derive(Debug, Clone, Copy)]
struct FileExtent {
    start: u64,
    record: ExtentRecord,
}

impl FileExtent {
    fn end(&self) -> u64 {
        self.start + u64::from(self.record.len)
    }

    fn same_mapping(&self, other: &FileExtent) -> bool {
        self.start == other.start
            && self.record.len == other.record.len
            && self.record.chunk_hash == other.record.chunk_hash
    }
}

/// Every extent of a small file, sorted by offset.
#[derive(Clone)]
struct SmallFileReadPlan {
    file_size: u64,
    extents: Arc<Vec<FileExtent>>,
}

#[derive(Clone)]
struct FileHandleState {
    ino: u64,
    inode_data_version: u64,
    /// Where the next read starts if the handle reads sequentially.
    next_read_offset: u64,
    /// End of the range readahead has been started for.
    readahead_end: u64,
}

/// How far ahead of a sequential reader its chunks are loaded into the chunk
/// cache. The kernel sends FUSE reads of at most 128 KiB with about one in
/// flight, so without readahead every request waits for its own pack reads.
const READAHEAD_WINDOW_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy)]
struct ZeroRefCandidate {
    id: ChunkId,
    pack_id: u64,
    block_size_bytes: u32,
}

/// Who references a chunk, as seen by the statistics scan.
#[derive(Debug, Clone, Copy, Default)]
struct ChunkHolders {
    /// Extents that reference it, from this same scan.
    extent_refs: u64,
    /// The live folder or the vault references it.
    current: bool,
    /// The only snapshot that references it, while no other snapshot does.
    snapshot: Option<usize>,
    /// More than one snapshot references it.
    shared_by_snapshots: bool,
    /// An inode outside the live tree and the snapshot trees references it.
    referenced_elsewhere: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerFsStats {
    pub live_logical_size_bytes: u64,
    pub snapshots_logical_size_bytes: u64,
    pub hidden_vault_logical_size_bytes: u64,
    pub all_logical_size_bytes: u64,
    /// Live folder plus vault: the data the user would store without snapshots.
    pub current_logical_size_bytes: u64,
    pub referenced_data_bytes: u64,
    /// Extent bytes referenced by the live folder and vault only.
    pub current_referenced_data_bytes: u64,
    /// Unique chunks reachable from the live folder or vault.
    pub current_unique_uncompressed_bytes: u64,
    pub current_unique_compressed_bytes: u64,
    /// Compressed payload referenced only by snapshots: the disk cost of keeping history.
    pub snapshot_only_compressed_bytes: u64,
    /// Per-snapshot share of that payload: the compressed bytes deleting each
    /// snapshot on its own makes reclaimable. Payload another snapshot still
    /// references is not counted there.
    #[serde(default)]
    pub snapshot_usage: Vec<SnapshotUsage>,
    /// Dedup and compression savings below cover current data only, so
    /// snapshot sharing is never reported as saved space.
    pub dedup_saved_bytes: u64,
    pub dedup_savings_ratio: f64,
    pub stored_unique_uncompressed_bytes: u64,
    pub stored_unique_compressed_bytes: u64,
    pub compression_saved_bytes: i128,
    pub compression_savings_ratio: f64,
    pub unreachable_chunk_compressed_bytes: u64,
    /// Discarded chunk payloads still stored in packs until compaction.
    pub obsolete_awaiting_cleanup_bytes: u64,
    pub net_saved_bytes: i128,
    pub net_savings_ratio: f64,
    pub metadata_size_bytes: u64,
    pub data_dir_size_bytes: u64,
    pub vault_locked: bool,
    pub chunk_refcount_mismatch_count: u64,
    pub missing_chunk_records_for_extents: u64,
    pub orphan_extent_records: u64,
    pub pack_crc32_read_error_count: u64,
    pub cache_hits: u64,
    pub cache_requests: u64,
    pub cache_hit_rate: f64,
    pub process_private_memory_bytes: u64,
    pub process_rss_bytes: u64,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
    pub uptime_secs: f64,
    pub throughput_sample_secs: f64,
    pub read_throughput_bps: f64,
    pub write_throughput_bps: f64,
    pub metadata_cache_entries: u64,
    pub chunk_cache_entries: u64,
    pub approx_cache_memory_bytes: u64,
}

/// Cheap daemon state for frequent polling (the desktop app). Unlike
/// [`VerFsStats`] it never scans metadata: every field is an atomic, a lock
/// probe, a short metrics snapshot or a point lookup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DaemonStatus {
    pub vault_enabled: bool,
    pub vault_initialized: bool,
    pub vault_locked: bool,
    pub gc_in_progress: bool,
    pub uptime_secs: f64,
    pub read_bytes_total: u64,
    pub write_bytes_total: u64,
    pub throughput_sample_secs: f64,
    pub read_throughput_bps: f64,
    pub write_throughput_bps: f64,
    pub process_rss_bytes: u64,
    pub cache_hit_rate: f64,
    pub approx_cache_memory_bytes: u64,
    /// Rates and readings of the last second that moved data, kept after the
    /// transfer ends. `None` until the first activity of this daemon.
    pub last_activity: Option<LastActivity>,
}

/// The most recent one-second sample that moved data. Its rates are reported
/// live by the fields above while the transfer runs and through this record
/// afterwards, together with the memory and cache readings taken with it, so
/// clients can still show how the last copy ran.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LastActivity {
    pub read_bps: f64,
    pub write_bps: f64,
    pub cache_hit_rate: f64,
    pub cache_memory_bytes: u64,
    pub process_rss_bytes: u64,
    /// Seconds since the sample was taken; grows while the folder is idle.
    pub ago_secs: f64,
}

#[derive(Clone, Copy, Default)]
struct ThroughputSample {
    interval_secs: f64,
    read_bps: f64,
    write_bps: f64,
}

/// A completed statistics scan kept for instant answers, including across
/// daemon restarts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSnapshot {
    /// When the scan finished, in milliseconds since the Unix epoch.
    pub scanned_at_ms: u64,
    pub stats: VerFsStats,
}

/// Answer to a statistics request: the scan to show and the state of a newer
/// one.
pub struct StatsAnswer {
    pub stats: VerFsStats,
    /// When the returned scan finished, in milliseconds since the Unix epoch.
    pub scanned_at_ms: u64,
    /// True while a newer scan runs in the background.
    pub refreshing: bool,
    /// Failure of the last background refresh, kept until one succeeds.
    pub refresh_error: Option<String>,
}

/// What the last active sample measured, with the clock reading that ages it.
#[derive(Clone, Copy)]
struct ActivitySnapshot {
    read_bps: f64,
    write_bps: f64,
    cache_hit_rate: f64,
    cache_memory_bytes: u64,
    process_rss_bytes: u64,
    at_ms: u64,
}

/// Cache bookkeeping shared by the stats scan and the activity sampler.
#[derive(Clone, Copy)]
struct CacheTotals {
    hits: u64,
    requests: u64,
    hit_rate: f64,
    metadata_entries: u64,
    chunk_entries: u64,
    memory_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PackCrc32ReadErrorCounters {
    pub persisted_sys_total: u64,
    pub current_total: u64,
}

/// The user who owns a newly created `/.vault` directory: the user who asked
/// for it, not the daemon's user.
#[derive(Debug, Clone, Copy)]
pub struct VaultOwner {
    pub uid: u32,
    pub gid: u32,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OfflineGcReport {
    pub zero_ref_records_deleted: u64,
    pub packs_scanned: u64,
    pub pack_index_entries_scanned: u64,
    pub live_entries_found: u64,
    pub dead_entries_found: u64,
    pub discard_records_written: u64,
    pub duplicate_dead_entries_collapsed: u64,
    pub discard_checkpoint_bytes: u64,
    pub rewrite_phase_requested: bool,
    pub rewrite_cycles: u64,
    pub packs_rewritten: u64,
}

pub struct VerFs {
    core: Arc<FsCore>,
    batcher: WriteBatcher,
    sync_service: SyncService,
    throughput_worker: tokio::task::JoinHandle<()>,
    shutdown_started: AtomicBool,
}

struct FsCore {
    config: Config,
    chunker: UltraCdc,
    /// Upper bound of every extent length in this data directory
    /// (`SYS:max_extent_len`), fixed for the session.
    max_extent_len: u64,
    meta: MetaStore,
    packs: Arc<PackStore>,
    chunk_meta_cache: FillCache<ChunkId, ChunkRecord>,
    chunk_data_cache: Cache<ChunkId, Arc<Vec<u8>>>,
    uid_groups_cache: Cache<u32, Arc<Vec<u32>>>,
    inode_cache: FillCache<u64, Option<InodeRecord>>,
    dirent_cache: FillCache<DirentCacheKey, Option<DirentRecord>>,
    next_handle: AtomicU64,
    last_persisted_active_pack_id: AtomicU64,
    last_persisted_pack_crc32_read_error_count: AtomicU64,
    dedup_hits: AtomicU64,
    dedup_misses: AtomicU64,
    chunk_meta_cache_hits: AtomicU64,
    chunk_meta_cache_misses: AtomicU64,
    chunk_data_cache_hits: AtomicU64,
    chunk_data_cache_misses: AtomicU64,
    throughput: ParkingMutex<ThroughputSample>,
    last_activity: ParkingMutex<Option<ActivitySnapshot>>,
    /// Last completed statistics scan, persisted so a fresh daemon (and a
    /// fresh boot) can answer before its first scan finishes.
    stats_snapshot: ParkingMutex<Option<StatsSnapshot>>,
    stats_scan_in_progress: AtomicBool,
    stats_refresh_error: ParkingMutex<String>,
    read_bytes_total: AtomicU64,
    write_bytes_total: AtomicU64,
    pack_crc32_read_error_count: AtomicU64,
    /// Chunks whose every stored copy failed verification on read. Writes
    /// store such content again instead of deduplicating against it; the
    /// sync cycle persists the mark as `CHUNK_FLAG_CORRUPT`.
    corrupt_chunks: ParkingMutex<HashSet<ChunkId>>,
    /// Inline chunks created since the last promotion pass. They are likely
    /// still the open end of a file being written, so that pass leaves them
    /// inline; the next one moves them to a pack if they are still alive.
    young_inline_chunks: ParkingMutex<HashSet<ChunkId>>,
    stats_started_ms: AtomicU64,
    last_mutation_ms: AtomicU64,
    last_activity_ms: AtomicU64,
    write_lock: AsyncRwLock<()>,
    inode_write_locks: ParkingMutex<HashMap<u64, Weak<Mutex<()>>>>,
    inode_data_versions: ParkingMutex<HashMap<u64, u64>>,
    gc_lock: Mutex<()>,
    file_locks: ParkingMutex<HashMap<u64, Vec<FileLockState>>>,
    dir_handles: ParkingMutex<HashMap<u64, DirHandleState>>,
    file_handles: ParkingMutex<HashMap<u64, FileHandleState>>,
    file_read_plan_cache: Cache<u64, Arc<SmallFileReadPlan>>,
    /// Persistent read-plan cache keyed by (ino, data_version).
    /// Survives across file close/reopen, unlike the fh-keyed cache.
    file_read_plan_inode_cache: Cache<(u64, u64), Arc<SmallFileReadPlan>>,
    notifier: ParkingRwLock<Option<Arc<SessionNotifier>>>,
    open_file_counts: ParkingMutex<HashMap<u64, u64>>,
    /// Lookup references the kernel holds per inode (entry replies minus FORGET).
    kernel_lookups: ParkingMutex<HashMap<u64, u64>>,
    vault: ParkingRwLock<VaultRuntime>,
    /// Bumped under the write side each time the vault locks. Vault chunk
    /// plaintext enters the chunk cache only under the read side and only
    /// while the epoch it was decoded under is still current, so a read that
    /// finishes after a lock cannot put plaintext back into the cache.
    vault_cache_epoch: ParkingRwLock<u64>,
}

impl VerFs {
    pub async fn new(config: Config) -> Result<Self> {
        config.ensure_dirs()?;
        normalize_data_tree(&config.data_dir)?;

        let pack_barrier = Arc::new(std::sync::OnceLock::new());
        let meta =
            MetaStore::open_with_pack_barrier(&config, Arc::clone(&pack_barrier))
                .await?;
        ensure_pack_index_crc32_compat(&config, &meta).await?;
        if let Err(err) = ensure_pack_size_metadata_compat(&meta, config.pack_max_size_mb).await {
            error!(
                error = %err,
                configured_pack_max_size_mb = config.pack_max_size_mb,
                "pack-size metadata compatibility check failed"
            );
            return Err(err);
        }
        ensure_vault_chunk_keys_migrated(&meta).await?;
        let configured_active_pack_id = meta.get_u64_sys("active_pack_id")?;
        let persisted_pack_crc32_read_error_count =
            meta.get_u64_sys(SYS_PACK_CRC32_READ_ERRORS).unwrap_or(0);
        let packs = PackStore::open(
            &config.packs_dir(),
            configured_active_pack_id,
            config.pack_index_cache_capacity_entries,
            config.pack_max_size_mb.saturating_mul(1024 * 1024),
        )?;
        packs.verify_pack_headers(packs.active_pack_id())?;
        let packs = Arc::new(packs);
        if pack_barrier.set(Arc::clone(&packs)).is_err() {
            unreachable!("pack barrier is filled once, right after the pack store opens");
        }
        let vault_initialized = meta
            .get_sys(SYS_VAULT_WRAP)?
            .map(|raw| !raw.is_empty())
            .unwrap_or(false);
        if vault_initialized && !vault::vault_namespace_present(&meta)? {
            const REPAIR_HINT: &str = "vault record exists but the /.vault namespace is missing; \
                 run `verfsnext crypt -c -p <password> -k <key file>` with the original credentials \
                 to rebuild the namespace";
            warn!(REPAIR_HINT);
            eprintln!("VerFSNext: {REPAIR_HINT}");
        }
        // No kernel holds references before the mount, so every indexed orphan
        // left by a crash or an unclean stop is unreachable.
        let reclaimed_orphans = FsCore::reclaim_orphans(&meta).await?;
        if reclaimed_orphans > 0 {
            info!(reclaimed_orphans, "reclaimed unlinked inodes left by a previous run");
        }
        let max_extent_len = FsCore::raise_max_extent_len(&meta, &config).await?;
        if meta.get_sys(SYS_VAULT_STATE)?.is_none() {
            meta.write_txn(|txn| {
                txn.set(sys_key(SYS_VAULT_STATE), vec![VAULT_STATE_LOCKED])?;
                Ok(())
            })
            .await?;
        }

        let core = Arc::new(FsCore {
            config: config.clone(),
            chunker: UltraCdc::new(
                config.ultracdc_min_size_bytes,
                config.ultracdc_avg_size_bytes,
                config.ultracdc_max_size_bytes,
            ),
            max_extent_len,
            meta,
            packs,
            chunk_meta_cache: FillCache::new(config.metadata_cache_capacity_entries),
            chunk_data_cache: Cache::builder()
                .max_capacity(config.chunk_cache_capacity_mb.saturating_mul(1024 * 1024))
                .weigher(|_k, v: &Arc<Vec<u8>>| v.capacity() as u32)
                .build(),
            uid_groups_cache: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(60))
                .build(),
            inode_cache: FillCache::new(config.metadata_cache_capacity_entries),
            dirent_cache: FillCache::new(config.metadata_cache_capacity_entries),
            next_handle: AtomicU64::new(1),
            last_persisted_active_pack_id: AtomicU64::new(configured_active_pack_id),
            last_persisted_pack_crc32_read_error_count: AtomicU64::new(
                persisted_pack_crc32_read_error_count,
            ),
            dedup_hits: AtomicU64::new(0),
            dedup_misses: AtomicU64::new(0),
            chunk_meta_cache_hits: AtomicU64::new(0),
            chunk_meta_cache_misses: AtomicU64::new(0),
            chunk_data_cache_hits: AtomicU64::new(0),
            chunk_data_cache_misses: AtomicU64::new(0),
            throughput: ParkingMutex::new(ThroughputSample::default()),
            last_activity: ParkingMutex::new(None),
            stats_snapshot: ParkingMutex::new(None),
            stats_scan_in_progress: AtomicBool::new(false),
            stats_refresh_error: ParkingMutex::new(String::new()),
            read_bytes_total: AtomicU64::new(0),
            write_bytes_total: AtomicU64::new(0),
            pack_crc32_read_error_count: AtomicU64::new(persisted_pack_crc32_read_error_count),
            corrupt_chunks: ParkingMutex::new(HashSet::new()),
            young_inline_chunks: ParkingMutex::new(HashSet::new()),
            stats_started_ms: AtomicU64::new(now_millis()),
            last_mutation_ms: AtomicU64::new(now_millis()),
            last_activity_ms: AtomicU64::new(now_millis()),
            write_lock: AsyncRwLock::new(()),
            inode_write_locks: ParkingMutex::new(HashMap::new()),
            inode_data_versions: ParkingMutex::new(HashMap::new()),
            gc_lock: Mutex::new(()),
            file_locks: ParkingMutex::new(HashMap::new()),
            dir_handles: ParkingMutex::new(HashMap::new()),
            file_handles: ParkingMutex::new(HashMap::new()),
            file_read_plan_cache: Cache::builder()
                .max_capacity(config.metadata_cache_capacity_entries)
                .build(),
            file_read_plan_inode_cache: Cache::builder()
                .max_capacity(config.metadata_cache_capacity_entries)
                .build(),
            notifier: ParkingRwLock::new(None),
            open_file_counts: ParkingMutex::new(HashMap::new()),
            kernel_lookups: ParkingMutex::new(HashMap::new()),
            vault: ParkingRwLock::new(VaultRuntime::new(vault_initialized)),
            vault_cache_epoch: ParkingRwLock::new(0),
        });
        core.load_stats_snapshot();

        let throughput_core = Arc::downgrade(&core);
        let throughput_worker = tokio::spawn(async move {
            let mut sampled_at = Instant::now();
            let mut previous_read = 0_u64;
            let mut previous_write = 0_u64;
            loop {
                sleep(Duration::from_secs(1)).await;
                let Some(core) = throughput_core.upgrade() else {
                    break;
                };
                let now = Instant::now();
                let interval_secs = now.duration_since(sampled_at).as_secs_f64();
                let read = core.read_bytes_total.load(Ordering::Relaxed);
                let written = core.write_bytes_total.load(Ordering::Relaxed);
                let read_bps = (read - previous_read) as f64 / interval_secs;
                let write_bps = (written - previous_write) as f64 / interval_secs;
                *core.throughput.lock() = ThroughputSample {
                    interval_secs,
                    read_bps,
                    write_bps,
                };
                if read_bps > 0.0 || write_bps > 0.0 {
                    let cache = core.cache_totals();
                    *core.last_activity.lock() = Some(ActivitySnapshot {
                        read_bps,
                        write_bps,
                        cache_hit_rate: cache.hit_rate,
                        cache_memory_bytes: cache.memory_bytes,
                        process_rss_bytes: read_process_rss_bytes().unwrap_or(0),
                        at_ms: now_millis(),
                    });
                }
                sampled_at = now;
                previous_read = read;
                previous_write = written;
            }
        });

        let write_sink: Arc<dyn WriteApply> = core.clone();
        let sync_target: Arc<dyn SyncTarget> = core.clone();

        let batcher = WriteBatcher::new(
            write_sink,
            config.batch_max_size_mb.saturating_mul(1024 * 1024),
            Duration::from_millis(config.batch_flush_interval_ms),
        );
        let sync_service =
            SyncService::start(sync_target, Duration::from_millis(config.sync_interval_ms));

        Ok(Self {
            core,
            batcher,
            sync_service,
            throughput_worker,
            shutdown_started: AtomicBool::new(false),
        })
    }

    pub async fn graceful_shutdown(&self) -> Result<()> {
        if self.shutdown_started.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        self.throughput_worker.abort();
        // The batcher reports per-write failures to their own waiters, so a
        // shutdown error here only means its worker is gone; it must not skip
        // the final full sync. Skipping it would leave acknowledged writes
        // only in the page cache after a clean stop.
        if let Err(err) = self.batcher.shutdown().await {
            warn!(error = %format!("{err:#}"), "write batcher shutdown reported an error");
        }
        // The metadata close flushes every memtable into fsynced SSTables. It
        // must not run when the pack sync failed: it would make durable the
        // extents and chunk records that point at pack bytes that may never
        // reach the disk, while the WAL hand-off deliberately holds them back
        // until their pack data is durable. Leaving the store unclosed keeps
        // the on-disk state consistent; the next open recovers it like a crash.
        if let Err(err) = self.sync_service.shutdown_with_full_sync().await {
            error!(
                error = %format!("{err:#}"),
                "final full sync failed; metadata close skipped so no metadata that references unsynced pack data becomes durable"
            );
            return Err(err.context("final full sync failed; metadata close skipped"));
        }
        self.core
            .meta
            .close()
            .await
            .context("metadata close failed")?;
        Ok(())
    }

    pub fn list_snapshots(&self) -> Result<Vec<String>> {
        let snapshots = SnapshotManager::new(&self.core.meta);
        snapshots.list()
    }

    pub async fn create_snapshot(&self, name: &str) -> Result<()> {
        let _guard = self.core.write_lock.write().await;
        let snapshots = SnapshotManager::new(&self.core.meta);
        snapshots.create(name).await?;
        self.core.invalidate_metadata_caches_all();
        self.core.mark_namespace_mutation().await;
        if let Ok(Some(snapshots_dir)) = self
            .core
            .load_dirent_cached(ROOT_INODE, SNAPSHOTS_DIR_NAME.as_bytes())
        {
            self.core.invalidate_entry_best_effort(snapshots_dir.ino, name);
            self.core.invalidate_inode_attr_best_effort(snapshots_dir.ino);
        }
        Ok(())
    }

    pub async fn delete_snapshot(&self, name: &str) -> Result<()> {
        let _guard = self.core.write_lock.write().await;
        let snapshots = SnapshotManager::new(&self.core.meta);
        snapshots
            .delete(name, |ino| self.core.inode_referenced(ino))
            .await?;
        self.core.invalidate_metadata_caches_all();
        self.core.mark_namespace_mutation().await;
        if let Ok(Some(snapshots_dir)) = self
            .core
            .load_dirent_cached(ROOT_INODE, SNAPSHOTS_DIR_NAME.as_bytes())
        {
            self.core.invalidate_entry_best_effort(snapshots_dir.ino, name);
            self.core.invalidate_inode_attr_best_effort(snapshots_dir.ino);
        }
        Ok(())
    }

    pub async fn create_vault(
        &self,
        password: &str,
        key_material: &[u8; 32],
        owner: VaultOwner,
    ) -> Result<()> {
        self.core.create_vault(password, key_material, owner).await
    }

    pub async fn unlock_vault(&self, password: &str, key_material: &[u8; 32]) -> Result<()> {
        self.core.unlock_vault(password, key_material).await
    }

    /// Locks the vault of this mounted daemon. Data the kernel still holds
    /// dirty is written back first, while the vault can still store it, and
    /// the kernel's cached pages of vault files are dropped afterwards.
    pub async fn lock_vault(&self) -> Result<()> {
        self.core.write_back_mount().await?;
        self.core.lock_vault().await?;
        let core = Arc::clone(&self.core);
        tokio::task::spawn_blocking(move || core.drop_vault_pages_from_kernel())
            .await
            .context("vault page invalidation task failed")?
    }

    /// Locks the vault in the metadata store when no daemon is mounted: no
    /// kernel holds pages of it.
    pub async fn lock_vault_offline(&self) -> Result<()> {
        self.core.lock_vault().await
    }

    /// Runs a full statistics scan now, records it as the kept snapshot and
    /// returns it. This is what the `stats` control command uses.
    pub async fn collect_stats(&self) -> Result<StatsSnapshot> {
        let core = Arc::clone(&self.core);
        tokio::task::spawn_blocking(move || core.collect_stats())
            .await
            .unwrap_or_else(|e| Err(anyhow!("stats task panicked: {:?}", e)))
    }

    /// Statistics answer for a client that can show the kept scan while a
    /// newer one is being taken (the desktop app). Without any kept scan — the
    /// first run after setup — the request waits for the first scan.
    pub async fn stats_for_display(self: &Arc<Self>) -> Result<StatsAnswer> {
        let Some(kept) = self.core.stats_snapshot() else {
            let snapshot = self.collect_stats().await?;
            return Ok(StatsAnswer {
                stats: snapshot.stats,
                scanned_at_ms: snapshot.scanned_at_ms,
                refreshing: false,
                refresh_error: None,
            });
        };

        if now_millis().saturating_sub(kept.scanned_at_ms) >= STATS_REFRESH_MIN_AGE_MS {
            self.start_stats_refresh();
        }
        Ok(StatsAnswer {
            stats: kept.stats,
            scanned_at_ms: kept.scanned_at_ms,
            refreshing: self.core.stats_scan_in_progress.load(Ordering::Relaxed),
            refresh_error: self.core.stats_refresh_error(),
        })
    }

    /// Starts a background scan unless one is already running. Its result
    /// replaces the kept snapshot; a failure is kept for the next answer and
    /// logged here.
    fn start_stats_refresh(self: &Arc<Self>) {
        if self.core.stats_scan_in_progress.swap(true, Ordering::AcqRel) {
            return;
        }
        let fs = Arc::clone(self);
        tokio::spawn(async move {
            match fs.collect_stats().await {
                Ok(_) => fs.core.clear_stats_refresh_error(),
                Err(err) => {
                    let message = format!("{err:#}");
                    error!(error = %message, "background statistics refresh failed");
                    fs.core.set_stats_refresh_error(message);
                }
            }
            fs.core.stats_scan_in_progress.store(false, Ordering::Release);
        });
    }

    /// Full records-consistency report naming the affected files. Expensive:
    /// scans all extents and chunk records.
    pub async fn check_records(&self) -> Result<RecordsCheckReport> {
        let core = Arc::clone(&self.core);
        tokio::task::spawn_blocking(move || core.check_records())
            .await
            .unwrap_or_else(|e| Err(anyhow!("records check task panicked: {:?}", e)))
    }

    pub fn pack_crc32_read_error_counters(&self) -> PackCrc32ReadErrorCounters {
        PackCrc32ReadErrorCounters {
            persisted_sys_total: self
                .core
                .last_persisted_pack_crc32_read_error_count
                .load(Ordering::Relaxed),
            current_total: self
                .core
                .pack_crc32_read_error_count
                .load(Ordering::Relaxed),
        }
    }

    pub fn is_gc_in_progress(&self) -> bool {
        self.core.gc_lock.try_lock().is_err()
    }

    pub fn daemon_status(&self) -> Result<DaemonStatus> {
        let vault_initialized = self
            .core
            .meta
            .get_sys(SYS_VAULT_WRAP)?
            .is_some_and(|raw| !raw.is_empty());
        let elapsed_ms =
            now_millis().saturating_sub(self.core.stats_started_ms.load(Ordering::Relaxed));
        let throughput = *self.core.throughput.lock();
        let cache = self.core.cache_totals();
        Ok(DaemonStatus {
            vault_enabled: self.core.config.vault_enabled,
            vault_initialized,
            vault_locked: self.core.vault_locked(),
            gc_in_progress: self.is_gc_in_progress(),
            uptime_secs: elapsed_ms as f64 / 1000.0,
            read_bytes_total: self.core.read_bytes_total.load(Ordering::Relaxed),
            write_bytes_total: self.core.write_bytes_total.load(Ordering::Relaxed),
            throughput_sample_secs: throughput.interval_secs,
            read_throughput_bps: throughput.read_bps,
            write_throughput_bps: throughput.write_bps,
            process_rss_bytes: read_process_rss_bytes().unwrap_or(0),
            cache_hit_rate: cache.hit_rate,
            approx_cache_memory_bytes: cache.memory_bytes,
            last_activity: self.core.last_activity(),
        })
    }

    pub fn install_session_notifier(&self, notifier: Arc<SessionNotifier>) {
        self.core.install_session_notifier(notifier);
    }
}

impl FsCore {
    /// Records the configured maximum chunk size in `SYS:max_extent_len`
    /// when it exceeds every extent written so far, and returns the bound.
    /// Every extent written in this session is at most the configured
    /// maximum, so the bound never changes while mounted.
    async fn raise_max_extent_len(meta: &MetaStore, config: &Config) -> Result<u64> {
        let configured = config.ultracdc_max_size_bytes as u64;
        let stored = match meta.get_sys(SYS_MAX_EXTENT_LEN)? {
            Some(_) => meta.get_u64_sys(SYS_MAX_EXTENT_LEN)?,
            None => 0,
        };
        if configured > stored {
            meta.write_txn(|txn| {
                txn.set(
                    sys_key(SYS_MAX_EXTENT_LEN),
                    configured.to_le_bytes().to_vec(),
                )?;
                Ok(())
            })
            .await?;
            return Ok(configured);
        }
        Ok(stored)
    }

    fn configured_attr_ttl(&self) -> Duration {
        Duration::from_millis(self.config.fuse_attr_ttl_ms)
    }

    fn configured_entry_ttl(&self) -> Duration {
        Duration::from_millis(self.config.fuse_entry_ttl_ms)
    }

    fn attr_ttl(&self) -> Duration {
        if self.invalidation_enabled() {
            self.configured_attr_ttl()
        } else {
            Duration::ZERO
        }
    }

    fn entry_ttl(&self) -> Duration {
        if self.invalidation_enabled() {
            self.configured_entry_ttl()
        } else {
            Duration::ZERO
        }
    }

    fn invalidation_enabled(&self) -> bool {
        self.notifier.read().is_some()
    }

    fn dirent_cache_key(parent: u64, name: &[u8]) -> DirentCacheKey {
        DirentCacheKey {
            parent,
            name: name.to_vec(),
        }
    }

    fn load_inode_cached(&self, ino: u64) -> Result<Option<InodeRecord>> {
        if let Some(inode) = self.inode_cache.get(&ino) {
            return Ok(inode);
        }
        let ticket = self.inode_cache.ticket(&ino);
        let inode = self.meta.get_inode(ino)?;
        self.inode_cache.insert_if_current(ino, inode, ticket);
        Ok(inode)
    }

    fn load_dirent_cached(&self, parent: u64, name: &[u8]) -> Result<Option<DirentRecord>> {
        let key = Self::dirent_cache_key(parent, name);
        if let Some(dirent) = self.dirent_cache.get(&key) {
            return Ok(dirent);
        }
        let ticket = self.dirent_cache.ticket(&key);
        let dirent = match self.meta.get_value(&dirent_key(parent, name))? {
            Some(raw) => {
                let d: DirentRecord = decode_rkyv(&raw)?;
                Some(d)
            }
            None => None,
        };
        self.dirent_cache.insert_if_current(key, dirent, ticket);
        Ok(dirent)
    }

    fn invalidate_inode_cache(&self, ino: u64) {
        self.inode_cache.invalidate(&ino);
    }

    fn invalidate_dirent_cache(&self, parent: u64, name: &[u8]) {
        self.dirent_cache
            .invalidate(&Self::dirent_cache_key(parent, name));
    }

    fn invalidate_metadata_caches_all(&self) {
        self.inode_cache.invalidate_all();
        self.dirent_cache.invalidate_all();
    }

    fn inode_write_lock(&self, ino: u64) -> Arc<Mutex<()>> {
        let mut locks = self.inode_write_locks.lock();
        if let Some(lock) = locks.get(&ino).and_then(Weak::upgrade) {
            return lock;
        }

        let lock = Arc::new(Mutex::new(()));
        locks.insert(ino, Arc::downgrade(&lock));
        lock
    }

    fn inode_data_version(&self, ino: u64) -> u64 {
        let mut versions = self.inode_data_versions.lock();
        *versions.entry(ino).or_insert(0)
    }

    fn bump_inode_data_version(&self, ino: u64) {
        let mut versions = self.inode_data_versions.lock();
        let entry = versions.entry(ino).or_insert(0);
        *entry = entry.saturating_add(1);
    }

    fn new_handle(&self) -> u64 {
        self.next_handle.fetch_add(1, Ordering::Relaxed)
    }

    fn increment_open_file_count(&self, ino: u64) {
        let mut counts = self.open_file_counts.lock();
        let entry = counts.entry(ino).or_insert(0);
        *entry = entry.saturating_add(1);
    }

    fn decrement_open_file_count(&self, ino: u64) -> u64 {
        let mut counts = self.open_file_counts.lock();
        let Some(entry) = counts.get_mut(&ino) else {
            return 0;
        };
        *entry = entry.saturating_sub(1);
        let remaining = *entry;
        if remaining == 0 {
            counts.remove(&ino);
        }
        remaining
    }

    fn open_file_count(&self, ino: u64) -> u64 {
        self.open_file_counts.lock().get(&ino).copied().unwrap_or(0)
    }

    fn add_kernel_lookup(&self, ino: u64) {
        let mut lookups = self.kernel_lookups.lock();
        let entry = lookups.entry(ino).or_insert(0);
        *entry = entry.saturating_add(1);
    }

    /// Drops `nlookup` kernel references and returns the references left.
    fn forget_kernel_lookups(&self, ino: u64, nlookup: u64) -> u64 {
        let mut lookups = self.kernel_lookups.lock();
        let Some(entry) = lookups.get_mut(&ino) else {
            return 0;
        };
        *entry = entry.saturating_sub(nlookup);
        let remaining = *entry;
        if remaining == 0 {
            lookups.remove(&ino);
        }
        remaining
    }

    /// An unlinked inode must outlive both its open handles and the kernel's
    /// lookup references, which stay valid until FORGET.
    fn inode_referenced(&self, ino: u64) -> bool {
        self.open_file_count(ino) > 0 || self.kernel_lookups.lock().contains_key(&ino)
    }

    fn register_file_handle(&self, fh: u64, ino: u64, inode_data_version: u64) {
        let mut handles = self.file_handles.lock();
        handles.insert(
            fh,
            FileHandleState {
                ino,
                inode_data_version,
                next_read_offset: 0,
                readahead_end: 0,
            },
        );
        self.file_read_plan_cache.invalidate(&fh);
    }

    fn validate_file_handle(&self, fh: u64, ino: u64) -> Result<()> {
        let handles = self.file_handles.lock();
        let Some(state) = handles.get(&fh) else {
            return Err(anyhow_errno(
                Errno::EBADF,
                format!("unknown file handle {}", fh),
            ));
        };
        if state.ino != ino {
            return Err(anyhow_errno(
                Errno::EBADF,
                format!(
                    "file handle {} does not match inode {} (got {})",
                    fh, ino, state.ino
                ),
            ));
        }
        Ok(())
    }

    fn get_small_file_read_plan(
        &self,
        fh: u64,
        ino: u64,
        inode_data_version: u64,
    ) -> Result<Option<SmallFileReadPlan>> {
        let handles = self.file_handles.lock();
        let Some(state) = handles.get(&fh) else {
            return Err(anyhow_errno(
                Errno::EBADF,
                format!("unknown file handle {}", fh),
            ));
        };
        if state.ino != ino {
            return Err(anyhow_errno(
                Errno::EBADF,
                format!(
                    "file handle {} does not match inode {} (got {})",
                    fh, ino, state.ino
                ),
            ));
        }
        if state.inode_data_version != inode_data_version {
            return Ok(None);
        }
        // Check file-handle-keyed cache first (fastest)
        if let Some(plan) = self.file_read_plan_cache.get(&fh) {
            return Ok(Some(plan.as_ref().clone()));
        }
        // Fall back to persistent inode-keyed cache (survives close/reopen)
        Ok(self
            .file_read_plan_inode_cache
            .get(&(ino, inode_data_version))
            .map(|plan| plan.as_ref().clone()))
    }

    fn store_small_file_read_plan(
        &self,
        fh: u64,
        ino: u64,
        inode_data_version: u64,
        small_file_read_plan: Option<SmallFileReadPlan>,
    ) -> Result<()> {
        let mut handles = self.file_handles.lock();
        let Some(state) = handles.get_mut(&fh) else {
            return Err(anyhow_errno(
                Errno::EBADF,
                format!("unknown file handle {}", fh),
            ));
        };
        if state.ino != ino {
            return Err(anyhow_errno(
                Errno::EBADF,
                format!(
                    "file handle {} does not match inode {} (got {})",
                    fh, ino, state.ino
                ),
            ));
        }
        state.inode_data_version = inode_data_version;
        drop(handles);
        if let Some(plan) = small_file_read_plan {
            let arc = Arc::new(plan);
            self.file_read_plan_cache.insert(fh, Arc::clone(&arc));
            // Also insert into persistent inode-keyed cache
            self.file_read_plan_inode_cache
                .insert((ino, inode_data_version), arc);
        } else {
            self.file_read_plan_cache.invalidate(&fh);
        }
        Ok(())
    }

    /// Records the read `[offset, read_end)` on `fh` and returns the range
    /// whose chunks should be loaded ahead of the reader: none unless the
    /// handle reads sequentially and less than half the window is left
    /// ahead of it.
    fn claim_readahead(
        &self,
        fh: u64,
        offset: u64,
        read_end: u64,
        file_size: u64,
    ) -> Option<(u64, u64)> {
        let mut handles = self.file_handles.lock();
        let state = handles.get_mut(&fh)?;
        let sequential = offset == state.next_read_offset;
        state.next_read_offset = read_end;
        if !sequential {
            state.readahead_end = read_end;
            return None;
        }
        let target = read_end.saturating_add(READAHEAD_WINDOW_BYTES).min(file_size);
        let from = state.readahead_end.max(read_end);
        if from >= target || from >= read_end.saturating_add(READAHEAD_WINDOW_BYTES / 2) {
            return None;
        }
        state.readahead_end = target;
        Some((from, target))
    }

    fn unregister_file_handle(&self, fh: u64) {
        let mut handles = self.file_handles.lock();
        handles.remove(&fh);
        drop(handles);
        self.file_read_plan_cache.invalidate(&fh);
    }

    fn install_session_notifier(&self, notifier: Arc<SessionNotifier>) {
        let mut slot = self.notifier.write();
        *slot = Some(notifier);
    }

    fn invalidate_inode_attr_best_effort(&self, ino: u64) {
        self.invalidate_inode_cache(ino);
        let Some(notifier) = self.notifier.read().clone() else {
            return;
        };
        if let Err(err) = notifier.invalidate_inode(ino, -1, 0) {
            if err != Errno::ENOENT {
                warn!(ino, error = %err, "inode attr invalidation notify failed");
            }
        }
    }

    fn invalidate_entry_best_effort(&self, parent: u64, name: &str) {
        self.invalidate_dirent_cache(parent, name.as_bytes());
        let Some(notifier) = self.notifier.read().clone() else {
            return;
        };
        if let Err(err) = notifier.invalidate_entry(parent, name) {
            if err != Errno::ENOENT {
                warn!(
                    parent,
                    name,
                    error = %err,
                    "directory entry invalidation notify failed"
                );
            }
        }
    }

    async fn mark_namespace_mutation(&self) {
        self.mark_mutation();
    }

    fn build_dir_snapshot(&self, ino: u64, inode: &InodeRecord) -> Result<Vec<CachedDirEntry>> {
        let mut entries = Vec::new();
        entries.push(CachedDirEntry {
            ino,
            name: ".".to_owned(),
            file_type: FileType::Dir,
        });
        entries.push(CachedDirEntry {
            ino: inode.parent,
            name: "..".to_owned(),
            file_type: FileType::Dir,
        });

        let dir_entries = self.meta.read_txn(|txn| {
            let prefix = dirent_prefix(ino);
            let end = prefix_end(&prefix);
            let mut out = Vec::new();
            for pair in scan_range_pairs(txn, prefix, end)? {
                let (key, value) = pair?;
                let Some(name_bytes) = decode_dirent_name(&key) else {
                    continue;
                };
                let name = String::from_utf8_lossy(name_bytes).to_string();
                let dirent: DirentRecord = decode_rkyv(&value)?;
                if txn.get(inode_key(dirent.ino))?.is_none() {
                    continue;
                }
                if self.vault_locked() && ino == ROOT_INODE && name == VAULT_DIR_NAME {
                    continue;
                }
                out.push(CachedDirEntry {
                    ino: dirent.ino,
                    name,
                    file_type: FsCore::inode_kind_to_file_type(dirent.kind),
                });
            }
            Ok(out)
        })?;
        entries.extend(dir_entries);
        Ok(entries)
    }

    fn snapshot_to_dir_entries(snapshot: &[CachedDirEntry]) -> Vec<DirEntry> {
        snapshot
            .iter()
            .map(|entry| DirEntry::new(entry.ino, entry.name.clone(), entry.file_type.clone()))
            .collect()
    }

    /// Walks the namespace under 'start_ino' once, adding each file's logical
    /// size and reporting every inode it reaches.
    fn walk_namespace_logical_size(
        txn: &verfsnext_surrealkv::Transaction,
        start_ino: u64,
        seen_inodes: &mut HashSet<u64>,
        mut visit: impl FnMut(&InodeRecord),
    ) -> Result<u64> {
        let mut logical_size_bytes = 0_u64;
        let mut stack = vec![start_ino];

        while let Some(ino) = stack.pop() {
            if !seen_inodes.insert(ino) {
                continue;
            }

            let Some(raw) = txn.get(inode_key(ino))? else {
                continue;
            };
            let inode: InodeRecord = decode_rkyv(&raw)?;
            visit(&inode);

            match inode.kind {
                INODE_KIND_FILE => {
                    logical_size_bytes = logical_size_bytes.saturating_add(inode.size);
                }
                INODE_KIND_DIR => {
                    let prefix = dirent_prefix(ino);
                    let end = prefix_end(&prefix);
                    for pair in scan_range_pairs(txn, prefix, end)? {
                        let (_, value) = pair?;
                        let dirent: DirentRecord = decode_rkyv(&value)?;
                        stack.push(dirent.ino);
                    }
                }
                _ => {}
            }
        }

        Ok(logical_size_bytes)
    }

    fn extent_owner_ino_from_key(key: &[u8]) -> Option<u64> {
        if key.len() != 17 || key[0] != crate::types::KEY_PREFIX_EXTENT {
            return None;
        }
        let mut ino = [0_u8; 8];
        ino.copy_from_slice(&key[1..9]);
        Some(u64::from_be_bytes(ino))
    }

    fn collect_stats(&self) -> Result<StatsSnapshot> {
        self.chunk_meta_cache.run_pending_tasks();
        self.chunk_data_cache.run_pending_tasks();
        let vault_locked = self.vault_locked();

        let (
            live_logical_size_bytes,
            snapshots_logical_size_bytes,
            hidden_vault_logical_size_bytes,
            all_logical_size_bytes,
            referenced_data_bytes,
            current_referenced_data_bytes,
            stored_unique_uncompressed_bytes,
            stored_unique_compressed_bytes,
            current_unique_uncompressed_bytes,
            current_unique_compressed_bytes,
            snapshot_only_compressed_bytes,
            snapshot_usage,
            unreachable_chunk_compressed_bytes,
            chunk_refcount_mismatch_count,
            missing_chunk_records_for_extents,
            orphan_extent_records,
        ) = self.meta.read_txn(|txn| {
            let snapshot_records = records_in_txn(txn)?;
            let mut snapshot_reclaimable_bytes = vec![0_u64; snapshot_records.len()];
            // Inode -> index into snapshot_records, from each snapshot root.
            let mut snapshot_inodes = HashMap::<u64, usize>::new();

            let mut live_logical_size_bytes = 0_u64;
            let mut snapshots_logical_size_bytes = 0_u64;
            let mut hidden_vault_logical_size_bytes = 0_u64;

            let mut live_seen_inodes = HashSet::<u64>::new();
            let mut snapshots_seen_inodes = HashSet::<u64>::new();
            let mut hidden_vault_seen_inodes = HashSet::<u64>::new();

            // Snapshot usage follows the S: records, the same source the
            // snapshot list uses. Only files can own extents.
            for (index, (_, record)) in snapshot_records.iter().enumerate() {
                snapshots_logical_size_bytes = snapshots_logical_size_bytes.saturating_add(
                    FsCore::walk_namespace_logical_size(
                        txn,
                        record.root_ino,
                        &mut snapshots_seen_inodes,
                        |inode| {
                            if inode.kind == INODE_KIND_FILE {
                                snapshot_inodes.insert(inode.ino, index);
                            }
                        },
                    )?,
                );
            }

            let root_prefix = dirent_prefix(ROOT_INODE);
            let root_end = prefix_end(&root_prefix);
            for pair in scan_range_pairs(txn, root_prefix, root_end)? {
                let (key, value) = pair?;
                let Some(name) = decode_dirent_name(&key) else {
                    continue;
                };
                let dirent: DirentRecord = decode_rkyv(&value)?;

                if name == SNAPSHOTS_DIR_NAME.as_bytes() {
                    continue;
                }

                if vault_locked && name == VAULT_DIR_NAME.as_bytes() {
                    hidden_vault_logical_size_bytes = hidden_vault_logical_size_bytes
                        .saturating_add(FsCore::walk_namespace_logical_size(
                            txn,
                            dirent.ino,
                            &mut hidden_vault_seen_inodes,
                            |_| {},
                        )?);
                    continue;
                }

                live_logical_size_bytes = live_logical_size_bytes.saturating_add(
                    FsCore::walk_namespace_logical_size(
                        txn,
                        dirent.ino,
                        &mut live_seen_inodes,
                        |_| {},
                    )?,
                );
            }

            let all_logical_size_bytes = live_logical_size_bytes
                .saturating_add(snapshots_logical_size_bytes)
                .saturating_add(hidden_vault_logical_size_bytes);

            let current_inodes: HashSet<u64> = live_seen_inodes
                .union(&hidden_vault_seen_inodes)
                .copied()
                .collect();
            let mut chunk_holders = HashMap::<ChunkId, ChunkHolders>::new();
            let mut referenced_data_bytes = 0_u64;
            let mut current_referenced_data_bytes = 0_u64;
            let mut inode_domain_cache = HashMap::<u64, Option<bool>>::new();
            let mut orphan_extent_records = 0_u64;
            let extent_prefix_all = vec![crate::types::KEY_PREFIX_EXTENT];
            let extent_end_all = prefix_end(&extent_prefix_all);
            for pair in scan_range_pairs(txn, extent_prefix_all, extent_end_all)? {
                let (key, value) = pair?;
                let Some(owner_ino) = FsCore::extent_owner_ino_from_key(&key) else {
                    continue;
                };

                let owner_vault = if let Some(vault) = inode_domain_cache.get(&owner_ino) {
                    *vault
                } else {
                    let vault = match txn.get(inode_key(owner_ino))? {
                        Some(raw) => Some(
                            decode_rkyv::<InodeRecord>(&raw)?.flags & INODE_FLAG_VAULT != 0,
                        ),
                        None => None,
                    };
                    inode_domain_cache.insert(owner_ino, vault);
                    vault
                };
                // An extent whose inode is gone cannot be read and its domain
                // is unknowable, so it counts as an orphan only.
                let Some(vault) = owner_vault else {
                    orphan_extent_records = orphan_extent_records.saturating_add(1);
                    continue;
                };

                let extent: ExtentRecord = decode_rkyv(&value)?;
                let id = ChunkId {
                    vault,
                    hash: extent.chunk_hash,
                };
                let in_current = current_inodes.contains(&owner_ino);
                let in_snapshot = snapshot_inodes.get(&owner_ino).copied();
                if in_current || in_snapshot.is_some() {
                    referenced_data_bytes += u64::from(extent.len);
                }
                if in_current {
                    current_referenced_data_bytes += u64::from(extent.len);
                }

                let holders = chunk_holders.entry(id).or_default();
                holders.extent_refs = holders.extent_refs.saturating_add(1);
                if in_current {
                    holders.current = true;
                }
                if let Some(index) = in_snapshot {
                    match holders.snapshot {
                        None => holders.snapshot = Some(index),
                        Some(existing) if existing == index => {}
                        Some(_) => holders.shared_by_snapshots = true,
                    }
                }
                if !in_current && in_snapshot.is_none() {
                    holders.referenced_elsewhere = true;
                }
            }

            let mut stored_unique_uncompressed_bytes = 0_u64;
            let mut stored_unique_compressed_bytes = 0_u64;
            let mut current_unique_uncompressed_bytes = 0_u64;
            let mut current_unique_compressed_bytes = 0_u64;
            let mut snapshot_only_compressed_bytes = 0_u64;
            let mut unreachable_chunk_compressed_bytes = 0_u64;
            let mut chunk_refcount_mismatch_count = 0_u64;
            let mut seen_chunk_records = HashSet::<ChunkId>::new();
            let chunk_prefix = vec![KEY_PREFIX_CHUNK];
            let chunk_end = prefix_end(&chunk_prefix);
            for pair in scan_range_pairs(txn, chunk_prefix, chunk_end)? {
                let (key, value) = pair?;
                let Some(id) = decode_chunk_key(&key) else {
                    continue;
                };
                let chunk: ChunkRecord = decode_rkyv(&value)?;

                let chunk_uncompressed = chunk.uncompressed_len as u64;
                let chunk_compressed = chunk.compressed_len as u64;
                let holders = chunk_holders.get(&id).copied();
                let expected_refcount = holders.map_or(0, |holders| holders.extent_refs);
                let refcount_consistent = chunk.refcount == expected_refcount;
                match holders {
                    Some(holders)
                        if holders.current
                            || holders.snapshot.is_some()
                            || holders.shared_by_snapshots =>
                    {
                        stored_unique_uncompressed_bytes += chunk_uncompressed;
                        stored_unique_compressed_bytes += chunk_compressed;
                        if holders.current {
                            current_unique_uncompressed_bytes += chunk_uncompressed;
                            current_unique_compressed_bytes += chunk_compressed;
                        } else {
                            snapshot_only_compressed_bytes += chunk_compressed;
                            let exclusive = !holders.shared_by_snapshots
                                && !holders.referenced_elsewhere
                                && refcount_consistent;
                            if let Some(index) = holders.snapshot.filter(|_| exclusive) {
                                let total = &mut snapshot_reclaimable_bytes[index];
                                *total = total.saturating_add(chunk_compressed);
                            }
                        }
                    }
                    _ => unreachable_chunk_compressed_bytes += chunk_compressed,
                }
                if !refcount_consistent {
                    chunk_refcount_mismatch_count = chunk_refcount_mismatch_count.saturating_add(1);
                }
                seen_chunk_records.insert(id);
            }

            let missing_chunk_records_for_extents = chunk_holders
                .keys()
                .filter(|id| !seen_chunk_records.contains(*id))
                .count() as u64;

            let snapshot_usage = snapshot_records
                .into_iter()
                .zip(snapshot_reclaimable_bytes)
                .map(|((name, _), reclaimable_bytes)| SnapshotUsage {
                    name,
                    reclaimable_bytes,
                })
                .collect();

            Ok((
                live_logical_size_bytes,
                snapshots_logical_size_bytes,
                hidden_vault_logical_size_bytes,
                all_logical_size_bytes,
                referenced_data_bytes,
                current_referenced_data_bytes,
                stored_unique_uncompressed_bytes,
                stored_unique_compressed_bytes,
                current_unique_uncompressed_bytes,
                current_unique_compressed_bytes,
                snapshot_only_compressed_bytes,
                snapshot_usage,
                unreachable_chunk_compressed_bytes,
                chunk_refcount_mismatch_count,
                missing_chunk_records_for_extents,
                orphan_extent_records,
            ))
        })?;

        let dedup_saved_bytes = current_referenced_data_bytes
            .checked_sub(current_unique_uncompressed_bytes)
            .ok_or_else(|| anyhow!("invalid dedup accounting: current unique bytes exceed current referenced data bytes"))?;
        let dedup_savings_ratio = if current_referenced_data_bytes == 0 {
            0.0
        } else {
            dedup_saved_bytes as f64 / current_referenced_data_bytes as f64
        };

        let metadata_size_bytes = dir_size_recursive(&self.config.metadata_dir())?;
        let data_dir_size_bytes = dir_size_recursive(&self.config.data_dir)?;
        let obsolete_awaiting_cleanup_bytes = self.obsolete_awaiting_cleanup_bytes()?;

        let compression_saved_bytes = current_unique_uncompressed_bytes as i128
            - current_unique_compressed_bytes as i128;
        let compression_savings_ratio = if current_unique_uncompressed_bytes == 0 {
            0.0
        } else {
            compression_saved_bytes as f64 / current_unique_uncompressed_bytes as f64
        };
        // Snapshot history is a cost, not a saving: its exclusive payload is
        // taken off the disk side and its logical size never enters the baseline.
        let current_logical_size_bytes =
            live_logical_size_bytes.saturating_add(hidden_vault_logical_size_bytes);
        let net_saved_bytes = current_logical_size_bytes as i128
            - (data_dir_size_bytes as i128 - snapshot_only_compressed_bytes as i128);
        let net_savings_ratio = if current_logical_size_bytes == 0 {
            0.0
        } else {
            net_saved_bytes as f64 / current_logical_size_bytes as f64
        };

        let cache = self.cache_totals();

        let process_rss_bytes = read_process_rss_bytes().unwrap_or(0);
        let process_private_memory_bytes =
            read_process_private_memory_bytes().unwrap_or(process_rss_bytes);
        let read_bytes_total = self.read_bytes_total.load(Ordering::Relaxed);
        let write_bytes_total = self.write_bytes_total.load(Ordering::Relaxed);
        let pack_crc32_read_error_count = self.pack_crc32_read_error_count.load(Ordering::Relaxed);
        let elapsed_ms = now_millis().saturating_sub(self.stats_started_ms.load(Ordering::Relaxed));
        let uptime_secs = (elapsed_ms as f64 / 1000.0).max(0.001);
        let throughput = *self.throughput.lock();

        let stats = VerFsStats {
            live_logical_size_bytes,
            snapshots_logical_size_bytes,
            hidden_vault_logical_size_bytes,
            all_logical_size_bytes,
            current_logical_size_bytes,
            referenced_data_bytes,
            current_referenced_data_bytes,
            current_unique_uncompressed_bytes,
            current_unique_compressed_bytes,
            snapshot_only_compressed_bytes,
            snapshot_usage,
            dedup_saved_bytes,
            dedup_savings_ratio,
            stored_unique_uncompressed_bytes,
            stored_unique_compressed_bytes,
            compression_saved_bytes,
            compression_savings_ratio,
            unreachable_chunk_compressed_bytes,
            obsolete_awaiting_cleanup_bytes,
            net_saved_bytes,
            net_savings_ratio,
            metadata_size_bytes,
            data_dir_size_bytes,
            vault_locked,
            chunk_refcount_mismatch_count,
            missing_chunk_records_for_extents,
            orphan_extent_records,
            pack_crc32_read_error_count,
            cache_hits: cache.hits,
            cache_requests: cache.requests,
            cache_hit_rate: cache.hit_rate,
            process_private_memory_bytes,
            process_rss_bytes,
            read_bytes_total,
            write_bytes_total,
            uptime_secs,
            throughput_sample_secs: throughput.interval_secs,
            read_throughput_bps: throughput.read_bps,
            write_throughput_bps: throughput.write_bps,
            metadata_cache_entries: cache.metadata_entries,
            chunk_cache_entries: cache.chunk_entries,
            approx_cache_memory_bytes: cache.memory_bytes,
        };
        Ok(self.record_stats_snapshot(stats))
    }

    /// Records a completed scan as the kept snapshot and writes it to disk, so
    /// a later daemon — or the same one after a reboot — answers instantly.
    fn record_stats_snapshot(&self, stats: VerFsStats) -> StatsSnapshot {
        let snapshot = StatsSnapshot {
            scanned_at_ms: now_millis(),
            stats,
        };
        *self.stats_snapshot.lock() = Some(snapshot.clone());
        self.persist_stats_snapshot(&snapshot);
        snapshot
    }

    /// Last completed scan, if any. Cheap: a lock and a clone.
    pub(crate) fn stats_snapshot(&self) -> Option<StatsSnapshot> {
        self.stats_snapshot.lock().clone()
    }

    pub(crate) fn stats_refresh_error(&self) -> Option<String> {
        let error = self.stats_refresh_error.lock().clone();
        if error.is_empty() {
            None
        } else {
            Some(error)
        }
    }

    pub(crate) fn clear_stats_refresh_error(&self) {
        self.stats_refresh_error.lock().clear();
    }

    pub(crate) fn set_stats_refresh_error(&self, message: String) {
        *self.stats_refresh_error.lock() = message;
    }

    /// Reads the persisted scan. Absent on the first run; a file that cannot be
    /// read or parsed is reported and ignored because it is only a cache.
    fn load_stats_snapshot(&self) {
        let path = self.config.data_dir.join(STATS_SNAPSHOT_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return,
            Err(err) => {
                warn!(path = %path.display(), error = %err, "kept statistics scan is unreadable");
                return;
            }
        };
        match serde_json::from_slice::<StatsSnapshot>(&bytes) {
            Ok(snapshot) => *self.stats_snapshot.lock() = Some(snapshot),
            Err(err) => {
                warn!(path = %path.display(), error = %err, "kept statistics scan does not parse");
            }
        }
    }

    /// Replaces the persisted scan. A failed write is reported: the file is a
    /// cache, so the running daemon keeps answering from memory.
    fn persist_stats_snapshot(&self, snapshot: &StatsSnapshot) {
        let path = self.config.data_dir.join(STATS_SNAPSHOT_FILE);
        let bytes = match serde_json::to_vec(snapshot) {
            Ok(bytes) => bytes,
            Err(err) => {
                error!(error = %err, "failed to encode the statistics scan");
                return;
            }
        };
        let tmp = path.with_extension("json.tmp");
        if let Err(err) = std::fs::write(&tmp, bytes) {
            error!(path = %tmp.display(), error = %err, "failed to write the statistics scan");
            return;
        }
        if let Err(err) = std::fs::rename(&tmp, &path) {
            error!(path = %path.display(), error = %err, "failed to publish the statistics scan");
        }
    }

    /// Last second that moved data, with the readings taken at that moment.
    /// `None` until the first activity of this daemon.
    fn last_activity(&self) -> Option<LastActivity> {
        let snapshot = (*self.last_activity.lock())?;
        Some(LastActivity {
            read_bps: snapshot.read_bps,
            write_bps: snapshot.write_bps,
            cache_hit_rate: snapshot.cache_hit_rate,
            cache_memory_bytes: snapshot.cache_memory_bytes,
            process_rss_bytes: snapshot.process_rss_bytes,
            ago_secs: now_millis().saturating_sub(snapshot.at_ms) as f64 / 1000.0,
        })
    }

    /// Cache counters, entries and approximate memory in one place, so the
    /// stats scan and the one-second activity sampler agree on them.
    fn cache_totals(&self) -> CacheTotals {
        let hits = self
            .chunk_meta_cache_hits
            .load(Ordering::Relaxed)
            .saturating_add(self.chunk_data_cache_hits.load(Ordering::Relaxed));
        let requests = hits
            .saturating_add(self.chunk_meta_cache_misses.load(Ordering::Relaxed))
            .saturating_add(self.chunk_data_cache_misses.load(Ordering::Relaxed));
        let metadata_entries = self.chunk_meta_cache.entry_count();
        let chunk_entries = self.chunk_data_cache.entry_count();
        CacheTotals {
            hits,
            requests,
            hit_rate: if requests == 0 {
                0.0
            } else {
                hits as f64 / requests as f64
            },
            metadata_entries,
            chunk_entries,
            memory_bytes: metadata_entries
                .saturating_mul(size_of::<(ChunkId, ChunkRecord)>() as u64)
                .saturating_add(
                    chunk_entries.saturating_mul(size_of::<(ChunkId, Arc<Vec<u8>>)>() as u64),
                )
                .saturating_add(self.chunk_data_cache.weighted_size()),
        }
    }

    /// Bytes of chunk payloads already recorded in `.DISCARD` and still sitting
    /// in packs. This is obsolete data waiting for compaction, not metadata.
    fn obsolete_awaiting_cleanup_bytes(&self) -> Result<u64> {
        let discard_path = self
            .config
            .packs_dir()
            .join(&self.config.gc_discard_filename);
        // Same bound as the rewrite phase: records past the checkpoint are not
        // handed to cleanup yet. Without a checkpoint the whole file counts.
        let checkpoint = self
            .meta
            .get_u64_sys("gc.discard_checkpoint")
            .unwrap_or(u64::MAX);
        let parsed = read_records(&discard_path, checkpoint)?;
        let mut seen = HashSet::<(u64, [u8; 16])>::new();
        let mut total = 0_u64;
        for record in parsed.records {
            if seen.insert((record.pack_id, record.chunk_hash128)) {
                total = total.saturating_add(record.block_size_bytes as u64);
            }
        }
        Ok(total)
    }

}

impl FsCore {
    /// Makes every write acknowledged so far durable for an explicit
    /// fsync/fdatasync/fsyncdir: the packs first, then the metadata WAL
    /// records that reference them, both synced to disk before returning.
    /// The packs are synced only when records were appended since the last
    /// pack sync (most small files live inline in metadata and append
    /// nothing), and with `sync_data`, which covers their size: the pack
    /// files' timestamps play no part in reading data back. The WAL is
    /// always synced, because the inode size, extents and chunk records are
    /// needed to read the data back; it skips the disk itself when nothing
    /// was committed since its last sync.
    pub(crate) async fn sync_durably(&self) -> Result<()> {
        tokio::task::block_in_place(|| {
            self.packs.sync_to(self.packs.append_watermark())?;
            self.meta.flush_wal(true)
        })
    }
}

#[async_trait]
impl SyncTarget for FsCore {
    async fn sync_cycle(&self, full: bool) -> Result<()> {
        self.persist_active_pack_id_if_needed().await?;
        self.persist_pack_crc32_read_error_count_if_needed().await?;
        self.persist_corrupt_chunk_marks().await?;
        // Both block on the disk. The periodic cycle syncs the packs only
        // when something was appended since the last pack sync, and hands
        // off only WAL records whose pack data is durable: flushing the whole
        // buffer would sync packs again under the WAL lock and pause every
        // commit while writes are streaming in. A full cycle (shutdown)
        // makes everything durable.
        tokio::task::block_in_place(|| {
            if full {
                self.packs.sync(true)?;
                self.meta.flush_wal(true)
            } else {
                self.packs.sync_to(self.packs.append_watermark())?;
                self.meta.write_durable_wal_prefix()
            }
        })?;
        if !full {
            self.maybe_run_gc().await?;
        }
        Ok(())
    }

    async fn hand_off_wal(&self) -> Result<()> {
        let packs = Arc::clone(&self.packs);
        tokio::task::spawn_blocking(move || packs.sync_to(packs.append_watermark()))
            .await
            .map_err(|err| anyhow!("pack sync task failed: {err}"))??;
        self.meta.write_durable_wal_prefix()
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

    #[tokio::test(flavor = "multi_thread")]
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
            metadata_block_cache_mb: 16,
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
            fuse_allow_other: false,
            fuse_attr_ttl_ms: 0,
            fuse_entry_ttl_ms: 0,
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

        let (_entry_ttl, _attr_ttl, attr, _gen, _fh, _) = fs
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
        let duration = start.elapsed();
        println!("Overwrite took: {:?}", duration);

        fs.graceful_shutdown().await?;
        fs::remove_dir_all(&root)?;

        Ok(())
    }
}
