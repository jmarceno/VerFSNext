//! On-demand records check: turns the stats consistency counters into a report
//! that names the affected files, so the desktop app can show exactly what was
//! lost and let the user clean it up.
//!
//! The check walks extents, chunk records and the namespace. It never writes
//! metadata and never reads payloads, so it is a metadata-only pass.

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::types::{
    decode_dirent_name, decode_rkyv, dirent_prefix, inode_key, prefix_end, ChunkRecord,
    DirentRecord, ExtentRecord, InodeRecord, INODE_KIND_DIR, KEY_PREFIX_CHUNK, KEY_PREFIX_EXTENT,
    ROOT_INODE,
};
use crate::vault::CHUNK_FLAG_CORRUPT;

use super::{scan_range_pairs, FsCore};

/// Most files listed per problem; the counters stay exact.
const MAX_FILES_PER_PROBLEM: usize = 200;

/// Full records-consistency report, including the files behind every counter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordsCheckReport {
    /// Extents that point at a chunk record that does not exist. Any file with
    /// one of these extents fails to read at that offset.
    pub missing_chunk_records_for_extents: u64,
    /// Chunk records whose stored reference count differs from the number of
    /// extents that point at them.
    pub chunk_refcount_mismatch_count: u64,
    /// Extent records whose owning inode does not exist.
    pub orphan_extent_records: u64,
    /// Payload copies rejected on read because their checksum did not match,
    /// since this mount started.
    pub pack_crc32_read_error_count: u64,
    /// Files that reference at least one missing chunk record.
    pub files_with_missing_pieces: Vec<AffectedFile>,
    /// Files that reference at least one chunk known to be damaged.
    pub files_with_damaged_copies: Vec<AffectedFile>,
    /// Files that reference at least one chunk whose reference count drifted.
    pub files_with_refcount_drift: Vec<AffectedFile>,
    /// True when a file list was capped at [`MAX_FILES_PER_PROBLEM`].
    pub list_truncated: bool,
}

impl RecordsCheckReport {
    /// True when nothing at all needs attention.
    pub fn is_clean(&self) -> bool {
        self.missing_chunk_records_for_extents == 0
            && self.chunk_refcount_mismatch_count == 0
            && self.orphan_extent_records == 0
            && self.files_with_damaged_copies.is_empty()
    }
}

/// One file that a consistency problem affects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffectedFile {
    /// Path relative to the folder root (no leading slash), or `None` when no
    /// directory entry reaches the inode any more.
    pub path: Option<String>,
    pub ino: u64,
    pub size_bytes: u64,
    /// Extents of this file that hit the problem.
    pub pieces: u64,
    /// Lowest file offset of an extent that hits the problem.
    pub first_offset: u64,
}

#[derive(Default)]
struct InodeDamage {
    missing_pieces: u64,
    first_missing_offset: Option<u64>,
    damaged_pieces: u64,
    first_damaged_offset: Option<u64>,
    drifted_pieces: u64,
    first_drifted_offset: Option<u64>,
}

impl FsCore {
    /// Scans metadata and reports every consistency problem with the files it
    /// affects. Expensive: reads all extents and chunk records.
    pub(crate) fn check_records(&self) -> Result<RecordsCheckReport> {
        // Chunks found damaged by a read before the sync cycle persisted the
        // `CHUNK_FLAG_CORRUPT` mark.
        let freshly_damaged: HashSet<[u8; 16]> = self.corrupt_chunks.lock().clone();

        let (
            missing_chunk_records_for_extents,
            chunk_refcount_mismatch_count,
            orphan_extent_records,
            files_with_missing_pieces,
            files_with_damaged_copies,
            files_with_refcount_drift,
            list_truncated,
        ) = self.meta.read_txn(|txn| {
            // Pass 1: count how many extents reference each chunk, and count
            // extents whose owning inode is gone.
            let extent_prefix = vec![KEY_PREFIX_EXTENT];
            let extent_end = prefix_end(&extent_prefix);
            let mut extent_refs = HashMap::<[u8; 16], u64>::new();
            let mut orphan_extent_records = 0_u64;
            let mut inode_present_cache = HashMap::<u64, bool>::new();
            for pair in scan_range_pairs(txn, extent_prefix.clone(), extent_end.clone())? {
                let (key, value) = pair?;
                let Some(owner_ino) = FsCore::extent_owner_ino_from_key(&key) else {
                    continue;
                };
                let owner_exists = match inode_present_cache.get(&owner_ino) {
                    Some(exists) => *exists,
                    None => {
                        let exists = txn.get(inode_key(owner_ino))?.is_some();
                        inode_present_cache.insert(owner_ino, exists);
                        exists
                    }
                };
                if !owner_exists {
                    orphan_extent_records = orphan_extent_records.saturating_add(1);
                }
                let extent: ExtentRecord = decode_rkyv(&value)?;
                let counter = extent_refs.entry(extent.chunk_hash).or_insert(0);
                *counter = counter.saturating_add(1);
            }

            // Pass 2: compare every chunk record with the extents that point
            // at it. Extents whose hash has no chunk record lost their data;
            // a corrupt flag means the stored copy is damaged.
            let chunk_prefix = vec![KEY_PREFIX_CHUNK];
            let chunk_end = prefix_end(&chunk_prefix);
            let mut missing_hashes = HashSet::<[u8; 16]>::new();
            let mut damaged_hashes = HashSet::<[u8; 16]>::new();
            let mut drifted_hashes = HashSet::<[u8; 16]>::new();
            let mut seen_hashes = HashSet::<[u8; 16]>::new();
            let mut chunk_refcount_mismatch_count = 0_u64;
            for pair in scan_range_pairs(txn, chunk_prefix, chunk_end)? {
                let (key, value) = pair?;
                if key.len() != 17 {
                    continue;
                }
                let mut hash = [0_u8; 16];
                hash.copy_from_slice(&key[1..17]);
                let chunk: ChunkRecord = decode_rkyv(&value)?;
                seen_hashes.insert(hash);
                if (chunk.flags & CHUNK_FLAG_CORRUPT) != 0 || freshly_damaged.contains(&hash) {
                    damaged_hashes.insert(hash);
                }
                let expected_refcount = extent_refs.get(&hash).copied().unwrap_or(0);
                if chunk.refcount != expected_refcount {
                    chunk_refcount_mismatch_count = chunk_refcount_mismatch_count.saturating_add(1);
                    drifted_hashes.insert(hash);
                }
            }
            for hash in extent_refs.keys() {
                if !seen_hashes.contains(hash) {
                    missing_hashes.insert(*hash);
                }
            }
            let missing_chunk_records_for_extents = missing_hashes.len() as u64;

            // Pass 3: group the affected extents by owning file.
            let mut damaged = HashMap::<u64, InodeDamage>::new();
            if !missing_hashes.is_empty() || !damaged_hashes.is_empty() || !drifted_hashes.is_empty()
            {
                for pair in scan_range_pairs(txn, extent_prefix, extent_end)? {
                    let (key, value) = pair?;
                    let Some(owner_ino) = FsCore::extent_owner_ino_from_key(&key) else {
                        continue;
                    };
                    let extent: ExtentRecord = decode_rkyv(&value)?;
                    let start = u64::from_be_bytes(
                        key[9..17].try_into().expect("extent key holds a u64 offset"),
                    );
                    let entry = damaged.entry(owner_ino).or_default();
                    if missing_hashes.contains(&extent.chunk_hash) {
                        entry.missing_pieces = entry.missing_pieces.saturating_add(1);
                        entry.first_missing_offset = Some(
                            entry
                                .first_missing_offset
                                .map_or(start, |offset| offset.min(start)),
                        );
                    } else if damaged_hashes.contains(&extent.chunk_hash) {
                        entry.damaged_pieces = entry.damaged_pieces.saturating_add(1);
                        entry.first_damaged_offset = Some(
                            entry
                                .first_damaged_offset
                                .map_or(start, |offset| offset.min(start)),
                        );
                    } else if drifted_hashes.contains(&extent.chunk_hash) {
                        entry.drifted_pieces = entry.drifted_pieces.saturating_add(1);
                        entry.first_drifted_offset = Some(
                            entry
                                .first_drifted_offset
                                .map_or(start, |offset| offset.min(start)),
                        );
                    }
                }
            }

            // Pass 4: walk the namespace so every damaged inode that is still
            // reachable gets its current path.
            let mut paths = HashMap::<u64, String>::new();
            if !damaged.is_empty() {
                let mut visited = HashSet::<u64>::new();
                let mut stack = vec![(ROOT_INODE, String::new())];
                while let Some((ino, prefix)) = stack.pop() {
                    if !visited.insert(ino) {
                        continue;
                    }
                    let Some(raw) = txn.get(inode_key(ino))? else {
                        continue;
                    };
                    let inode: InodeRecord = decode_rkyv(&raw)?;
                    if inode.kind != INODE_KIND_DIR {
                        continue;
                    }
                    let dir_prefix = dirent_prefix(ino);
                    let dir_end = prefix_end(&dir_prefix);
                    for pair in scan_range_pairs(txn, dir_prefix, dir_end)? {
                        let (key, value) = pair?;
                        let Some(name) = decode_dirent_name(&key) else {
                            continue;
                        };
                        let dirent: DirentRecord = decode_rkyv(&value)?;
                        let name = String::from_utf8_lossy(name);
                        let child_path = if prefix.is_empty() {
                            name.into_owned()
                        } else {
                            format!("{prefix}/{name}")
                        };
                        if damaged.contains_key(&dirent.ino) && !paths.contains_key(&dirent.ino) {
                            paths.insert(dirent.ino, child_path.clone());
                        }
                        stack.push((dirent.ino, child_path));
                    }
                }
            }

            // Attach paths and sizes, then split the report per problem.
            let mut files_with_missing_pieces = Vec::new();
            let mut files_with_damaged_copies = Vec::new();
            let mut files_with_refcount_drift = Vec::new();
            for (ino, damage) in damaged {
                let size_bytes = match txn.get(inode_key(ino))? {
                    Some(raw) => decode_rkyv::<InodeRecord>(&raw)?.size,
                    None => 0,
                };
                let path = paths.get(&ino).cloned();
                if damage.missing_pieces > 0 {
                    files_with_missing_pieces.push(AffectedFile {
                        path: path.clone(),
                        ino,
                        size_bytes,
                        pieces: damage.missing_pieces,
                        first_offset: damage.first_missing_offset.unwrap_or(0),
                    });
                }
                if damage.damaged_pieces > 0 {
                    files_with_damaged_copies.push(AffectedFile {
                        path: path.clone(),
                        ino,
                        size_bytes,
                        pieces: damage.damaged_pieces,
                        first_offset: damage.first_damaged_offset.unwrap_or(0),
                    });
                }
                if damage.drifted_pieces > 0 {
                    files_with_refcount_drift.push(AffectedFile {
                        path,
                        ino,
                        size_bytes,
                        pieces: damage.drifted_pieces,
                        first_offset: damage.first_drifted_offset.unwrap_or(0),
                    });
                }
            }

            let mut list_truncated = false;
            sort_and_cap(&mut files_with_missing_pieces, &mut list_truncated);
            sort_and_cap(&mut files_with_damaged_copies, &mut list_truncated);
            sort_and_cap(&mut files_with_refcount_drift, &mut list_truncated);

            Ok((
                missing_chunk_records_for_extents,
                chunk_refcount_mismatch_count,
                orphan_extent_records,
                files_with_missing_pieces,
                files_with_damaged_copies,
                files_with_refcount_drift,
                list_truncated,
            ))
        })?;

        Ok(RecordsCheckReport {
            missing_chunk_records_for_extents,
            chunk_refcount_mismatch_count,
            orphan_extent_records,
            pack_crc32_read_error_count: self
                .pack_crc32_read_error_count
                .load(std::sync::atomic::Ordering::Relaxed),
            files_with_missing_pieces,
            files_with_damaged_copies,
            files_with_refcount_drift,
            list_truncated,
        })
    }
}

/// Orders reachable files by path and drops the tail past the cap; the caller
/// records that the list was truncated.
fn sort_and_cap(files: &mut Vec<AffectedFile>, truncated: &mut bool) {
    files.sort_by(|a, b| match (&a.path, &b.path) {
        (Some(a_path), Some(b_path)) => a_path.cmp(b_path),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => a.ino.cmp(&b.ino),
    });
    if files.len() > MAX_FILES_PER_PROBLEM {
        *truncated = true;
        files.truncate(MAX_FILES_PER_PROBLEM);
    }
}
