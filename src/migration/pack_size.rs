use std::fs;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use tokio::net::UnixStream;

use crate::config::Config;
use crate::data::pack::PackStore;
use crate::fs::utils::scan_range_pairs;
use crate::meta::MetaStore;
use crate::migration::pack_index_crc32::ensure_pack_index_crc32_compat;
use crate::permissions::{ensure_dir, set_file_mode};
use crate::types::{
    chunk_key, decode_rkyv, encode_rkyv, prefix_end, sys_key, ChunkRecord, KEY_PREFIX_CHUNK,
};

pub const SYS_PACK_MAX_SIZE_MB: &str = "pack_max_size_mb";

#[derive(Clone)]
struct ChunkRewritePlan {
    hash: [u8; 16],
    chunk: ChunkRecord,
    new_pack_id: u64,
}

pub fn read_stored_pack_size_mb(meta: &MetaStore) -> Result<Option<u64>> {
    let Some(raw) = meta.get_sys(SYS_PACK_MAX_SIZE_MB)? else {
        return Ok(None);
    };
    if raw.len() != 8 {
        bail!(
            "invalid SYS:{} length {}, expected 8",
            SYS_PACK_MAX_SIZE_MB,
            raw.len()
        );
    }
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&raw);
    Ok(Some(u64::from_le_bytes(bytes)))
}

pub async fn ensure_pack_size_metadata_compat(meta: &MetaStore, configured_mb: u64) -> Result<()> {
    let Some(stored_mb) = read_stored_pack_size_mb(meta)? else {
        meta.write_txn(|txn| {
            txn.set(
                sys_key(SYS_PACK_MAX_SIZE_MB),
                configured_mb.to_le_bytes().to_vec(),
            )?;
            Ok(())
        })
        .await?;
        return Ok(());
    };

    if stored_mb != configured_mb {
        bail!(
            "configured pack_max_size_mb ({configured_mb}) differs from metadata SYS:{sys_key_name} ({stored_mb}); run `verfsnext pack-size-migrate` with the daemon stopped",
            sys_key_name = SYS_PACK_MAX_SIZE_MB
        );
    }

    Ok(())
}

pub async fn run_pack_size_migration(config: &Config) -> Result<()> {
    ensure_daemon_offline(config).await?;

    let meta = MetaStore::open(&config.metadata_dir()).await?;
    let action_result = run_pack_size_migration_inner(config, &meta).await;
    let close_result = meta.close().await;
    action_result?;
    close_result?;
    Ok(())
}

async fn run_pack_size_migration_inner(config: &Config, meta: &MetaStore) -> Result<()> {
    ensure_pack_index_crc32_compat(config, meta).await?;
    let target_pack_size_mb = config.pack_max_size_mb;
    let Some(current_pack_size_mb) = read_stored_pack_size_mb(meta)? else {
        meta.write_txn(|txn| {
            txn.set(
                sys_key(SYS_PACK_MAX_SIZE_MB),
                target_pack_size_mb.to_le_bytes().to_vec(),
            )?;
            Ok(())
        })
        .await?;
        println!(
            "SYS:{} was missing; set to {} MiB. No pack rewrite was needed.",
            SYS_PACK_MAX_SIZE_MB, target_pack_size_mb
        );
        return Ok(());
    };

    if current_pack_size_mb == target_pack_size_mb {
        println!(
            "Pack size already matches metadata ({} MiB). Nothing to do.",
            target_pack_size_mb
        );
        return Ok(());
    }

    let confirmed =
        describe_and_confirm(current_pack_size_mb, target_pack_size_mb, &config.data_dir)?;
    if !confirmed {
        println!("Migration cancelled. No changes were applied.");
        return Ok(());
    }

    let old_max_pack_id = max_existing_pack_id(&config.packs_dir())?;
    let next_pack_id = old_max_pack_id
        .checked_add(1)
        .context("pack id overflow while preparing migration")?;
    let max_pack_size_bytes = target_pack_size_mb.saturating_mul(1024 * 1024);

    println!("Scanning chunk metadata...");
    let mut plans = collect_chunk_rewrite_plans(meta)?;
    plans.sort_by_key(|plan| (plan.chunk.pack_id, plan.hash));
    println!("Found {} chunk records to rewrite.", plans.len());

    println!("Rewriting chunks into new packs...");
    let packs = PackStore::open(
        &config.packs_dir(),
        next_pack_id,
        config.pack_index_cache_capacity_entries,
        max_pack_size_bytes,
    )?;
    for (idx, plan) in plans.iter_mut().enumerate() {
        let payload = packs.read_chunk_payload(
            plan.chunk.pack_id,
            plan.hash,
            plan.chunk.codec,
            plan.chunk.uncompressed_len,
            plan.chunk.compressed_len,
        )?;
        let new_pack_id = packs.append_chunk(
            plan.hash,
            plan.chunk.codec,
            plan.chunk.uncompressed_len,
            &payload,
        )?;
        plan.new_pack_id = new_pack_id;
        if (idx + 1) % 10_000 == 0 {
            println!("  rewritten {} chunks...", idx + 1);
        }
    }
    packs.sync(true)?;
    let new_active_pack_id = packs.active_pack_id();
    drop(packs);

    println!("Updating metadata to point chunks to rewritten packs...");
    meta.write_txn(|txn| {
        for plan in plans.iter() {
            let mut updated = plan.chunk.clone();
            updated.pack_id = plan.new_pack_id;
            txn.set(chunk_key(&plan.hash), encode_rkyv(&updated)?)?;
        }
        txn.set(
            sys_key("active_pack_id"),
            new_active_pack_id.to_le_bytes().to_vec(),
        )?;
        txn.set(
            sys_key(SYS_PACK_MAX_SIZE_MB),
            target_pack_size_mb.to_le_bytes().to_vec(),
        )?;
        txn.set(
            sys_key("gc.discard_checkpoint"),
            0_u64.to_le_bytes().to_vec(),
        )?;
        txn.set(sys_key("gc.phase"), 0_u64.to_le_bytes().to_vec())?;
        txn.delete(sys_key("gc.scan_cursor"))?;
        Ok(())
    })
    .await?;

    let discard_path = config.packs_dir().join(&config.gc_discard_filename);
    fs::File::create(&discard_path).with_context(|| {
        format!(
            "failed to reset discard file after pack-size migration {}",
            discard_path.display()
        )
    })?;
    set_file_mode(&discard_path)?;

    let backup_dir =
        move_old_packs_to_backup(&config.packs_dir(), &config.data_dir, old_max_pack_id)?;
    println!(
        "Pack-size migration completed.\nOld packs were moved to: {}\nDelete that directory manually after you validate the filesystem.",
        backup_dir.display()
    );
    Ok(())
}

fn collect_chunk_rewrite_plans(meta: &MetaStore) -> Result<Vec<ChunkRewritePlan>> {
    meta.read_txn(|txn| {
        let prefix = vec![KEY_PREFIX_CHUNK];
        let end = prefix_end(&prefix);
        let mut out = Vec::new();
        for pair in scan_range_pairs(txn, prefix, end)? {
            let (key, value) = pair?;
            if key.len() != 17 {
                continue;
            }
            let mut hash = [0_u8; 16];
            hash.copy_from_slice(&key[1..17]);
            let chunk: ChunkRecord = decode_rkyv(&value)?;
            out.push(ChunkRewritePlan {
                hash,
                chunk,
                new_pack_id: 0,
            });
        }
        Ok(out)
    })
}

fn describe_and_confirm(current_mb: u64, target_mb: u64, data_dir: &Path) -> Result<bool> {
    println!("Pack-size migration requested.");
    println!("Current metadata pack size : {} MiB", current_mb);
    println!("Target config pack size    : {} MiB", target_mb);
    println!("This operation will:");
    println!("  1. Read every chunk payload from existing packs.");
    println!("  2. Rewrite all chunks into new packs using the target size.");
    println!("  3. Update metadata chunk records to the new pack IDs.");
    println!(
        "  4. Move previous pack files to a backup directory under {}.",
        data_dir.display()
    );
    println!("Risks and requirements:");
    println!("  - The daemon must stay stopped for the whole operation.");
    println!("  - It can take a long time on large datasets.");
    println!("  - It needs temporary extra disk space (roughly data size).");
    println!("  - If interrupted, old data stays on disk, and the command can be rerun.");
    print!("Continue? [y/N]: ");
    std::io::stdout()
        .flush()
        .context("failed to flush confirmation prompt")?;
    let mut answer = String::new();
    std::io::stdin()
        .read_line(&mut answer)
        .context("failed to read migration confirmation")?;
    let proceed = matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes");
    Ok(proceed)
}

async fn ensure_daemon_offline(config: &Config) -> Result<()> {
    let socket_path = config.control_socket_path();
    match UnixStream::connect(&socket_path).await {
        Ok(_) => bail!("pack-size-migrate must run offline (daemon appears to be running)"),
        Err(err) if socket_unavailable(&err) => Ok(()),
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to verify daemon state via {}",
                socket_path.display()
            )
        }),
    }
}

fn socket_unavailable(err: &std::io::Error) -> bool {
    err.kind() == ErrorKind::NotFound
        || err.kind() == ErrorKind::ConnectionRefused
        || err.kind() == ErrorKind::ConnectionReset
}

fn max_existing_pack_id(packs_dir: &Path) -> Result<u64> {
    let mut max_id = 0_u64;
    for subdir in ["A", "B", "C", "D", "E", "F"] {
        let subdir_path = packs_dir.join(subdir);
        let entries = match fs::read_dir(&subdir_path) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!(
                        "failed to read packs subdirectory {}",
                        subdir_path.display()
                    )
                });
            }
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(pack_id) = parse_pack_id_from_filename(name) else {
                continue;
            };
            max_id = max_id.max(pack_id);
        }
    }
    Ok(max_id)
}

fn move_old_packs_to_backup(
    packs_dir: &Path,
    data_dir: &Path,
    max_old_pack_id: u64,
) -> Result<PathBuf> {
    let backup_dir = next_backup_dir(data_dir);
    ensure_dir(&backup_dir)?;
    for subdir in ["A", "B", "C", "D", "E", "F"] {
        ensure_dir(&backup_dir.join(subdir))
            .with_context(|| format!("failed to create backup packs subdirectory {}", subdir))?;
    }

    for subdir in ["A", "B", "C", "D", "E", "F"] {
        let src_subdir = packs_dir.join(subdir);
        let entries = match fs::read_dir(&src_subdir) {
            Ok(entries) => entries,
            Err(err) if err.kind() == ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read packs subdirectory {}", src_subdir.display())
                });
            }
        };
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(pack_id) = parse_pack_id_from_filename(name) else {
                continue;
            };
            if pack_id > max_old_pack_id {
                continue;
            }
            let dst_path = backup_dir.join(subdir).join(name);
            fs::rename(&path, &dst_path).with_context(|| {
                format!(
                    "failed moving old pack file {} to {}",
                    path.display(),
                    dst_path.display()
                )
            })?;
        }
    }

    Ok(backup_dir)
}

fn parse_pack_id_from_filename(name: &str) -> Option<u64> {
    if !name.starts_with("pack-") {
        return None;
    }
    let number = if let Some(n) = name
        .strip_prefix("pack-")
        .and_then(|n| n.strip_suffix(".vpk"))
    {
        n
    } else if let Some(n) = name
        .strip_prefix("pack-")
        .and_then(|n| n.strip_suffix(".idx"))
    {
        n
    } else {
        return None;
    };
    number.parse::<u64>().ok()
}

fn next_backup_dir(data_dir: &Path) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let base = format!("packs.backup.pack-size-migration.{stamp}");
    let mut candidate = data_dir.join(&base);
    let mut suffix = 1_u32;
    while candidate.exists() {
        candidate = data_dir.join(format!("{base}.{suffix}"));
        suffix = suffix.saturating_add(1);
    }
    candidate
}

#[cfg(test)]
mod tests {
    use super::parse_pack_id_from_filename;

    #[test]
    fn parse_pack_id_supports_vpk_and_idx() {
        assert_eq!(
            parse_pack_id_from_filename("pack-00000000000000000123.vpk"),
            Some(123)
        );
        assert_eq!(
            parse_pack_id_from_filename("pack-00000000000000000123.idx"),
            Some(123)
        );
    }

    #[test]
    fn parse_pack_id_rejects_other_names() {
        assert_eq!(parse_pack_id_from_filename(".DISCARD"), None);
        assert_eq!(parse_pack_id_from_filename("pack-abc.vpk"), None);
        assert_eq!(parse_pack_id_from_filename("random-file"), None);
    }
}
