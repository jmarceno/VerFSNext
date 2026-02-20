use super::*;

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}
pub(crate) fn sflag_for_kind(kind: u8) -> SFlag {
    match kind {
        INODE_KIND_DIR => SFlag::S_IFDIR,
        INODE_KIND_FILE => SFlag::S_IFREG,
        INODE_KIND_SYMLINK => SFlag::S_IFLNK,
        _ => SFlag::S_IFREG,
    }
}
pub(crate) fn map_anyhow_to_fuse(err: anyhow::Error) -> AsyncFusexError {
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
pub(crate) fn anyhow_errno(message_errno: Errno, message: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(message_errno).context(message.into())
}
pub(crate) fn scan_range_pairs(
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
pub(crate) fn scan_range_pairs_limited(
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
pub(crate) fn dir_size_recursive(path: &Path) -> Result<u64> {
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
pub(crate) fn read_process_rss_bytes() -> Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    for line in status.lines() {
        if let Some(bytes) = parse_proc_kib_line(line, "VmRSS:") {
            return Ok(bytes?);
        }
    }
    Ok(0)
}
pub(crate) fn read_process_private_memory_bytes() -> Result<u64> {
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
pub(crate) fn parse_proc_kib_line(line: &str, key: &str) -> Option<Result<u64>> {
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
pub(crate) fn can_coalesce_write(existing: &WriteOp, incoming: &WriteOp) -> bool {
    if existing.ino != incoming.ino {
        return false;
    }
    let existing_end = existing.offset.saturating_add(existing.data.len() as u64);
    let incoming_end = incoming.offset.saturating_add(incoming.data.len() as u64);
    incoming.offset <= existing_end && incoming_end >= existing.offset
        || incoming.offset == existing_end
}
pub(crate) fn merge_write_op(existing: &mut WriteOp, incoming: WriteOp) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sflag_for_kind() {
        assert_eq!(sflag_for_kind(INODE_KIND_DIR), SFlag::S_IFDIR);
        assert_eq!(sflag_for_kind(INODE_KIND_FILE), SFlag::S_IFREG);
        assert_eq!(sflag_for_kind(INODE_KIND_SYMLINK), SFlag::S_IFLNK);

        // Fallback for unknown kind
        assert_eq!(sflag_for_kind(255), SFlag::S_IFREG);
    }
}
