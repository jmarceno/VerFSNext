use super::*;
use crate::fs::FsCore;

impl FsCore {
    pub(crate) fn file_attr_from_inode(&self, inode: &InodeRecord) -> FileAttr {
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
    pub(crate) fn load_inode_or_errno(
        &self,
        ino: u64,
        context: &'static str,
    ) -> Result<InodeRecord> {
        self.meta.get_inode(ino)?.ok_or_else(|| {
            anyhow_errno(
                Errno::ENOENT,
                format!("{}: inode {} not found", context, ino),
            )
        })
    }
    pub(crate) fn load_inode_with_vault_access(
        &self,
        ino: u64,
        context: &'static str,
    ) -> Result<InodeRecord> {
        let inode = self.load_inode_or_errno(ino, context)?;
        self.ensure_inode_vault_access(&inode, context)?;
        Ok(inode)
    }
    pub(crate) fn lookup_dirent(&self, parent: u64, name: &str) -> Result<Option<DirentRecord>> {
        self.meta.read_txn(|txn| {
            let Some(raw) = txn.get(dirent_key(parent, name.as_bytes()))? else {
                return Ok(None);
            };
            let dirent: DirentRecord = decode_rkyv(&raw)?;
            Ok(Some(dirent))
        })
    }
    pub(crate) fn ensure_dirent_target(
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
    pub(crate) fn ensure_parent_dir_writable_in_txn(
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
    pub(crate) fn create_inode_in_txn(
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
    pub(crate) fn ensure_inode_writable(inode: &InodeRecord, context: &'static str) -> Result<()> {
        if (inode.flags & INODE_FLAG_READONLY) != 0 {
            return Err(anyhow_errno(
                Errno::EROFS,
                format!("{context}: read-only inode {}", inode.ino),
            ));
        }
        Ok(())
    }
    pub(crate) fn remove_inode_payload_in_txn(
        txn: &mut surrealkv::Transaction,
        inode: &InodeRecord,
    ) -> Result<()> {
        let extent_prefix_bytes = extent_prefix(inode.ino);
        let extent_end = prefix_end(&extent_prefix_bytes);
        let mut extent_keys = Vec::new();
        let mut hashes = Vec::new();
        for pair in scan_range_pairs(txn, extent_prefix_bytes, extent_end)? {
            let (key, value) = pair?;
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
        let pairs =
            scan_range_pairs(txn, xattr_prefix_bytes, xattr_end)?.collect::<Result<Vec<_>>>()?;
        for (key, _) in pairs {
            txn.delete(key)?;
        }

        if inode.kind == INODE_KIND_SYMLINK {
            txn.delete(symlink_target_key(inode.ino))?;
        }
        Ok(())
    }
    pub(crate) fn remove_name_from_inode_in_txn(
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
    pub(crate) fn is_dir_empty(txn: &surrealkv::Transaction, ino: u64) -> Result<bool> {
        let prefix = dirent_prefix(ino);
        let end = prefix_end(&prefix);
        let mut iter = txn.range(prefix, end)?;
        let not_empty = iter.seek_first()?;
        Ok(!not_empty)
    }
    pub(crate) fn touch_parent_inode_in_txn(
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
    pub(crate) fn check_access_for_mask(
        &self,
        inode: &InodeRecord,
        uid: u32,
        gid: u32,
        mask: u32,
    ) -> Result<()> {
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
            let groups = self.uid_groups_cache.get_with(uid, || {
                let mut fallback = vec![gid];
                if let Ok(Some(user)) = nix::unistd::User::from_uid(nix::unistd::Uid::from_raw(uid))
                {
                    if let Ok(cname) = std::ffi::CString::new(user.name) {
                        if let Ok(sups) = nix::unistd::getgrouplist(
                            cname.as_c_str(),
                            nix::unistd::Gid::from_raw(gid),
                        ) {
                            fallback.extend(sups.into_iter().map(|g| g.as_raw()));
                        }
                    }
                }
                std::sync::Arc::new(fallback)
            });
            if groups.contains(&inode.gid) {
                (mode_bits >> 3) & 0o7
            } else {
                mode_bits & 0o7
            }
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
    pub(crate) fn xattr_name_nonempty(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(anyhow_errno(Errno::EINVAL, "xattr name must not be empty"));
        }
        Ok(())
    }
    pub(crate) fn inode_kind_to_file_type(kind: u8) -> FileType {
        match kind {
            INODE_KIND_DIR => FileType::Dir,
            INODE_KIND_FILE => FileType::File,
            INODE_KIND_SYMLINK => FileType::Symlink,
            _ => FileType::File,
        }
    }
}
