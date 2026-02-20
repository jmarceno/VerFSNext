use super::*;
use async_trait::async_trait;
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
                    PERM_SYMLINK_DEFAULT as u32,
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

                let start_key = extent_key(ino, start_block);
                let end_key = end_block
                    .checked_add(1)
                    .map(|end| extent_key(ino, end))
                    .unwrap_or_else(|| prefix_end(&extent_prefix(ino)));

                let mut iter = txn.range(start_key, end_key)?;
                let mut valid = iter.seek_first()?;
                while valid {
                    let key = iter.key().user_key();
                    if let Some(block_idx) = FsCore::extent_block_idx_from_key(key) {
                        if block_idx >= start_block && block_idx <= end_block {
                            let val = iter.value()?;
                            let extent: ExtentRecord = decode_rkyv(&val)?;
                            hashes.insert(extent.chunk_hash);
                            extents.insert(block_idx, extent);
                        }
                    }
                    valid = iter.next()?;
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
                for pair in scan_range_pairs(txn, prefix, end)? {
                    let (key, value) = pair?;
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
                for pair in scan_range_pairs(txn, prefix, end)? {
                    let (key, _) = pair?;
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
