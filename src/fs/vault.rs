use super::*;
use crate::fs::FsCore;
use zeroize::Zeroize;

/// True when the `/.vault` namespace exists: a root directory entry backed by
/// an inode. An entry whose inode is gone (a leftover of an interrupted
/// removal) does not count, and the next create repairs it.
pub(crate) fn vault_namespace_present(meta: &MetaStore) -> Result<bool> {
    meta.read_txn(|txn| {
        let Some(raw) = txn.get(dirent_key(ROOT_INODE, VAULT_DIR_NAME.as_bytes()))? else {
            return Ok(false);
        };
        let dirent: DirentRecord = decode_rkyv(&raw)?;
        Ok(txn.get(inode_key(dirent.ino))?.is_some())
    })
}

impl FsCore {
    pub(crate) fn vault_locked(&self) -> bool {
        !self.vault.read().unlocked()
    }
    /// Refuses to detach the vault namespace. `/.vault` is created by the
    /// vault code and, once its inode is gone, only the vault's original
    /// password and key file can bring it back (`create_vault`), so a removal
    /// has to fail loudly instead of silently bricking every vault command.
    /// The inode flag covers the vault root inode, the name covers a root
    /// entry whose inode was damaged. (`/.snapshots` needs no check here: its
    /// inode is read-only, which every removal already refuses.)
    pub(crate) fn ensure_entry_removable(
        parent: u64,
        name: &str,
        inode: &InodeRecord,
        context: &'static str,
    ) -> Result<()> {
        if (inode.flags & INODE_FLAG_VAULT_ROOT) != 0 {
            return Err(anyhow_errno(
                Errno::EPERM,
                format!("{context}: the vault namespace is removed only with the vault"),
            ));
        }
        if parent == ROOT_INODE && name == VAULT_DIR_NAME {
            return Err(anyhow_errno(
                Errno::EPERM,
                format!("{context}: {name} is a filesystem-managed namespace and cannot be removed"),
            ));
        }
        Ok(())
    }
    pub(crate) fn inode_is_vault(inode: &InodeRecord) -> bool {
        (inode.flags & INODE_FLAG_VAULT) != 0
    }
    pub(crate) fn hash_for_inode(inode: &InodeRecord, data: &[u8]) -> [u8; 16] {
        FsCore::chunk_content_hash(FsCore::inode_is_vault(inode), data)
    }
    /// Content hash of a chunk's plaintext; vault chunks hash in their own
    /// domain so they never deduplicate against plain chunks.
    pub(crate) fn chunk_content_hash(vault: bool, data: &[u8]) -> [u8; 16] {
        if vault {
            hash128_with_domain(VAULT_DOMAIN_BYTE, data)
        } else {
            hash128(data)
        }
    }
    /// True when a chunk record belongs to the domain its key names. The key
    /// space separates the domains ([`ChunkId`]); a disagreement means the
    /// record was damaged or written by a buggy version, and its copy must not
    /// be deduplicated against or served.
    pub(crate) fn chunk_record_matches_domain(id: &ChunkId, record: &ChunkRecord) -> bool {
        ((record.flags & CHUNK_FLAG_ENCRYPTED) != 0) == id.vault
    }
    pub(crate) fn check_vault_parent_name_gate(&self, parent: u64, name: &str) -> Result<()> {
        if self.vault_locked() && parent == ROOT_INODE && name == VAULT_DIR_NAME {
            return Err(anyhow_errno(Errno::ENOENT, "vault is locked"));
        }
        Ok(())
    }
    pub(crate) fn ensure_inode_vault_access(
        &self,
        inode: &InodeRecord,
        context: &'static str,
    ) -> Result<()> {
        // Check inode flags first (no &self needed) to avoid vault RwLock
        // for non-vault files, which is the common case.
        if FsCore::inode_is_vault(inode) && self.vault_locked() {
            return Err(anyhow_errno(
                Errno::ENOENT,
                format!("{context}: vault is locked"),
            ));
        }
        Ok(())
    }
    pub(crate) fn current_vault_key(&self) -> Result<[u8; 32]> {
        let vault = self.vault.read();
        if !vault.unlocked() {
            return Err(anyhow_errno(Errno::EACCES, "vault is locked"));
        }
        vault
            .key()
            .ok_or_else(|| anyhow_errno(Errno::EIO, "vault key is unavailable"))
    }
    /// Initializes the vault. `key_material` is the content of the caller's
    /// key file; the caller writes that file itself, so it is created with the
    /// caller's identity even when the daemon runs as another user.
    ///
    /// A vault record whose `/.vault` namespace is missing (removed by a
    /// pre-B041 version) is rebuilt from that record after the caller's
    /// password and key file are verified against it, so the vault keeps
    /// working with its original credentials.
    pub(crate) async fn create_vault(
        &self,
        password: &str,
        key_material: &[u8; 32],
        owner: VaultOwner,
    ) -> Result<()> {
        if !self.config.vault_enabled {
            return Err(anyhow_errno(
                Errno::EOPNOTSUPP,
                "vault is disabled by configuration",
            ));
        }
        let _guard = self.write_lock.write().await;

        let existing_wrap_raw = self.meta.get_sys(SYS_VAULT_WRAP)?;
        if vault_namespace_present(&self.meta)? {
            return Err(anyhow_errno(
                Errno::EEXIST,
                match existing_wrap_raw {
                    Some(_) => "vault is already initialized",
                    None => "vault namespace /.vault exists without a vault record",
                },
            ));
        }

        // A vault record without its namespace is a vault whose `/.vault`
        // directory was removed (a version that let `rmdir /.vault` succeed).
        // The wrap record still holds the folder key, so the namespace can be
        // rebuilt and every vault chunk stays readable — but only for a caller
        // that can unwrap it: the rebuilt namespace keeps the original
        // credentials, and a new password or key file would only take over a
        // namespace it can never open.
        let (wrap, new_wrap) = match existing_wrap_raw {
            Some(raw) => {
                let wrap: VaultWrapRecord = decode_rkyv(&raw)?;
                let mut folder_key =
                    unwrap_folder_key(password, key_material, &wrap).map_err(|err| {
                        anyhow_errno(
                            Errno::EPERM,
                            format!(
                                "vault is already initialized but its /.vault namespace is \
                                 missing, and the supplied password and key file do not match \
                                 the existing vault record: {err}"
                            ),
                        )
                    })?;
                folder_key.zeroize();
                (wrap, false)
            }
            None => (
                build_wrap_record(
                    password,
                    key_material,
                    &generate_folder_key(),
                    VaultArgon2Params {
                        mem_kib: self.config.vault_argon2_mem_kib,
                        iters: self.config.vault_argon2_iters,
                        parallelism: self.config.vault_argon2_parallelism,
                    },
                )?,
                true,
            ),
        };
        let now = SystemTime::now();
        let (sec, nsec) = system_time_to_parts(now);
        let mut created_vault_ino: Option<u64> = None;

        self.meta
            .write_txn(|txn| {
                if let Some(raw) = txn.get(dirent_key(ROOT_INODE, VAULT_DIR_NAME.as_bytes()))? {
                    let dirent: DirentRecord = decode_rkyv(&raw)?;
                    if txn.get(inode_key(dirent.ino))?.is_some() {
                        return Err(anyhow_errno(
                            Errno::EEXIST,
                            "vault namespace already exists",
                        ));
                    }
                    // Stale entry left by an interrupted create or removal.
                    txn.delete(dirent_key(ROOT_INODE, VAULT_DIR_NAME.as_bytes()))?;
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
                created_vault_ino = Some(vault_ino);

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
                    perm: PERM_VAULT_DIRECTORY,
                    uid: owner.uid,
                    gid: owner.gid,
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
                if new_wrap {
                    txn.set(sys_key(SYS_VAULT_WRAP), encode_rkyv(&wrap)?)?;
                }
                txn.set(sys_key(SYS_VAULT_STATE), vec![VAULT_STATE_LOCKED])?;
                Ok(())
            })
            .await?;

        {
            let mut vault = self.vault.write();
            vault.set_initialized(true);
            vault.lock();
        }
        if !new_wrap {
            const REBUILT: &str = "rebuilt the missing /.vault namespace from the existing vault \
                 record; the vault keeps its original password and key file";
            warn!(REBUILT);
            eprintln!("VerFSNext: {REBUILT}");
        }
        self.chunk_meta_cache.invalidate_all();
        self.chunk_data_cache.invalidate_all();
        self.mark_namespace_mutation().await;
        self.invalidate_entry_best_effort(ROOT_INODE, VAULT_DIR_NAME);
        self.invalidate_inode_attr_best_effort(ROOT_INODE);
        if let Some(vault_ino) = created_vault_ino {
            self.invalidate_inode_attr_best_effort(vault_ino);
        }
        Ok(())
    }
    pub(crate) async fn unlock_vault(&self, password: &str, key_material: &[u8; 32]) -> Result<()> {
        if !self.config.vault_enabled {
            return Err(anyhow_errno(
                Errno::EOPNOTSUPP,
                "vault is disabled by configuration",
            ));
        }
        let _guard = self.write_lock.write().await;

        let wrap_raw = self
            .meta
            .get_sys(SYS_VAULT_WRAP)?
            .ok_or_else(|| anyhow_errno(Errno::ENOENT, "vault is not initialized"))?;
        let wrap: VaultWrapRecord = decode_rkyv(&wrap_raw)?;
        let folder_key = unwrap_folder_key(password, key_material, &wrap)?;
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
        self.mark_mutation();
        self.invalidate_entry_best_effort(ROOT_INODE, VAULT_DIR_NAME);
        if let Ok(Some(vault_dirent)) = self.load_dirent_cached(ROOT_INODE, VAULT_DIR_NAME.as_bytes()) {
            self.invalidate_inode_attr_best_effort(vault_dirent.ino);
        }
        Ok(())
    }
    pub(crate) async fn lock_vault(&self) -> Result<()> {
        let _guard = self.write_lock.write().await;
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
        self.forget_vault_plaintext();
        self.mark_mutation();
        self.invalidate_entry_best_effort(ROOT_INODE, VAULT_DIR_NAME);
        if let Ok(Some(vault_dirent)) = self.load_dirent_cached(ROOT_INODE, VAULT_DIR_NAME.as_bytes()) {
            self.invalidate_inode_attr_best_effort(vault_dirent.ino);
        }
        Ok(())
    }

    /// Empties the chunk cache and starts a new vault cache epoch, both under
    /// the write side of the epoch gate: plaintext decoded before this call
    /// can no longer be cached by a read still in flight.
    fn forget_vault_plaintext(&self) {
        let mut epoch = self.vault_cache_epoch.write();
        *epoch += 1;
        self.chunk_data_cache.invalidate_all();
        self.chunk_data_cache.run_pending_tasks();
    }

    /// The current vault cache epoch. Read it before decoding a chunk and
    /// pass it to [`FsCore::cache_chunk_payload`].
    pub(crate) fn vault_cache_epoch(&self) -> u64 {
        *self.vault_cache_epoch.read()
    }

    /// Caches the plaintext of chunk `id`. A vault chunk is cached only when
    /// the vault has not locked since `epoch` was read; its plaintext is then
    /// still returned to the reader that decoded it, but not kept.
    pub(crate) fn cache_chunk_payload(&self, id: ChunkId, payload: Arc<Vec<u8>>, epoch: u64) {
        if !id.vault {
            self.chunk_data_cache.insert(id, payload);
            return;
        }
        let current = self.vault_cache_epoch.read();
        if *current == epoch {
            self.chunk_data_cache.insert(id, payload);
        }
    }

    /// Writes back everything the kernel holds dirty for the mount, through
    /// this filesystem, so writes made before a vault lock are stored while
    /// the vault can still encrypt them. Runs without the mutation gate,
    /// which the write-back's own writes need.
    pub(crate) async fn write_back_mount(&self) -> Result<()> {
        let mount_point = self.config.mount_point.clone();
        tokio::task::spawn_blocking(move || {
            let root = std::fs::File::open(&mount_point).with_context(|| {
                format!(
                    "failed to open {} to write back before the vault lock",
                    mount_point.display()
                )
            })?;
            nix::unistd::syncfs(std::os::fd::AsRawFd::as_raw_fd(&root)).with_context(|| {
                format!("syncfs of {} before the vault lock failed", mount_point.display())
            })
        })
        .await
        .context("vault lock write-back task failed")?
    }

    /// Drops the kernel's cached pages and attributes of every vault file the
    /// kernel holds (looked up or open), so plaintext it cached while the
    /// vault was unlocked is neither kept in memory nor readable through a
    /// descriptor opened before the lock: the next read of such a page comes
    /// back to this filesystem, which refuses it. Blocks on the FUSE device
    /// (dropping a page that was dirtied after the write-back writes it back
    /// first), so it runs off the async workers and after the lock released
    /// the mutation gate.
    pub(crate) fn drop_vault_pages_from_kernel(&self) -> Result<()> {
        let Some(notifier) = self.notifier.read().clone() else {
            return Err(anyhow_errno(
                Errno::EIO,
                "the vault is locked, but the mount has no kernel notifier: pages of vault files \
                 the kernel cached stay in memory until it evicts them",
            ));
        };
        let mut inos = self.kernel_lookups.lock().keys().copied().collect::<Vec<_>>();
        inos.extend(self.file_handles.lock().values().map(|state| state.ino));
        inos.sort_unstable();
        inos.dedup();
        for ino in inos {
            let Some(inode) = self.load_inode_cached(ino)? else {
                continue;
            };
            if !FsCore::inode_is_vault(&inode) || inode.kind != INODE_KIND_FILE {
                continue;
            }
            match notifier.invalidate_inode(ino, 0, 0) {
                Ok(_) | Err(Errno::ENOENT) => {}
                Err(err) => {
                    return Err(anyhow_errno(
                        err,
                        format!(
                            "the vault is locked, but the kernel could not drop the cached pages of \
                             vault inode {ino}: {err}"
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}
