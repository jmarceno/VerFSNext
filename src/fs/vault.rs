use super::*;
use crate::fs::FsCore;

const VAULT_HASH_DOMAIN: u8 = 0xA7;

impl FsCore {
    pub(crate) fn vault_locked(&self) -> bool {
        !self.vault.read().unlocked()
    }
    pub(crate) fn inode_is_vault(inode: &InodeRecord) -> bool {
        (inode.flags & INODE_FLAG_VAULT) != 0
    }
    pub(crate) fn hash_for_inode(inode: &InodeRecord, data: &[u8]) -> [u8; 16] {
        if FsCore::inode_is_vault(inode) {
            hash128_with_domain(VAULT_HASH_DOMAIN, data)
        } else {
            hash128(data)
        }
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
        if self.vault_locked() && FsCore::inode_is_vault(inode) {
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
    pub(crate) async fn create_vault(
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
                    perm: PERM_VAULT_DIRECTORY,
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
    pub(crate) async fn unlock_vault(&self, password: &str, key_file: &Path) -> Result<()> {
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
    pub(crate) async fn lock_vault(&self) -> Result<()> {
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
}
