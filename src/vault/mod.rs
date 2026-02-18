use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use rand::rngs::OsRng;
use rand::RngCore;
use rkyv::{Archive, Deserialize, Serialize};
use zeroize::Zeroize;

pub const KEY_FILE_NAME: &str = "verfsnext.vault.key";
pub const SYS_VAULT_STATE: &str = "vault.state";
pub const SYS_VAULT_WRAP: &str = "vault.wrap";
pub const SYS_VAULT_POLICY: &str = "vault.policy";

pub const VAULT_STATE_LOCKED: u8 = 0;
pub const VAULT_STATE_UNLOCKED: u8 = 1;

pub const CHUNK_FLAG_ENCRYPTED: u8 = 1 << 0;

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
pub struct VaultWrapRecord {
    pub version: u16,
    pub flags: u16,
    pub argon2_mem_kib: u32,
    pub argon2_iters: u32,
    pub argon2_parallelism: u32,
    pub salt: [u8; 16],
    pub nonce: [u8; 24],
    pub wrapped_key: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct VaultArgon2Params {
    pub mem_kib: u32,
    pub iters: u32,
    pub parallelism: u32,
}

#[derive(Debug, Clone)]
pub struct VaultRuntime {
    initialized: bool,
    unlocked: bool,
    key: Option<[u8; 32]>,
}

impl VaultRuntime {
    pub fn new(initialized: bool) -> Self {
        Self {
            initialized,
            unlocked: false,
            key: None,
        }
    }

    pub fn unlocked(&self) -> bool {
        self.unlocked
    }

    pub fn set_initialized(&mut self, initialized: bool) {
        self.initialized = initialized;
    }

    pub fn lock(&mut self) {
        if let Some(mut key) = self.key.take() {
            key.zeroize();
        }
        self.unlocked = false;
    }

    pub fn unlock_with(&mut self, key: [u8; 32]) {
        self.key = Some(key);
        self.unlocked = true;
    }

    pub fn key(&self) -> Option<[u8; 32]> {
        self.key
    }
}

pub fn generate_folder_key() -> [u8; 32] {
    let mut key = [0_u8; 32];
    OsRng.fill_bytes(&mut key);
    key
}

pub fn generate_key_file_material() -> [u8; 32] {
    let mut key = [0_u8; 32];
    OsRng.fill_bytes(&mut key);
    key
}

pub fn resolve_create_key_path(path_opt: Option<&Path>) -> Result<PathBuf> {
    let key_dir = match path_opt {
        Some(path) => path.to_path_buf(),
        None => std::env::current_dir().context("failed to resolve current directory")?,
    };
    if let Ok(meta) = std::fs::metadata(&key_dir) {
        if !meta.is_dir() {
            bail!(
                "key output path must be a directory (got file: {})",
                key_dir.display()
            );
        }
    }
    std::fs::create_dir_all(&key_dir)
        .with_context(|| format!("failed to create key directory {}", key_dir.display()))?;

    Ok(key_dir.join(KEY_FILE_NAME))
}

pub fn write_key_file(path: &Path, material: &[u8; 32]) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)
        .with_context(|| format!("failed to open key file {}", path.display()))?;
    file.write_all(material)
        .with_context(|| format!("failed to write key file {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        std::fs::set_permissions(path, perms)
            .with_context(|| format!("failed to set key file permissions {}", path.display()))?;
    }
    Ok(())
}

pub fn read_key_file(path: &Path) -> Result<[u8; 32]> {
    let bytes = std::fs::read(path)
        .with_context(|| format!("failed to read key file {}", path.display()))?;
    if bytes.len() != 32 {
        bail!(
            "invalid key file length {} at {}, expected 32",
            bytes.len(),
            path.display()
        );
    }
    let mut out = [0_u8; 32];
    out.copy_from_slice(&bytes);
    Ok(out)
}

pub fn build_wrap_record(
    password: &str,
    key_material: &[u8; 32],
    folder_key: &[u8; 32],
    params: VaultArgon2Params,
) -> Result<VaultWrapRecord> {
    let mut salt = [0_u8; 16];
    OsRng.fill_bytes(&mut salt);

    let mut kek = derive_kek(password, key_material, salt, params)?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&kek));
    let mut nonce = [0_u8; 24];
    OsRng.fill_bytes(&mut nonce);
    let wrapped_key = cipher
        .encrypt(XNonce::from_slice(&nonce), folder_key.as_slice())
        .map_err(|_| anyhow!("failed to wrap vault folder key"))?;
    kek.zeroize();

    Ok(VaultWrapRecord {
        version: 1,
        flags: 0,
        argon2_mem_kib: params.mem_kib,
        argon2_iters: params.iters,
        argon2_parallelism: params.parallelism,
        salt,
        nonce,
        wrapped_key,
    })
}

pub fn unwrap_folder_key(
    password: &str,
    key_material: &[u8; 32],
    wrap: &VaultWrapRecord,
) -> Result<[u8; 32]> {
    let params = VaultArgon2Params {
        mem_kib: wrap.argon2_mem_kib,
        iters: wrap.argon2_iters,
        parallelism: wrap.argon2_parallelism,
    };
    let mut kek = derive_kek(password, key_material, wrap.salt, params)?;
    let cipher = XChaCha20Poly1305::new(Key::from_slice(&kek));
    let plain = cipher
        .decrypt(XNonce::from_slice(&wrap.nonce), wrap.wrapped_key.as_ref())
        .map_err(|_| anyhow!("invalid vault password or key file"))?;
    kek.zeroize();
    if plain.len() != 32 {
        bail!(
            "invalid unwrapped vault key length {}, expected 32",
            plain.len()
        );
    }
    let mut out = [0_u8; 32];
    out.copy_from_slice(&plain);
    Ok(out)
}

pub fn encrypt_chunk_payload(folder_key: &[u8; 32], plain: &[u8]) -> Result<(Vec<u8>, [u8; 24])> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(folder_key));
    let mut nonce = [0_u8; 24];
    OsRng.fill_bytes(&mut nonce);
    let encrypted = cipher
        .encrypt(XNonce::from_slice(&nonce), plain)
        .map_err(|_| anyhow!("failed to encrypt vault chunk"))?;
    Ok((encrypted, nonce))
}

pub fn decrypt_chunk_payload(
    folder_key: &[u8; 32],
    nonce: &[u8; 24],
    encrypted: &[u8],
) -> Result<Vec<u8>> {
    let cipher = XChaCha20Poly1305::new(Key::from_slice(folder_key));
    cipher
        .decrypt(XNonce::from_slice(nonce), encrypted)
        .map_err(|_| anyhow!("failed to decrypt vault chunk"))
}

fn derive_kek(
    password: &str,
    key_material: &[u8; 32],
    salt: [u8; 16],
    params: VaultArgon2Params,
) -> Result<[u8; 32]> {
    let argon = Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(params.mem_kib, params.iters, params.parallelism, Some(32))
            .map_err(|err| anyhow!("invalid Argon2 parameters: {err}"))?,
    );

    let mut input = Vec::with_capacity(password.len() + 1 + key_material.len());
    input.extend_from_slice(password.as_bytes());
    input.push(0x00);
    input.extend_from_slice(key_material);

    let mut out = [0_u8; 32];
    argon
        .hash_password_into(&input, &salt, &mut out)
        .map_err(|err| anyhow!("Argon2 key derivation failed: {err}"))?;
    input.zeroize();
    Ok(out)
}
