use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

pub const ROOT_INODE: u64 = 1;
pub const BLOCK_SIZE: usize = 1 * 1024 * 1024; // 1 MiB

pub const INODE_KIND_FILE: u8 = 1;
pub const INODE_KIND_DIR: u8 = 2;
pub const INODE_KIND_SYMLINK: u8 = 3;

pub const KEY_PREFIX_INODE: u8 = b'I';
pub const KEY_PREFIX_DIRENT: u8 = b'D';
pub const KEY_PREFIX_EXTENT: u8 = b'E';
pub const KEY_PREFIX_CHUNK: u8 = b'C';
pub const KEY_PREFIX_XATTR: u8 = b'X';
pub const KEY_PREFIX_SYMLINK: u8 = b'Y';
pub const KEY_PREFIX_SNAPSHOT: u8 = b'S';

pub const INODE_FLAG_READONLY: u32 = 1 << 0;
pub const INODE_FLAG_VAULT: u32 = 1 << 1;
pub const INODE_FLAG_VAULT_ROOT: u32 = 1 << 2;

pub const PERM_DIRECTORY_DEFAULT: u16 = 0o755;
pub const PERM_VAULT_DIRECTORY: u16 = 0o700;
pub const PERM_SYMLINK_DEFAULT: u16 = 0o777;
pub const PERM_KEY_FILE: u32 = 0o600;

pub const SNAPSHOTS_DIR_NAME: &str = ".snapshots";
pub const VAULT_DIR_NAME: &str = ".vault";

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
pub struct InodeRecord {
    pub ino: u64,
    pub parent: u64,
    pub kind: u8,
    pub perm: u16,
    pub uid: u32,
    pub gid: u32,
    pub nlink: u32,
    pub size: u64,
    pub atime_sec: i64,
    pub atime_nsec: u32,
    pub mtime_sec: i64,
    pub mtime_nsec: u32,
    pub ctime_sec: i64,
    pub ctime_nsec: u32,
    pub generation: u64,
    pub flags: u32,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
pub struct DirentRecord {
    pub ino: u64,
    pub kind: u8,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
pub struct ExtentRecord {
    pub chunk_hash: [u8; 16],
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
pub struct ChunkRecord {
    pub refcount: u64,
    pub pack_id: u64,
    pub codec: u8,
    pub flags: u8,
    pub nonce: [u8; 24],
    pub uncompressed_len: u32,
    pub compressed_len: u32,
}

#[derive(Archive, Serialize, Deserialize, Debug, Clone)]
#[rkyv(bytecheck())]
pub struct SnapshotRecord {
    pub root_ino: u64,
    pub created_at_sec: i64,
    pub created_at_nsec: u32,
}

pub fn encode_rkyv<T>(value: &T) -> Result<Vec<u8>>
where
    T: Archive
        + for<'a> Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rkyv::rancor::Error,
            >,
        >,
{
    rkyv::to_bytes::<rkyv::rancor::Error>(value)
        .map(|bytes| bytes.to_vec())
        .context("rkyv encode failed")
}

pub fn decode_rkyv<T>(bytes: &[u8]) -> Result<T>
where
    T: Archive,
    T::Archived: for<'a> CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>
        + Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>,
{
    rkyv::from_bytes::<T, rkyv::rancor::Error>(bytes).context("rkyv decode failed")
}

pub fn inode_key(ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8);
    key.push(KEY_PREFIX_INODE);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

pub fn dirent_prefix(parent: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + 1);
    key.push(KEY_PREFIX_DIRENT);
    key.extend_from_slice(&parent.to_be_bytes());
    key.push(0);
    key
}

pub fn dirent_key(parent: u64, name: &[u8]) -> Vec<u8> {
    let mut key = dirent_prefix(parent);
    key.extend_from_slice(name);
    key
}

pub fn extent_prefix(ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8);
    key.push(KEY_PREFIX_EXTENT);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

pub fn extent_key(ino: u64, block_idx: u64) -> Vec<u8> {
    let mut key = extent_prefix(ino);
    key.extend_from_slice(&block_idx.to_be_bytes());
    key
}

pub fn chunk_key(hash: &[u8; 16]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 16);
    key.push(KEY_PREFIX_CHUNK);
    key.extend_from_slice(hash);
    key
}

pub fn xattr_prefix(ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8 + 1);
    key.push(KEY_PREFIX_XATTR);
    key.extend_from_slice(&ino.to_be_bytes());
    key.push(0);
    key
}

pub fn xattr_key(ino: u64, name: &[u8]) -> Vec<u8> {
    let mut key = xattr_prefix(ino);
    key.extend_from_slice(name);
    key
}

pub fn symlink_target_key(ino: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 8);
    key.push(KEY_PREFIX_SYMLINK);
    key.extend_from_slice(&ino.to_be_bytes());
    key
}

pub fn sys_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + name.len());
    key.extend_from_slice(b"SYS:");
    key.extend_from_slice(name.as_bytes());
    key
}

pub fn snapshot_prefix() -> Vec<u8> {
    b"S:".to_vec()
}

pub fn snapshot_key(name: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + name.len());
    key.extend_from_slice(b"S:");
    key.extend_from_slice(name);
    key
}

pub fn decode_snapshot_name(key: &[u8]) -> Option<&[u8]> {
    if key.len() < 2 || key[0] != KEY_PREFIX_SNAPSHOT || key[1] != b':' {
        return None;
    }
    Some(&key[2..])
}

pub fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    for idx in (0..end.len()).rev() {
        if end[idx] != 0xFF {
            end[idx] = end[idx].saturating_add(1);
            end.truncate(idx + 1);
            return end;
        }
    }
    end.push(0);
    end
}

pub fn decode_dirent_name(key: &[u8]) -> Option<&[u8]> {
    if key.len() < 10 || key[0] != KEY_PREFIX_DIRENT {
        return None;
    }
    if key[9] != 0 {
        return None;
    }
    Some(&key[10..])
}

pub fn decode_xattr_name(key: &[u8]) -> Option<&[u8]> {
    if key.len() < 10 || key[0] != KEY_PREFIX_XATTR {
        return None;
    }
    if key[9] != 0 {
        return None;
    }
    Some(&key[10..])
}

pub fn system_time_to_parts(ts: SystemTime) -> (i64, u32) {
    match ts.duration_since(UNIX_EPOCH) {
        Ok(duration) => (
            i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
            duration.subsec_nanos(),
        ),
        Err(err) => {
            let duration = err.duration();
            (
                -i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
                duration.subsec_nanos(),
            )
        }
    }
}

pub fn parts_to_system_time(sec: i64, nsec: u32) -> SystemTime {
    if sec >= 0 {
        UNIX_EPOCH + Duration::new(sec as u64, nsec)
    } else {
        UNIX_EPOCH - Duration::new(sec.unsigned_abs(), nsec)
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_snapshot_name, snapshot_key, sys_key};

    #[test]
    fn snapshot_decode_rejects_sys_keys() {
        assert!(decode_snapshot_name(&sys_key("next_inode")).is_none());
    }

    #[test]
    fn snapshot_encode_decode_roundtrip() {
        let key = snapshot_key(b"snap01");
        assert_eq!(decode_snapshot_name(&key), Some(&b"snap01"[..]));
    }
}
