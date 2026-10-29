use std::collections::HashSet;

use anyhow::Result;
use nix::errno::Errno;

use super::*;

pub(crate) const ACL_XATTR_ACCESS: &str = "system.posix_acl_access";
pub(crate) const ACL_XATTR_DEFAULT: &str = "system.posix_acl_default";

const ACL_VERSION: u32 = 2;
const ACL_UNDEFINED_ID: u32 = u32::MAX;

const ACL_USER_OBJ: u16 = 0x01;
const ACL_USER: u16 = 0x02;
const ACL_GROUP_OBJ: u16 = 0x04;
const ACL_GROUP: u16 = 0x08;
const ACL_MASK: u16 = 0x10;
const ACL_OTHER: u16 = 0x20;

#[derive(Clone, Copy, Debug)]
struct AclEntry {
    tag: u16,
    perm: u16,
    id: u32,
}

#[derive(Clone, Debug)]
pub(crate) struct PosixAcl {
    entries: Vec<AclEntry>,
}

pub(crate) struct ChildAcls {
    pub(crate) mode: u16,
    pub(crate) access: Option<Vec<u8>>,
    pub(crate) default: Option<Vec<u8>>,
}

fn invalid_acl(message: impl Into<String>) -> anyhow::Error {
    anyhow_errno(Errno::EINVAL, message.into())
}

fn entry_perm(entry: &AclEntry) -> u16 {
    entry.perm & 0o7
}

impl PosixAcl {
    pub(crate) fn parse(value: &[u8], xattr_name: &str) -> Result<Self> {
        if value.len() < 4 || !(value.len() - 4).is_multiple_of(8) {
            return Err(invalid_acl(format!(
                "{xattr_name}: ACL value has invalid length {}",
                value.len()
            )));
        }

        let mut version_bytes = [0_u8; 4];
        version_bytes.copy_from_slice(&value[..4]);
        let version = u32::from_le_bytes(version_bytes);
        if version != ACL_VERSION {
            return Err(invalid_acl(format!(
                "{xattr_name}: unsupported POSIX ACL version {version}"
            )));
        }

        let mut entries = Vec::with_capacity((value.len() - 4) / 8);
        let mut user_obj = 0_u8;
        let mut group_obj = 0_u8;
        let mut other = 0_u8;
        let mut masks = 0_u8;
        let mut named_entries = 0_u8;
        let mut named_ids = HashSet::<(u16, u32)>::new();

        for raw in value[4..].chunks(8) {
            let mut tag_bytes = [0_u8; 2];
            tag_bytes.copy_from_slice(&raw[..2]);
            let mut perm_bytes = [0_u8; 2];
            perm_bytes.copy_from_slice(&raw[2..4]);
            let mut id_bytes = [0_u8; 4];
            id_bytes.copy_from_slice(&raw[4..8]);
            let entry = AclEntry {
                tag: u16::from_le_bytes(tag_bytes),
                perm: u16::from_le_bytes(perm_bytes),
                id: u32::from_le_bytes(id_bytes),
            };
            if entry.perm & !0o7 != 0 {
                return Err(invalid_acl(format!(
                    "{xattr_name}: ACL entry has permission bits {:#o}",
                    entry.perm
                )));
            }

            match entry.tag {
                ACL_USER_OBJ => {
                    user_obj = user_obj.saturating_add(1);
                    if entry.id != ACL_UNDEFINED_ID {
                        return Err(invalid_acl(format!("{xattr_name}: ACL_USER_OBJ has an id")));
                    }
                }
                ACL_USER => {
                    named_entries = named_entries.saturating_add(1);
                    if entry.id == ACL_UNDEFINED_ID {
                        return Err(invalid_acl(
                            format!("{xattr_name}: ACL_USER has the undefined id"),
                        ));
                    }
                    if !named_ids.insert((entry.tag, entry.id)) {
                        return Err(invalid_acl(format!(
                            "{xattr_name}: duplicate ACL_USER entry for id {}",
                            entry.id
                        )));
                    }
                }
                ACL_GROUP_OBJ => {
                    group_obj = group_obj.saturating_add(1);
                    if entry.id != ACL_UNDEFINED_ID {
                        return Err(invalid_acl(
                            format!("{xattr_name}: ACL_GROUP_OBJ has an id"),
                        ));
                    }
                }
                ACL_GROUP => {
                    named_entries = named_entries.saturating_add(1);
                    if entry.id == ACL_UNDEFINED_ID {
                        return Err(invalid_acl(
                            format!("{xattr_name}: ACL_GROUP has the undefined id"),
                        ));
                    }
                    if !named_ids.insert((entry.tag, entry.id)) {
                        return Err(invalid_acl(format!(
                            "{xattr_name}: duplicate ACL_GROUP entry for id {}",
                            entry.id
                        )));
                    }
                }
                ACL_MASK => {
                    masks = masks.saturating_add(1);
                    if entry.id != ACL_UNDEFINED_ID {
                        return Err(invalid_acl(format!("{xattr_name}: ACL_MASK has an id")));
                    }
                }
                ACL_OTHER => {
                    other = other.saturating_add(1);
                    if entry.id != ACL_UNDEFINED_ID {
                        return Err(invalid_acl(format!("{xattr_name}: ACL_OTHER has an id")));
                    }
                }
                tag => {
                    return Err(invalid_acl(
                        format!("{xattr_name}: unknown ACL tag {tag:#x}"),
                    ));
                }
            }
            entries.push(entry);
        }

        if user_obj != 1 || group_obj != 1 || other != 1 {
            return Err(invalid_acl(format!(
                "{xattr_name}: ACL must contain exactly one USER_OBJ, GROUP_OBJ and OTHER entry"
            )));
        }
        if masks > 1 {
            return Err(invalid_acl(format!(
                "{xattr_name}: ACL contains more than one MASK entry"
            )));
        }
        if named_entries != 0 && masks == 0 {
            return Err(invalid_acl(format!(
                "{xattr_name}: an ACL with named entries must contain a MASK entry"
            )));
        }

        entries.sort_by_key(|entry| (entry.tag, entry.id));
        Ok(Self { entries })
    }

    pub(crate) fn to_xattr(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.entries.len() * 8);
        out.extend_from_slice(&ACL_VERSION.to_le_bytes());
        for entry in &self.entries {
            out.extend_from_slice(&entry.tag.to_le_bytes());
            out.extend_from_slice(&entry.perm.to_le_bytes());
            out.extend_from_slice(&entry.id.to_le_bytes());
        }
        out
    }

    fn has_mask(&self) -> bool {
        self.entries.iter().any(|entry| entry.tag == ACL_MASK)
    }

    fn has_extended_entries(&self) -> bool {
        self.entries.iter().any(|entry| {
            matches!(entry.tag, ACL_USER | ACL_GROUP | ACL_MASK)
        })
    }

    fn entry(&self, tag: u16) -> Result<&AclEntry> {
        self.entries
            .iter()
            .find(|entry| entry.tag == tag)
            .ok_or_else(|| invalid_acl(format!("ACL is missing tag {tag:#x}")))
    }

    fn entry_mut(&mut self, tag: u16) -> Result<&mut AclEntry> {
        self.entries
            .iter_mut()
            .find(|entry| entry.tag == tag)
            .ok_or_else(|| invalid_acl(format!("ACL is missing tag {tag:#x}")))
    }

    /// POSIX mode bits corresponding to an access ACL. File-type and special
    /// bits are supplied by the caller because they are not stored in an ACL.
    pub(crate) fn mode_bits(&self, special_bits: u16) -> Result<u16> {
        let user = entry_perm(self.entry(ACL_USER_OBJ)?);
        let group = if self.has_mask() {
            entry_perm(self.entry(ACL_MASK)?)
        } else {
            entry_perm(self.entry(ACL_GROUP_OBJ)?)
        };
        let other = entry_perm(self.entry(ACL_OTHER)?);
        Ok((special_bits & 0o7000) | (user << 6) | (group << 3) | other)
    }

    /// Applies chmod to an access ACL. When an ACL mask is present it is the
    /// group-class mode, while GROUP_OBJ keeps its effective-entry permission.
    pub(crate) fn sync_mode(&mut self, mode: u16) -> Result<()> {
        let user = (mode >> 6) & 0o7;
        let group = (mode >> 3) & 0o7;
        let other = mode & 0o7;
        self.entry_mut(ACL_USER_OBJ)?.perm = user;
        if self.has_mask() {
            self.entry_mut(ACL_MASK)?.perm = group;
        } else {
            self.entry_mut(ACL_GROUP_OBJ)?.perm = group;
        }
        self.entry_mut(ACL_OTHER)?.perm = other;
        Ok(())
    }

    /// Applies the POSIX default-ACL-to-access-ACL transformation to the
    /// requested mode. Owner, mask and other permissions are narrowed to the
    /// requested class bits; named entries keep their default permissions and
    /// are limited by the generated mask.
    fn inherit_access(&self, requested_mode: u16) -> Result<(Self, u16)> {
        let owner = (requested_mode >> 6) & 0o7;
        let group = (requested_mode >> 3) & 0o7;
        let other = requested_mode & 0o7;
        let has_mask = self.has_mask();
        let mut access = self.clone();
        for entry in &mut access.entries {
            match entry.tag {
                ACL_USER_OBJ => entry.perm &= owner,
                ACL_MASK => entry.perm &= group,
                ACL_GROUP_OBJ if !has_mask => entry.perm &= group,
                ACL_OTHER => entry.perm &= other,
                _ => {}
            }
        }
        let mode = access.mode_bits(requested_mode & 0o7000)?;
        Ok((access, mode))
    }
}

/// Builds the post-create mode and the access/default ACL xattrs inherited
/// from a parent directory's default ACL.
pub(crate) fn inherit_child_acls(
    parent_default_acl: &[u8],
    requested_mode: u16,
    kind: u8,
) -> Result<ChildAcls> {
    if kind == INODE_KIND_SYMLINK {
        return Ok(ChildAcls {
            mode: requested_mode,
            access: None,
            default: None,
        });
    }

    let parent_default = PosixAcl::parse(parent_default_acl, ACL_XATTR_DEFAULT)?;
    let (access, mode) = parent_default.inherit_access(requested_mode)?;
    Ok(ChildAcls {
        mode,
        access: access.has_extended_entries().then(|| access.to_xattr()),
        default: (kind == INODE_KIND_DIR).then(|| parent_default.to_xattr()),
    })
}
