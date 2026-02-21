use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result};
use tracing::warn;

pub const DIR_MODE: u32 = 0o770;
pub const FILE_MODE: u32 = 0o660;
pub const SOCKET_MODE: u32 = 0o660;

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create directory {}", path.display()))?;
    set_mode(path, DIR_MODE)
}

pub fn normalize_data_tree(root: &Path) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }

    set_mode(root, DIR_MODE)?;
    normalize_children(root)
}

fn normalize_children(dir: &Path) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type().with_context(|| {
            format!(
                "failed to read file type while normalizing permissions for {}",
                path.display()
            )
        })?;

        if file_type.is_symlink() {
            continue;
        }
        if file_type.is_dir() {
            set_mode(&path, DIR_MODE)?;
            normalize_children(&path)?;
            continue;
        }

        set_mode(&path, FILE_MODE)?;
    }
    Ok(())
}

pub fn set_socket_mode(path: &Path) -> Result<()> {
    set_mode(path, SOCKET_MODE)
}

pub fn set_file_mode(path: &Path) -> Result<()> {
    set_mode(path, FILE_MODE)
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let perms = fs::Permissions::from_mode(mode);
        if let Err(err) = fs::set_permissions(path, perms) {
            if err.kind() == ErrorKind::PermissionDenied {
                warn!(
                    path = %path.display(),
                    mode = format_args!("{mode:o}"),
                    error = %err,
                    "insufficient permission to adjust mode; continuing with existing mode"
                );
                return Ok(());
            }
            return Err(err)
                .with_context(|| format!("failed to set mode {mode:o} on {}", path.display()));
        }
    }
    Ok(())
}
