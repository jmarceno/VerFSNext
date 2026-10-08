//! Folder (and key file) browsing for the in-app picker. A themed picker of
//! our own works the same on every desktop and inside a portable bundle,
//! where native dialogs depend on host platform plugins.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::paths;

/// Lists `path`: sub-folders always, regular files when `include_files`.
/// Hidden entries are included and flagged; the picker filters them.
pub fn list(path: &Path, include_files: bool) -> Result<Value> {
    if !path.is_absolute() {
        bail!("{} is not a full path", path.display());
    }
    let mut entries = Vec::new();
    for entry in
        std::fs::read_dir(path).with_context(|| format!("can't open {}", path.display()))?
    {
        let entry = entry.with_context(|| format!("can't read {}", path.display()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        // Follow symlinks so linked folders behave like folders.
        let is_dir = match std::fs::metadata(entry.path()) {
            Ok(meta) => meta.is_dir(),
            // Broken symlink or a stale mount: list it as a non-folder.
            Err(_) => false,
        };
        if !is_dir && !include_files {
            continue;
        }
        entries.push(json!({
            "name": name,
            "path": entry.path().to_string_lossy(),
            "isDir": is_dir,
            "hidden": name.starts_with('.'),
        }));
    }
    entries.sort_by(|a, b| {
        b["isDir"].as_bool().cmp(&a["isDir"].as_bool()).then_with(|| {
            let an = a["name"].as_str().unwrap_or_default().to_lowercase();
            let bn = b["name"].as_str().unwrap_or_default().to_lowercase();
            an.cmp(&bn)
        })
    });
    let crumbs: Vec<Value> = path
        .ancestors()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(|p| {
            let name = p
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| "/".to_owned());
            json!({ "name": name, "path": p.to_string_lossy() })
        })
        .collect();
    Ok(json!({
        "path": path.to_string_lossy(),
        "parent": path.parent().map(|p| p.to_string_lossy().into_owned()),
        "crumbs": crumbs,
        "entries": entries,
        "error": "",
    }))
}

/// Common starting points that exist on this machine.
pub fn places() -> Result<Value> {
    let home = paths::home()?;
    let user = home
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let candidates: Vec<(&str, PathBuf)> = vec![
        ("Home", home.clone()),
        ("Documents", home.join("Documents")),
        ("Desktop", home.join("Desktop")),
        ("Drives", PathBuf::from("/media").join(&user)),
        ("Drives", PathBuf::from("/run/media").join(&user)),
        ("/mnt", PathBuf::from("/mnt")),
        ("Computer", PathBuf::from("/")),
    ];
    Ok(Value::Array(
        candidates
            .into_iter()
            .filter(|(_, p)| p.is_dir())
            .map(|(name, p)| json!({ "name": name, "path": p.to_string_lossy() }))
            .collect(),
    ))
}

/// Creates `name` inside `parent` and returns its path.
pub fn make_folder(parent: &Path, name: &str) -> Result<PathBuf> {
    let name = name.trim();
    if name.is_empty() || name == "." || name == ".." || name.contains('/') {
        bail!("Use a folder name without slashes.");
    }
    let path = parent.join(name);
    if path.exists() {
        bail!("\"{name}\" already exists here.");
    }
    std::fs::create_dir(&path).with_context(|| format!("can't create {}", path.display()))?;
    Ok(path)
}
