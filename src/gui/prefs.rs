//! App-only preferences (`~/.config/verfsnext/desktop.toml`).

use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Prefs {
    /// Key file last used to create or unlock the vault, offered next time.
    pub vault_key_file: Option<PathBuf>,
}

pub fn load() -> Result<Prefs> {
    let path = super::paths::prefs_path()?;
    match std::fs::read_to_string(&path) {
        Ok(text) => toml::from_str(&text).with_context(|| format!("invalid {}", path.display())),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Prefs::default()),
        Err(err) => Err(err).with_context(|| format!("failed to read {}", path.display())),
    }
}

pub fn save(prefs: &Prefs) -> Result<()> {
    let path = super::paths::prefs_path()?;
    let text = toml::to_string_pretty(prefs).context("failed to serialize preferences")?;
    super::write_atomic(&path, text.as_bytes())
}
