use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

fn default_sync_interval_ms() -> u64 {
    5_000
}

fn default_batch_max_blocks() -> usize {
    3_000
}

fn default_batch_flush_interval_ms() -> u64 {
    500
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub mount_point: PathBuf,
    pub data_dir: PathBuf,
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
    #[serde(default = "default_batch_max_blocks")]
    pub batch_max_blocks: usize,
    #[serde(default = "default_batch_flush_interval_ms")]
    pub batch_flush_interval_ms: u64,
}

impl Config {
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file at {}", path.display()))?;
        let cfg: Self = toml::from_str(&raw).context("failed to parse config.toml")?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.mount_point.as_os_str().is_empty() {
            bail!("mount_point must not be empty");
        }
        if self.data_dir.as_os_str().is_empty() {
            bail!("data_dir must not be empty");
        }
        if self.sync_interval_ms == 0 {
            bail!("sync_interval_ms must be > 0");
        }
        if self.batch_max_blocks == 0 {
            bail!("batch_max_blocks must be > 0");
        }
        if self.batch_flush_interval_ms == 0 {
            bail!("batch_flush_interval_ms must be > 0");
        }
        Ok(())
    }

    pub fn metadata_dir(&self) -> PathBuf {
        self.data_dir.join("metadata")
    }

    pub fn packs_dir(&self) -> PathBuf {
        self.data_dir.join("packs")
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)
            .with_context(|| format!("failed to create data dir {}", self.data_dir.display()))?;
        std::fs::create_dir_all(self.metadata_dir()).with_context(|| {
            format!(
                "failed to create metadata dir {}",
                self.metadata_dir().display()
            )
        })?;
        std::fs::create_dir_all(self.packs_dir())
            .with_context(|| format!("failed to create packs dir {}", self.packs_dir().display()))?;
        Ok(())
    }
}
