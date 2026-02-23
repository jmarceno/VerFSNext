use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::permissions::ensure_dir;
use crate::types::BLOCK_SIZE;

fn default_sync_interval_ms() -> u64 {
    5_000
}

fn default_batch_max_size_mb() -> usize {
    512
}

fn default_batch_flush_interval_ms() -> u64 {
    500
}

fn default_metadata_cache_capacity_entries() -> u64 {
    131_072
}

fn default_chunk_cache_capacity_mb() -> u64 {
    1024
}

fn default_pack_index_cache_capacity_entries() -> u64 {
    524_288
}

fn default_pack_max_size_mb() -> u64 {
    10 * 1024
}

fn default_zstd_compression_level() -> i32 {
    3
}

fn default_ultracdc_min_size_bytes() -> usize {
    16 * 1024
}

fn default_ultracdc_avg_size_bytes() -> usize {
    32 * 1024
}

fn default_ultracdc_max_size_bytes() -> usize {
    64 * 1024
}

fn default_fuse_max_write_bytes() -> u32 {
    u32::try_from(BLOCK_SIZE).unwrap_or(1024 * 1024)
}

fn default_fuse_direct_io() -> bool {
    false
}

fn default_fuse_fsname() -> String {
    "verfsnext".to_owned()
}

fn default_fuse_subtype() -> String {
    "verfsnext".to_owned()
}

fn default_gc_idle_min_ms() -> u64 {
    15_000
}

fn default_gc_pack_rewrite_min_reclaim_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_gc_pack_rewrite_min_reclaim_percent() -> f64 {
    25.0
}

fn default_gc_discard_filename() -> String {
    ".DISCARD".to_owned()
}

fn default_vault_enabled() -> bool {
    true
}

fn default_vault_argon2_mem_kib() -> u32 {
    131_072
}

fn default_vault_argon2_iters() -> u32 {
    3
}

fn default_vault_argon2_parallelism() -> u32 {
    1
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub mount_point: PathBuf,
    pub data_dir: PathBuf,
    #[serde(default = "default_sync_interval_ms")]
    pub sync_interval_ms: u64,
    #[serde(default = "default_batch_max_size_mb")]
    pub batch_max_size_mb: usize,
    #[serde(default = "default_batch_flush_interval_ms")]
    pub batch_flush_interval_ms: u64,
    #[serde(default = "default_metadata_cache_capacity_entries")]
    pub metadata_cache_capacity_entries: u64,
    #[serde(default = "default_chunk_cache_capacity_mb")]
    pub chunk_cache_capacity_mb: u64,
    #[serde(default = "default_pack_index_cache_capacity_entries")]
    pub pack_index_cache_capacity_entries: u64,
    #[serde(default = "default_pack_max_size_mb")]
    pub pack_max_size_mb: u64,
    #[serde(default = "default_zstd_compression_level")]
    pub zstd_compression_level: i32,
    #[serde(default = "default_ultracdc_min_size_bytes")]
    pub ultracdc_min_size_bytes: usize,
    #[serde(default = "default_ultracdc_avg_size_bytes")]
    pub ultracdc_avg_size_bytes: usize,
    #[serde(default = "default_ultracdc_max_size_bytes")]
    pub ultracdc_max_size_bytes: usize,
    #[serde(default = "default_fuse_max_write_bytes")]
    pub fuse_max_write_bytes: u32,
    #[serde(default = "default_fuse_direct_io")]
    pub fuse_direct_io: bool,
    #[serde(default = "default_fuse_fsname")]
    pub fuse_fsname: String,
    #[serde(default = "default_fuse_subtype")]
    pub fuse_subtype: String,
    #[serde(default = "default_gc_idle_min_ms")]
    pub gc_idle_min_ms: u64,
    #[serde(default = "default_gc_pack_rewrite_min_reclaim_bytes")]
    pub gc_pack_rewrite_min_reclaim_bytes: u64,
    #[serde(default = "default_gc_pack_rewrite_min_reclaim_percent")]
    pub gc_pack_rewrite_min_reclaim_percent: f64,
    #[serde(default = "default_gc_discard_filename")]
    pub gc_discard_filename: String,
    #[serde(default = "default_vault_enabled")]
    pub vault_enabled: bool,
    #[serde(default = "default_vault_argon2_mem_kib")]
    pub vault_argon2_mem_kib: u32,
    #[serde(default = "default_vault_argon2_iters")]
    pub vault_argon2_iters: u32,
    #[serde(default = "default_vault_argon2_parallelism")]
    pub vault_argon2_parallelism: u32,
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
        if self.batch_max_size_mb == 0 {
            bail!("batch_max_size_mb must be > 0");
        }
        if self.batch_flush_interval_ms == 0 {
            bail!("batch_flush_interval_ms must be > 0");
        }
        if self.metadata_cache_capacity_entries == 0 {
            bail!("metadata_cache_capacity_entries must be > 0");
        }
        if self.chunk_cache_capacity_mb == 0 {
            bail!("chunk_cache_capacity_mb must be > 0");
        }
        if self.pack_index_cache_capacity_entries == 0 {
            bail!("pack_index_cache_capacity_entries must be > 0");
        }
        if self.pack_max_size_mb == 0 {
            bail!("pack_max_size_mb must be > 0");
        }
        if self.zstd_compression_level < -7 || self.zstd_compression_level > 22 {
            bail!("zstd_compression_level must be in range -7..=22");
        }
        if self.ultracdc_min_size_bytes == 0 {
            bail!("ultracdc_min_size_bytes must be > 0");
        }
        if self.ultracdc_avg_size_bytes < self.ultracdc_min_size_bytes {
            bail!("ultracdc_avg_size_bytes must be >= ultracdc_min_size_bytes");
        }
        if self.ultracdc_max_size_bytes < self.ultracdc_avg_size_bytes {
            bail!("ultracdc_max_size_bytes must be >= ultracdc_avg_size_bytes");
        }
        if self.fuse_max_write_bytes < 4096 {
            bail!("fuse_max_write_bytes must be >= 4096");
        }
        if self.fuse_max_write_bytes > 16 * 1024 * 1024 {
            bail!("fuse_max_write_bytes must be <= 16777216");
        }
        if self.fuse_fsname.trim().is_empty() {
            bail!("fuse_fsname must not be empty");
        }
        if self.fuse_subtype.trim().is_empty() {
            bail!("fuse_subtype must not be empty");
        }
        if self.gc_idle_min_ms == 0 {
            bail!("gc_idle_min_ms must be > 0");
        }
        if self.gc_pack_rewrite_min_reclaim_bytes == 0 {
            bail!("gc_pack_rewrite_min_reclaim_bytes must be > 0");
        }
        if !(0.0..=100.0).contains(&self.gc_pack_rewrite_min_reclaim_percent) {
            bail!("gc_pack_rewrite_min_reclaim_percent must be in range 0..=100");
        }
        if self.gc_discard_filename.is_empty() {
            bail!("gc_discard_filename must not be empty");
        }
        if self.vault_argon2_mem_kib < 8 * 1024 {
            bail!("vault_argon2_mem_kib must be >= 8192");
        }
        if self.vault_argon2_iters == 0 {
            bail!("vault_argon2_iters must be > 0");
        }
        if self.vault_argon2_parallelism == 0 {
            bail!("vault_argon2_parallelism must be > 0");
        }
        Ok(())
    }

    pub fn metadata_dir(&self) -> PathBuf {
        self.data_dir.join("metadata")
    }

    pub fn control_socket_path(&self) -> PathBuf {
        self.data_dir.join("verfsnext.sock")
    }

    pub fn packs_dir(&self) -> PathBuf {
        self.data_dir.join("packs")
    }

    pub fn ensure_dirs(&self) -> Result<()> {
        ensure_dir(&self.data_dir)?;
        ensure_dir(&self.metadata_dir())?;
        ensure_dir(&self.packs_dir())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::Config;

    fn valid_config() -> Config {
        Config {
            mount_point: PathBuf::from("/tmp/verfsnext-mnt"),
            data_dir: PathBuf::from("/tmp/verfsnext-data"),
            sync_interval_ms: 1,
            batch_max_size_mb: 1,
            batch_flush_interval_ms: 1,
            metadata_cache_capacity_entries: 1,
            chunk_cache_capacity_mb: 1,
            pack_index_cache_capacity_entries: 1,
            pack_max_size_mb: 1,
            zstd_compression_level: 3,
            ultracdc_min_size_bytes: 1024,
            ultracdc_avg_size_bytes: 2048,
            ultracdc_max_size_bytes: 4096,
            fuse_max_write_bytes: 4096,
            fuse_direct_io: false,
            fuse_fsname: "verfsnext".to_owned(),
            fuse_subtype: "verfsnext".to_owned(),
            gc_idle_min_ms: 1,
            gc_pack_rewrite_min_reclaim_bytes: 1,
            gc_pack_rewrite_min_reclaim_percent: 25.0,
            gc_discard_filename: ".DISCARD".to_owned(),
            vault_enabled: true,
            vault_argon2_mem_kib: 8192,
            vault_argon2_iters: 1,
            vault_argon2_parallelism: 1,
        }
    }

    #[test]
    fn validate_accepts_boundary_values() {
        let mut cfg = valid_config();
        cfg.zstd_compression_level = -7;
        cfg.validate().expect("zstd lower bound must be valid");

        cfg.zstd_compression_level = 22;
        cfg.validate().expect("zstd upper bound must be valid");

        cfg.gc_pack_rewrite_min_reclaim_percent = 0.0;
        cfg.validate().expect("gc lower bound must be valid");

        cfg.gc_pack_rewrite_min_reclaim_percent = 100.0;
        cfg.validate().expect("gc upper bound must be valid");
    }

    #[test]
    fn validate_rejects_invalid_size_relationships() {
        let mut cfg = valid_config();
        cfg.ultracdc_avg_size_bytes = cfg.ultracdc_min_size_bytes - 1;
        let err = cfg.validate().expect_err("avg < min must fail");
        assert!(
            err.to_string()
                .contains("ultracdc_avg_size_bytes must be >= ultracdc_min_size_bytes"),
            "unexpected error: {err}"
        );

        let mut cfg = valid_config();
        cfg.ultracdc_max_size_bytes = cfg.ultracdc_avg_size_bytes - 1;
        let err = cfg.validate().expect_err("max < avg must fail");
        assert!(
            err.to_string()
                .contains("ultracdc_max_size_bytes must be >= ultracdc_avg_size_bytes"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_fuse_write_out_of_range() {
        let mut cfg = valid_config();
        cfg.fuse_max_write_bytes = 4095;
        let err = cfg.validate().expect_err("small fuse write must fail");
        assert!(
            err.to_string()
                .contains("fuse_max_write_bytes must be >= 4096"),
            "unexpected error: {err}"
        );

        let mut cfg = valid_config();
        cfg.fuse_max_write_bytes = 16 * 1024 * 1024 + 1;
        let err = cfg.validate().expect_err("large fuse write must fail");
        assert!(
            err.to_string()
                .contains("fuse_max_write_bytes must be <= 16777216"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_rejects_gc_percent_and_vault_argon2_values() {
        let mut cfg = valid_config();
        cfg.gc_pack_rewrite_min_reclaim_percent = 100.1;
        let err = cfg.validate().expect_err("gc percent must fail");
        assert!(
            err.to_string()
                .contains("gc_pack_rewrite_min_reclaim_percent must be in range 0..=100"),
            "unexpected error: {err}"
        );

        let mut cfg = valid_config();
        cfg.vault_argon2_mem_kib = 8191;
        let err = cfg.validate().expect_err("vault memory must fail");
        assert!(
            err.to_string()
                .contains("vault_argon2_mem_kib must be >= 8192"),
            "unexpected error: {err}"
        );
    }
}
