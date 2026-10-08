//! Per-user locations used by the desktop app.

use std::path::PathBuf;

use anyhow::{Context, Result};

pub const APP_ID: &str = "verfsnext";

pub fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .context("HOME is not set")
}

fn xdg(var: &str, fallback: &str) -> Result<PathBuf> {
    match std::env::var_os(var).filter(|v| !v.is_empty()) {
        Some(v) => Ok(PathBuf::from(v)),
        None => Ok(home()?.join(fallback)),
    }
}

/// The daemon config the app manages. Same file the CLI discovers as
/// `~/.config/verfsnext/config.toml`, so terminal commands see it too.
pub fn config_path() -> Result<PathBuf> {
    Ok(home()?.join(".config").join(APP_ID).join("config.toml"))
}

/// App-only preferences, kept out of the daemon config.
pub fn prefs_path() -> Result<PathBuf> {
    Ok(home()?.join(".config").join(APP_ID).join("desktop.toml"))
}

pub fn config_home() -> Result<PathBuf> {
    xdg("XDG_CONFIG_HOME", ".config")
}

pub fn data_home() -> Result<PathBuf> {
    xdg("XDG_DATA_HOME", ".local/share")
}

/// Log of a daemon started by the app (not used by the user service, whose
/// output goes to the journal).
pub fn daemon_log_path() -> Result<PathBuf> {
    Ok(xdg("XDG_STATE_HOME", ".local/state")?
        .join(APP_ID)
        .join("daemon.log"))
}

/// `$XDG_RUNTIME_DIR/verfsnext` (single-instance socket).
pub fn app_runtime_dir() -> PathBuf {
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(format!("/run/user/{}", nix::unistd::getuid())));
    base.join(APP_ID)
}

/// Binary that desktop entries and the user service launch.
pub fn current_exe() -> Result<PathBuf> {
    std::env::current_exe().context("failed to resolve the running executable")
}
