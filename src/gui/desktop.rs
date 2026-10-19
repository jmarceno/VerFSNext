//! XDG desktop integration: app-menu entry and login autostart. Both launch
//! the bare binary, which opens the desktop app.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::paths::{self, APP_ID};

const ICON_SVG: &[u8] = include_bytes!("../../assets/verfsnext.svg");

fn entry_file_name() -> String {
    format!("{APP_ID}.desktop")
}

pub fn menu_entry_path() -> Result<PathBuf> {
    Ok(paths::data_home()?.join("applications").join(entry_file_name()))
}

pub fn autostart_path() -> Result<PathBuf> {
    Ok(paths::config_home()?.join("autostart").join(entry_file_name()))
}

/// Quote for a desktop-entry `Exec=` key.
fn quote_exec(path: &Path) -> String {
    let s = path.to_string_lossy();
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn desktop_entry(exec: &Path, icon: &Path) -> String {
    format!(
        "[Desktop Entry]\n\
         Type=Application\n\
         Name=VerFSNext\n\
         GenericName=Space-saving folder\n\
         Comment=A folder that deduplicates, compresses and snapshots your files\n\
         Exec={exec}\n\
         Icon={icon}\n\
         Terminal=false\n\
         Categories=Utility;System;FileTools;\n\
         StartupNotify=false\n",
        exec = quote_exec(exec),
        icon = icon.display(),
    )
}

/// Write the embedded SVG next to the user's other icons.
fn install_icon() -> Result<PathBuf> {
    let path = paths::data_home()?
        .join("icons/hicolor/scalable/apps")
        .join(format!("{APP_ID}.svg"));
    let parent = path.parent().context("icon path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    std::fs::write(&path, ICON_SVG)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(path)
}

fn write_entry(path: &Path, exec: &Path) -> Result<()> {
    let parent = path.parent().context("desktop entry path has no parent")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    let icon = install_icon()?;
    std::fs::write(path, desktop_entry(exec, &icon))
        .with_context(|| format!("failed to write {}", path.display()))
}

pub fn write_menu_entry(exec: &Path) -> Result<()> {
    write_entry(&menu_entry_path()?, exec)
}

pub fn autostart_enabled() -> Result<bool> {
    Ok(autostart_path()?.is_file())
}

pub fn set_autostart(exec: &Path, enabled: bool) -> Result<()> {
    let path = autostart_path()?;
    if enabled {
        return write_entry(&path, exec);
    }
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to remove {}", path.display())),
    }
}

/// Shows a desktop notification (org.freedesktop.Notifications). Blocks on
/// D-Bus; call from a worker thread.
pub fn send_notification(summary: &str, body: &str) -> Result<()> {
    use std::collections::HashMap;

    let conn = zbus::blocking::Connection::session().context("no D-Bus session bus")?;
    let hints: HashMap<&str, zbus::zvariant::Value<'_>> = HashMap::new();
    conn.call_method(
        Some("org.freedesktop.Notifications"),
        "/org/freedesktop/Notifications",
        Some("org.freedesktop.Notifications"),
        "Notify",
        &(
            "VerFSNext",
            0u32,
            APP_ID,
            summary,
            body,
            Vec::<&str>::new(),
            hints,
            -1i32,
        ),
    )
    .context("failed to show a desktop notification")?;
    Ok(())
}
