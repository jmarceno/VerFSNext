//! Desktop app: tray icon, first-run setup and the control center (Qt Quick
//! via cxx-qt). Started by a bare `verfsnext`.
//!
//! Threading: Qt owns the main thread. Anything that can block (processes,
//! sockets, metadata scans) runs on worker threads that report back over a
//! channel drained by `AppController::tick()` (QML Timer). Control-socket
//! requests run on a small tokio runtime owned here.

mod browse;
mod controller;
mod daemon;
mod desktop;
mod paths;
mod prefs;
mod settings;
mod single_instance;
mod tray;

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use cxx_qt_lib::{QGuiApplication, QQmlApplicationEngine, QQuickStyle, QString, QUrl};

use self::controller::{install_bootstrap, UiBootstrap};
use self::single_instance::AcquireOutcome;

const MAIN_QML: &str = "qrc:/qt/qml/app/verfsnext/qml/Main.qml";

pub fn run() -> i32 {
    let guard = match single_instance::acquire() {
        Ok(AcquireOutcome::Secondary) => {
            eprintln!("VerFSNext is already running; showed the existing window.");
            return 0;
        }
        Ok(AcquireOutcome::Primary(g)) => g,
        Err(e) => {
            eprintln!("VerFSNext could not start: {e:#}");
            return 1;
        }
    };

    if std::env::var_os("DISPLAY").is_none() && std::env::var_os("WAYLAND_DISPLAY").is_none() {
        eprintln!(
            "No graphical session (DISPLAY/WAYLAND_DISPLAY unset).\n\
             To mount from a terminal, run `verfsnext mount` or `verfsnext --config <file>`."
        );
        return 1;
    }

    let tray = match tray::TrayHandle::start() {
        Ok(tray) => tray,
        Err(e) => {
            eprintln!("VerFSNext needs a system tray: {e:#}");
            return 1;
        }
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("verfsnext-gui-rt")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("VerFSNext could not start its runtime: {e}");
            return 1;
        }
    };

    // Some distros send Qt messages to journald only, which hides fatal QML
    // errors from the terminal. Mirror them to stderr unless overridden.
    // Set on the main thread before any Qt object or Qt thread exists.
    if std::env::var_os("QT_FORCE_STDERR_LOGGING").is_none() {
        std::env::set_var("QT_FORCE_STDERR_LOGGING", "1");
    }
    apply_render_backend_default();

    install_bootstrap(UiBootstrap {
        activations: guard.receiver(),
        tray,
        runtime: runtime.handle().clone(),
    });
    spawn_bootstrap_watchdog();

    // "Basic" works everywhere and is fully customizable; native styles
    // depend on host plugins.
    QQuickStyle::set_style(&QString::from("Basic"));

    let mut app = QGuiApplication::new();
    let mut engine = QQmlApplicationEngine::new();
    if let Some(app) = app.as_mut() {
        app.set_application_name(&QString::from(paths::APP_ID));
    }
    let Some(engine) = engine.as_mut() else {
        eprintln!("VerFSNext failed to start: Qt QML engine is null.");
        return 1;
    };
    engine.load(&QUrl::from(MAIN_QML));

    let code = app.as_mut().map(|app| app.exec()).unwrap_or(1);
    drop(guard);
    code
}

/// Default Qt Quick to software rendering unless a backend was chosen: it
/// needs no GL integration from the host and is plenty for this UI. Set
/// `QT_QUICK_BACKEND` / `QSG_RHI_BACKEND` to opt into GPU rendering.
fn apply_render_backend_default() {
    if std::env::var_os("QT_QUICK_BACKEND").is_none()
        && std::env::var_os("QSG_RHI_BACKEND").is_none()
    {
        std::env::set_var("QT_QUICK_BACKEND", "software");
    }
}

/// Exit loudly if QML never calls `bootstrap()`: when `Main.qml` fails to
/// load, `app.exec()` would otherwise run forever with no window.
fn spawn_bootstrap_watchdog() {
    let spawned = std::thread::Builder::new()
        .name("qml-watchdog".into())
        .spawn(|| {
            std::thread::sleep(Duration::from_secs(10));
            if controller::is_bootstrap_pending() {
                eprintln!(
                    "VerFSNext failed to start: the QML UI did not load.\n\
                     See the QML error above, or `journalctl --user --since '5 minutes ago' | grep -i qml`."
                );
                std::process::exit(2);
            }
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "failed to start the QML watchdog");
    }
}

/// Write via a temp file + rename so a crash never leaves a truncated file.
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path.parent().context("path has no parent folder")?;
    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create {}", parent.display()))?;
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("failed to replace {}", path.display()))
}
