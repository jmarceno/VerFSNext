//! StatusNotifier tray icon via ksni (no GTK).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use crossbeam_channel::{unbounded, Receiver, Sender};
use ksni::menu::{MenuItem, StandardItem};
use ksni::{Handle, Tray, TrayService};

const ICON_SVG: &[u8] = include_bytes!("../../assets/verfsnext.svg");
const ICON_SIZE: u32 = 64;

/// How long the first registration gets to fail before the tray is declared up.
const FIRST_ATTEMPT_WINDOW: Duration = Duration::from_millis(800);
/// Pause between attempts when the session bus dropped the tray connection.
const RECONNECT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayEvent {
    OpenControl,
    OpenFolder,
    ToggleRunning,
    TakeSnapshot,
    Quit,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrayState {
    pub running: bool,
    pub configured: bool,
    pub status_line: String,
    pub toggle_label: String,
    pub toggle_enabled: bool,
    pub quit_label: String,
}

impl Default for TrayState {
    fn default() -> Self {
        Self {
            running: false,
            configured: false,
            status_line: "VerFSNext".into(),
            toggle_label: "Start VerFSNext".into(),
            toggle_enabled: false,
            quit_label: "Quit".into(),
        }
    }
}

struct VerfsTray {
    tx: Sender<TrayEvent>,
    /// Shared with `TrayHandle`: a reconnected `TrayService` reads the current
    /// state from here instead of the values it was built with, so the menu and
    /// icon never revert to their startup defaults after a bus hiccup.
    shared: Arc<Mutex<TrayState>>,
    icon_running: Arc<Vec<ksni::Icon>>,
    icon_stopped: Arc<Vec<ksni::Icon>>,
}

impl VerfsTray {
    fn state(&self) -> TrayState {
        // A poisoned lock still holds the last known state; a tray must never
        // panic on the D-Bus thread just because a UI thread died holding it.
        self.shared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn send(&self, ev: TrayEvent) {
        if self.tx.send(ev).is_err() {
            tracing::error!(?ev, "tray event dropped: the app stopped listening");
        }
    }
}

impl Tray for VerfsTray {
    fn id(&self) -> String {
        "verfsnext".into()
    }

    fn title(&self) -> String {
        "VerFSNext".into()
    }

    fn category(&self) -> ksni::Category {
        ksni::Category::ApplicationStatus
    }

    fn status(&self) -> ksni::Status {
        ksni::Status::Active
    }

    fn icon_pixmap(&self) -> Vec<ksni::Icon> {
        if self.state().running {
            self.icon_running.as_ref().clone()
        } else {
            self.icon_stopped.as_ref().clone()
        }
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        ksni::ToolTip {
            title: self.state().status_line,
            description: String::new(),
            icon_name: String::new(),
            icon_pixmap: self.icon_pixmap(),
        }
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        self.send(TrayEvent::OpenControl);
    }

    fn menu(&self) -> Vec<MenuItem<Self>> {
        let state = self.state();
        vec![
            StandardItem {
                label: state.status_line.clone(),
                enabled: false,
                ..Default::default()
            }
            .into(),
            MenuItem::Separator,
            StandardItem {
                label: "Open VerFSNext Folder".into(),
                enabled: state.running,
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::OpenFolder)),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Take a Snapshot Now".into(),
                enabled: state.running,
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::TakeSnapshot)),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: state.toggle_label.clone(),
                enabled: state.toggle_enabled,
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::ToggleRunning)),
                ..Default::default()
            }
            .into(),
            MenuItem::Separator,
            StandardItem {
                label: if state.configured {
                    "Open Control Center…".into()
                } else {
                    "Set Up VerFSNext…".into()
                },
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::OpenControl)),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: state.quit_label.clone(),
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::Quit)),
                ..Default::default()
            }
            .into(),
        ]
    }

    /// The watcher only blips (a restart, or a session-bus reconnect). Returning
    /// `false` here makes ksni shut the service down for good, so keep it alive:
    /// ksni re-registers by itself when the watcher name comes back.
    fn watcher_offine(&self) -> bool {
        tracing::warn!("system tray host went away; waiting for it to come back");
        true
    }
}

pub struct TrayHandle {
    pub rx: Receiver<TrayEvent>,
    /// Swapped for a new handle every time the service reconnects, so
    /// `update()` always reaches the `TrayService` that is actually running.
    handle: Arc<Mutex<Handle<VerfsTray>>>,
    /// The live state, read by whichever service instance is running.
    shared: Arc<Mutex<TrayState>>,
    stop: Arc<AtomicBool>,
    last: TrayState,
}

impl Drop for TrayHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Ok(handle) = self.handle.lock() {
            handle.shutdown();
        }
    }
}

impl TrayHandle {
    /// Runs before any Qt object exists, so blocking here is fine.
    pub fn start() -> Result<Self> {
        // At login (XDG autostart) the app routinely starts before the
        // desktop's tray host registers; wait for it instead of exiting.
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut waited = false;
        while !watcher_present()? {
            if Instant::now() >= deadline {
                bail!("no system tray found (StatusNotifierWatcher missing on the session bus)");
            }
            if !waited {
                tracing::info!("waiting up to 120 s for the system tray");
                waited = true;
            }
            thread::sleep(Duration::from_millis(500));
        }

        let (tx, rx) = unbounded();
        let icon_running = Arc::new(vec![render_icon(1.0)?]);
        let icon_stopped = Arc::new(vec![render_icon(0.4)?]);
        let shared = Arc::new(Mutex::new(TrayState::default()));
        let service = build_service(tx.clone(), shared.clone(), icon_running.clone(), icon_stopped.clone());
        let handle = Arc::new(Mutex::new(service.handle()));
        let stop = Arc::new(AtomicBool::new(false));

        let (err_tx, err_rx) = std::sync::mpsc::channel();
        {
            let thread_handle = handle.clone();
            let thread_shared = shared.clone();
            let thread_stop = stop.clone();
            thread::Builder::new()
                .name("verfsnext-tray".into())
                .spawn(move || {
                    // One service per session-bus connection. ksni returns an
                    // error when the connection dies and never retries, so a
                    // new connection comes from a new `TrayService`.
                    let mut next = Some(service);
                    let mut reported = false;
                    while !thread_stop.load(Ordering::Acquire) {
                        let Some(service) = next.take() else {
                            break;
                        };
                        match service.run() {
                            // A clean stop is a real shutdown, not a reconnect.
                            Ok(()) => break,
                            Err(e) => {
                                if !reported {
                                    reported = true;
                                    let _ = err_tx.send(e.to_string());
                                }
                                tracing::warn!(error = %e, "tray connection lost; reconnecting");
                            }
                        }
                        if thread_stop.load(Ordering::Acquire) {
                            break;
                        }
                        thread::sleep(RECONNECT_INTERVAL);
                        let rebuilt = build_service(
                            tx.clone(),
                            thread_shared.clone(),
                            icon_running.clone(),
                            icon_stopped.clone(),
                        );
                        *thread_handle
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = rebuilt.handle();
                        next = Some(rebuilt);
                    }
                })
                .context("failed to start tray thread")?;
        }

        match err_rx.recv_timeout(FIRST_ATTEMPT_WINDOW) {
            Ok(e) => bail!("could not create the tray icon: {e}"),
            Err(RecvTimeoutError::Disconnected) => {
                bail!("tray thread exited before registering")
            }
            // Still running: the first registration succeeded.
            Err(RecvTimeoutError::Timeout) => {}
        }

        Ok(Self {
            rx,
            handle,
            shared,
            stop,
            last: TrayState::default(),
        })
    }

    pub fn update(&mut self, state: TrayState) {
        if state == self.last {
            return;
        }
        self.last = state.clone();
        *self
            .shared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = state.clone();
        self.handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .update(move |tray| {
                *tray
                    .shared
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = state;
            });
    }
}

fn build_service(
    tx: Sender<TrayEvent>,
    shared: Arc<Mutex<TrayState>>,
    icon_running: Arc<Vec<ksni::Icon>>,
    icon_stopped: Arc<Vec<ksni::Icon>>,
) -> TrayService<VerfsTray> {
    TrayService::new(VerfsTray {
        tx,
        shared,
        icon_running,
        icon_stopped,
    })
}

fn watcher_present() -> Result<bool> {
    let conn = zbus::blocking::Connection::session().context("no D-Bus session bus")?;
    let dbus = zbus::blocking::fdo::DBusProxy::new(&conn).context("D-Bus proxy failed")?;
    for name in [
        "org.kde.StatusNotifierWatcher",
        "org.freedesktop.StatusNotifierWatcher",
    ] {
        let bus_name = zbus::names::BusName::try_from(name).context("invalid bus name")?;
        if dbus
            .name_has_owner(bus_name)
            .context("D-Bus NameHasOwner failed")?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Renders the app SVG to an ARGB32 pixmap, faded by `opacity` when stopped.
fn render_icon(opacity: f32) -> Result<ksni::Icon> {
    let tree = usvg::Tree::from_data(ICON_SVG, &usvg::Options::default())
        .context("invalid tray icon SVG")?;
    let mut pixmap =
        resvg::tiny_skia::Pixmap::new(ICON_SIZE, ICON_SIZE).context("tray pixmap allocation")?;
    let scale = ICON_SIZE as f32 / tree.size().width().max(tree.size().height());
    resvg::render(
        &tree,
        resvg::tiny_skia::Transform::from_scale(scale, scale),
        &mut pixmap.as_mut(),
    );
    let mut data = Vec::with_capacity((ICON_SIZE * ICON_SIZE * 4) as usize);
    for px in pixmap.pixels() {
        let c = px.demultiply();
        let a = (c.alpha() as f32 * opacity).round() as u8;
        data.extend_from_slice(&[a, c.red(), c.green(), c.blue()]);
    }
    Ok(ksni::Icon {
        width: ICON_SIZE as i32,
        height: ICON_SIZE as i32,
        data,
    })
}
