//! StatusNotifier tray icon via ksni (no GTK).

use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use crossbeam_channel::{unbounded, Receiver, Sender};
use ksni::menu::{MenuItem, StandardItem};
use ksni::{Handle, Tray, TrayService};

const ICON_SVG: &[u8] = include_bytes!("../../assets/verfsnext.svg");
const ICON_SIZE: u32 = 64;

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
    state: TrayState,
    icon_running: Vec<ksni::Icon>,
    icon_stopped: Vec<ksni::Icon>,
}

impl VerfsTray {
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
        if self.state.running {
            self.icon_running.clone()
        } else {
            self.icon_stopped.clone()
        }
    }

    fn tool_tip(&self) -> ksni::ToolTip {
        ksni::ToolTip {
            title: self.state.status_line.clone(),
            description: String::new(),
            icon_name: String::new(),
            icon_pixmap: self.icon_pixmap(),
        }
    }

    fn activate(&mut self, _x: i32, _y: i32) {
        self.send(TrayEvent::OpenControl);
    }

    fn menu(&self) -> Vec<MenuItem<Self>> {
        vec![
            StandardItem {
                label: self.state.status_line.clone(),
                enabled: false,
                ..Default::default()
            }
            .into(),
            MenuItem::Separator,
            StandardItem {
                label: "Open VerFSNext Folder".into(),
                enabled: self.state.running,
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::OpenFolder)),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: "Take a Snapshot Now".into(),
                enabled: self.state.running,
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::TakeSnapshot)),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: self.state.toggle_label.clone(),
                enabled: self.state.toggle_enabled,
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::ToggleRunning)),
                ..Default::default()
            }
            .into(),
            MenuItem::Separator,
            StandardItem {
                label: if self.state.configured {
                    "Open Control Center…".into()
                } else {
                    "Set Up VerFSNext…".into()
                },
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::OpenControl)),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: self.state.quit_label.clone(),
                activate: Box::new(|t: &mut Self| t.send(TrayEvent::Quit)),
                ..Default::default()
            }
            .into(),
        ]
    }

    fn watcher_offine(&self) -> bool {
        tracing::error!("system tray host went away; the VerFSNext icon is gone until it returns");
        true
    }
}

pub struct TrayHandle {
    pub rx: Receiver<TrayEvent>,
    handle: Handle<VerfsTray>,
    last: TrayState,
}

impl Drop for TrayHandle {
    fn drop(&mut self) {
        self.handle.shutdown();
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
        let service = TrayService::new(VerfsTray {
            tx,
            state: TrayState::default(),
            icon_running: vec![render_icon(1.0)?],
            icon_stopped: vec![render_icon(0.4)?],
        });
        let handle = service.handle();
        let (err_tx, err_rx) = std::sync::mpsc::channel();
        thread::Builder::new()
            .name("verfsnext-tray".into())
            .spawn(move || {
                if let Err(e) = service.run() {
                    tracing::error!(error = %e, "tray service stopped");
                    if err_tx.send(e.to_string()).is_err() {
                        tracing::error!("tray startup error could not be reported");
                    }
                }
            })
            .context("failed to start tray thread")?;

        match err_rx.recv_timeout(Duration::from_millis(800)) {
            Ok(e) => bail!("could not create the tray icon: {e}"),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                bail!("tray thread exited before registering")
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }

        Ok(Self {
            rx,
            handle,
            last: TrayState::default(),
        })
    }

    pub fn update(&mut self, state: TrayState) {
        if state == self.last {
            return;
        }
        self.last = state.clone();
        self.handle.update(move |tray| tray.state = state);
    }
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
