//! Rust <-> QML bridge.
//!
//! - QML creates `AppController`, so it cannot receive constructor
//!   arguments. `gui::run` parks startup state in [`install_bootstrap`] and
//!   QML calls `bootstrap()` once from `Component.onCompleted`.
//! - Invokables run on the Qt GUI thread. Anything that can block runs on a
//!   worker thread and reports back as an [`Event`], drained by `tick()`.
//! - Property names stay snake_case in QML (`controller.daemon_state`);
//!   invokables get camelCase via `cxx_name`.

use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use crossbeam_channel::{Receiver, Sender};
use cxx_qt::CxxQtType;
use cxx_qt_lib::QString;
use serde_json::{json, Map, Value as Json};

use super::single_instance::Activation;
use super::tray::{TrayEvent, TrayHandle, TrayState};
use super::{browse, daemon, desktop, paths, prefs, settings};
use crate::config::Config;
use crate::control::{self, require_daemon, ControlRequest};
use crate::fs::{DaemonStatus, VerFsStats};

/// Startup state handed from `gui::run` to the QML-created controller.
pub struct UiBootstrap {
    pub activations: Receiver<Activation>,
    pub tray: TrayHandle,
    pub runtime: tokio::runtime::Handle,
}

static BOOTSTRAP: OnceLock<Mutex<Option<UiBootstrap>>> = OnceLock::new();

pub fn install_bootstrap(boot: UiBootstrap) {
    let cell = BOOTSTRAP.get_or_init(|| Mutex::new(None));
    *cell.lock().expect("bootstrap lock") = Some(boot);
}

fn take_bootstrap() -> Option<UiBootstrap> {
    BOOTSTRAP
        .get()
        .and_then(|c| c.lock().expect("bootstrap lock").take())
}

/// True until QML has consumed the bootstrap payload (see the watchdog).
pub(crate) fn is_bootstrap_pending() -> bool {
    BOOTSTRAP
        .get()
        .is_none_or(|c| c.lock().expect("bootstrap lock").is_some())
}

#[cxx_qt::bridge]
mod qobject {
    unsafe extern "C++" {
        include!("cxx-qt-lib/qstring.h");
        type QString = cxx_qt_lib::QString;
    }

    unsafe extern "RustQt" {
        #[qobject]
        #[qml_element]
        #[qproperty(bool, setup_visible)]
        #[qproperty(bool, control_visible)]
        #[qproperty(i32, control_tab)]
        #[qproperty(bool, configured)]
        #[qproperty(QString, config_error)]
        #[qproperty(QString, config_path)]
        #[qproperty(QString, mount_point)]
        #[qproperty(QString, data_dir)]
        #[qproperty(QString, home_dir)]
        #[qproperty(QString, suggested_mount)]
        #[qproperty(QString, suggested_data)]
        #[qproperty(QString, daemon_state)]
        #[qproperty(QString, state_detail)]
        #[qproperty(bool, stale_mount)]
        #[qproperty(bool, service_mode)]
        #[qproperty(bool, autostart)]
        #[qproperty(bool, busy)]
        #[qproperty(QString, busy_text)]
        #[qproperty(QString, toast)]
        #[qproperty(bool, toast_error)]
        #[qproperty(i32, toast_serial)]
        #[qproperty(QString, status_json)]
        #[qproperty(QString, stats_json)]
        #[qproperty(bool, stats_loading)]
        #[qproperty(QString, stats_error)]
        #[qproperty(QString, snapshots_json)]
        #[qproperty(bool, snapshots_loading)]
        #[qproperty(QString, settings_json)]
        #[qproperty(QString, setup_settings_json)]
        #[qproperty(bool, restart_needed)]
        #[qproperty(QString, browser_json)]
        #[qproperty(QString, places_json)]
        #[qproperty(QString, vault_key_file)]
        #[qproperty(bool, setup_running)]
        #[qproperty(QString, setup_error)]
        type AppController = super::AppControllerRust;

        #[qinvokable]
        fn bootstrap(self: Pin<&mut Self>);

        #[qinvokable]
        fn tick(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "showControl"]
        fn show_control(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "hideControl"]
        fn hide_control(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "selectTab"]
        fn select_tab(self: Pin<&mut Self>, tab: i32);

        #[qinvokable]
        #[cxx_name = "checkLocations"]
        fn check_locations(self: &AppController, mount: &QString, data: &QString) -> QString;

        #[qinvokable]
        #[cxx_name = "finishSetup"]
        fn finish_setup(
            self: Pin<&mut Self>,
            mount: &QString,
            data: &QString,
            edits_json: &QString,
            use_service: bool,
            autostart: bool,
        );

        #[qinvokable]
        #[cxx_name = "startDaemon"]
        fn start_daemon(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "stopDaemon"]
        fn stop_daemon(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "restartDaemon"]
        fn restart_daemon(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "repairMount"]
        fn repair_mount(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "openMountFolder"]
        fn open_mount_folder(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "openPath"]
        fn open_path(self: Pin<&mut Self>, path: &QString);

        #[qinvokable]
        #[cxx_name = "refreshStats"]
        fn refresh_stats(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "refreshSnapshots"]
        fn refresh_snapshots(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "createSnapshot"]
        fn create_snapshot(self: Pin<&mut Self>, name: &QString);

        #[qinvokable]
        #[cxx_name = "deleteSnapshot"]
        fn delete_snapshot(self: Pin<&mut Self>, name: &QString);

        #[qinvokable]
        #[cxx_name = "openSnapshot"]
        fn open_snapshot(self: Pin<&mut Self>, name: &QString);

        #[qinvokable]
        #[cxx_name = "createVault"]
        fn create_vault(self: Pin<&mut Self>, password: &QString, key_dir: &QString);

        #[qinvokable]
        #[cxx_name = "unlockVault"]
        fn unlock_vault(self: Pin<&mut Self>, password: &QString, key_file: &QString);

        #[qinvokable]
        #[cxx_name = "lockVault"]
        fn lock_vault(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "openVault"]
        fn open_vault(self: Pin<&mut Self>);

        #[qinvokable]
        #[cxx_name = "saveSettings"]
        fn save_settings(self: Pin<&mut Self>, edits_json: &QString);

        #[qinvokable]
        #[cxx_name = "saveLocations"]
        fn save_locations(self: Pin<&mut Self>, mount: &QString, data: &QString);

        #[qinvokable]
        #[cxx_name = "setServiceMode"]
        fn change_service_mode(self: Pin<&mut Self>, enabled: bool);

        #[qinvokable]
        #[cxx_name = "setAutostartEnabled"]
        fn set_autostart_enabled(self: Pin<&mut Self>, enabled: bool);

        #[qinvokable]
        fn browse(self: Pin<&mut Self>, path: &QString, include_files: bool);

        #[qinvokable]
        #[cxx_name = "makeFolder"]
        fn make_folder(self: &AppController, parent: &QString, name: &QString) -> QString;

        #[qinvokable]
        #[cxx_name = "quitApp"]
        fn quit_app(self: Pin<&mut Self>);
    }
}

const PROBE_INTERVAL: Duration = Duration::from_secs(2);
/// The full stats report scans all metadata; keep auto-refresh rare.
const STATS_INTERVAL: Duration = Duration::from_secs(30);

/// Side effects applied on the GUI thread after a worker operation.
enum Effect {
    ReloadConfig,
    RefreshSnapshots,
    RememberKeyFile(PathBuf),
    Quit,
}

struct Outcome {
    message: String,
    effects: Vec<Effect>,
}

impl Outcome {
    fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            effects: Vec::new(),
        }
    }

    fn with(mut self, effect: Effect) -> Self {
        self.effects.push(effect);
        self
    }
}

enum Event {
    Probe(Result<(daemon::Probe, Option<DaemonStatus>), String>),
    OpDone(Result<Outcome, String>),
    Notice(Result<String, String>),
    Stats(Result<VerFsStats, String>),
    Snapshots(Result<Vec<String>, String>),
    Browse(Json),
    SetupDone(Result<Config, String>),
}

struct Runtime {
    activations: Receiver<Activation>,
    tray: TrayHandle,
    handle: tokio::runtime::Handle,
    events_tx: Sender<Event>,
    events_rx: Receiver<Event>,
    config_path: PathBuf,
    config: Option<Config>,
    child: daemon::SharedChild,
    prefs: prefs::Prefs,
    probe_in_flight: bool,
    last_probe: Option<Instant>,
    last_stats: Option<Instant>,
    /// Shown instead of the probed state while a start/stop runs.
    pending_state: Option<&'static str>,
}

#[derive(Default)]
pub struct AppControllerRust {
    setup_visible: bool,
    control_visible: bool,
    control_tab: i32,
    configured: bool,
    config_error: QString,
    config_path: QString,
    mount_point: QString,
    data_dir: QString,
    home_dir: QString,
    suggested_mount: QString,
    suggested_data: QString,
    daemon_state: QString,
    state_detail: QString,
    stale_mount: bool,
    service_mode: bool,
    autostart: bool,
    busy: bool,
    busy_text: QString,
    toast: QString,
    toast_error: bool,
    toast_serial: i32,
    status_json: QString,
    stats_json: QString,
    stats_loading: bool,
    stats_error: QString,
    snapshots_json: QString,
    snapshots_loading: bool,
    settings_json: QString,
    setup_settings_json: QString,
    restart_needed: bool,
    browser_json: QString,
    places_json: QString,
    vault_key_file: QString,
    setup_running: bool,
    setup_error: QString,
    runtime: Option<Runtime>,
}

fn qs(s: impl AsRef<str>) -> QString {
    QString::from(s.as_ref())
}

fn err_text(err: &anyhow::Error) -> String {
    format!("{err:#}")
}

fn spawn_worker(name: &str, tx: Sender<Event>, job: impl FnOnce() -> Event + Send + 'static) -> Result<()> {
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            if tx.send(job()).is_err() {
                tracing::error!("worker result dropped: the UI stopped listening");
            }
        })
        .with_context(|| format!("failed to start worker thread {name}"))?;
    Ok(())
}

/// Local time as `YYYY-MM-DD_HH-MM-SS`, for snapshot names.
fn timestamp_name() -> Result<String> {
    use nix::libc;
    // SAFETY: `time` with a null pointer only returns the current time.
    let now = unsafe { libc::time(std::ptr::null_mut()) };
    // SAFETY: zeroed `tm` is a valid out-parameter for localtime_r.
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    // SAFETY: both pointers are valid for the duration of the call.
    if unsafe { libc::localtime_r(&now, &mut tm) }.is_null() {
        bail!("failed to read the local time");
    }
    Ok(format!(
        "{:04}-{:02}-{:02}_{:02}-{:02}-{:02}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    ))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

fn defaults_for(config: &Config) -> Result<Config> {
    Config::with_defaults(&config.mount_point, &config.data_dir)
}

/// Parses the editor's `{key: value}` JSON.
fn parse_edits(edits_json: &QString) -> Result<Map<String, Json>> {
    match serde_json::from_str(&edits_json.to_string()).context("invalid settings payload")? {
        Json::Object(map) => Ok(map),
        _ => bail!("settings payload is not an object"),
    }
}

fn location_errors(report: &Json) -> Option<String> {
    let mut problems = Vec::new();
    for (key, label) in [("mountError", "VerFSNext folder"), ("dataError", "Data folder")] {
        if let Some(text) = report[key].as_str().filter(|s| !s.is_empty()) {
            problems.push(format!("{label}: {text}"));
        }
    }
    (!problems.is_empty()).then(|| problems.join("\n"))
}

impl qobject::AppController {
    fn rt(&self) -> &Runtime {
        self.rust()
            .runtime
            .as_ref()
            .expect("AppController used before bootstrap()")
    }

    fn rt_mut(self: Pin<&mut Self>) -> &mut Runtime {
        self.rust_mut()
            .get_mut()
            .runtime
            .as_mut()
            .expect("AppController used before bootstrap()")
    }

    fn notify(mut self: Pin<&mut Self>, message: impl AsRef<str>, error: bool) {
        let mut chars = message.as_ref().chars();
        let message = match chars.next() {
            Some(first) => first.to_uppercase().chain(chars).collect::<String>(),
            None => String::new(),
        };
        if error {
            tracing::error!("{message}");
        } else {
            tracing::info!("{message}");
        }
        // With no window open, the in-app toast would go unseen.
        if !*self.control_visible() && !*self.setup_visible() {
            let summary = if error { "VerFSNext needs attention" } else { "VerFSNext" };
            let body = message.clone();
            let tx = self.rt().events_tx.clone();
            let spawned = spawn_worker("notification", tx, move || {
                // Logged, not toasted: a failing toast would notify again.
                if let Err(err) = desktop::send_notification(summary, &body) {
                    tracing::error!("{err:#}");
                }
                Event::Notice(Ok(String::new()))
            });
            if let Err(err) = spawned {
                tracing::error!("{err:#}");
            }
        }
        self.as_mut().set_toast(qs(message));
        self.as_mut().set_toast_error(error);
        let serial = *self.toast_serial() + 1;
        self.as_mut().set_toast_serial(serial);
    }

    fn bootstrap(mut self: Pin<&mut Self>) {
        let Some(boot) = take_bootstrap() else {
            tracing::error!("bootstrap() called twice or without a payload");
            return;
        };
        let setup = || -> Result<(PathBuf, PathBuf, PathBuf)> {
            Ok((paths::config_path()?, paths::home()?, paths::data_home()?))
        };
        let (config_path, home, data_home) = match setup() {
            Ok(v) => v,
            Err(err) => {
                eprintln!("VerFSNext could not start: {err:#}");
                std::process::exit(1);
            }
        };
        let (events_tx, events_rx) = crossbeam_channel::unbounded();
        self.as_mut().rust_mut().get_mut().runtime = Some(Runtime {
            activations: boot.activations,
            tray: boot.tray,
            handle: boot.runtime,
            events_tx,
            events_rx,
            config_path: config_path.clone(),
            config: None,
            child: Arc::new(Mutex::new(None)),
            prefs: prefs::Prefs::default(),
            probe_in_flight: false,
            last_probe: None,
            last_stats: None,
            pending_state: None,
        });

        self.as_mut().set_home_dir(qs(home.to_string_lossy()));
        self.as_mut()
            .set_suggested_mount(qs(home.join("VerFS").to_string_lossy()));
        self.as_mut()
            .set_suggested_data(qs(data_home.join("verfsnext").to_string_lossy()));
        self.as_mut().set_config_path(qs(config_path.to_string_lossy()));
        self.as_mut().set_daemon_state(qs("stopped"));
        self.as_mut().refresh_integration();

        match browse::places() {
            Ok(places) => self.as_mut().set_places_json(qs(places.to_string())),
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }
        match prefs::load() {
            Ok(p) => {
                if let Some(key) = &p.vault_key_file {
                    self.as_mut().set_vault_key_file(qs(key.to_string_lossy()));
                }
                self.as_mut().rt_mut().prefs = p;
            }
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }

        if config_path.is_file() {
            match Config::load_from_file(&config_path) {
                Ok(config) => {
                    self.as_mut().apply_config(config);
                    self.as_mut().start_daemon();
                }
                Err(err) => {
                    self.as_mut().set_config_error(qs(err_text(&err)));
                    self.as_mut().set_control_visible(true);
                }
            }
        } else {
            let model = Config::with_defaults(&home.join("VerFS"), &data_home.join("verfsnext"))
                .and_then(|d| settings::editor_model(&d, &d));
            match model {
                Ok(model) => self.as_mut().set_setup_settings_json(qs(model.to_string())),
                Err(err) => self.as_mut().set_setup_error(qs(err_text(&err))),
            }
            self.as_mut().set_setup_visible(true);
        }
        self.as_mut().update_tray();
        tracing::info!("QML bootstrap complete");
    }

    /// Re-reads which background integrations are installed.
    fn refresh_integration(mut self: Pin<&mut Self>) {
        match daemon::service_installed() {
            Ok(v) => self.as_mut().set_service_mode(v),
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }
        match desktop::autostart_enabled() {
            Ok(v) => self.as_mut().set_autostart(v),
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }
    }

    fn apply_config(mut self: Pin<&mut Self>, config: Config) {
        let model = defaults_for(&config).and_then(|d| settings::editor_model(&config, &d));
        match model {
            Ok(model) => self.as_mut().set_settings_json(qs(model.to_string())),
            Err(err) => self.as_mut().set_config_error(qs(err_text(&err))),
        }
        self.as_mut()
            .set_mount_point(qs(config.mount_point.to_string_lossy()));
        self.as_mut().set_data_dir(qs(config.data_dir.to_string_lossy()));
        self.as_mut().set_configured(true);
        self.as_mut().set_config_error(QString::default());
        self.as_mut().rt_mut().config = Some(config);
    }

    /// Non-blocking drain of every channel. Keep it that way.
    fn tick(mut self: Pin<&mut Self>) {
        if self.rust().runtime.is_none() {
            return;
        }
        let activations = self.rt().activations.try_iter().count();
        if activations > 0 {
            self.as_mut().show_control();
        }
        let tray_events: Vec<TrayEvent> = self.rt().tray.rx.try_iter().collect();
        for ev in tray_events {
            self.as_mut().on_tray(ev);
        }
        let events: Vec<Event> = self.rt().events_rx.try_iter().collect();
        for ev in events {
            self.as_mut().on_event(ev);
        }
        self.as_mut().maybe_probe();
        self.as_mut().maybe_refresh_stats();
        self.as_mut().update_tray();
    }

    fn on_tray(mut self: Pin<&mut Self>, ev: TrayEvent) {
        match ev {
            TrayEvent::OpenControl => self.as_mut().show_control(),
            TrayEvent::OpenFolder => self.as_mut().open_mount_folder(),
            TrayEvent::ToggleRunning => {
                if self.daemon_state().to_string() == "running" {
                    self.as_mut().stop_daemon();
                } else {
                    self.as_mut().start_daemon();
                }
            }
            TrayEvent::TakeSnapshot => match timestamp_name() {
                Ok(name) => self.as_mut().create_snapshot(&qs(name)),
                Err(err) => self.as_mut().notify(err_text(&err), true),
            },
            TrayEvent::Quit => self.as_mut().quit_app(),
        }
    }

    fn on_event(mut self: Pin<&mut Self>, ev: Event) {
        match ev {
            Event::Probe(result) => {
                self.as_mut().rt_mut().probe_in_flight = false;
                match result {
                    Ok((probe, status)) => self.as_mut().apply_probe(probe, status),
                    Err(err) => {
                        self.as_mut().set_daemon_state(qs("failed"));
                        self.as_mut().set_state_detail(qs(err));
                    }
                }
            }
            Event::OpDone(result) => {
                self.as_mut().rt_mut().pending_state = None;
                self.as_mut().set_busy(false);
                self.as_mut().set_busy_text(QString::default());
                // Show the new state right away instead of on the next tick.
                self.as_mut().rt_mut().last_probe = None;
                match result {
                    Ok(outcome) => {
                        if !outcome.message.is_empty() {
                            self.as_mut().notify(&outcome.message, false);
                        }
                        for effect in outcome.effects {
                            self.as_mut().apply_effect(effect);
                        }
                    }
                    Err(err) => self.as_mut().notify(err, true),
                }
            }
            Event::Notice(result) => match result {
                Ok(msg) if msg.is_empty() => {}
                Ok(msg) => self.as_mut().notify(msg, false),
                Err(err) => self.as_mut().notify(err, true),
            },
            Event::Stats(result) => {
                self.as_mut().set_stats_loading(false);
                self.as_mut().rt_mut().last_stats = Some(Instant::now());
                match result {
                    Ok(stats) => {
                        let mut value = json!(stats);
                        value["updatedAtMs"] = json!(now_ms());
                        self.as_mut().set_stats_json(qs(value.to_string()));
                        self.as_mut().set_stats_error(QString::default());
                    }
                    Err(err) => self.as_mut().set_stats_error(qs(err)),
                }
            }
            Event::Snapshots(result) => {
                self.as_mut().set_snapshots_loading(false);
                match result {
                    Ok(names) => self.as_mut().set_snapshots_json(qs(json!(names).to_string())),
                    Err(err) => self.as_mut().notify(err, true),
                }
            }
            Event::Browse(listing) => self.as_mut().set_browser_json(qs(listing.to_string())),
            Event::SetupDone(result) => {
                self.as_mut().rt_mut().pending_state = None;
                self.as_mut().rt_mut().last_probe = None;
                self.as_mut().set_setup_running(false);
                match result {
                    Ok(config) => {
                        let mount = config.mount_point.to_string_lossy().into_owned();
                        self.as_mut().apply_config(config);
                        self.as_mut().refresh_integration();
                        self.as_mut().set_setup_visible(false);
                        self.as_mut().notify(
                            format!("VerFSNext is ready. Your folder is {mount}"),
                            false,
                        );
                    }
                    Err(err) => {
                        tracing::error!("setup failed: {err}");
                        self.as_mut().set_setup_error(qs(err));
                    }
                }
            }
        }
    }

    fn apply_effect(mut self: Pin<&mut Self>, effect: Effect) {
        match effect {
            Effect::ReloadConfig => {
                let path = self.rt().config_path.clone();
                match Config::load_from_file(&path) {
                    Ok(config) => self.as_mut().apply_config(config),
                    Err(err) => self.as_mut().set_config_error(qs(err_text(&err))),
                }
                self.as_mut().refresh_integration();
            }
            Effect::RefreshSnapshots => self.as_mut().refresh_snapshots(),
            Effect::RememberKeyFile(path) => {
                self.as_mut()
                    .set_vault_key_file(qs(path.to_string_lossy()));
                let updated = {
                    let rt = self.as_mut().rt_mut();
                    rt.prefs.vault_key_file = Some(path);
                    rt.prefs.clone()
                };
                if let Err(err) = prefs::save(&updated) {
                    self.as_mut().notify(err_text(&err), true);
                }
            }
            Effect::Quit => std::process::exit(0),
        }
    }

    fn apply_probe(mut self: Pin<&mut Self>, probe: daemon::Probe, status: Option<DaemonStatus>) {
        let pending = self.rt().pending_state;
        let state = pending.unwrap_or(probe.state);
        let was_running = self.daemon_state().to_string() == "running";
        self.as_mut().set_daemon_state(qs(state));
        self.as_mut().set_state_detail(qs(&probe.detail));
        self.as_mut().set_stale_mount(probe.stale_mount);
        match status {
            Some(status) => self.as_mut().set_status_json(qs(json!(status).to_string())),
            None => self.as_mut().set_status_json(QString::default()),
        }
        if state == "running" && !was_running {
            self.as_mut().rt_mut().last_stats = None;
            self.as_mut().refresh_snapshots();
        }
        if state != "running" {
            self.as_mut().set_restart_needed(false);
        }
    }

    fn maybe_probe(mut self: Pin<&mut Self>) {
        let rt = self.as_mut().rt_mut();
        let Some(config) = rt.config.clone() else {
            return;
        };
        if rt.probe_in_flight || rt.last_probe.is_some_and(|t| t.elapsed() < PROBE_INTERVAL) {
            return;
        }
        rt.probe_in_flight = true;
        rt.last_probe = Some(Instant::now());
        let child = rt.child.clone();
        let handle = rt.handle.clone();
        let tx = rt.events_tx.clone();
        let spawned = spawn_worker("probe", tx, move || {
            let result = (|| -> Result<_> {
                let probe = daemon::probe(&config, &child)?;
                let status = if probe.state == "running" {
                    handle
                        .block_on(require_daemon(&config, &ControlRequest::Status))?
                        .status
                } else {
                    None
                };
                Ok((probe, status))
            })();
            Event::Probe(result.map_err(|e| err_text(&e)))
        });
        if let Err(err) = spawned {
            self.as_mut().rt_mut().probe_in_flight = false;
            self.as_mut().notify(err_text(&err), true);
        }
    }

    fn maybe_refresh_stats(mut self: Pin<&mut Self>) {
        if !*self.control_visible()
            || *self.control_tab() != 0
            || *self.stats_loading()
            || self.daemon_state().to_string() != "running"
            || self.rt().last_stats.is_some_and(|t| t.elapsed() < STATS_INTERVAL)
        {
            return;
        }
        self.as_mut().refresh_stats();
    }

    fn update_tray(mut self: Pin<&mut Self>) {
        let state = self.daemon_state().to_string();
        let configured = *self.configured();
        let running = state == "running";
        let status_line = if !configured {
            "VerFSNext · Not set up yet".to_owned()
        } else {
            let label = match state.as_str() {
                "running" => "Running",
                "starting" => "Starting…",
                "stopping" => "Stopping…",
                "failed" => "Needs attention",
                _ => "Stopped",
            };
            format!("VerFSNext · {label}")
        };
        let quit_label = if *self.service_mode() {
            "Quit (folder stays available)"
        } else if running {
            "Quit and Disconnect Folder"
        } else {
            "Quit"
        };
        let tray_state = TrayState {
            running,
            configured,
            status_line,
            toggle_label: if running {
                "Stop VerFSNext".into()
            } else {
                "Start VerFSNext".into()
            },
            toggle_enabled: configured && !*self.busy() && (running || state != "starting"),
            quit_label: quit_label.into(),
        };
        self.as_mut().rt_mut().tray.update(tray_state);
    }

    fn show_control(mut self: Pin<&mut Self>) {
        if *self.setup_visible() || (!*self.configured() && self.config_error().is_empty()) {
            self.as_mut().set_setup_visible(false);
            self.as_mut().set_setup_visible(true);
            return;
        }
        self.as_mut().set_control_visible(false);
        self.as_mut().set_control_visible(true);
        if *self.control_tab() == 0 {
            self.as_mut().rt_mut().last_stats = None;
        }
    }

    fn hide_control(mut self: Pin<&mut Self>) {
        self.as_mut().set_control_visible(false);
    }

    fn select_tab(mut self: Pin<&mut Self>, tab: i32) {
        self.as_mut().set_control_tab(tab);
        match tab {
            0 => self.as_mut().rt_mut().last_stats = None,
            1 => self.as_mut().refresh_snapshots(),
            _ => {}
        }
    }

    /// Runs `job` on a worker as the one user-visible operation in flight.
    fn run_op(
        mut self: Pin<&mut Self>,
        busy_text: &str,
        pending_state: Option<&'static str>,
        job: impl FnOnce(OpContext) -> Result<Outcome> + Send + 'static,
    ) {
        if *self.busy() {
            let message = format!("Please wait: {}", self.busy_text());
            self.as_mut().notify(message, true);
            return;
        }
        let rt = self.as_mut().rt_mut();
        let Some(config) = rt.config.clone() else {
            self.as_mut().notify("VerFSNext is not set up yet.", true);
            return;
        };
        let ctx = OpContext {
            config,
            config_path: rt.config_path.clone(),
            child: rt.child.clone(),
            handle: rt.handle.clone(),
        };
        let tx = rt.events_tx.clone();
        rt.pending_state = pending_state;
        let spawned = spawn_worker("operation", tx, move || {
            Event::OpDone(job(ctx).map_err(|e| err_text(&e)))
        });
        match spawned {
            Ok(()) => {
                self.as_mut().set_busy(true);
                self.as_mut().set_busy_text(qs(busy_text));
                if let Some(state) = pending_state {
                    self.as_mut().set_daemon_state(qs(state));
                }
            }
            Err(err) => {
                self.as_mut().rt_mut().pending_state = None;
                self.as_mut().notify(err_text(&err), true);
            }
        }
    }

    /// Runs `job` on a worker without blocking other operations.
    fn run_background(
        mut self: Pin<&mut Self>,
        job: impl FnOnce() -> Result<String> + Send + 'static,
    ) {
        let tx = self.rt().events_tx.clone();
        if let Err(err) = spawn_worker("background", tx, move || {
            Event::Notice(job().map_err(|e| err_text(&e)))
        }) {
            self.as_mut().notify(err_text(&err), true);
        }
    }

    fn check_locations(&self, mount: &QString, data: &QString) -> QString {
        let current = self.rust().runtime.as_ref().and_then(|rt| rt.config.as_ref());
        qs(daemon::check_locations(&mount.to_string(), &data.to_string(), current).to_string())
    }

    fn finish_setup(
        mut self: Pin<&mut Self>,
        mount: &QString,
        data: &QString,
        edits_json: &QString,
        use_service: bool,
        autostart: bool,
    ) {
        if *self.setup_running() {
            return;
        }
        let mount = PathBuf::from(mount.to_string().trim());
        let data = PathBuf::from(data.to_string().trim());
        let edits = match parse_edits(edits_json) {
            Ok(edits) => edits,
            Err(err) => {
                self.as_mut().set_setup_error(qs(err_text(&err)));
                return;
            }
        };
        let rt = self.as_mut().rt_mut();
        let config_path = rt.config_path.clone();
        let child = rt.child.clone();
        let tx = rt.events_tx.clone();
        let spawned = spawn_worker("setup", tx, move || {
            Event::SetupDone(
                run_setup(&mount, &data, &edits, use_service, autostart, &config_path, &child)
                    .map_err(|e| err_text(&e)),
            )
        });
        match spawned {
            Ok(()) => {
                self.as_mut().set_setup_error(QString::default());
                self.as_mut().set_setup_running(true);
            }
            Err(err) => self.as_mut().set_setup_error(qs(err_text(&err))),
        }
    }

    fn start_daemon(mut self: Pin<&mut Self>) {
        let service = *self.service_mode();
        self.as_mut()
            .run_op("Starting VerFSNext…", Some("starting"), move |ctx| {
                if service {
                    let exe = paths::current_exe()?;
                    if daemon::refresh_service_unit(&exe, &ctx.config_path)? {
                        tracing::info!("user service now points at {}", exe.display());
                    }
                }
                daemon::start(&ctx.config, &ctx.config_path, &ctx.child)?;
                Ok(Outcome::message(""))
            });
    }

    fn stop_daemon(mut self: Pin<&mut Self>) {
        self.as_mut().run_op(
            "Stopping VerFSNext (saving everything to disk)…",
            Some("stopping"),
            |ctx| {
                daemon::stop(&ctx.config, &ctx.child)?;
                Ok(Outcome::message("VerFSNext stopped. Everything was saved."))
            },
        );
    }

    fn restart_daemon(mut self: Pin<&mut Self>) {
        self.as_mut()
            .run_op("Restarting VerFSNext…", Some("stopping"), |ctx| {
                daemon::stop(&ctx.config, &ctx.child)?;
                daemon::start(&ctx.config, &ctx.config_path, &ctx.child)?;
                Ok(Outcome::message("VerFSNext restarted with your new settings."))
            });
        self.as_mut().set_restart_needed(false);
    }

    fn repair_mount(mut self: Pin<&mut Self>) {
        self.as_mut().run_op("Repairing the folder…", None, |ctx| {
            daemon::repair_mount(&ctx.config.mount_point)?;
            Ok(Outcome::message("Folder repaired."))
        });
    }

    fn open_mount_folder(mut self: Pin<&mut Self>) {
        let path = self.mount_point().to_string();
        self.as_mut().open_path(&qs(path));
    }

    fn open_path(mut self: Pin<&mut Self>, path: &QString) {
        let path = PathBuf::from(path.to_string());
        self.as_mut().run_background(move || {
            daemon::open_path(&path)?;
            Ok(String::new())
        });
    }

    fn refresh_stats(mut self: Pin<&mut Self>) {
        if *self.stats_loading() {
            return;
        }
        let rt = self.as_mut().rt_mut();
        let Some(config) = rt.config.clone() else {
            return;
        };
        let handle = rt.handle.clone();
        let tx = rt.events_tx.clone();
        let spawned = spawn_worker("stats", tx, move || {
            let result = handle
                .block_on(require_daemon(&config, &ControlRequest::Stats))
                .and_then(|resp| resp.stats.context("the filesystem did not return statistics"));
            Event::Stats(result.map_err(|e| err_text(&e)))
        });
        match spawned {
            Ok(()) => self.as_mut().set_stats_loading(true),
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }
    }

    fn refresh_snapshots(mut self: Pin<&mut Self>) {
        if *self.snapshots_loading() || self.daemon_state().to_string() != "running" {
            return;
        }
        let rt = self.as_mut().rt_mut();
        let Some(config) = rt.config.clone() else {
            return;
        };
        let handle = rt.handle.clone();
        let tx = rt.events_tx.clone();
        let spawned = spawn_worker("snapshots", tx, move || {
            let result = handle
                .block_on(require_daemon(&config, &ControlRequest::SnapshotList))
                .map(|resp| resp.names);
            Event::Snapshots(result.map_err(|e| err_text(&e)))
        });
        match spawned {
            Ok(()) => self.as_mut().set_snapshots_loading(true),
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }
    }

    fn create_snapshot(mut self: Pin<&mut Self>, name: &QString) {
        let name = name.to_string().trim().to_owned();
        if name.is_empty() || name.contains('/') {
            self.as_mut()
                .notify("Give the snapshot a name without slashes.", true);
            return;
        }
        self.as_mut()
            .run_op("Taking a snapshot…", None, move |ctx| {
                ctx.request(ControlRequest::SnapshotCreate { name: name.clone() })?;
                Ok(Outcome::message(format!("Snapshot \"{name}\" saved."))
                    .with(Effect::RefreshSnapshots))
            });
    }

    fn delete_snapshot(mut self: Pin<&mut Self>, name: &QString) {
        let name = name.to_string();
        self.as_mut()
            .run_op("Deleting the snapshot…", None, move |ctx| {
                ctx.request(ControlRequest::SnapshotDelete { name: name.clone() })?;
                Ok(Outcome::message(format!(
                    "Snapshot \"{name}\" deleted. Its space is reclaimed during the next cleanup."
                ))
                .with(Effect::RefreshSnapshots))
            });
    }

    fn open_snapshot(mut self: Pin<&mut Self>, name: &QString) {
        let path = PathBuf::from(self.mount_point().to_string())
            .join(".snapshots")
            .join(name.to_string());
        self.as_mut().open_path(&qs(path.to_string_lossy()));
    }

    fn create_vault(mut self: Pin<&mut Self>, password: &QString, key_dir: &QString) {
        let password = password.to_string();
        let key_dir = PathBuf::from(key_dir.to_string().trim());
        if !key_dir.is_absolute() {
            self.as_mut()
                .notify("Choose a folder for the key file.", true);
            return;
        }
        self.as_mut()
            .run_op("Creating your vault…", None, move |ctx| {
                let key_file = ctx
                    .handle
                    .block_on(control::create_vault(&ctx.config, &password, Some(&key_dir)))?;
                Ok(Outcome::message(format!(
                    "Vault created. Your key file is {}. Keep a copy somewhere safe: without it the vault can't be opened.",
                    key_file.display()
                ))
                .with(Effect::RememberKeyFile(key_file)))
            });
    }

    fn unlock_vault(mut self: Pin<&mut Self>, password: &QString, key_file: &QString) {
        let password = password.to_string();
        let key_file = PathBuf::from(key_file.to_string().trim());
        self.as_mut()
            .run_op("Unlocking your vault…", None, move |ctx| {
                ctx.handle
                    .block_on(control::unlock_vault(&ctx.config, &password, &key_file))?;
                Ok(Outcome::message("Vault unlocked. It stays open until you lock it or VerFSNext stops.")
                    .with(Effect::RememberKeyFile(key_file)))
            });
    }

    fn lock_vault(mut self: Pin<&mut Self>) {
        self.as_mut().run_op("Locking your vault…", None, |ctx| {
            ctx.request(ControlRequest::VaultLock)?;
            Ok(Outcome::message("Vault locked."))
        });
    }

    fn open_vault(mut self: Pin<&mut Self>) {
        let path = PathBuf::from(self.mount_point().to_string()).join(".vault");
        self.as_mut().open_path(&qs(path.to_string_lossy()));
    }

    fn save_settings(mut self: Pin<&mut Self>, edits_json: &QString) {
        let result = (|| -> Result<Config> {
            let edits = parse_edits(edits_json)?;
            let rt = self.rt();
            let base = rt.config.as_ref().context("VerFSNext is not set up yet")?;
            let config = settings::apply_edits(base, &edits, false)?;
            settings::write_config(&config, &rt.config_path)?;
            Ok(config)
        })();
        match result {
            Ok(config) => {
                self.as_mut().apply_config(config);
                if self.daemon_state().to_string() == "running" {
                    self.as_mut().set_restart_needed(true);
                    self.as_mut()
                        .notify("Settings saved. Restart VerFSNext to use them.", false);
                } else {
                    self.as_mut().notify("Settings saved.", false);
                }
            }
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }
    }

    fn save_locations(mut self: Pin<&mut Self>, mount: &QString, data: &QString) {
        let report = daemon::check_locations(
            &mount.to_string(),
            &data.to_string(),
            self.rt().config.as_ref(),
        );
        if let Some(problems) = location_errors(&report) {
            self.as_mut().notify(problems, true);
            return;
        }
        let mount = PathBuf::from(mount.to_string().trim());
        let data = PathBuf::from(data.to_string().trim());
        self.as_mut()
            .run_op("Moving to the new folders…", Some("stopping"), move |ctx| {
                let was_running = daemon::socket_live(&ctx.config.control_socket_path());
                let mut config = ctx.config.clone();
                config.mount_point = mount;
                config.data_dir = data;
                config.validate()?;
                if was_running {
                    daemon::stop(&ctx.config, &ctx.child)?;
                }
                for dir in [&config.mount_point, &config.data_dir] {
                    std::fs::create_dir_all(dir)
                        .with_context(|| format!("failed to create {}", dir.display()))?;
                }
                settings::write_config(&config, &ctx.config_path)?;
                if was_running {
                    daemon::start(&config, &ctx.config_path, &ctx.child)?;
                }
                Ok(Outcome::message("Folders updated.").with(Effect::ReloadConfig))
            });
    }

    fn change_service_mode(mut self: Pin<&mut Self>, enabled: bool) {
        if enabled == *self.service_mode() {
            return;
        }
        let busy = if enabled {
            "Setting up the background service…"
        } else {
            "Removing the background service…"
        };
        self.as_mut().run_op(busy, None, move |ctx| {
            let was_running = daemon::socket_live(&ctx.config.control_socket_path());
            if enabled {
                // Register first: if systemd refuses, nothing was stopped.
                daemon::install_service(&paths::current_exe()?, &ctx.config_path)?;
                if was_running {
                    daemon::stop_app_mode(&ctx.config, &ctx.child)?;
                }
            } else {
                if was_running {
                    daemon::stop_service()?;
                }
                daemon::remove_service()?;
            }
            if was_running {
                daemon::start(&ctx.config, &ctx.config_path, &ctx.child)?;
            }
            Ok(Outcome::message(if enabled {
                "VerFSNext now runs in the background and starts when you log in."
            } else {
                "VerFSNext now runs only while this app is open."
            })
            .with(Effect::ReloadConfig))
        });
    }

    fn set_autostart_enabled(mut self: Pin<&mut Self>, enabled: bool) {
        let result = paths::current_exe().and_then(|exe| desktop::set_autostart(&exe, enabled));
        match result {
            Ok(()) => self.as_mut().set_autostart(enabled),
            Err(err) => self.as_mut().notify(err_text(&err), true),
        }
    }

    fn browse(mut self: Pin<&mut Self>, path: &QString, include_files: bool) {
        let requested = PathBuf::from(path.to_string());
        let tx = self.rt().events_tx.clone();
        let spawned = spawn_worker("browse", tx, move || {
            // A typed path may not exist yet: open the closest folder that does.
            let path = requested
                .ancestors()
                .find(|p| p.is_dir())
                .map(Path::to_path_buf)
                .unwrap_or(requested);
            Event::Browse(browse::list(&path, include_files).unwrap_or_else(|err| {
                json!({
                    "path": path.to_string_lossy(),
                    "parent": path.parent().map(|p| p.to_string_lossy().into_owned()),
                    "crumbs": [],
                    "entries": [],
                    "error": err_text(&err),
                })
            }))
        });
        if let Err(err) = spawned {
            self.as_mut().notify(err_text(&err), true);
        }
    }

    fn make_folder(&self, parent: &QString, name: &QString) -> QString {
        let result = browse::make_folder(Path::new(&parent.to_string()), &name.to_string());
        qs(match result {
            Ok(path) => json!({ "path": path.to_string_lossy(), "error": "" }),
            Err(err) => json!({ "path": "", "error": err_text(&err) }),
        }
        .to_string())
    }

    fn quit_app(mut self: Pin<&mut Self>) {
        let has_child = self
            .rt()
            .child
            .lock()
            .expect("child lock poisoned")
            .is_some();
        // App mode: the folder goes away with the app, even when the daemon
        // was started by an earlier app session.
        let running = self.daemon_state().to_string() == "running";
        if *self.service_mode() || (!has_child && !running) {
            std::process::exit(0);
        }
        self.as_mut().run_op(
            "Disconnecting your folder (saving everything to disk)…",
            Some("stopping"),
            |ctx| {
                daemon::stop(&ctx.config, &ctx.child)?;
                Ok(Outcome::message("").with(Effect::Quit))
            },
        );
    }
}

struct OpContext {
    config: Config,
    config_path: PathBuf,
    child: daemon::SharedChild,
    handle: tokio::runtime::Handle,
}

impl OpContext {
    fn request(&self, req: ControlRequest) -> Result<()> {
        self.handle.block_on(require_daemon(&self.config, &req))?;
        Ok(())
    }
}

/// First-run setup: folders, config file, background mode, menu entries,
/// then start and wait until the folder is mounted.
fn run_setup(
    mount: &Path,
    data: &Path,
    edits: &Map<String, Json>,
    use_service: bool,
    autostart: bool,
    config_path: &Path,
    child: &daemon::SharedChild,
) -> Result<Config> {
    let report = daemon::check_locations(
        &mount.to_string_lossy(),
        &data.to_string_lossy(),
        None,
    );
    if let Some(problems) = location_errors(&report) {
        bail!(problems);
    }
    for dir in [mount, data] {
        std::fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))?;
    }
    let defaults = Config::with_defaults(mount, data)?;
    let config = settings::apply_edits(&defaults, edits, true)?;
    settings::write_config(&config, config_path)?;

    let exe = paths::current_exe()?;
    if use_service {
        daemon::install_service(&exe, config_path)?;
    } else if daemon::service_installed()? {
        daemon::remove_service()?;
    }
    desktop::write_menu_entry(&exe)?;
    desktop::set_autostart(&exe, autostart)?;
    daemon::start(&config, config_path, child)?;
    Ok(config)
}
