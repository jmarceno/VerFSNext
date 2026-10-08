//! Friendly view of `config.toml` for the settings editor.
//!
//! Every config key except the two paths has one [`FieldMeta`] entry here;
//! building the editor model or writing the file fails loudly when a key
//! has no entry, so a new config option cannot silently go missing from the
//! app. Values are shown in friendly units (`scale`): e.g. bytes as KiB.

use std::path::Path;

use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value as Json};
use toml::Value;

use crate::config::Config;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Bool,
    /// Unsigned integer in the file, shown divided by `scale`.
    Unsigned,
    /// Float in the file, shown divided by `scale`.
    Float,
    Text,
    /// zstd level, shown as a slider.
    Level,
}

struct FieldMeta {
    key: &'static str,
    group: &'static str,
    label: &'static str,
    help: &'static str,
    kind: Kind,
    scale: f64,
    unit: &'static str,
    advanced: bool,
    /// Can only be chosen before the first start (changing it later needs
    /// an offline migration or would orphan existing files).
    setup_only: bool,
}

const fn field(
    key: &'static str,
    group: &'static str,
    label: &'static str,
    help: &'static str,
    kind: Kind,
) -> FieldMeta {
    FieldMeta {
        key,
        group,
        label,
        help,
        kind,
        scale: 1.0,
        unit: "",
        advanced: false,
        setup_only: false,
    }
}

impl FieldMeta {
    const fn unit(mut self, unit: &'static str, scale: f64) -> Self {
        self.unit = unit;
        self.scale = scale;
        self
    }

    const fn advanced(mut self) -> Self {
        self.advanced = true;
        self
    }

    const fn setup_only(mut self) -> Self {
        self.setup_only = true;
        self
    }
}

const SPACE: &str = "Space saving";
const PERF: &str = "Speed and memory";
const CLEANUP: &str = "Automatic cleanup";
const ACCESS: &str = "Access";
const VAULT: &str = "Encrypted vault";
const FS: &str = "Filesystem internals";

const FIELDS: &[FieldMeta] = &[
    field(
        "zstd_compression_level",
        SPACE,
        "Compression",
        "Lower is faster, higher squeezes files smaller. The default favors speed.",
        Kind::Level,
    ),
    field(
        "ultracdc_min_size_bytes",
        SPACE,
        "Smallest piece",
        "Files are cut into pieces so identical pieces are stored only once. Smaller pieces find more duplicates but need more bookkeeping.",
        Kind::Unsigned,
    )
    .unit("KiB", 1024.0)
    .advanced(),
    field(
        "ultracdc_avg_size_bytes",
        SPACE,
        "Typical piece",
        "Target size of the pieces files are cut into.",
        Kind::Unsigned,
    )
    .unit("KiB", 1024.0)
    .advanced(),
    field(
        "ultracdc_max_size_bytes",
        SPACE,
        "Largest piece",
        "Upper limit for the pieces files are cut into.",
        Kind::Unsigned,
    )
    .unit("KiB", 1024.0)
    .advanced(),
    field(
        "pack_max_size_mb",
        SPACE,
        "Storage file size",
        "Pieces are stored together in container files of up to this size. It can only be chosen before the first start.",
        Kind::Unsigned,
    )
    .unit("MB", 1.0)
    .advanced()
    .setup_only(),
    field(
        "chunk_cache_capacity_mb",
        PERF,
        "Memory for caching",
        "More memory makes opening the same files again faster.",
        Kind::Unsigned,
    )
    .unit("MB", 1.0),
    field(
        "sync_interval_ms",
        PERF,
        "Save to disk every",
        "How often recent changes are made safe on disk.",
        Kind::Unsigned,
    )
    .unit("seconds", 1000.0),
    field(
        "metadata_cache_capacity_entries",
        PERF,
        "File information cache",
        "How many file records are kept in memory.",
        Kind::Unsigned,
    )
    .unit("entries", 1.0)
    .advanced(),
    field(
        "pack_index_cache_capacity_entries",
        PERF,
        "Storage index cache",
        "How many storage index records are kept in memory.",
        Kind::Unsigned,
    )
    .unit("entries", 1.0)
    .advanced(),
    field(
        "batch_max_size_mb",
        PERF,
        "Write batch size",
        "How much new data is gathered before it is processed.",
        Kind::Unsigned,
    )
    .unit("MB", 1.0)
    .advanced(),
    field(
        "batch_flush_interval_ms",
        PERF,
        "Write batch delay",
        "Longest time new data waits to be processed.",
        Kind::Unsigned,
    )
    .unit("ms", 1.0)
    .advanced(),
    field(
        "gc_idle_min_ms",
        CLEANUP,
        "Clean up after being idle for",
        "Space from deleted files is reclaimed once the folder has been quiet this long.",
        Kind::Unsigned,
    )
    .unit("seconds", 1000.0),
    field(
        "gc_pack_rewrite_min_reclaim_percent",
        CLEANUP,
        "Compact a storage file when unused space reaches",
        "Lower values reclaim space sooner but rewrite more data.",
        Kind::Float,
    )
    .unit("%", 1.0),
    field(
        "gc_pack_rewrite_min_reclaim_bytes",
        CLEANUP,
        "Compact only when at least",
        "Minimum amount of space a compaction must free.",
        Kind::Unsigned,
    )
    .unit("MiB", 1024.0 * 1024.0)
    .advanced(),
    field(
        "gc_discard_filename",
        CLEANUP,
        "Cleanup journal name",
        "Name of the file that tracks space waiting to be reclaimed. It can only be chosen before the first start.",
        Kind::Text,
    )
    .advanced()
    .setup_only(),
    field(
        "fuse_allow_other",
        ACCESS,
        "Let other people on this computer open the folder",
        "Off: only you can see the files. On: other accounts can too, following normal file permissions. Needs \"user_allow_other\" in /etc/fuse.conf (a one-time administrator change).",
        Kind::Bool,
    ),
    field(
        "fuse_fsname",
        ACCESS,
        "Name shown by file managers",
        "How the folder is labeled by mount tools and file managers.",
        Kind::Text,
    )
    .advanced(),
    field(
        "fuse_subtype",
        ACCESS,
        "Filesystem type label",
        "Driver name reported to the system.",
        Kind::Text,
    )
    .advanced(),
    field(
        "vault_enabled",
        VAULT,
        "Allow an encrypted vault",
        "Lets you keep a password-protected .vault folder inside VerFSNext.",
        Kind::Bool,
    ),
    field(
        "vault_argon2_mem_kib",
        VAULT,
        "Password protection memory",
        "Memory used to turn your password into a key. Applies to vaults created afterwards.",
        Kind::Unsigned,
    )
    .unit("MiB", 1024.0)
    .advanced(),
    field(
        "vault_argon2_iters",
        VAULT,
        "Password protection rounds",
        "More rounds make guessing your password slower. Applies to vaults created afterwards.",
        Kind::Unsigned,
    )
    .advanced(),
    field(
        "vault_argon2_parallelism",
        VAULT,
        "Password protection threads",
        "Applies to vaults created afterwards.",
        Kind::Unsigned,
    )
    .advanced(),
    field(
        "fuse_max_write_bytes",
        FS,
        "Largest single write",
        "Biggest write request accepted from the system at once.",
        Kind::Unsigned,
    )
    .unit("KiB", 1024.0)
    .advanced(),
    field(
        "fuse_direct_io",
        FS,
        "Bypass the system cache",
        "Not recommended: makes everything much slower.",
        Kind::Bool,
    )
    .advanced(),
    field(
        "fuse_attr_ttl_ms",
        FS,
        "File attribute cache time",
        "How long the system may reuse file details before asking again.",
        Kind::Unsigned,
    )
    .unit("ms", 1.0)
    .advanced(),
    field(
        "fuse_entry_ttl_ms",
        FS,
        "Folder entry cache time",
        "How long the system may reuse folder listings before asking again.",
        Kind::Unsigned,
    )
    .unit("ms", 1.0)
    .advanced(),
];

const PATH_KEYS: [&str; 2] = ["mount_point", "data_dir"];

fn meta(key: &str) -> Result<&'static FieldMeta> {
    FIELDS
        .iter()
        .find(|f| f.key == key)
        .with_context(|| format!("config option '{key}' has no settings entry"))
}

fn to_table(config: &Config) -> Result<toml::Table> {
    let Value::Table(table) = Value::try_from(config).context("failed to serialize config")? else {
        bail!("config did not serialize to a table");
    };
    for key in table.keys() {
        if !PATH_KEYS.contains(&key.as_str()) {
            meta(key)?;
        }
    }
    for field in FIELDS {
        if !table.contains_key(field.key) {
            bail!("settings entry '{}' is not a config option", field.key);
        }
    }
    Ok(table)
}

fn display_value(field: &FieldMeta, value: &Value) -> Result<Json> {
    Ok(match (field.kind, value) {
        (Kind::Bool, Value::Boolean(b)) => json!(b),
        (Kind::Text, Value::String(s)) => json!(s),
        (Kind::Level, Value::Integer(i)) => json!(i),
        (Kind::Unsigned, Value::Integer(i)) => json!(*i as f64 / field.scale),
        (Kind::Float, Value::Float(f)) => json!(f / field.scale),
        (Kind::Float, Value::Integer(i)) => json!(*i as f64 / field.scale),
        _ => bail!("unexpected value {value} for '{}'", field.key),
    })
}

/// Editor model: one object per field, in display order.
pub fn editor_model(current: &Config, defaults: &Config) -> Result<Json> {
    let table = to_table(current)?;
    let default_table = to_table(defaults)?;
    let mut rows = Vec::with_capacity(FIELDS.len());
    for field in FIELDS {
        let kind = match field.kind {
            Kind::Bool => "bool",
            Kind::Unsigned | Kind::Float => "number",
            Kind::Text => "text",
            Kind::Level => "level",
        };
        rows.push(json!({
            "key": field.key,
            "group": field.group,
            "label": field.label,
            "help": field.help,
            "kind": kind,
            "unit": field.unit,
            "advanced": field.advanced,
            "setupOnly": field.setup_only,
            "value": display_value(field, &table[field.key])?,
            "defaultValue": display_value(field, &default_table[field.key])?,
        }));
    }
    Ok(Json::Array(rows))
}

/// Applies editor values (`{key: display value}`) on top of `base`.
/// `setup` allows changing setup-only fields.
pub fn apply_edits(base: &Config, edits: &Map<String, Json>, setup: bool) -> Result<Config> {
    let mut table = to_table(base)?;
    for (key, value) in edits {
        let field = meta(key)?;
        let new = match field.kind {
            Kind::Bool => Value::Boolean(
                value
                    .as_bool()
                    .with_context(|| format!("{}: expected on or off", field.label))?,
            ),
            Kind::Text => Value::String(
                value
                    .as_str()
                    .with_context(|| format!("{}: expected text", field.label))?
                    .trim()
                    .to_owned(),
            ),
            Kind::Level => Value::Integer(
                value
                    .as_i64()
                    .with_context(|| format!("{}: expected a whole number", field.label))?,
            ),
            Kind::Unsigned => {
                let shown = number(field, value)?;
                if shown < 0.0 {
                    bail!("{} can't be negative", field.label);
                }
                Value::Integer((shown * field.scale).round() as i64)
            }
            Kind::Float => Value::Float(number(field, value)? * field.scale),
        };
        if field.setup_only && !setup && table[field.key] != new {
            bail!("{} can only be chosen before the first start", field.label);
        }
        table.insert(key.clone(), new);
    }
    let config: Config = toml::Value::Table(table)
        .try_into()
        .map_err(|err: toml::de::Error| anyhow::anyhow!(friendly(&err.to_string())))?;
    config
        .validate()
        .map_err(|err| anyhow::anyhow!(friendly(&err.to_string())))?;
    Ok(config)
}

fn number(field: &FieldMeta, value: &Json) -> Result<f64> {
    let n = value
        .as_f64()
        .with_context(|| format!("{}: expected a number", field.label))?;
    if !n.is_finite() {
        bail!("{}: expected a number", field.label);
    }
    Ok(n)
}

/// Replaces config keys in a validation message with their friendly labels.
fn friendly(message: &str) -> String {
    let mut fields: Vec<&FieldMeta> = FIELDS.iter().collect();
    fields.sort_by_key(|f| std::cmp::Reverse(f.key.len()));
    let mut out = message.to_owned();
    for field in fields {
        out = out.replace(field.key, &format!("\"{}\"", field.label));
    }
    out
}

/// Writes `config` as a commented TOML file, atomically.
pub fn write_config(config: &Config, path: &Path) -> Result<()> {
    let table = to_table(config)?;
    let mut out = String::from(
        "# VerFSNext configuration, written by the VerFSNext app.\n\
         # Edit it from the app (Settings) or by hand while VerFSNext is stopped.\n\n",
    );
    out.push_str(&format!(
        "mount_point = {} # Folder where your files appear.\n",
        table["mount_point"]
    ));
    out.push_str(&format!(
        "data_dir = {} # Folder where VerFSNext stores its data.\n",
        table["data_dir"]
    ));
    let mut group = "";
    for field in FIELDS {
        if field.group != group {
            group = field.group;
            out.push_str(&format!("\n# {group}\n"));
        }
        out.push_str(&format!(
            "{} = {} # {}\n",
            field.key, table[field.key], field.help
        ));
    }
    super::write_atomic(path, out.as_bytes())
}
