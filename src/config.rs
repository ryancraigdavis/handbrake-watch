//! Config loading, path expansion, validation, and preset resolution.

use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use tracing::warn;

use crate::preset;

/// Watch backend. `poll` is required for network shares (NAS).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchMode {
    Poll,
    Native,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    #[serde(default = "default_handbrake")]
    pub handbrake_cli: String,
    #[serde(default = "default_extensions")]
    pub extensions: Vec<String>,
    #[serde(default = "default_watch_mode")]
    pub watch_mode: WatchMode,
    #[serde(default = "default_poll")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_debounce")]
    pub debounce_secs: u64,
    #[serde(default = "default_stab_interval")]
    pub stabilize_interval_secs: u64,
    #[serde(default = "default_stab_checks")]
    pub stabilize_checks: u32,
    #[serde(default = "default_workers")]
    pub workers: u32,
    #[serde(default = "default_min_output")]
    pub min_output_bytes: u64,
    #[serde(default)]
    pub encode_timeout_secs: u64,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_state_file")]
    pub state_file: String,
    #[serde(default)]
    pub preset_dir: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Notifications {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_server")]
    pub server: String,
    #[serde(default)]
    pub topic: String,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub on_failure: bool,
    #[serde(default)]
    pub on_item_complete: bool,
    #[serde(default)]
    pub on_queue_drain: bool,
}

impl Default for Notifications {
    fn default() -> Self {
        Self {
            enabled: false,
            server: default_server(),
            topic: String::new(),
            token: None,
            on_failure: false,
            on_item_complete: false,
            on_queue_drain: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Folder {
    pub name: String,
    pub watch_dir: String,
    #[serde(default)]
    pub preset_file: Option<String>,
    #[serde(default)]
    pub preset_name: Option<String>,
    pub output_dir: String,
    pub originals_dir: String,
    pub output_extension: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub settings: Settings,
    #[serde(default)]
    pub notifications: Notifications,
    #[serde(rename = "folder", default)]
    pub folders: Vec<Folder>,
}

/// A folder with all paths expanded and its preset resolved.
#[derive(Debug, Clone)]
pub struct ResolvedFolder {
    pub name: String,
    pub watch_dir: PathBuf,
    pub output_dir: PathBuf,
    pub originals_dir: PathBuf,
    pub output_extension: String,
    pub preset_file: PathBuf,
    pub preset_name: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    pub settings: Settings,
    pub notifications: Notifications,
    pub folders: Vec<ResolvedFolder>,
    pub state_file: PathBuf,
}

/// Read and deserialize the TOML config file.
pub fn load(path: &Path) -> Result<Config> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read config: {}", path.display()))?;
    let config = toml::from_str(&text).context("failed to parse config TOML")?;
    Ok(config)
}

/// Validate the config and resolve every path and preset name.
pub fn resolve(raw: Config) -> Result<ResolvedConfig> {
    ensure!(!raw.folders.is_empty(), "config has no [[folder]] entries");
    let watch_dirs = collect_watch_dirs(&raw)?;
    let folders = raw
        .folders
        .iter()
        .map(|f| resolve_folder(f, &raw.settings, &watch_dirs))
        .collect::<Result<Vec<_>>>()?;
    let notifications = resolve_notifications(raw.notifications)?;
    let settings = clamp_settings(raw.settings);
    let state_file = expand(&settings.state_file);
    Ok(ResolvedConfig {
        settings,
        notifications,
        folders,
        state_file,
    })
}

fn collect_watch_dirs(raw: &Config) -> Result<Vec<PathBuf>> {
    raw.folders
        .iter()
        .map(|f| validate_watch_dir(&f.watch_dir))
        .collect()
}

fn validate_watch_dir(raw: &str) -> Result<PathBuf> {
    let dir = expand(raw);
    ensure!(dir.is_dir(), "watch_dir does not exist: {}", dir.display());
    Ok(canonical(&dir))
}

fn resolve_folder(
    f: &Folder,
    settings: &Settings,
    watch_dirs: &[PathBuf],
) -> Result<ResolvedFolder> {
    let watch_dir = expand(&f.watch_dir);
    let output_dir = prepare_dir(&f.output_dir)?;
    let originals_dir = prepare_dir(&f.originals_dir)?;
    assert_outside(&output_dir, watch_dirs, "output_dir")?;
    assert_outside(&originals_dir, watch_dirs, "originals_dir")?;
    let preset_file = resolve_preset_file(f, settings)?;
    let info = preset::load_preset(&preset_file, f.preset_name.as_deref())?;
    warn_on_format_mismatch(f, &info);
    Ok(ResolvedFolder {
        name: f.name.clone(),
        watch_dir,
        output_dir,
        originals_dir,
        output_extension: f.output_extension.clone(),
        preset_file,
        preset_name: info.name,
    })
}

fn resolve_preset_file(f: &Folder, settings: &Settings) -> Result<PathBuf> {
    let path = match (&f.preset_file, &settings.preset_dir) {
        (Some(pf), _) => expand(pf),
        (None, Some(dir)) => expand(dir).join(format!("{}.json", f.name)),
        (None, None) => bail!(
            "folder '{}' has no preset_file and no preset_dir is set",
            f.name
        ),
    };
    ensure!(path.is_file(), "preset file not found: {}", path.display());
    Ok(path)
}

fn warn_on_format_mismatch(f: &Folder, info: &preset::PresetInfo) {
    let matches = info
        .file_format
        .as_deref()
        .map(|fmt| fmt.contains(&f.output_extension))
        .unwrap_or(true);
    if !matches {
        warn!(
            folder = %f.name,
            output_extension = %f.output_extension,
            preset_format = %info.file_format.as_deref().unwrap_or("?"),
            "output_extension does not match preset FileFormat"
        );
    }
}

fn resolve_notifications(mut n: Notifications) -> Result<Notifications> {
    if n.server.trim().is_empty() {
        n.server = default_server();
    }
    if n.enabled {
        ensure!(!n.topic.trim().is_empty(), "notifications.enabled requires a topic");
    }
    Ok(n)
}

fn clamp_settings(mut s: Settings) -> Settings {
    s.workers = s.workers.clamp(1, 5);
    s.stabilize_checks = s.stabilize_checks.max(1);
    s
}

fn prepare_dir(raw: &str) -> Result<PathBuf> {
    let dir = expand(raw);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create directory: {}", dir.display()))?;
    Ok(canonical(&dir))
}

fn assert_outside(dir: &Path, watch_dirs: &[PathBuf], label: &str) -> Result<()> {
    let bad = watch_dirs.iter().find(|w| dir.starts_with(w));
    match bad {
        Some(w) => bail!(
            "{} {} must not be inside watch_dir {}",
            label,
            dir.display(),
            w.display()
        ),
        None => Ok(()),
    }
}

/// Expand a leading `~` to the user's home directory.
fn expand(path: &str) -> PathBuf {
    let expanded = match path.strip_prefix("~/") {
        Some(rest) => home().map(|h| h.join(rest)).unwrap_or_else(|| PathBuf::from(path)),
        None => PathBuf::from(path),
    };
    expanded
}

fn canonical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

fn default_handbrake() -> String {
    "HandBrakeCLI".to_string()
}
fn default_extensions() -> Vec<String> {
    ["mkv", "mp4", "mov", "avi", "m4v", "ts", "wmv", "flv"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}
fn default_watch_mode() -> WatchMode {
    WatchMode::Poll
}
fn default_poll() -> u64 {
    30
}
fn default_debounce() -> u64 {
    3
}
fn default_stab_interval() -> u64 {
    2
}
fn default_stab_checks() -> u32 {
    2
}
fn default_workers() -> u32 {
    1
}
fn default_min_output() -> u64 {
    1_000_000
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_state_file() -> String {
    "~/.config/hbwatch/queue-state.json".to_string()
}
fn default_server() -> String {
    "https://ntfy.sh".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_outside_rejects_nested_dir() {
        let watch = vec![PathBuf::from("/data/inbox")];
        let nested = PathBuf::from("/data/inbox/out");
        assert!(assert_outside(&nested, &watch, "output_dir").is_err());
    }

    #[test]
    fn assert_outside_allows_sibling_dir() {
        let watch = vec![PathBuf::from("/data/inbox")];
        let sibling = PathBuf::from("/data/encoded");
        assert!(assert_outside(&sibling, &watch, "output_dir").is_ok());
    }
}
