//! Reconciliation scan: pick up files present at launch, and re-scan on an
//! interval to catch anything the watcher missed (e.g. files that appeared
//! while a NAS mount was disconnected).
//!
//! The scan is idempotent — the dispatcher dedups via the tracked-set and the
//! output-exists guard — so re-running it is always safe.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::Sender;
use tracing::{info, warn};
use walkdir::WalkDir;

use crate::config::ResolvedConfig;
use crate::watcher::Candidate;

/// Run one scan at startup, then repeat every `rescan_interval_secs` (0 = once).
pub fn spawn_scan(cfg: Arc<ResolvedConfig>, tx: Sender<Candidate>) {
    tokio::spawn(async move {
        scan_all(&cfg, &tx).await;
        info!("startup reconciliation scan complete");
        run_periodic(cfg, tx).await;
    });
}

async fn run_periodic(cfg: Arc<ResolvedConfig>, tx: Sender<Candidate>) {
    let interval = cfg.settings.rescan_interval_secs;
    if interval == 0 {
        return;
    }
    loop {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        scan_all(&cfg, &tx).await;
    }
}

async fn scan_all(cfg: &ResolvedConfig, tx: &Sender<Candidate>) {
    for (index, folder) in cfg.folders.iter().enumerate() {
        scan_folder(&folder.watch_dir, &folder.name, index, tx).await;
    }
}

async fn scan_folder(dir: &Path, name: &str, index: usize, tx: &Sender<Candidate>) {
    if !dir.is_dir() {
        warn!(folder = name, path = %dir.display(), "watch_dir unavailable (mount down?)");
        return;
    }
    let entries = WalkDir::new(dir)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file());
    for entry in entries {
        let candidate = Candidate {
            path: entry.path().to_path_buf(),
            folder: index,
        };
        let _ = tx.send(candidate).await;
    }
}
