//! Reconciliation scan: pick up files present at launch, and re-scan on an
//! interval to catch anything the watcher missed (e.g. files that appeared
//! while a NAS mount was disconnected).
//!
//! The scan is idempotent — the dispatcher dedups via the tracked-set and the
//! output-exists guard — so re-running it is always safe.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::Sender;
use tracing::{info, warn};
use walkdir::WalkDir;

use crate::config::{ResolvedConfig, ResolvedFolder};
use crate::watcher::Candidate;

/// Run one scan at startup, then repeat every `rescan_interval_secs` (0 = once).
pub fn spawn_scan(cfg: Arc<ResolvedConfig>, tx: Sender<Candidate>) {
    let interval = cfg.settings.rescan_interval_secs;
    tokio::spawn(async move {
        scan_all(&cfg.folders, &tx).await;
        info!("startup reconciliation scan complete");
        run_periodic(cfg.folders.clone(), interval, tx).await;
    });
}

/// Scan a single folder now and keep re-scanning it on the interval. Used when a
/// folder is added at runtime, after the initial startup scan has already run.
pub fn spawn_folder_scan(folder: Arc<ResolvedFolder>, interval: u64, tx: Sender<Candidate>) {
    tokio::spawn(async move {
        let set = [folder];
        scan_all(&set, &tx).await;
        run_periodic(set.to_vec(), interval, tx).await;
    });
}

async fn run_periodic(folders: Vec<Arc<ResolvedFolder>>, interval: u64, tx: Sender<Candidate>) {
    if interval == 0 {
        return;
    }
    loop {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        scan_all(&folders, &tx).await;
    }
}

async fn scan_all(folders: &[Arc<ResolvedFolder>], tx: &Sender<Candidate>) {
    for folder in folders {
        scan_folder(folder, tx).await;
    }
}

async fn scan_folder(folder: &Arc<ResolvedFolder>, tx: &Sender<Candidate>) {
    if !folder.watch_dir.is_dir() {
        warn!(folder = %folder.name, path = %folder.watch_dir.display(), "watch_dir unavailable (mount down?)");
        return;
    }
    let entries = WalkDir::new(&folder.watch_dir)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file());
    for entry in entries {
        let candidate = Candidate {
            path: entry.path().to_path_buf(),
            folder: folder.clone(),
        };
        let _ = tx.send(candidate).await;
    }
}
