//! Startup reconciliation scan: pick up files already present at launch.
//!
//! Native and poll watchers only report *changes*, so files sitting in a
//! folder when the daemon starts would otherwise be ignored.

use std::sync::Arc;

use tokio::sync::mpsc::Sender;
use tracing::info;
use walkdir::WalkDir;

use crate::config::ResolvedConfig;
use crate::watcher::Candidate;

/// Walk each watch folder once and feed existing files into the pipeline.
pub fn spawn_scan(cfg: Arc<ResolvedConfig>, tx: Sender<Candidate>) {
    tokio::spawn(async move {
        for (index, folder) in cfg.folders.iter().enumerate() {
            scan_folder(&folder.watch_dir, index, &tx).await;
        }
        info!("startup reconciliation scan complete");
    });
}

async fn scan_folder(dir: &std::path::Path, index: usize, tx: &Sender<Candidate>) {
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
