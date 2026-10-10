//! Folder watching. Poll-based by default (required for NAS); native optional.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{
    Config as NotifyConfig, Event, EventKind, PollWatcher, RecommendedWatcher, RecursiveMode,
    Watcher,
};
use tokio::sync::mpsc::Sender;

use crate::config::{ResolvedConfig, ResolvedFolder, Settings, WatchMode};

/// A file path observed in a watched folder, tagged with its resolved folder.
///
/// Carrying the folder (rather than an index into the config) keeps each
/// candidate self-describing, so folders can be added at runtime without any
/// shared index bookkeeping.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub path: PathBuf,
    pub folder: Arc<ResolvedFolder>,
}

/// A live OS folder watch. Dropping it stops the watch, so handles are kept
/// alive for the daemon's lifetime.
pub type WatcherHandle = Box<dyn Watcher + Send>;

/// Start one watcher per folder. The returned handles must be kept alive.
pub fn spawn_watchers(cfg: &ResolvedConfig, tx: Sender<Candidate>) -> Result<Vec<WatcherHandle>> {
    cfg.folders
        .iter()
        .map(|folder| watch_folder(folder.clone(), &cfg.settings, tx.clone()))
        .collect()
}

/// Start a single folder watcher. Reusable for runtime folder additions.
pub fn watch_folder(
    folder: Arc<ResolvedFolder>,
    settings: &Settings,
    tx: Sender<Candidate>,
) -> Result<WatcherHandle> {
    let watch_dir = folder.watch_dir.clone();
    let handler = move |res: notify::Result<Event>| {
        if let Ok(event) = res {
            forward(event, &folder, &tx);
        }
    };
    let mut watcher: Box<dyn Watcher + Send> = match settings.watch_mode {
        WatchMode::Poll => Box::new(PollWatcher::new(
            handler,
            NotifyConfig::default()
                .with_poll_interval(Duration::from_secs(settings.poll_interval_secs)),
        )?),
        WatchMode::Native => Box::new(RecommendedWatcher::new(handler, NotifyConfig::default())?),
    };
    watcher
        .watch(&watch_dir, RecursiveMode::NonRecursive)
        .with_context(|| format!("failed to watch {}", watch_dir.display()))?;
    Ok(watcher)
}

fn forward(event: Event, folder: &Arc<ResolvedFolder>, tx: &Sender<Candidate>) {
    if !is_relevant(&event.kind) {
        return;
    }
    for path in event.paths {
        let _ = tx.blocking_send(Candidate {
            path,
            folder: folder.clone(),
        });
    }
}

fn is_relevant(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Any
    )
}
