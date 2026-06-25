//! Folder watching. Poll-based by default (required for NAS); native optional.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use notify::{
    Config as NotifyConfig, Event, EventKind, PollWatcher, RecommendedWatcher, RecursiveMode,
    Watcher,
};
use tokio::sync::mpsc::Sender;

use crate::config::{ResolvedConfig, WatchMode};

/// A file path observed in a watched folder, tagged with its folder index.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub path: PathBuf,
    pub folder: usize,
}

/// Start one watcher per folder. The returned handles must be kept alive.
pub fn spawn_watchers(
    cfg: &ResolvedConfig,
    tx: Sender<Candidate>,
) -> Result<Vec<Box<dyn Watcher + Send>>> {
    let handles = cfg
        .folders
        .iter()
        .enumerate()
        .map(|(i, _)| start_one(cfg, i, tx.clone()))
        .collect::<Result<Vec<_>>>()?;
    Ok(handles)
}

fn start_one(
    cfg: &ResolvedConfig,
    index: usize,
    tx: Sender<Candidate>,
) -> Result<Box<dyn Watcher + Send>> {
    let folder = &cfg.folders[index];
    let handler = move |res: notify::Result<Event>| {
        if let Ok(event) = res {
            forward(event, index, &tx);
        }
    };
    let mut watcher: Box<dyn Watcher + Send> = match cfg.settings.watch_mode {
        WatchMode::Poll => Box::new(PollWatcher::new(
            handler,
            NotifyConfig::default()
                .with_poll_interval(Duration::from_secs(cfg.settings.poll_interval_secs)),
        )?),
        WatchMode::Native => Box::new(RecommendedWatcher::new(handler, NotifyConfig::default())?),
    };
    watcher
        .watch(&folder.watch_dir, RecursiveMode::NonRecursive)
        .with_context(|| format!("failed to watch {}", folder.watch_dir.display()))?;
    Ok(watcher)
}

fn forward(event: Event, index: usize, tx: &Sender<Candidate>) {
    if !is_relevant(&event.kind) {
        return;
    }
    for path in event.paths {
        let _ = tx.blocking_send(Candidate { path, folder: index });
    }
}

fn is_relevant(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::Create(_) | EventKind::Modify(_) | EventKind::Any
    )
}
