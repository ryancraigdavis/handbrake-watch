//! Orchestration: wire watchers, scan, stabilizer, queue, and the worker.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::watch;
use tracing::{info, warn};

use crate::config::{ResolvedConfig, ResolvedFolder};
use crate::notify_ntfy::Notifier;
use crate::progress::{self, Reporter};
use crate::queue::{self, Job, Queue};
use crate::watcher::{self, Candidate};
use crate::{encoder, mover, scan};

type Tracked = Arc<Mutex<HashSet<PathBuf>>>;

/// Start the daemon and run until a shutdown signal.
pub async fn run(cfg: ResolvedConfig) -> Result<()> {
    let cfg = Arc::new(cfg);
    let queue = Arc::new(Queue::load(&cfg.state_file));
    let tracked: Tracked = Arc::new(Mutex::new(HashSet::new()));
    queue.resume(&tracked);

    let reporter = progress::make_reporter(&cfg, queue.clone());
    let (tx, rx) = mpsc::channel::<Candidate>(1024);
    let _watchers = watcher::spawn_watchers(&cfg, tx.clone())?;
    scan::spawn_scan(cfg.clone(), tx.clone());
    tokio::spawn(dispatch(
        cfg.clone(),
        queue.clone(),
        tracked.clone(),
        reporter.clone(),
        rx,
    ));

    let (sd_tx, sd_rx) = watch::channel(false);
    spawn_signal(sd_tx);
    let notifier = Notifier::new(cfg.notifications.clone());

    reporter.note(&format!(
        "hbwatch started — watching {} folder(s)",
        cfg.folders.len()
    ));
    worker_loop(cfg.clone(), queue, tracked, notifier, reporter, sd_rx).await;
    info!("hbwatch stopped");
    Ok(())
}

fn spawn_signal(tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to install SIGTERM handler");
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
        info!("shutdown signal received");
        let _ = tx.send(true);
    });
}

/// Consume candidate paths, dedupe, and spawn a stabilizer per new file.
async fn dispatch(
    cfg: Arc<ResolvedConfig>,
    queue: Arc<Queue>,
    tracked: Tracked,
    reporter: Arc<dyn Reporter>,
    mut rx: Receiver<Candidate>,
) {
    while let Some(candidate) = rx.recv().await {
        if !eligible(&cfg, &candidate) {
            continue;
        }
        if !claim(&tracked, &candidate.path) {
            continue;
        }
        tokio::spawn(stabilize(
            cfg.clone(),
            queue.clone(),
            tracked.clone(),
            reporter.clone(),
            candidate,
        ));
    }
}

fn claim(tracked: &Tracked, path: &Path) -> bool {
    tracked.lock().unwrap().insert(path.to_path_buf())
}

fn release(tracked: &Tracked, path: &Path) {
    tracked.lock().unwrap().remove(path);
}

/// Filter out files we should never enqueue (wrong type, temp, already done).
fn eligible(cfg: &ResolvedConfig, candidate: &Candidate) -> bool {
    let folder = match cfg.folders.get(candidate.folder) {
        Some(f) => f,
        None => return false,
    };
    let path = &candidate.path;
    path.is_file()
        && extension_allowed(cfg, path)
        && !is_ignored_name(path)
        && !output_exists(folder, path)
}

fn extension_allowed(cfg: &ResolvedConfig, path: &Path) -> bool {
    match path.extension().and_then(|e| e.to_str()) {
        Some(ext) => {
            let lower = ext.to_ascii_lowercase();
            cfg.settings
                .extensions
                .iter()
                .any(|e| e.eq_ignore_ascii_case(&lower))
        }
        None => false,
    }
}

fn is_ignored_name(path: &Path) -> bool {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name.starts_with('.')
        || [
            ".tmp",
            ".part",
            ".partial",
            ".download",
            ".crdownload",
            ".hbtmp",
        ]
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

fn output_exists(folder: &ResolvedFolder, input: &Path) -> bool {
    output_path_for(folder, input).exists()
}

/// `<output_dir>/<input stem>.<output_extension>`
pub fn output_path_for(folder: &ResolvedFolder, input: &Path) -> PathBuf {
    let stem = input.file_stem().unwrap_or_default().to_string_lossy();
    folder
        .output_dir
        .join(format!("{stem}.{}", folder.output_extension))
}

/// Wait for the file to stop changing, then enqueue it.
async fn stabilize(
    cfg: Arc<ResolvedConfig>,
    queue: Arc<Queue>,
    tracked: Tracked,
    reporter: Arc<dyn Reporter>,
    candidate: Candidate,
) {
    tokio::time::sleep(Duration::from_secs(cfg.settings.debounce_secs)).await;
    let stable = wait_stable(
        &candidate.path,
        cfg.settings.stabilize_interval_secs,
        cfg.settings.stabilize_checks,
    )
    .await;
    if !stable {
        release(&tracked, &candidate.path);
        return;
    }
    let folder = &cfg.folders[candidate.folder];
    let output = output_path_for(folder, &candidate.path);
    let job = queue::new_job(folder, candidate.path.clone(), output);
    reporter.note(&format!(
        "queued: {} [{}]",
        file_name(&candidate.path),
        folder.name
    ));
    queue.push(job);
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Poll file size until unchanged for `checks` consecutive reads.
async fn wait_stable(path: &Path, interval_secs: u64, checks: u32) -> bool {
    let mut last: Option<u64> = None;
    let mut stable = 0u32;
    loop {
        let size = match tokio::fs::metadata(path).await {
            Ok(meta) => meta.len(),
            Err(_) => break false,
        };
        stable = match last {
            Some(prev) if prev == size => stable + 1,
            _ => 0,
        };
        if stable >= checks {
            break true;
        }
        last = Some(size);
        tokio::time::sleep(Duration::from_secs(interval_secs)).await;
    }
}

/// Drain the queue serially until shutdown.
async fn worker_loop(
    cfg: Arc<ResolvedConfig>,
    queue: Arc<Queue>,
    tracked: Tracked,
    notifier: Notifier,
    reporter: Arc<dyn Reporter>,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut batch_count = 0u64;
    let mut batch_start: Option<Instant> = None;
    while !*shutdown.borrow() {
        match queue.take_next() {
            None => {
                fire_drain_if_needed(&notifier, &reporter, &mut batch_count, &mut batch_start)
                    .await;
                wait_for_work(&queue, &mut shutdown).await;
            }
            Some(job) => {
                batch_start.get_or_insert_with(Instant::now);
                reporter.job_start(
                    &file_name(&job.input_path),
                    &job.folder_name,
                    batch_count,
                    queue.pending_len(),
                );
                let done = run_one(
                    &cfg,
                    &queue,
                    &tracked,
                    &notifier,
                    &reporter,
                    &job,
                    &mut shutdown,
                )
                .await;
                if !done {
                    break;
                }
                batch_count += 1;
            }
        }
    }
}

async fn wait_for_work(queue: &Queue, shutdown: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = queue.work.notified() => {},
        _ = shutdown.changed() => {},
    }
}

/// Process one job, or return false if shutdown interrupted it mid-encode.
async fn run_one(
    cfg: &ResolvedConfig,
    queue: &Queue,
    tracked: &Tracked,
    notifier: &Notifier,
    reporter: &Arc<dyn Reporter>,
    job: &Job,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        result = process_job(cfg, job, reporter) => {
            queue.complete();
            release(tracked, &job.input_path);
            reporter.job_done(result.is_ok());
            report(notifier, reporter, job, result).await;
            true
        }
        _ = shutdown.changed() => {
            info!(job = %job.id, "shutdown during encode; will resume on restart");
            false
        }
    }
}

async fn report(notifier: &Notifier, reporter: &Arc<dyn Reporter>, job: &Job, result: Result<()>) {
    match result {
        Ok(()) => {
            reporter.note(&format!("done: {}", file_name(&job.input_path)));
            notifier.item_complete(job).await;
        }
        Err(e) => {
            warn!(job = %job.id, error = %e, "encode failed");
            reporter.note(&format!("FAILED: {} — {e}", file_name(&job.input_path)));
            notifier.failure(job, &e.to_string()).await;
        }
    }
}

async fn fire_drain_if_needed(
    notifier: &Notifier,
    reporter: &Arc<dyn Reporter>,
    count: &mut u64,
    start: &mut Option<Instant>,
) {
    if *count > 0 {
        let secs = start.map(|s| s.elapsed().as_secs()).unwrap_or(0);
        reporter.batch_idle();
        reporter.note(&format!("queue empty — {} item(s) in {secs}s", *count));
        notifier.queue_drain(*count, secs).await;
        *count = 0;
        *start = None;
    }
}

async fn process_job(cfg: &ResolvedConfig, job: &Job, reporter: &Arc<dyn Reporter>) -> Result<()> {
    let temp = mover::temp_path(&job.output_path);
    let result = encode_and_move(cfg, job, &temp, reporter).await;
    if result.is_err() {
        let _ = tokio::fs::remove_file(&temp).await;
    }
    result
}

async fn encode_and_move(
    cfg: &ResolvedConfig,
    job: &Job,
    temp: &Path,
    reporter: &Arc<dyn Reporter>,
) -> Result<()> {
    encoder::run(&cfg.settings, job, temp, reporter).await?;
    mover::finalize(job, temp).await?;
    Ok(())
}
