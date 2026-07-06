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
use crate::queue::{self, Job, JobStatus, Queue};
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
                if !wait_until_ready(&job, &reporter, &mut shutdown).await {
                    break;
                }
                batch_start.get_or_insert_with(Instant::now);
                reporter.job_start(
                    &file_name(&job.input_path),
                    &job.folder_name,
                    batch_count,
                    queue.pending_len(),
                );
                match run_one(
                    &cfg,
                    &queue,
                    &tracked,
                    &notifier,
                    &reporter,
                    job,
                    &mut shutdown,
                )
                .await
                {
                    Outcome::Interrupted => break,
                    Outcome::Retried => {}
                    Outcome::Terminal => batch_count += 1,
                }
            }
        }
    }
}

/// The result of attempting one job.
enum Outcome {
    /// Finished for good (encoded, or failed past max attempts).
    Terminal,
    /// Failed but re-queued for another attempt.
    Retried,
    /// Shutdown interrupted the encode; the job stays in-flight to resume.
    Interrupted,
}

/// Honor a job's `retry_after` delay, aborting early on shutdown.
async fn wait_until_ready(
    job: &Job,
    reporter: &Arc<dyn Reporter>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let wait = job
        .retry_after
        .and_then(|t| t.checked_sub(queue::now_secs()))
        .unwrap_or(0);
    if wait == 0 {
        return true;
    }
    reporter.note(&format!("retry in {wait}s: {}", file_name(&job.input_path)));
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(wait)) => true,
        _ = shutdown.changed() => false,
    }
}

async fn wait_for_work(queue: &Queue, shutdown: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = queue.work.notified() => {},
        _ = shutdown.changed() => {},
    }
}

/// Process one job to a terminal/retry outcome, unless shutdown interrupts it.
async fn run_one(
    cfg: &ResolvedConfig,
    queue: &Queue,
    tracked: &Tracked,
    notifier: &Notifier,
    reporter: &Arc<dyn Reporter>,
    job: Job,
    shutdown: &mut watch::Receiver<bool>,
) -> Outcome {
    tokio::select! {
        result = process_job(cfg, &job, reporter) => {
            finish(cfg, queue, tracked, notifier, reporter, job, result).await
        }
        _ = shutdown.changed() => {
            info!(job = %job.id, "shutdown during encode; will resume on restart");
            Outcome::Interrupted
        }
    }
}

/// Clear the in-flight job and route success vs. failure.
async fn finish(
    cfg: &ResolvedConfig,
    queue: &Queue,
    tracked: &Tracked,
    notifier: &Notifier,
    reporter: &Arc<dyn Reporter>,
    job: Job,
    result: Result<()>,
) -> Outcome {
    queue.complete();
    match result {
        Ok(()) => {
            release(tracked, &job.input_path);
            reporter.job_done(true);
            reporter.note(&format!("done: {}", file_name(&job.input_path)));
            notifier.item_complete(&job).await;
            Outcome::Terminal
        }
        Err(e) => handle_failure(cfg, queue, tracked, notifier, reporter, job, e).await,
    }
}

/// Decide whether a failed job retries or is moved aside for good.
async fn handle_failure(
    cfg: &ResolvedConfig,
    queue: &Queue,
    tracked: &Tracked,
    notifier: &Notifier,
    reporter: &Arc<dyn Reporter>,
    job: Job,
    error: anyhow::Error,
) -> Outcome {
    let attempts = job.attempts + 1;
    let max = cfg.settings.max_attempts;
    warn!(job = %job.id, attempts, max, error = %error, "encode attempt failed");
    match decide(attempts, max) {
        FailureAction::Retry => {
            let name = file_name(&job.input_path);
            let retry = retry_job(job, attempts, cfg.settings.retry_delay_secs);
            reporter.job_requeued();
            reporter.note(&format!("retry {attempts}/{max}: {name} — {error}"));
            queue.push(retry);
            Outcome::Retried
        }
        FailureAction::GiveUp => {
            release(tracked, &job.input_path);
            reporter.job_done(false);
            give_up(&job, &error.to_string(), reporter).await;
            notifier.failure(&job, &error.to_string()).await;
            Outcome::Terminal
        }
    }
}

/// Whether a failed job (with `attempts` tries made) should retry.
fn decide(attempts: u32, max: u32) -> FailureAction {
    match attempts < max {
        true => FailureAction::Retry,
        false => FailureAction::GiveUp,
    }
}

enum FailureAction {
    Retry,
    GiveUp,
}

fn retry_job(mut job: Job, attempts: u32, delay: u64) -> Job {
    job.attempts = attempts;
    job.status = JobStatus::Pending;
    job.retry_after = Some(queue::now_secs() + delay);
    job
}

/// Move a permanently-failed input to its `failed/` dir with an error sidecar.
async fn give_up(job: &Job, reason: &str, reporter: &Arc<dyn Reporter>) {
    let name = file_name(&job.input_path);
    match mover::move_to_failed(&job.input_path, &job.failed_dir, reason).await {
        Ok(dest) => reporter.note(&format!("FAILED (moved to {}): {name}", dest.display())),
        Err(e) => {
            warn!(job = %job.id, error = %e, "could not move failed input aside");
            reporter.note(&format!("FAILED: {name}"));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn is_retry(attempts: u32, max: u32) -> bool {
        matches!(decide(attempts, max), FailureAction::Retry)
    }

    #[test]
    fn retries_until_max_then_gives_up() {
        // max_attempts = 3 → try 1 and 2 retry, the 3rd gives up.
        assert!(is_retry(1, 3));
        assert!(is_retry(2, 3));
        assert!(!is_retry(3, 3));
    }

    #[test]
    fn max_attempts_one_never_retries() {
        assert!(!is_retry(1, 1));
    }

    #[test]
    fn is_ignored_name_skips_temp_and_hidden() {
        assert!(is_ignored_name(Path::new("/x/.hidden.mkv")));
        assert!(is_ignored_name(Path::new("/x/movie.mkv.part")));
        assert!(is_ignored_name(Path::new("/x/clip.mp4.hbtmp")));
        assert!(!is_ignored_name(Path::new("/x/movie.mkv")));
    }
}
