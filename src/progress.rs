//! Progress reporting. Renders indicatif bars on a TTY; logs plainly otherwise.

use std::collections::HashMap;
use std::io::IsTerminal;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use tracing::info;

use crate::config::ResolvedConfig;
use crate::queue::Queue;

/// Receives progress events from the worker and encoder.
pub trait Reporter: Send + Sync {
    /// A new job is starting. `completed` and `pending` size the overall bar.
    fn job_start(&self, film: &str, folder: &str, completed: u64, pending: usize);
    /// HandBrake reported progress (fraction is 0.0..=1.0).
    fn job_tick(&self, state: &str, fraction: f64, eta: Option<i64>, fps: Option<f64>);
    /// The current job finished.
    fn job_done(&self, ok: bool);
    /// The current job was requeued for a retry (not counted toward the batch).
    fn job_requeued(&self);
    /// The queue drained; nothing is encoding.
    fn batch_idle(&self);
    /// A general user-facing message.
    fn note(&self, msg: &str);
}

/// Pick a reporter based on whether stdout is an interactive terminal.
pub fn make_reporter(cfg: &ResolvedConfig, queue: Arc<Queue>) -> Arc<dyn Reporter> {
    let reporter: Arc<dyn Reporter> = match std::io::stdout().is_terminal() {
        true => Arc::new(TtyReporter::new(cfg, queue)),
        false => Arc::new(LogReporter),
    };
    reporter
}

/// Plain logging reporter for headless/service use.
struct LogReporter;

impl Reporter for LogReporter {
    fn job_start(&self, film: &str, folder: &str, completed: u64, pending: usize) {
        info!(
            folder,
            film,
            queued = pending,
            "encoding ({} done)",
            completed
        );
    }
    fn job_tick(&self, _state: &str, _fraction: f64, _eta: Option<i64>, _fps: Option<f64>) {}
    fn job_done(&self, ok: bool) {
        info!(ok, "job finished");
    }
    fn job_requeued(&self) {}
    fn batch_idle(&self) {}
    fn note(&self, msg: &str) {
        info!("{msg}");
    }
}

#[derive(Default)]
struct Current {
    folder: Option<String>,
    film: Option<String>,
}

/// indicatif-backed reporter: overall bar, current-film bar, per-folder lines.
struct TtyReporter {
    mp: MultiProgress,
    overall: ProgressBar,
    current: ProgressBar,
    state: Arc<Mutex<Current>>,
}

impl TtyReporter {
    fn new(cfg: &ResolvedConfig, queue: Arc<Queue>) -> Self {
        let mp = MultiProgress::new();
        let folders = build_folder_lines(&mp, cfg);
        let overall = mp.add(ProgressBar::new(0));
        overall.set_style(overall_style());
        overall.set_prefix("Overall");
        let current = mp.add(ProgressBar::new(PROGRESS_SCALE));
        current.set_style(current_style());
        idle_current(&current);
        let state = Arc::new(Mutex::new(Current::default()));
        spawn_folder_refresh(folders, queue, state.clone());
        Self {
            mp,
            overall,
            current,
            state,
        }
    }

    fn clear_current(&self) {
        idle_current(&self.current);
        let mut state = self.state.lock().unwrap();
        state.folder = None;
        state.film = None;
    }
}

const PROGRESS_SCALE: u64 = 1000;

impl Reporter for TtyReporter {
    fn job_start(&self, film: &str, folder: &str, completed: u64, pending: usize) {
        let total = completed + 1 + pending as u64;
        self.overall.set_length(total);
        self.overall.set_position(completed);
        self.overall
            .set_message(format!("{}/{}", completed + 1, total));
        self.current.reset();
        self.current.set_length(PROGRESS_SCALE);
        self.current.set_prefix(truncate(film, 40));
        self.current.set_message("starting".to_string());
        let mut state = self.state.lock().unwrap();
        state.folder = Some(folder.to_string());
        state.film = Some(film.to_string());
    }

    fn job_tick(&self, state: &str, fraction: f64, eta: Option<i64>, fps: Option<f64>) {
        self.current
            .set_position((fraction * PROGRESS_SCALE as f64) as u64);
        self.current.set_message(tick_message(state, eta, fps));
    }

    fn job_done(&self, ok: bool) {
        self.overall.inc(1);
        self.clear_current();
        let _ = ok;
    }

    fn job_requeued(&self) {
        self.clear_current();
    }

    fn batch_idle(&self) {
        idle_current(&self.current);
        self.overall.set_message("idle".to_string());
    }

    fn note(&self, msg: &str) {
        let _ = self.mp.println(msg);
    }
}

fn build_folder_lines(mp: &MultiProgress, cfg: &ResolvedConfig) -> Vec<(String, ProgressBar)> {
    cfg.folders
        .iter()
        .map(|f| {
            let pb = mp.add(ProgressBar::new_spinner());
            pb.set_style(folder_style());
            pb.set_prefix(f.name.clone());
            pb.set_message("idle");
            pb.enable_steady_tick(Duration::from_millis(250));
            (f.name.clone(), pb)
        })
        .collect()
}

fn spawn_folder_refresh(
    folders: Vec<(String, ProgressBar)>,
    queue: Arc<Queue>,
    state: Arc<Mutex<Current>>,
) {
    tokio::spawn(async move {
        loop {
            let counts = queue.pending_counts();
            let current = { state.lock().unwrap().clone_parts() };
            for (name, pb) in &folders {
                pb.set_message(folder_message(name, &counts, &current));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

impl Current {
    fn clone_parts(&self) -> (Option<String>, Option<String>) {
        (self.folder.clone(), self.film.clone())
    }
}

fn folder_message(
    name: &str,
    counts: &HashMap<String, usize>,
    current: &(Option<String>, Option<String>),
) -> String {
    let pending = counts.get(name).copied().unwrap_or(0);
    let encoding = current.0.as_deref() == Some(name);
    let message = match (encoding, pending) {
        (true, n) => format!(
            "encoding {} ({n} queued)",
            current.1.clone().unwrap_or_default()
        ),
        (false, 0) => "idle".to_string(),
        (false, n) => format!("{n} queued"),
    };
    message
}

fn tick_message(state: &str, eta: Option<i64>, fps: Option<f64>) -> String {
    let message = match state {
        "WORKING" => format!("ETA {} {}", fmt_eta(eta), fmt_fps(fps)),
        other => other.to_lowercase(),
    };
    message
}

fn fmt_eta(eta: Option<i64>) -> String {
    match eta {
        Some(s) if s > 0 => format!("{}m{:02}s", s / 60, s % 60),
        _ => "--".to_string(),
    }
}

fn fmt_fps(fps: Option<f64>) -> String {
    match fps {
        Some(f) if f > 0.0 => format!("{f:.1}fps"),
        _ => String::new(),
    }
}

fn idle_current(pb: &ProgressBar) {
    pb.set_position(0);
    pb.set_prefix("(idle)");
    pb.set_message("waiting for files");
}

fn truncate(s: &str, max: usize) -> String {
    let result = match s.chars().count() > max {
        true => format!("{}…", s.chars().take(max - 1).collect::<String>()),
        false => s.to_string(),
    };
    result
}

fn overall_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:.bold.dim} [{bar:30.green/dim}] {msg}")
        .unwrap()
        .progress_chars("=>-")
}

fn current_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:.bold} [{bar:30.cyan/blue}] {percent:>3}% {msg}")
        .unwrap()
        .progress_chars("=>-")
}

fn folder_style() -> ProgressStyle {
    ProgressStyle::with_template("  {spinner:.green} {prefix:.bold}: {msg}").unwrap()
}
