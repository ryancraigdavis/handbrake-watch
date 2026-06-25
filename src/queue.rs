//! Persistent serial job queue with resume support.

use std::collections::{HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tracing::{info, warn};
use uuid::Uuid;

use crate::mover;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Processing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: Uuid,
    pub input_path: PathBuf,
    pub folder_name: String,
    pub preset_file: PathBuf,
    pub preset_name: String,
    pub output_path: PathBuf,
    pub originals_dir: PathBuf,
    pub status: JobStatus,
    pub attempts: u32,
    pub enqueued_at: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct QueueState {
    pending: VecDeque<Job>,
    in_flight: Option<Job>,
}

/// In-memory queue backed by a JSON state file. Persists on every change.
pub struct Queue {
    state: Mutex<QueueState>,
    path: PathBuf,
    pub work: Notify,
}

impl Queue {
    /// Load persisted state, or start empty if none exists.
    pub fn load(path: &Path) -> Self {
        let state = read_state(path).unwrap_or_default();
        Self {
            state: Mutex::new(state),
            path: path.to_path_buf(),
            work: Notify::new(),
        }
    }

    /// Re-enqueue an interrupted in-flight job and mark all pending paths tracked.
    pub fn resume(&self, tracked: &Mutex<HashSet<PathBuf>>) {
        let mut state = self.state.lock().unwrap();
        if let Some(mut job) = state.in_flight.take() {
            let _ = std::fs::remove_file(mover::temp_path(&job.output_path));
            job.status = JobStatus::Pending;
            info!(job = %job.id, "resuming interrupted job");
            state.pending.push_front(job);
        }
        let mut set = tracked.lock().unwrap();
        for job in &state.pending {
            set.insert(job.input_path.clone());
        }
        write_state(&self.path, &state);
    }

    /// Enqueue a job and wake the worker.
    pub fn push(&self, job: Job) {
        {
            let mut state = self.state.lock().unwrap();
            state.pending.push_back(job);
            write_state(&self.path, &state);
        }
        self.work.notify_one();
    }

    /// Move the next pending job to in-flight and return it.
    pub fn take_next(&self) -> Option<Job> {
        let mut state = self.state.lock().unwrap();
        let next = state.pending.pop_front().map(|mut job| {
            job.status = JobStatus::Processing;
            state.in_flight = Some(job.clone());
            job
        });
        write_state(&self.path, &state);
        next
    }

    /// Clear the in-flight job after it finishes.
    pub fn complete(&self) {
        let mut state = self.state.lock().unwrap();
        state.in_flight = None;
        write_state(&self.path, &state);
    }
}

fn read_state(path: &Path) -> Option<QueueState> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn write_state(path: &Path, state: &QueueState) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match serde_json::to_string_pretty(state) {
        Ok(text) => {
            if let Err(e) = std::fs::write(path, text) {
                warn!(error = %e, "failed to persist queue state");
            }
        }
        Err(e) => warn!(error = %e, "failed to serialize queue state"),
    }
}

/// Seconds since the Unix epoch.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Build a fresh pending job for an input file.
pub fn new_job(folder: &crate::config::ResolvedFolder, input: PathBuf, output: PathBuf) -> Job {
    Job {
        id: Uuid::new_v4(),
        input_path: input,
        folder_name: folder.name.clone(),
        preset_file: folder.preset_file.clone(),
        preset_name: folder.preset_name.clone(),
        output_path: output,
        originals_dir: folder.originals_dir.clone(),
        status: JobStatus::Pending,
        attempts: 0,
        enqueued_at: now_secs(),
    }
}
