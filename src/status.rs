//! Live status store shared between the worker/encoder and the HTTP server.
//!
//! Implements `Reporter`, so it is fed by the exact same events that drive the
//! progress bars — the HTTP server just reads snapshots from it.

use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::progress::Reporter;
use crate::queue::now_secs;

/// One folder's live state, as reported by the HTTP endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderStatus {
    pub name: String,
    pub pending: usize,
    pub encoding: bool,
}

/// The full status payload served at `/status.json` and consumed by `status`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub uptime_secs: u64,
    pub processed: u64,
    pub failed: u64,
    pub pending_total: usize,
    pub current: Option<Current>,
    pub folders: Vec<FolderStatus>,
}

/// The job currently encoding, if any.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Current {
    pub folder: String,
    pub film: String,
    pub fraction: f64,
    pub eta_secs: Option<i64>,
    pub fps: Option<f64>,
    pub state: String,
}

#[derive(Debug, Default)]
struct Inner {
    current: Option<Current>,
    processed: u64,
    failed: u64,
}

/// Thread-safe live view of what the daemon is doing.
pub struct StatusStore {
    started_at: u64,
    inner: Mutex<Inner>,
}

/// A point-in-time view for the HTTP endpoints.
#[derive(Debug, Clone, Serialize)]
pub struct Snapshot {
    pub uptime_secs: u64,
    pub current: Option<Current>,
    pub processed: u64,
    pub failed: u64,
}

impl StatusStore {
    pub fn new(started_at: u64) -> Self {
        Self {
            started_at,
            inner: Mutex::new(Inner::default()),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock().unwrap();
        Snapshot {
            uptime_secs: now_secs().saturating_sub(self.started_at),
            current: inner.current.clone(),
            processed: inner.processed,
            failed: inner.failed,
        }
    }
}

impl Reporter for StatusStore {
    fn job_start(&self, film: &str, folder: &str, _completed: u64, _pending: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.current = Some(Current {
            folder: folder.to_string(),
            film: film.to_string(),
            fraction: 0.0,
            eta_secs: None,
            fps: None,
            state: "starting".to_string(),
        });
    }

    fn job_tick(&self, state: &str, fraction: f64, eta: Option<i64>, fps: Option<f64>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(current) = inner.current.as_mut() {
            current.fraction = fraction;
            current.eta_secs = eta;
            current.fps = fps;
            current.state = state.to_string();
        }
    }

    fn job_done(&self, ok: bool) {
        let mut inner = self.inner.lock().unwrap();
        match ok {
            true => inner.processed += 1,
            false => inner.failed += 1,
        }
        inner.current = None;
    }

    fn job_requeued(&self) {
        self.inner.lock().unwrap().current = None;
    }

    fn batch_idle(&self) {
        self.inner.lock().unwrap().current = None;
    }

    fn note(&self, _msg: &str) {}
}
