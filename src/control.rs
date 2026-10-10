//! Runtime control shared between the HTTP server and the worker: a pause gate
//! between jobs, a skip signal for the running job, the CPU throttle level, and
//! the live HandBrakeCLI PID the throttle and freeze act on.
//!
//! Throttling uses `taskpolicy -b` (background QoS: efficiency cores, low I/O
//! priority) and `taskpolicy -B` to restore. `-B` cannot promote threads that
//! were *created* while the process was already at background QoS, so a
//! background level that is set before a job starts is applied only after the
//! encoder's first progress tick, once its worker threads exist.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Mutex;

use anyhow::{anyhow, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::{watch, Notify};
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ThrottleLevel {
    #[default]
    Normal,
    Background,
}

impl ThrottleLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            ThrottleLevel::Normal => "normal",
            ThrottleLevel::Background => "background",
        }
    }
}

/// Point-in-time control state, merged into `/status.json`.
#[derive(Debug, Clone, Serialize)]
pub struct ControlView {
    pub paused: bool,
    pub stopped: bool,
    pub throttle: ThrottleLevel,
    pub encoder_pid: Option<u32>,
}

pub struct Control {
    paused: watch::Sender<bool>,
    /// Wakes the worker's skip arm. Uses `notify_waiters`, so a skip with no
    /// job running is a no-op rather than a stored permit.
    pub skip: Notify,
    throttle: Mutex<ThrottleLevel>,
    encoder_pid: AtomicU32,
    stopped: AtomicBool,
    pending_apply: AtomicBool,
    taskpolicy: PathBuf,
}

impl Default for Control {
    fn default() -> Self {
        Self::new()
    }
}

impl Control {
    pub fn new() -> Self {
        Self::with_taskpolicy("/usr/sbin/taskpolicy")
    }

    pub fn with_taskpolicy(taskpolicy: impl Into<PathBuf>) -> Self {
        let (paused, _) = watch::channel(false);
        Self {
            paused,
            skip: Notify::new(),
            throttle: Mutex::new(ThrottleLevel::Normal),
            encoder_pid: AtomicU32::new(0),
            stopped: AtomicBool::new(false),
            pending_apply: AtomicBool::new(false),
            taskpolicy: taskpolicy.into(),
        }
    }

    pub fn paused_rx(&self) -> watch::Receiver<bool> {
        self.paused.subscribe()
    }

    pub fn is_paused(&self) -> bool {
        *self.paused.borrow()
    }

    pub fn pid(&self) -> Option<u32> {
        match self.encoder_pid.load(Ordering::SeqCst) {
            0 => None,
            p => Some(p),
        }
    }

    pub fn throttle(&self) -> ThrottleLevel {
        *self.throttle.lock().unwrap()
    }

    /// Stop starting new jobs. With `stop_current`, also freeze the running encoder (SIGSTOP).
    pub fn pause(&self, stop_current: bool) -> Result<()> {
        self.paused.send_replace(true);
        if stop_current {
            if let Some(pid) = self.pid() {
                signal(pid, libc::SIGSTOP).context("SIGSTOP encoder")?;
                self.stopped.store(true, Ordering::SeqCst);
                info!(pid, "encoder frozen");
            }
        }
        Ok(())
    }

    /// Thaw a frozen encoder and reopen the gate.
    pub fn resume(&self) -> Result<()> {
        if self.stopped.swap(false, Ordering::SeqCst) {
            if let Some(pid) = self.pid() {
                signal(pid, libc::SIGCONT).context("SIGCONT encoder")?;
                info!(pid, "encoder thawed");
            }
        }
        self.paused.send_replace(false);
        Ok(())
    }

    /// Ask the worker to abandon the running job. Returns false if nothing is encoding.
    pub fn skip(&self) -> bool {
        if self.pid().is_none() {
            return false;
        }
        self.skip.notify_waiters();
        true
    }

    /// Set the throttle level, applying it to the running encoder if there is one.
    pub fn set_throttle(&self, level: ThrottleLevel) -> Result<()> {
        *self.throttle.lock().unwrap() = level;
        self.pending_apply.store(false, Ordering::SeqCst);
        if let Some(pid) = self.pid() {
            apply(&self.taskpolicy, pid, level)?;
            info!(pid, level = level.as_str(), "encoder throttle applied");
        }
        Ok(())
    }

    /// Called by the encoder when HandBrakeCLI starts (`Some(pid)`) and exits (`None`).
    pub fn set_pid(&self, pid: Option<u32>) {
        match pid {
            Some(p) => {
                self.encoder_pid.store(p, Ordering::SeqCst);
                // Defer a non-default level until worker threads exist (see module docs).
                let defer = self.throttle() != ThrottleLevel::Normal;
                self.pending_apply.store(defer, Ordering::SeqCst);
            }
            None => {
                self.encoder_pid.store(0, Ordering::SeqCst);
                self.stopped.store(false, Ordering::SeqCst);
                self.pending_apply.store(false, Ordering::SeqCst);
            }
        }
    }

    /// Apply a deferred throttle level. Cheap; the encoder calls it on every progress tick.
    pub fn apply_pending(&self) {
        if !self.pending_apply.swap(false, Ordering::SeqCst) {
            return;
        }
        if let Some(pid) = self.pid() {
            let level = self.throttle();
            match apply(&self.taskpolicy, pid, level) {
                Ok(()) => info!(pid, level = level.as_str(), "deferred encoder throttle applied"),
                Err(e) => warn!(pid, error = %e, "deferred throttle failed"),
            }
        }
    }

    pub fn view(&self) -> ControlView {
        ControlView {
            paused: self.is_paused(),
            stopped: self.stopped.load(Ordering::SeqCst),
            throttle: self.throttle(),
            encoder_pid: self.pid(),
        }
    }
}

fn signal(pid: u32, sig: libc::c_int) -> Result<()> {
    // SAFETY: plain kill(2) on a PID we spawned; no memory is touched.
    let rc = unsafe { libc::kill(pid as libc::pid_t, sig) };
    match rc {
        0 => Ok(()),
        _ => Err(anyhow!(std::io::Error::last_os_error())),
    }
}

/// Run `taskpolicy -b|-B -p PID`. Uses an absolute path because launchd's PATH is minimal.
pub fn apply(taskpolicy: &Path, pid: u32, level: ThrottleLevel) -> Result<()> {
    let flag = match level {
        ThrottleLevel::Normal => "-B",
        ThrottleLevel::Background => "-b",
    };
    let out = std::process::Command::new(taskpolicy)
        .arg(flag)
        .arg("-p")
        .arg(pid.to_string())
        .output()
        .with_context(|| format!("failed to run {}", taskpolicy.display()))?;
    ensure!(
        out.status.success(),
        "taskpolicy {flag} -p {pid} failed: {}",
        String::from_utf8_lossy(&out.stderr).trim()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn throttle_level_serde_is_lowercase() {
        assert_eq!(serde_json::to_string(&ThrottleLevel::Background).unwrap(), "\"background\"");
        let parsed: ThrottleLevel = serde_json::from_str("\"normal\"").unwrap();
        assert_eq!(parsed, ThrottleLevel::Normal);
        assert!(serde_json::from_str::<ThrottleLevel>("\"nice\"").is_err());
    }

    #[test]
    fn pause_resume_toggle_gate_without_pid() {
        let c = Control::with_taskpolicy("/usr/bin/true");
        assert!(!c.is_paused());
        c.pause(true).unwrap(); // no pid: nothing to freeze, gate still closes
        assert!(c.is_paused());
        assert!(!c.view().stopped);
        c.resume().unwrap();
        assert!(!c.is_paused());
    }

    #[test]
    fn skip_is_noop_when_idle() {
        let c = Control::with_taskpolicy("/usr/bin/true");
        assert!(!c.skip());
    }

    #[test]
    fn set_pid_defers_background_and_clears_on_exit() {
        let c = Control::with_taskpolicy("/usr/bin/true");
        c.set_throttle(ThrottleLevel::Background).unwrap(); // no pid yet: stored only
        c.set_pid(Some(std::process::id()));
        assert!(c.pending_apply.load(Ordering::SeqCst));
        c.apply_pending(); // /usr/bin/true succeeds
        assert!(!c.pending_apply.load(Ordering::SeqCst));
        assert_eq!(c.view().encoder_pid, Some(std::process::id()));
        c.set_pid(None);
        assert_eq!(c.view().encoder_pid, None);
        assert_eq!(c.throttle(), ThrottleLevel::Background); // level persists across jobs
    }

    #[test]
    fn apply_reports_taskpolicy_failure() {
        let err = apply(Path::new("/usr/bin/false"), 1, ThrottleLevel::Background).unwrap_err();
        assert!(err.to_string().contains("taskpolicy -b -p 1 failed"));
        assert!(apply(Path::new("/nonexistent/taskpolicy"), 1, ThrottleLevel::Normal).is_err());
    }
}
