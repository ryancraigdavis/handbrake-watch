//! ntfy.sh push notifications. Fire-and-forget; failures never propagate.

use std::time::Duration;

use reqwest::Client;
use tracing::warn;

use crate::config::Notifications;
use crate::queue::Job;

pub struct Notifier {
    cfg: Notifications,
    client: Client,
}

impl Notifier {
    pub fn new(cfg: Notifications) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { cfg, client }
    }

    pub async fn failure(&self, job: &Job, reason: &str) {
        if self.cfg.on_failure {
            let body = format!("{} / {}\n{}", job.folder_name, file_name(job), reason);
            self.send("hbwatch: encode failed", "high", "warning", &body).await;
        }
    }

    pub async fn item_complete(&self, job: &Job) {
        if self.cfg.on_item_complete {
            let body = format!("{} → {}", file_name(job), job.output_path.display());
            self.send("hbwatch: done", "default", "white_check_mark", &body).await;
        }
    }

    pub async fn queue_drain(&self, count: u64, secs: u64) {
        if self.cfg.on_queue_drain {
            let body = format!("processed {count} item(s) in {secs}s");
            self.send("hbwatch: queue empty", "low", "checkered_flag", &body).await;
        }
    }

    async fn send(&self, title: &str, priority: &str, tags: &str, body: &str) {
        if !self.cfg.enabled {
            return;
        }
        let url = format!("{}/{}", self.cfg.server.trim_end_matches('/'), self.cfg.topic);
        let mut req = self
            .client
            .post(&url)
            .header("Title", title)
            .header("Priority", priority)
            .header("Tags", tags)
            .body(body.to_string());
        if let Some(token) = &self.cfg.token {
            req = req.bearer_auth(token);
        }
        if let Err(e) = req.send().await {
            warn!(error = %e, "ntfy notification failed");
        }
    }
}

fn file_name(job: &Job) -> String {
    job.input_path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}
