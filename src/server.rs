//! Optional embedded HTTP server: live dashboard, JSON, and Prometheus metrics.
//!
//! Bind it to your LAN and reach it over your home VPN — do not expose it to
//! the public internet. An optional token guards the data endpoints.

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use tracing::info;

use crate::config::Server;
use crate::queue::Queue;
use crate::status::{FolderStatus, StatusResponse, StatusStore};

#[derive(Clone)]
struct AppState {
    store: Arc<StatusStore>,
    queue: Arc<Queue>,
    folders: Vec<String>,
    token: Option<String>,
}

#[derive(Deserialize)]
struct TokenQuery {
    token: Option<String>,
}

/// Start the status server if enabled. Returns immediately after binding.
pub async fn spawn(
    cfg: &Server,
    store: Arc<StatusStore>,
    queue: Arc<Queue>,
    folders: Vec<String>,
) -> Result<()> {
    if !cfg.enabled {
        return Ok(());
    }
    let addr = format!("{}:{}", cfg.bind, cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind status server to {addr}"))?;
    let state = AppState {
        store,
        queue,
        folders,
        token: cfg.token.clone(),
    };
    let app = Router::new()
        .route("/", get(index))
        .route("/status.json", get(status_json))
        .route("/metrics", get(metrics))
        .with_state(state);
    info!(%addr, "status server listening");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    Ok(())
}

fn authorized(state: &AppState, headers: &HeaderMap, query: &TokenQuery) -> bool {
    let required = match &state.token {
        None => return true,
        Some(t) => t,
    };
    let from_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    from_header == Some(required.as_str()) || query.token.as_deref() == Some(required.as_str())
}

fn build_status(state: &AppState) -> StatusResponse {
    let snapshot = state.store.snapshot();
    let counts = state.queue.pending_counts();
    let encoding_folder = snapshot.current.as_ref().map(|c| c.folder.clone());
    let folders = state
        .folders
        .iter()
        .map(|name| FolderStatus {
            name: name.clone(),
            pending: counts.get(name).copied().unwrap_or(0),
            encoding: encoding_folder.as_deref() == Some(name),
        })
        .collect();
    StatusResponse {
        uptime_secs: snapshot.uptime_secs,
        processed: snapshot.processed,
        failed: snapshot.failed,
        pending_total: counts.values().sum(),
        current: snapshot.current,
        folders,
    }
}

async fn status_json(
    State(state): State<AppState>,
    Query(query): Query<TokenQuery>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    Json(build_status(&state)).into_response()
}

async fn metrics(
    State(state): State<AppState>,
    Query(query): Query<TokenQuery>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    render_metrics(&build_status(&state)).into_response()
}

fn render_metrics(status: &StatusResponse) -> String {
    let mut out = String::new();
    out.push_str("# HELP hbwatch_uptime_seconds Daemon uptime.\n");
    out.push_str(&format!("hbwatch_uptime_seconds {}\n", status.uptime_secs));
    out.push_str("# HELP hbwatch_processed_total Successful encodes.\n");
    out.push_str(&format!("hbwatch_processed_total {}\n", status.processed));
    out.push_str("# HELP hbwatch_failed_total Permanently failed encodes.\n");
    out.push_str(&format!("hbwatch_failed_total {}\n", status.failed));
    out.push_str("# HELP hbwatch_queue_pending Jobs waiting per folder.\n");
    for folder in &status.folders {
        out.push_str(&format!(
            "hbwatch_queue_pending{{folder=\"{}\"}} {}\n",
            folder.name, folder.pending
        ));
    }
    let progress = status.current.as_ref().map(|c| c.fraction).unwrap_or(0.0);
    out.push_str("# HELP hbwatch_current_progress Progress of the active encode (0-1).\n");
    out.push_str(&format!("hbwatch_current_progress {progress}\n"));
    out
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

const INDEX_HTML: &str = include_str!("dashboard.html");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::Current;

    fn sample() -> StatusResponse {
        StatusResponse {
            uptime_secs: 10,
            processed: 3,
            failed: 1,
            pending_total: 2,
            current: Some(Current {
                folder: "movies".into(),
                film: "x.mkv".into(),
                fraction: 0.5,
                eta_secs: Some(30),
                fps: Some(24.0),
                state: "WORKING".into(),
            }),
            folders: vec![FolderStatus {
                name: "movies".into(),
                pending: 2,
                encoding: true,
            }],
        }
    }

    #[test]
    fn metrics_include_progress_and_labels() {
        let text = render_metrics(&sample());
        assert!(text.contains("hbwatch_processed_total 3"));
        assert!(text.contains("hbwatch_failed_total 1"));
        assert!(text.contains("hbwatch_queue_pending{folder=\"movies\"} 2"));
        assert!(text.contains("hbwatch_current_progress 0.5"));
    }

    #[test]
    fn dashboard_html_is_self_contained() {
        assert!(INDEX_HTML.contains("status.json"));
        assert!(
            !INDEX_HTML.contains("http://"),
            "must not reference external hosts"
        );
    }
}
