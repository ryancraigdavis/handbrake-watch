//! Optional embedded HTTP server: live dashboard, JSON, Prometheus metrics, and
//! a small control API (pause / resume / skip / throttle).
//!
//! Bind it to your LAN and reach it over your home VPN — do not expose it to
//! the public internet. An optional token guards the data and control endpoints.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Json, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::Deserialize;
use serde_json::json;
use tracing::{info, warn};

use crate::config::Server;
use crate::control::{Control, ThrottleLevel};
use crate::queue::Queue;
use crate::status::{FolderStatus, StatusResponse, StatusStore};

/// Live list of watched folder names, shared so a runtime folder addition can
/// append to it and have the dashboard reflect the new folder immediately.
pub type FolderNames = Arc<Mutex<Vec<String>>>;

#[derive(Clone)]
struct AppState {
    store: Arc<StatusStore>,
    queue: Arc<Queue>,
    folders: FolderNames,
    control: Arc<Control>,
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
    folders: FolderNames,
    control: Arc<Control>,
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
        control,
        token: cfg.token.clone(),
    };
    let app = router(state);
    info!(%addr, "status server listening");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app.into_make_service()).await;
    });
    Ok(())
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/status.json", get(status_json))
        .route("/metrics", get(metrics))
        .route("/control/pause", post(control_pause))
        .route("/control/resume", post(control_resume))
        .route("/control/skip", post(control_skip))
        .route("/control/throttle", post(control_throttle))
        .with_state(state)
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
    let names = state.folders.lock().unwrap().clone();
    let folders = names
        .iter()
        .map(|name| FolderStatus {
            name: name.clone(),
            pending: counts.get(name).copied().unwrap_or(0),
            encoding: encoding_folder.as_deref() == Some(name),
        })
        .collect();
    let control = state.control.view();
    StatusResponse {
        uptime_secs: snapshot.uptime_secs,
        processed: snapshot.processed,
        failed: snapshot.failed,
        pending_total: counts.values().sum(),
        current: snapshot.current,
        folders,
        paused: control.paused,
        stopped: control.stopped,
        throttle: control.throttle.as_str().to_string(),
        encoder_pid: control.encoder_pid,
        mount_down: control.mount_down,
    }
}

// ---------------------------------------------------------------------------
// Control API. Every route takes the same token, returns the fresh status on
// success, 400 for a bad body, 409 when the action doesn't apply, 500 if the
// OS-level step (signal / taskpolicy) fails.
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct PauseBody {
    #[serde(default)]
    stop_current: bool,
}

#[derive(Deserialize)]
struct ThrottleBody {
    level: ThrottleLevel,
}

fn parse_body<T: serde::de::DeserializeOwned + Default>(body: &Bytes) -> Result<T, Response> {
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body)
        .map_err(|e| error_response(StatusCode::BAD_REQUEST, &e.to_string()))
}

fn error_response(code: StatusCode, msg: &str) -> Response {
    (code, Json(json!({ "error": msg }))).into_response()
}

fn control_result(state: &AppState, action: &str, result: Result<()>) -> Response {
    match result {
        Ok(()) => {
            info!(action, "control API");
            Json(build_status(state)).into_response()
        }
        Err(e) => {
            warn!(action, error = %e, "control API failed");
            error_response(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e:#}"))
        }
    }
}

async fn control_pause(
    State(state): State<AppState>,
    Query(query): Query<TokenQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !authorized(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let req: PauseBody = match parse_body(&body) {
        Ok(b) => b,
        Err(r) => return r,
    };
    control_result(&state, "pause", state.control.pause(req.stop_current))
}

async fn control_resume(
    State(state): State<AppState>,
    Query(query): Query<TokenQuery>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    control_result(&state, "resume", state.control.resume())
}

async fn control_skip(
    State(state): State<AppState>,
    Query(query): Query<TokenQuery>,
    headers: HeaderMap,
) -> Response {
    if !authorized(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match state.control.skip() {
        true => control_result(&state, "skip", Ok(())),
        false => error_response(StatusCode::CONFLICT, "nothing is encoding"),
    }
}

async fn control_throttle(
    State(state): State<AppState>,
    Query(query): Query<TokenQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if !authorized(&state, &headers, &query) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let req: ThrottleBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("level must be \"normal\" or \"background\": {e}"),
            )
        }
    };
    control_result(&state, "throttle", state.control.set_throttle(req.level))
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
    out.push_str("# HELP hbwatch_paused Whether the queue gate is closed (0/1).\n");
    out.push_str(&format!("hbwatch_paused {}\n", status.paused as u8));
    out.push_str("# HELP hbwatch_throttled Whether the encoder runs at background QoS (0/1).\n");
    out.push_str(&format!(
        "hbwatch_throttled {}\n",
        (status.throttle == "background") as u8
    ));
    out.push_str("# HELP hbwatch_mount_down Whether encodes are held for a missing mount (0/1).\n");
    out.push_str(&format!("hbwatch_mount_down {}\n", status.mount_down as u8));
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
            paused: false,
            stopped: false,
            throttle: "normal".into(),
            encoder_pid: None,
            mount_down: false,
        }
    }

    fn test_state(token: Option<&str>) -> AppState {
        let dir = std::env::temp_dir().join(format!("hbwatch-test-{}", uuid::Uuid::new_v4()));
        AppState {
            store: Arc::new(StatusStore::new(0)),
            queue: Arc::new(Queue::load(&dir.join("queue.json"))),
            folders: Arc::new(Mutex::new(vec!["movies".into()])),
            control: Arc::new(Control::with_taskpolicy("/usr/bin/true")),
            token: token.map(str::to_string),
        }
    }

    async fn call(
        app: Router,
        method: &str,
        path: &str,
        auth: Option<&str>,
        body: &str,
    ) -> (StatusCode, serde_json::Value) {
        use axum::body::Body;
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        let mut req = axum::http::Request::builder().method(method).uri(path);
        if let Some(a) = auth {
            req = req.header("authorization", a);
        }
        let req = req
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    #[tokio::test]
    async fn control_routes_require_token() {
        let state = test_state(Some("secret"));
        let (code, _) = call(router(state.clone()), "POST", "/control/pause", None, "").await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
        let (code, _) = call(
            router(state),
            "POST",
            "/control/pause",
            Some("Bearer wrong"),
            "",
        )
        .await;
        assert_eq!(code, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn pause_then_status_reports_paused_and_resume_clears() {
        let state = test_state(Some("secret"));
        let (code, body) = call(
            router(state.clone()),
            "POST",
            "/control/pause",
            Some("Bearer secret"),
            "",
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["paused"], true);
        let (_, body) = call(
            router(state.clone()),
            "GET",
            "/status.json?token=secret",
            None,
            "",
        )
        .await;
        assert_eq!(body["paused"], true);
        assert_eq!(body["throttle"], "normal");
        let (code, body) = call(
            router(state),
            "POST",
            "/control/resume",
            Some("Bearer secret"),
            "",
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["paused"], false);
    }

    #[tokio::test]
    async fn throttle_validates_level_and_persists_without_pid() {
        let state = test_state(None);
        let (code, body) = call(
            router(state.clone()),
            "POST",
            "/control/throttle",
            None,
            r#"{"level":"fast"}"#,
        )
        .await;
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("level"));
        let (code, body) = call(
            router(state.clone()),
            "POST",
            "/control/throttle",
            None,
            r#"{"level":"background"}"#,
        )
        .await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body["throttle"], "background");
        assert_eq!(state.control.throttle(), ThrottleLevel::Background);
    }

    #[tokio::test]
    async fn skip_when_idle_is_conflict() {
        let state = test_state(None);
        let (code, body) = call(router(state), "POST", "/control/skip", None, "").await;
        assert_eq!(code, StatusCode::CONFLICT);
        assert_eq!(body["error"], "nothing is encoding");
    }

    #[test]
    fn metrics_include_control_gauges() {
        let mut s = sample();
        s.paused = true;
        s.throttle = "background".into();
        let text = render_metrics(&s);
        assert!(text.contains("hbwatch_paused 1"));
        assert!(text.contains("hbwatch_throttled 1"));
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
