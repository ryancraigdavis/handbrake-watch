//! Run HandBrakeCLI with --json, stream progress, and verify real success.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, Command};
use tracing::debug;

use crate::config::Settings;
use crate::progress::Reporter;
use crate::queue::Job;

/// One HandBrake `Progress: {...}` record.
#[derive(Debug, Deserialize)]
struct ProgressMsg {
    #[serde(rename = "State")]
    state: String,
    #[serde(rename = "Working")]
    working: Option<Working>,
    #[serde(rename = "WorkDone")]
    work_done: Option<WorkDone>,
}

#[derive(Debug, Deserialize)]
struct Working {
    #[serde(rename = "Progress")]
    progress: f64,
    #[serde(rename = "ETASeconds")]
    eta_seconds: Option<i64>,
    #[serde(rename = "Rate")]
    rate: Option<f64>,
    #[serde(rename = "RateAvg")]
    rate_avg: Option<f64>,
    #[serde(rename = "Pass")]
    pass: Option<i64>,
    #[serde(rename = "PassCount")]
    pass_count: Option<i64>,
}

impl Working {
    /// Fold multi-pass progress into a single monotonic 0.0..=1.0 fraction.
    fn fraction(&self) -> f64 {
        let combined = match (self.pass, self.pass_count) {
            (Some(p), Some(c)) if c > 1 && p >= 1 => ((p - 1) as f64 + self.progress) / c as f64,
            _ => self.progress,
        };
        combined.clamp(0.0, 1.0)
    }
}

#[derive(Debug, Deserialize)]
struct WorkDone {
    #[serde(rename = "Error")]
    error: i32,
}

/// Encode `job` into the temp output path. Returns Ok only on a real success.
pub async fn run(
    settings: &Settings,
    job: &Job,
    temp: &Path,
    reporter: &Arc<dyn Reporter>,
) -> Result<()> {
    ensure_parent(&job.output_path).await;
    let mut child = spawn(settings, job, temp)?;
    let stdout = child.stdout.take().context("missing HandBrake stdout")?;
    let stderr = child.stderr.take().context("missing HandBrake stderr")?;
    let stderr_task = tokio::spawn(drain(stderr));
    let (work_error, status) =
        supervise(child, stdout, reporter, settings.encode_timeout_secs).await?;
    let stderr_text = stderr_task.await.unwrap_or_default();
    debug!(folder = %job.folder_name, %stderr_text, "HandBrakeCLI finished");
    verify(settings, temp, status.success(), work_error, &stderr_text).await
}

fn spawn(settings: &Settings, job: &Job, temp: &Path) -> Result<Child> {
    let child = Command::new(&settings.handbrake_cli)
        .arg("--json")
        .arg("--preset-import-file")
        .arg(&job.preset_file)
        .arg("-Z")
        .arg(&job.preset_name)
        .arg("-i")
        .arg(&job.input_path)
        .arg("-o")
        .arg(temp)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to spawn {}", settings.handbrake_cli))?;
    Ok(child)
}

/// Read progress until stdout closes, then collect the exit status, with timeout.
async fn supervise(
    mut child: Child,
    stdout: tokio::process::ChildStdout,
    reporter: &Arc<dyn Reporter>,
    timeout_secs: u64,
) -> Result<(Option<i32>, std::process::ExitStatus)> {
    let work = async {
        let error = read_progress(stdout, reporter).await;
        let status = child.wait().await?;
        Ok::<_, anyhow::Error>((error, status))
    };
    let result = match timeout_secs {
        0 => work.await?,
        secs => tokio::time::timeout(Duration::from_secs(secs), work)
            .await
            .map_err(|_| anyhow!("encode timed out after {secs}s"))??,
    };
    Ok(result)
}

/// Parse HandBrake's pretty-printed `Progress:` blocks; return the final error code.
async fn read_progress(
    stdout: tokio::process::ChildStdout,
    reporter: &Arc<dyn Reporter>,
) -> Option<i32> {
    let mut lines = BufReader::new(stdout).lines();
    let mut buffer = String::new();
    let mut depth = 0i32;
    let mut error = None;
    while let Ok(Some(line)) = lines.next_line().await {
        accumulate(&line, &mut buffer, &mut depth);
        if !buffer.is_empty() && depth <= 0 {
            if let Some(code) = handle_block(&buffer, reporter) {
                error = Some(code);
            }
            buffer.clear();
        }
    }
    error
}

fn accumulate(line: &str, buffer: &mut String, depth: &mut i32) {
    let chunk = match buffer.is_empty() {
        true => line.strip_prefix("Progress:").map(str::trim_start),
        false => Some(line),
    };
    if let Some(text) = chunk {
        if !buffer.is_empty() {
            buffer.push('\n');
        }
        buffer.push_str(text);
        *depth += brace_delta(text);
    }
}

fn brace_delta(text: &str) -> i32 {
    let opens = text.matches('{').count() as i32;
    let closes = text.matches('}').count() as i32;
    opens - closes
}

fn handle_block(buffer: &str, reporter: &Arc<dyn Reporter>) -> Option<i32> {
    let msg: ProgressMsg = serde_json::from_str(buffer).ok()?;
    if let Some(w) = &msg.working {
        reporter.job_tick(
            &msg.state,
            w.fraction(),
            w.eta_seconds,
            w.rate_avg.or(w.rate),
        );
    }
    msg.work_done.map(|d| d.error)
}

async fn drain(stderr: tokio::process::ChildStderr) -> String {
    let mut buf = String::new();
    let _ = BufReader::new(stderr).read_to_string(&mut buf).await;
    buf
}

async fn verify(
    settings: &Settings,
    temp: &Path,
    status_ok: bool,
    work_error: Option<i32>,
    stderr: &str,
) -> Result<()> {
    ensure!(status_ok, "HandBrakeCLI exited unsuccessfully");
    ensure!(
        work_error.unwrap_or(0) == 0,
        "HandBrakeCLI reported WorkDone error {}",
        work_error.unwrap_or(0)
    );
    let meta = tokio::fs::metadata(temp)
        .await
        .context("output file missing after encode")?;
    ensure!(
        meta.len() >= settings.min_output_bytes,
        "output too small: {} bytes (min {})",
        meta.len(),
        settings.min_output_bytes
    );
    ensure!(
        !has_failure_marker(stderr),
        "HandBrakeCLI reported a failure in its output"
    );
    Ok(())
}

fn has_failure_marker(stderr: &str) -> bool {
    stderr.contains("Encode failed") || stderr.contains("Error:")
}

async fn ensure_parent(output: &Path) {
    if let Some(parent) = output.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(lines: &[&str]) -> ProgressMsg {
        let mut buffer = String::new();
        let mut depth = 0;
        for line in lines {
            accumulate(line, &mut buffer, &mut depth);
        }
        assert_eq!(depth, 0, "braces did not balance");
        serde_json::from_str(&buffer).unwrap()
    }

    #[test]
    fn assembles_and_parses_working_block() {
        let msg = parse(&[
            "Progress: {",
            "    \"State\": \"WORKING\",",
            "    \"Working\": {",
            "        \"Progress\": 0.5,",
            "        \"Pass\": 1,",
            "        \"PassCount\": 2",
            "    }",
            "}",
        ]);
        assert_eq!(msg.state, "WORKING");
        assert!((msg.working.unwrap().fraction() - 0.25).abs() < 1e-9);
    }

    #[test]
    fn reads_workdone_error() {
        let msg = parse(&[
            "Progress: {",
            "    \"State\": \"WORKDONE\",",
            "    \"WorkDone\": { \"Error\": 3 }",
            "}",
        ]);
        assert_eq!(msg.work_done.unwrap().error, 3);
    }

    #[test]
    fn single_pass_fraction_is_progress() {
        let w = Working {
            progress: 0.7,
            eta_seconds: None,
            rate: None,
            rate_avg: None,
            pass: Some(1),
            pass_count: Some(1),
        };
        assert!((w.fraction() - 0.7).abs() < 1e-9);
    }

    #[test]
    fn second_pass_fraction_is_offset() {
        let w = Working {
            progress: 0.5,
            eta_seconds: None,
            rate: None,
            rate_avg: None,
            pass: Some(2),
            pass_count: Some(2),
        };
        assert!((w.fraction() - 0.75).abs() < 1e-9);
    }
}
