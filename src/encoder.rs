//! Run HandBrakeCLI and decide whether the encode genuinely succeeded.

use std::path::Path;
use std::process::{Output, Stdio};
use std::time::Duration;

use anyhow::{anyhow, ensure, Context, Result};
use tokio::process::{Child, Command};
use tracing::debug;

use crate::config::Settings;
use crate::queue::Job;

/// Encode `job` into the temp output path. Returns Ok only on a real success.
pub async fn run(settings: &Settings, job: &Job, temp: &Path) -> Result<()> {
    ensure_parent(&job.output_path).await;
    let child = spawn(settings, job, temp)?;
    let output = wait(child, settings.encode_timeout_secs).await?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    debug!(folder = %job.folder_name, %stderr, "HandBrakeCLI finished");
    verify(settings, temp, &output, &stderr).await
}

fn spawn(settings: &Settings, job: &Job, temp: &Path) -> Result<Child> {
    let child = Command::new(&settings.handbrake_cli)
        .arg("--preset-import-file")
        .arg(&job.preset_file)
        .arg("-Z")
        .arg(&job.preset_name)
        .arg("-i")
        .arg(&job.input_path)
        .arg("-o")
        .arg(temp)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to spawn {}", settings.handbrake_cli))?;
    Ok(child)
}

async fn wait(child: Child, timeout_secs: u64) -> Result<Output> {
    let output = match timeout_secs {
        0 => child.wait_with_output().await?,
        secs => tokio::time::timeout(Duration::from_secs(secs), child.wait_with_output())
            .await
            .map_err(|_| anyhow!("encode timed out after {secs}s"))??,
    };
    Ok(output)
}

async fn verify(settings: &Settings, temp: &Path, output: &Output, stderr: &str) -> Result<()> {
    ensure!(
        output.status.success(),
        "HandBrakeCLI exited with status {:?}",
        output.status.code()
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
