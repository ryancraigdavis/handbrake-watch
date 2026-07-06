//! Finalize a successful encode: temp -> output, then original -> originals.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::queue::Job;

/// The in-progress encode path (final output path + `.hbtmp`).
pub fn temp_path(output: &Path) -> PathBuf {
    let mut name = OsString::from(output.as_os_str());
    name.push(".hbtmp");
    PathBuf::from(name)
}

/// Rename the temp output into place and move the original aside.
pub async fn finalize(job: &Job, temp: &Path) -> Result<()> {
    move_file(temp, &job.output_path)
        .await
        .context("failed to move encoded output into place")?;
    let dest = unique_dest(&job.originals_dir, &job.input_path).await;
    move_file(&job.input_path, &dest)
        .await
        .context("failed to move original aside")?;
    Ok(())
}

/// Move a permanently-failed input into `failed_dir` with an error sidecar.
pub async fn move_to_failed(input: &Path, failed_dir: &Path, reason: &str) -> Result<PathBuf> {
    let dest = unique_dest(failed_dir, input).await;
    move_file(input, &dest)
        .await
        .context("failed to move input into failed dir")?;
    write_sidecar(&dest, reason).await;
    Ok(dest)
}

async fn write_sidecar(moved: &Path, reason: &str) {
    let mut sidecar = OsString::from(moved.as_os_str());
    sidecar.push(".error.txt");
    let body = format!(
        "hbwatch: encode failed after retries\nfile: {}\nreason: {}\n",
        moved.display(),
        reason
    );
    let _ = tokio::fs::write(PathBuf::from(sidecar), body).await;
}

/// Move a file, falling back to copy+remove across filesystems (NAS shares).
async fn move_file(src: &Path, dst: &Path) -> Result<()> {
    if let Some(parent) = dst.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let result = match tokio::fs::rename(src, dst).await {
        Ok(()) => Ok(()),
        Err(_) => copy_remove(src, dst).await,
    };
    result
}

async fn copy_remove(src: &Path, dst: &Path) -> Result<()> {
    tokio::fs::copy(src, dst).await.context("copy failed")?;
    tokio::fs::remove_file(src).await.context("remove failed")?;
    Ok(())
}

/// Find a non-colliding destination in `dir` for `src`'s filename.
async fn unique_dest(dir: &Path, src: &Path) -> PathBuf {
    let name = src.file_name().unwrap_or_default();
    let base = dir.join(name);
    let dest = match exists(&base).await {
        false => base,
        true => disambiguate(dir, src).await,
    };
    dest
}

async fn disambiguate(dir: &Path, src: &Path) -> PathBuf {
    let stem = src
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned();
    let ext = src.extension().map(|e| e.to_string_lossy().into_owned());
    let mut i = 1u32;
    loop {
        let name = match &ext {
            Some(e) => format!("{stem} ({i}).{e}"),
            None => format!("{stem} ({i})"),
        };
        let candidate = dir.join(name);
        if !exists(&candidate).await {
            break candidate;
        }
        i += 1;
    }
}

async fn exists(path: &Path) -> bool {
    tokio::fs::try_exists(path).await.unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temp_path_appends_suffix() {
        let out = PathBuf::from("/media/encoded/clip.mp4");
        assert_eq!(
            temp_path(&out),
            PathBuf::from("/media/encoded/clip.mp4.hbtmp")
        );
    }

    #[tokio::test]
    async fn move_to_failed_moves_file_and_writes_sidecar() {
        let base = std::env::temp_dir().join(format!("hbwatch-failtest-{}", std::process::id()));
        let src_dir = base.join("in");
        let failed = base.join("failed");
        tokio::fs::create_dir_all(&src_dir).await.unwrap();
        let input = src_dir.join("bad.mkv");
        tokio::fs::write(&input, b"data").await.unwrap();

        let moved = move_to_failed(&input, &failed, "boom reason")
            .await
            .unwrap();

        assert!(moved.exists(), "moved file should exist in failed dir");
        assert!(!input.exists(), "original should be gone from source");
        let mut sidecar = OsString::from(moved.as_os_str());
        sidecar.push(".error.txt");
        let body = tokio::fs::read_to_string(PathBuf::from(sidecar))
            .await
            .unwrap();
        assert!(body.contains("boom reason"));
        let _ = tokio::fs::remove_dir_all(&base).await;
    }
}
