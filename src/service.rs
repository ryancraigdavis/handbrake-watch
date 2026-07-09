//! install/uninstall the background service: launchd on macOS, systemd on Linux.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// Write the service unit for the current OS, substituting real paths.
pub fn install(config_path: &Path) -> Result<()> {
    let bin = std::env::current_exe().context("cannot find own binary path")?;
    let config = absolute(config_path);
    match () {
        _ if cfg!(target_os = "macos") => install_launchd(&bin, &config),
        _ if cfg!(target_os = "linux") => install_systemd(&bin, &config),
        _ => bail!("install is only supported on macOS and Linux"),
    }
}

/// Remove the service unit for the current OS.
pub fn uninstall() -> Result<()> {
    match () {
        _ if cfg!(target_os = "macos") => remove_unit(&launchd_path()?, LAUNCHD_UNLOAD),
        _ if cfg!(target_os = "linux") => remove_unit(&systemd_path()?, SYSTEMD_DISABLE),
        _ => bail!("uninstall is only supported on macOS and Linux"),
    }
}

fn install_launchd(bin: &Path, config: &Path) -> Result<()> {
    let logs = config_dir(config);
    let contents = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>com.user.hbwatch</string>
    <key>ProgramArguments</key>
    <array>
        <string>{bin}</string>
        <string>run</string>
        <string>--config</string>
        <string>{config}</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>StandardOutPath</key><string>{logs}/hbwatch.log</string>
    <key>StandardErrorPath</key><string>{logs}/hbwatch.err.log</string>
</dict>
</plist>
"#,
        bin = bin.display(),
        config = config.display(),
        logs = logs.display(),
    );
    let path = launchd_path()?;
    write_unit(&path, &contents)?;
    println!("Installed launchd agent: {}", path.display());
    println!("Load it with:\n  launchctl load {}", path.display());
    Ok(())
}

fn install_systemd(bin: &Path, config: &Path) -> Result<()> {
    let contents = format!(
        r#"[Unit]
Description=hbwatch HandBrake auto-transcode watcher
After=network-online.target remote-fs.target
Wants=network-online.target

[Service]
ExecStart={bin} run --config {config}
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
"#,
        bin = bin.display(),
        config = config.display(),
    );
    let path = systemd_path()?;
    write_unit(&path, &contents)?;
    println!("Installed systemd user unit: {}", path.display());
    println!("Enable it with:");
    println!("  systemctl --user daemon-reload");
    println!("  systemctl --user enable --now hbwatch.service");
    println!("  loginctl enable-linger \"$USER\"   # survive logout on a 24/7 box");
    Ok(())
}

fn remove_unit(path: &Path, hint: &str) -> Result<()> {
    if path.exists() {
        std::fs::remove_file(path)
            .with_context(|| format!("failed to remove {}", path.display()))?;
        println!("Removed {}", path.display());
    } else {
        println!("Nothing to remove at {}", path.display());
    }
    println!("{hint}");
    Ok(())
}

fn write_unit(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::write(path, contents)
        .with_context(|| format!("failed to write {}", path.display()))?;
    Ok(())
}

fn launchd_path() -> Result<PathBuf> {
    Ok(home()?.join("Library/LaunchAgents/com.user.hbwatch.plist"))
}

fn systemd_path() -> Result<PathBuf> {
    Ok(home()?.join(".config/systemd/user/hbwatch.service"))
}

fn config_dir(config: &Path) -> PathBuf {
    config
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."))
}

fn absolute(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}

const LAUNCHD_UNLOAD: &str =
    "Unload it with:\n  launchctl unload ~/Library/LaunchAgents/com.user.hbwatch.plist";
const SYSTEMD_DISABLE: &str = "Disable it with:\n  systemctl --user disable --now hbwatch.service";
