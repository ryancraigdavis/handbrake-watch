//! `status` subcommand: query the running daemon's HTTP endpoint, or fall back
//! to reading the persisted queue file when the daemon isn't reachable.

use std::time::Duration;

use anyhow::Result;

use crate::config::ResolvedConfig;
use crate::queue::Queue;
use crate::status::StatusResponse;

pub async fn run(cfg: &ResolvedConfig) -> Result<()> {
    match fetch(cfg).await {
        Some(status) => print_live(&status),
        None => print_offline(cfg),
    }
    Ok(())
}

async fn fetch(cfg: &ResolvedConfig) -> Option<StatusResponse> {
    if !cfg.server.enabled {
        return None;
    }
    let host = local_host(&cfg.server.bind);
    let mut url = format!("http://{host}:{}/status.json", cfg.server.port);
    if let Some(token) = &cfg.server.token {
        url.push_str(&format!("?token={token}"));
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .ok()?;
    let response = client.get(url).send().await.ok()?;
    response.json::<StatusResponse>().await.ok()
}

/// Loopback address to reach a server that may be bound to all interfaces.
fn local_host(bind: &str) -> String {
    match bind {
        "0.0.0.0" | "::" | "[::]" => "127.0.0.1".to_string(),
        other => other.to_string(),
    }
}

fn print_live(status: &StatusResponse) {
    println!("hbwatch — up {}", fmt_dur(status.uptime_secs));
    match &status.current {
        Some(c) => {
            let pct = (c.fraction * 100.0).round() as u64;
            println!(
                "  encoding: {} [{}]  {pct}%  {}",
                c.film,
                c.folder,
                fmt_eta(c.eta_secs)
            );
        }
        None => println!("  idle"),
    }
    println!(
        "  queued {} · done {} · failed {}",
        status.pending_total, status.processed, status.failed
    );
    for folder in &status.folders {
        let state = match (folder.encoding, folder.pending) {
            (true, n) => format!("encoding ({n} queued)"),
            (false, 0) => "idle".to_string(),
            (false, n) => format!("{n} queued"),
        };
        println!("    {:<20} {state}", folder.name);
    }
}

fn print_offline(cfg: &ResolvedConfig) {
    println!(
        "(daemon not reachable — reading queue file {})",
        cfg.state_file.display()
    );
    let queue = Queue::load(&cfg.state_file);
    let counts = queue.pending_counts();
    let pending: usize = counts.values().sum();
    match queue.in_flight_name() {
        Some((folder, film)) => println!("  last in-flight: {film} [{folder}]"),
        None => println!("  idle"),
    }
    println!("  queued {pending}");
    for folder in &cfg.folders {
        let n = counts.get(&folder.name).copied().unwrap_or(0);
        println!(
            "    {:<20} {}",
            folder.name,
            if n > 0 {
                format!("{n} queued")
            } else {
                "idle".into()
            }
        );
    }
}

fn fmt_dur(secs: u64) -> String {
    let (d, h, m) = (secs / 86400, secs % 86400 / 3600, secs % 3600 / 60);
    match (d, h) {
        (0, 0) => format!("{m}m"),
        (0, _) => format!("{h}h {m}m"),
        _ => format!("{d}d {h}h"),
    }
}

fn fmt_eta(eta: Option<i64>) -> String {
    match eta {
        Some(s) if s > 0 => format!("ETA {}m{:02}s", s / 60, s % 60),
        _ => "ETA —".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_host_rewrites_wildcard_binds() {
        assert_eq!(local_host("0.0.0.0"), "127.0.0.1");
        assert_eq!(local_host("::"), "127.0.0.1");
        assert_eq!(local_host("192.168.1.5"), "192.168.1.5");
    }

    #[test]
    fn fmt_dur_scales_units() {
        assert_eq!(fmt_dur(90), "1m");
        assert_eq!(fmt_dur(3700), "1h 1m");
        assert_eq!(fmt_dur(90000), "1d 1h");
    }
}
