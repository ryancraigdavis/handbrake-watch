//! hbwatch — folder-watching HandBrake auto-transcode daemon.

mod config;
mod encoder;
mod mover;
mod notify_ntfy;
mod preset;
mod progress;
mod queue;
mod runtime;
mod scan;
mod watcher;

use std::io::IsTerminal;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use config::ResolvedConfig;

#[derive(Parser)]
#[command(
    name = "hbwatch",
    version,
    about = "Folder-watching HandBrake auto-transcoder"
)]
struct Cli {
    /// Path to the config file
    #[arg(long, short)]
    config: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Start the watcher daemon (default)
    Run,
    /// Validate config, print the resolved plan, and exit
    Check,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config_path = cli.config.unwrap_or_else(default_config_path);
    let raw = config::load(&config_path)?;
    init_tracing(&raw.settings.log_level);
    let resolved = config::resolve(raw)?;
    dispatch(cli.command, resolved).await
}

async fn dispatch(command: Option<Command>, cfg: ResolvedConfig) -> Result<()> {
    match command.unwrap_or(Command::Run) {
        Command::Run => runtime::run(cfg).await,
        Command::Check => {
            print_plan(&cfg);
            Ok(())
        }
    }
}

fn print_plan(cfg: &ResolvedConfig) {
    println!("hbwatch configuration ({} folder(s)):\n", cfg.folders.len());
    println!("  watch_mode: {:?}", cfg.settings.watch_mode);
    println!("  handbrake:  {}", cfg.settings.handbrake_cli);
    println!("  state_file: {}\n", cfg.state_file.display());
    for folder in &cfg.folders {
        println!("  [{}]", folder.name);
        println!("    watch:     {}", folder.watch_dir.display());
        println!(
            "    preset:    {} (-Z \"{}\")",
            folder.preset_file.display(),
            folder.preset_name
        );
        println!(
            "    output:    {}/*.{}",
            folder.output_dir.display(),
            folder.output_extension
        );
        println!("    originals: {}\n", folder.originals_dir.display());
    }
    let n = &cfg.notifications;
    println!(
        "  notifications: enabled={} failure={} item={} drain={}",
        n.enabled, n.on_failure, n.on_item_complete, n.on_queue_drain
    );
}

fn init_tracing(level: &str) {
    // On a TTY the indicatif bars own the screen, so keep logs quiet and on
    // stderr to avoid fighting the bars; under a service, log normally.
    let interactive = std::io::stdout().is_terminal();
    let chosen = match interactive {
        true => "warn",
        false => level,
    };
    let filter = EnvFilter::try_new(chosen).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

fn default_config_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".config/hbwatch/config.toml")
}
