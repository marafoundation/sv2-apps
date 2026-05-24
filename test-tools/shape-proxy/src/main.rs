mod config;
mod upstream;

use std::path::PathBuf;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser)]
#[command(name = "shape-proxy", about = "SV2 share-gating proxy for vardiff testing")]
struct Args {
    #[arg(short, long, help = "Path to TOML configuration file")]
    config: PathBuf,
}

#[tokio::main]
async fn main() {
    fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("shape_proxy=info".parse().unwrap()))
        .init();

    let args = Args::parse();

    let cfg = match config::Config::from_file(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to load config: {e}");
            std::process::exit(1);
        }
    };

    info!("Shape proxy starting");
    info!("  Upstream: {}", cfg.upstream_address);
    info!("  Downstream listen: {}", cfg.downstream_listen);
    info!("  Difficulty floor: {}", cfg.min_downstream_difficulty);
    info!("  API listen: {}", cfg.api_listen);

    let (mut reader, mut writer) = match upstream::connect_upstream(&cfg).await {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to connect upstream: {e}");
            std::process::exit(1);
        }
    };

    if let Err(e) = upstream::setup_connection(&mut reader, &mut writer).await {
        error!("SetupConnection failed: {e}");
        std::process::exit(1);
    }

    info!("Upstream connection established. Shape proxy ready.");

    // TODO: Step 2 — downstream listener + channel open
    // TODO: Step 3 — message passthrough
    // TODO: Step 4 — share gate + floor
    // For now, just hold the connection open
    tokio::signal::ctrl_c().await.ok();
    info!("Shutting down");
}
