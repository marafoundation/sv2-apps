//! CLI argument parsing for the Pool binary.
//!
//! Defines the `Args` struct and a function to process CLI arguments into a PoolConfig.

use clap::Parser;
use pool_sv2::config::PoolConfig;
use std::path::PathBuf;
use stratum_apps::config_helpers::load_config;

/// Holds the parsed CLI arguments for the Pool binary.
#[derive(Parser, Debug)]
#[command(author, version, about = "Pool CLI", long_about = None)]
pub struct Args {
    #[arg(
        short = 'c',
        long = "config",
        help = "Path to the TOML configuration file",
        default_value = "pool-config.toml"
    )]
    pub config_path: PathBuf,
    #[arg(
        short = 'f',
        long = "log-file",
        help = "Path to the log file. If not set, logs will only be written to stdout."
    )]
    pub log_file: Option<PathBuf>,
}

#[cfg_attr(not(test), hotpath::measure)]
/// Parses CLI arguments and loads the PoolConfig from the specified file.
pub fn process_cli_args() -> PoolConfig {
    let args = Args::parse();
    let config_path = args.config_path.to_str().expect("Invalid config path");

    // Configuration is read from the optional TOML file and the environment,
    // with environment variables (prefixed `POOL__`) taking precedence.
    let mut config: PoolConfig = load_config(
        config_path,
        "POOL",
        &["supported_extensions", "required_extensions"],
    )
    .expect("Failed to load or deserialize config");

    config.set_log_dir(args.log_file);

    config
}
