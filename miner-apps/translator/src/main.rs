mod args;
use stratum_apps::config_helpers::logging::init_logging;
pub use translator_sv2::{TranslatorSv2, config, error, sv1, sv2};

use crate::args::process_cli_args;

#[cfg(all(feature = "hotpath-alloc", not(test)))]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    inner_main().await;
}

#[cfg(not(all(feature = "hotpath-alloc", not(test))))]
#[tokio::main]
async fn main() {
    inner_main().await;
}

/// Waits for SIGINT (Ctrl+C) or, on Unix when `sigterm` is set, SIGTERM. Returns `false` if no
/// handler could be installed, in which case no signal-driven shutdown happens.
///
/// SIGTERM is only caught when a drain is configured: with `drain_seconds = 0` it keeps its
/// default action (immediate exit), exactly as before draining existed.
async fn shutdown_signal(sigterm: bool) -> bool {
    #[cfg(unix)]
    if sigterm {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    res = tokio::signal::ctrl_c() => {
                        if res.is_err() {
                            return false;
                        }
                        tracing::info!("Ctrl+C received — initiating graceful shutdown...");
                    }
                    _ = sigterm.recv() => {
                        tracing::info!("SIGTERM received — initiating graceful shutdown...");
                    }
                }
                return true;
            }
            Err(e) => tracing::error!("Failed to install SIGTERM handler: {e}"),
        }
    }
    #[cfg(not(unix))]
    let _ = sigterm;
    let received = tokio::signal::ctrl_c().await.is_ok();
    if received {
        tracing::info!("Ctrl+C received — initiating graceful shutdown...");
    }
    received
}

/// Entrypoint for the Translator binary.
///
/// Loads the configuration from TOML and initializes the main runtime
/// defined in `translator_sv2::TranslatorSv2`. Errors during startup are logged.
#[cfg_attr(not(test), hotpath::main(limit = 0))]
async fn inner_main() {
    let proxy_config = process_cli_args().unwrap_or_else(|e| {
        eprintln!("Translator proxy config error: {e}");
        std::process::exit(1);
    });

    init_logging(proxy_config.log_dir());

    let drain = !proxy_config.drain_window().is_zero();
    let translator = TranslatorSv2::new(proxy_config);
    tokio::spawn({
        let translator = translator.clone();
        async move {
            if shutdown_signal(drain).await {
                translator.drain_and_shutdown().await;
            }
        }
    });

    if translator.start().await.is_err() {
        std::process::exit(1);
    };
}
