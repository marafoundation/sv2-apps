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

/// Waits for SIGINT (Ctrl+C) or, on Unix, SIGTERM. Returns `false` if no handler could be
/// installed, in which case no signal-driven shutdown happens.
async fn shutdown_signal() -> bool {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(sigterm) => sigterm,
            Err(e) => {
                tracing::error!("Failed to install SIGTERM handler: {e}");
                return tokio::signal::ctrl_c().await.is_ok();
            }
        };
        tokio::select! {
            res = tokio::signal::ctrl_c() => match res {
                Ok(()) => tracing::info!("Ctrl+C received — initiating graceful shutdown..."),
                // The SIGTERM handler is installed by now, replacing SIGTERM's default action,
                // so returning here would leave the process ignoring SIGTERM.
                Err(e) => {
                    tracing::error!("Failed to listen for Ctrl+C: {e}");
                    sigterm.recv().await;
                    tracing::info!("SIGTERM received — initiating graceful shutdown...");
                }
            },
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM received — initiating graceful shutdown...");
            }
        }
        true
    }
    #[cfg(not(unix))]
    {
        let received = tokio::signal::ctrl_c().await.is_ok();
        if received {
            tracing::info!("Ctrl+C received — initiating graceful shutdown...");
        }
        received
    }
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

    let translator = TranslatorSv2::new(proxy_config);
    tokio::spawn({
        let translator = translator.clone();
        async move {
            if shutdown_signal().await {
                translator.drain_and_shutdown().await;
            }
        }
    });

    if translator.start().await.is_err() {
        std::process::exit(1);
    };
}
