mod args;
#[cfg(all(
    feature = "jemalloc",
    not(feature = "hotpath-alloc"),
    not(target_env = "msvc")
))]
mod jemalloc;
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
                    res = tokio::signal::ctrl_c() => match res {
                        Ok(()) => tracing::info!("Ctrl+C received — initiating graceful shutdown..."),
                        // The SIGTERM handler is installed by now, replacing SIGTERM's default
                        // action, so returning here would leave the process ignoring SIGTERM.
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

/// On every SIGUSR1, returns the allocator's free heap memory to the OS and logs heap usage
/// before and after. The handler is installed before this returns.
#[cfg(all(unix, feature = "monitoring"))]
fn release_heap_on_sigusr1() {
    use stratum_apps::monitoring::heap;
    use tokio::signal::unix::{SignalKind, signal};

    let mut signals = match signal(SignalKind::user_defined1()) {
        Ok(signals) => signals,
        Err(e) => {
            tracing::error!("Failed to install SIGUSR1 handler: {e}");
            return;
        }
    };
    tokio::spawn(async move {
        while signals.recv().await.is_some() {
            // Holds the allocator's locks for as long as it runs.
            let report = tokio::task::spawn_blocking(|| {
                let before = (heap::heap_stats(), heap::resident_bytes());
                let released = heap::release_free_heap();
                (
                    before,
                    released,
                    (heap::heap_stats(), heap::resident_bytes()),
                )
            })
            .await;
            match report {
                // Resident memory is what shows the effect: the release can lower it without
                // changing the heap figures, which only move when the heap itself shrinks, and
                // `released` may mean only that the release ran.
                Ok(((Some(before), rss_before), released, (Some(after), rss_after))) => {
                    tracing::info!(
                        released,
                        resident_before = rss_before,
                        resident_after = rss_after,
                        in_use_before = before.in_use,
                        in_use_after = after.in_use,
                        free_before = before.free,
                        free_after = after.free,
                        mapped_before = before.mapped,
                        mapped_after = after.mapped,
                        "SIGUSR1: asked the allocator to return free heap memory to the OS; compare \
                         resident_before and resident_after for what it returned"
                    )
                }
                Ok(_) => tracing::warn!(
                    "SIGUSR1: this build's allocator cannot report or release heap memory"
                ),
                Err(e) => tracing::error!("SIGUSR1: heap release failed: {e}"),
            }
        }
    });
}

/// Entrypoint for the Translator binary.
///
/// Loads the configuration from TOML and initializes the main runtime
/// defined in `translator_sv2::TranslatorSv2`. Errors during startup are logged.
#[cfg_attr(not(test), hotpath::main(limit = 0))]
async fn inner_main() {
    #[cfg(all(
        feature = "jemalloc",
        not(feature = "hotpath-alloc"),
        not(target_env = "msvc")
    ))]
    jemalloc::register_heap_allocator();
    let proxy_config = process_cli_args().unwrap_or_else(|e| {
        eprintln!("Translator proxy config error: {e}");
        std::process::exit(1);
    });

    init_logging(proxy_config.log_dir());

    if proxy_config.release_heap_on_sigusr1 {
        #[cfg(all(unix, feature = "monitoring"))]
        release_heap_on_sigusr1();
        #[cfg(not(all(unix, feature = "monitoring")))]
        tracing::warn!("release_heap_on_sigusr1 needs a Unix build with the monitoring feature");
    }

    #[cfg(all(feature = "jemalloc", feature = "hotpath-alloc"))]
    tracing::warn!("hotpath-alloc installs its own allocator, so this build does not use jemalloc");

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
