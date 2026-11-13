mod args;
#[cfg(all(
    feature = "jemalloc",
    not(feature = "hotpath-alloc"),
    not(target_env = "msvc")
))]
mod jemalloc;
use stratum_apps::{config_helpers::logging::init_logging, utils::shutdown::ShutdownListener};
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
                // Resident memory is logged too: the release can lower it without changing the
                // heap figures, which only move when the heap itself shrinks.
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
                        "SIGUSR1: returned free heap memory to the OS"
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
        feature = "monitoring",
        not(feature = "hotpath-alloc"),
        not(target_env = "msvc")
    ))]
    jemalloc::report_no_heap_stats();
    let proxy_config = process_cli_args().unwrap_or_else(|e| {
        eprintln!("Translator proxy config error: {e}");
        std::process::exit(1);
    });

    init_logging(proxy_config.log_dir());

    #[cfg(all(feature = "jemalloc", feature = "hotpath-alloc"))]
    tracing::warn!("hotpath-alloc installs its own allocator, so this build does not use jemalloc");

    if proxy_config.release_heap_on_sigusr1 {
        #[cfg(all(unix, feature = "monitoring"))]
        release_heap_on_sigusr1();
        #[cfg(not(all(unix, feature = "monitoring")))]
        tracing::warn!("release_heap_on_sigusr1 needs a Unix build with the monitoring feature");
    }

    let translator = TranslatorSv2::new(proxy_config);
    let mut shutdown_listener = ShutdownListener::new().unwrap_or_else(|e| {
        tracing::error!("Translator proxy could not listen for shutdown signals: {e}");
        std::process::exit(1);
    });
    tokio::spawn({
        let translator = translator.clone();
        async move { stop_on_signal(translator, &mut shutdown_listener).await }
    });

    if translator.start().await.is_err() {
        std::process::exit(1);
    };
}

/// Waits for a stop signal and starts the graceful path: draining the SV1 downstreams for up to
/// `drain_seconds`, then shutting down. With `drain_seconds = 0` that is an immediate shutdown. A
/// second signal abandons the graceful path and exits the process.
async fn stop_on_signal(translator: TranslatorSv2, listener: &mut ShutdownListener) {
    let signal = listener.wait().await;
    tracing::info!("{signal} received — initiating graceful shutdown...");
    tokio::select! {
        _ = translator.drain_and_shutdown() => {}
        signal = listener.wait() => {
            tracing::warn!("{signal} received again — abandoning graceful shutdown");
            std::process::exit(signal.exit_code());
        }
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::{process::Command, str::FromStr, time::Duration};
    use stratum_apps::key_utils::Secp256k1PublicKey;
    use translator_sv2::config::{DownstreamDifficultyConfig, TranslatorConfig, Upstream};

    /// The first stop signal starts a drain rather than an immediate shutdown. This is the only
    /// test in the binary because the signal is delivered to the whole process.
    #[tokio::test]
    async fn first_signal_starts_the_drain() {
        let pubkey =
            Secp256k1PublicKey::from_str("9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan")
                .unwrap();
        let upstream = Upstream::new("127.0.0.1".into(), 4444, pubkey, "test".into());
        let mut config = TranslatorConfig::new(
            vec![upstream],
            "127.0.0.1".into(),
            0,
            DownstreamDifficultyConfig::new(100.0, 5.0, true, 60),
            2,
            1,
            4,
            false,
            true,
            vec![],
            vec![],
            None,
            None,
        );
        config.drain_seconds = 30;
        let translator = TranslatorSv2::new(config);
        let mut listener = ShutdownListener::new().unwrap();
        let stop = tokio::spawn({
            let translator = translator.clone();
            async move { stop_on_signal(translator, &mut listener).await }
        });
        assert!(!translator.is_draining());

        let status = Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while !translator.is_draining() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the signal did not start a drain"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        stop.abort();
    }
}
