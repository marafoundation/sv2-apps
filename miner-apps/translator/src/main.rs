mod args;
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
