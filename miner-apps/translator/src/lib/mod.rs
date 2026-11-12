//! ## Translator Sv2
//!
//! Provides the core logic and main struct (`TranslatorSv2`) for running a
//! Stratum V1 to Stratum V2 translation proxy.
//!
//! This module orchestrates the interaction between downstream SV1 miners and upstream SV2
//! applications (proxies or pool servers).
//!
//! The central component is the `TranslatorSv2` struct, which encapsulates the state and
//! provides the `start` method as the main entry point for running the translator service.
//! It relies on several sub-modules (`config`, `downstream_sv1`, `upstream_sv2`, `proxy`, `status`,
//! etc.) for specialized functionalities.
use error::TproxyErrorKind;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

pub use stratum_apps::stratum_core::sv1_api::server_to_client;

use config::TranslatorConfig;

use crate::utils::TproxyMode;

use sv1::Sv1Server;
use translator_runtime::{Init, RuntimeEvent, TranslatorRuntime};

pub mod config;
pub mod error;
mod io_task;
#[cfg(feature = "monitoring")]
mod monitoring;
pub mod sv1;
pub mod sv2;
mod translator_runtime;
mod upstream_walk;
pub mod utils;

/// The main struct that manages the SV1/SV2 translator.
#[derive(Clone)]
pub struct TranslatorSv2 {
    config: TranslatorConfig,
    cancellation_token: CancellationToken,
    /// Cancelled to start draining SV1 downstreams ahead of shutdown.
    drain_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
    /// The SV1 server, once [`TranslatorSv2::start`] has created it. It outlives upstream
    /// generations, so its downstreams are the miners a drain has to wait for.
    sv1_server: Arc<OnceLock<Arc<Sv1Server>>>,
    /// The upstream generation monitoring reports on; outlives fallbacks like the monitoring
    /// server itself.
    #[cfg(feature = "monitoring")]
    current_generation: Arc<monitoring::CurrentGeneration>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl TranslatorSv2 {
    /// Creates a new `TranslatorSv2`.
    ///
    /// Initializes the translator with the given configuration and sets up
    /// the reconnect wait time.
    pub fn new(config: TranslatorConfig) -> Self {
        Self {
            config,
            cancellation_token: CancellationToken::new(),
            drain_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
            sv1_server: Arc::new(OnceLock::new()),
            #[cfg(feature = "monitoring")]
            current_generation: Arc::default(),
        }
    }

    /// Marks the Translator Proxy as stopped and releases anyone blocked on
    /// [`TranslatorSv2::shutdown`].
    ///
    /// Called by [`TranslatorRuntime::shutdown`] once teardown completes, and directly by
    /// [`TranslatorSv2::start`] on the failure path where no runtime exists to tear down.
    fn mark_stopped(&self) {
        self.is_alive.store(false, Ordering::Release);
        self.cancellation_token.cancel();
        self.shutdown_notify.notify_waiters();
    }

    /// Starts the main event loop for the Translator Proxy.
    ///
    /// The startup and execution sequence follows:
    /// 1. **Initialize:** Sets up the translator runtime state machine ([`TranslatorRuntime`]).
    /// 2. **Bootstrap:** Configures internal channels, initializes the Channel Manager,
    ///    instantiates the SV1 server, and establishes upstream SV2 connections.
    /// 3. **Run & Loop:** Activates all background loops/servers (SV1, Channel Manager, Monitoring)
    ///    and handles fallbacks or graceful shutdown.
    /// 4. **Teardown:** Performs a coordinated graceful cleanup of all services and tasks upon
    ///    termination.
    pub async fn start(&self) -> Result<(), TproxyErrorKind> {
        info!("Starting Translator Proxy...");
        self.config.warn_about_ignored_settings();

        let runtime = match TranslatorRuntime::<Init>::new(self.clone()) {
            Ok(runtime) => runtime,
            Err(e) => {
                self.mark_stopped();
                return Err(e);
            }
        };

        let mut running = match runtime.bootstrap().await {
            Ok(running) => running,
            Err(bootstrap_err) => {
                let (kind, runtime) = bootstrap_err.into_parts();
                error!(?kind, "Failed to bootstrap Translator Proxy");
                runtime.shutdown().await;
                return Err(kind);
            }
        };

        loop {
            let requested_reconnect = match running.wait().await {
                RuntimeEvent::Shutdown => {
                    running.shutdown().await;
                    return Ok(());
                }
                RuntimeEvent::Fallback => None,
                RuntimeEvent::Reconnect(request) => Some(request),
            };

            // A requested reconnect shuts the components down like a fallback, then connects to
            // the requested endpoint instead of the next configured upstream.
            let ready_runtime = match running.cleanup_for_fallback().await {
                Ok(runtime) => runtime,
                Err(bootstrap_err) => {
                    let (kind, runtime) = bootstrap_err.into_parts();
                    error!(?kind, "Giving up on upstream fallbacks");
                    runtime.shutdown().await;
                    return Err(kind);
                }
            };
            let upstream_ready = match requested_reconnect {
                Some(request) => ready_runtime.try_reconnect(request).await,
                None => ready_runtime.try_upstream().await,
            };

            running = match upstream_ready {
                Ok(new_running) => match new_running.start_services().await {
                    Ok(running) => running,
                    Err(bootstrap_err) => {
                        let (kind, runtime) = bootstrap_err.into_parts();
                        error!(?kind, "Failed to start services for Translator Proxy");
                        runtime.shutdown().await;
                        return Err(kind);
                    }
                },
                Err(bootstrap_err) => {
                    let (kind, runtime) = bootstrap_err.into_parts();
                    error!(?kind, "Failed to reconnect Translator Proxy");
                    runtime.shutdown().await;
                    return Err(kind);
                }
            };
        }
    }

    pub async fn shutdown(&self) {
        // The Notified future is guaranteed to receive wakeups from notify_waiters()
        // as soon as it has been created, even if it has not yet been polled. Creating it
        // before checking `is_alive` means a teardown that completes concurrently is either
        // seen here or wakes us.
        let notified = self.shutdown_notify.notified();
        if !self.is_alive.load(Ordering::Acquire) {
            return;
        }
        self.cancellation_token.cancel();
        notified.await;
    }

    /// Drains SV1 downstreams for up to `drain_seconds`, then shuts down.
    ///
    /// The SV1 listener is closed and connected miners are disconnected gradually across the
    /// window while the upstream connection stays up. Shutdown starts as soon as the last miner
    /// is gone, or when the window elapses, at which point any remaining miners are dropped by
    /// [`TranslatorSv2::shutdown`]. While no SV1 downstream is connected or held there is
    /// nothing to wait for, so shutdown starts at once. With `drain_seconds = 0` this is exactly
    /// [`TranslatorSv2::shutdown`].
    pub async fn drain_and_shutdown(&self) {
        let window = self.config.drain_window();
        if !window.is_zero() && self.is_alive.load(Ordering::Acquire) {
            self.drain_token.cancel();
            if self.has_sv1_downstreams() {
                info!("Draining SV1 downstreams for up to {}s", window.as_secs());
                if tokio::time::timeout(window, self.cancellation_token.cancelled())
                    .await
                    .is_err()
                {
                    warn!("Drain window elapsed; dropping remaining SV1 downstreams");
                }
            }
        }
        self.shutdown().await;
    }

    /// Whether any SV1 miner is connected, including miners held across an upstream fallback.
    fn has_sv1_downstreams(&self) -> bool {
        self.sv1_server
            .get()
            .is_some_and(|server| !server.downstreams.is_empty())
    }

    /// Whether a drain has been requested.
    pub fn is_draining(&self) -> bool {
        self.drain_token.is_cancelled()
    }
}

impl Drop for TranslatorSv2 {
    fn drop(&mut self) {
        info!("TranslatorSv2 dropped");
        self.cancellation_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::{DownstreamDifficultyConfig, Upstream};
    use std::{str::FromStr, time::Duration};
    use stratum_apps::key_utils::Secp256k1PublicKey;

    fn translator(drain_seconds: u64) -> TranslatorSv2 {
        let pubkey =
            Secp256k1PublicKey::from_str("9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan")
                .unwrap();
        let upstream = Upstream::new("127.0.0.1".into(), 4444, pubkey, "test".into());
        let mut config = TranslatorConfig::new(
            vec![upstream],
            "0.0.0.0".into(),
            3333,
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
        config.drain_seconds = drain_seconds;
        TranslatorSv2::new(config)
    }

    /// Registers a connected SV1 miner on `translator`, as `start` would have done.
    fn connect_a_miner(translator: &TranslatorSv2) {
        let (cm_sender, _cm_receiver) = async_channel::unbounded();
        let (_sv1_sender, cm_receiver) = async_channel::unbounded();
        let server = Sv1Server::new(
            "127.0.0.1:3333".parse().unwrap(),
            cm_receiver,
            cm_sender,
            translator.config.clone(),
            TproxyMode::Aggregated,
        );
        server.insert_idle_downstream(1);
        assert!(translator.sv1_server.set(Arc::new(server)).is_ok());
    }

    /// `drain_seconds = 0` shuts down at once without starting a drain.
    #[tokio::test(start_paused = true)]
    async fn zero_drain_window_shuts_down_immediately() {
        let translator = translator(0);
        // No runtime is running to acknowledge shutdown, so this never completes on its own.
        let shutdown = tokio::spawn({
            let translator = translator.clone();
            async move { translator.drain_and_shutdown().await }
        });
        tokio::task::yield_now().await;
        assert!(translator.cancellation_token.is_cancelled());
        assert!(!translator.drain_token.is_cancelled());
        shutdown.abort();
    }

    /// A drain that does not finish is cut at the window and turns into a normal shutdown.
    #[tokio::test(start_paused = true)]
    async fn drain_is_cut_at_the_window() {
        let translator = translator(30);
        connect_a_miner(&translator);
        let shutdown = tokio::spawn({
            let translator = translator.clone();
            async move { translator.drain_and_shutdown().await }
        });
        tokio::time::sleep(Duration::from_secs(29)).await;
        assert!(translator.drain_token.is_cancelled());
        assert!(!translator.cancellation_token.is_cancelled());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert!(translator.cancellation_token.is_cancelled());
        shutdown.abort();
    }

    /// With no SV1 miner connected or held there is nothing to drain, whether the server has not
    /// been created yet or has no downstreams.
    #[tokio::test(start_paused = true)]
    async fn drain_skips_the_wait_without_sv1_downstreams() {
        let translator = translator(30);
        let started = tokio::time::Instant::now();
        let shutdown = tokio::spawn({
            let translator = translator.clone();
            async move { translator.drain_and_shutdown().await }
        });
        tokio::task::yield_now().await;
        assert!(translator.cancellation_token.is_cancelled());
        assert_eq!(started.elapsed(), Duration::ZERO);
        shutdown.abort();

        let (cm_sender, _cm_receiver) = async_channel::unbounded();
        let (_sv1_sender, cm_receiver) = async_channel::unbounded();
        let translator = self::translator(30);
        translator
            .sv1_server
            .set(Arc::new(Sv1Server::new(
                "127.0.0.1:3333".parse().unwrap(),
                cm_receiver,
                cm_sender,
                translator.config.clone(),
                TproxyMode::Aggregated,
            )))
            .ok()
            .unwrap();
        let shutdown = tokio::spawn({
            let translator = translator.clone();
            async move { translator.drain_and_shutdown().await }
        });
        tokio::task::yield_now().await;
        assert!(translator.cancellation_token.is_cancelled());
        shutdown.abort();
    }
}
