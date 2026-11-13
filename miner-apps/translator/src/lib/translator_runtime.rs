//! ## Translator Runtime Module
//!
//! Provides [`TranslatorRuntime`], a structured state-machine orchestrating the translator proxy's
//! initialization, bootstrap stages, background service loops, and graceful teardown or fallback.

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use async_channel::{Receiver, Sender, unbounded};
use stratum_apps::{
    fallback_coordinator::FallbackCoordinator,
    payout::PayoutMode,
    stratum_core::parsers_sv2::MiningOwned,
    sync::SharedLock,
    task_manager::TaskManager,
    utils::types::{GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS, InboundFrame, OutboundFrame},
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    TranslatorSv2,
    error::{TproxyError, TproxyErrorKind},
    sv1::{GenerationEvent, Sv1Server},
    sv2::{ChannelManager, ProtocolReconnect, Upstream, UpstreamConnectOptions},
    upstream_walk::UpstreamWalk,
    utils::{ReconnectRequest, ShareOrigin, TproxyMode, UpstreamEntry},
};

/// Channels between the [`ChannelManager`] and the [`Upstream`] of one upstream generation.
struct Io {
    channel_manager_to_upstream_sender: Sender<OutboundFrame>,
    channel_manager_to_upstream_receiver: Receiver<OutboundFrame>,
    upstream_to_channel_manager_sender: Sender<InboundFrame>,
    upstream_to_channel_manager_receiver: Receiver<InboundFrame>,
}

/// The SV1 server and its channels to the channel manager, which outlive upstream generations:
/// every [`ChannelManager`] is built on the same pair of channels, and the server learns about
/// generation boundaries through `generation_events`.
struct Sv1Side {
    server: Arc<Sv1Server>,
    channel_manager_to_sv1_server_sender: Sender<MiningOwned>,
    sv1_server_to_channel_manager_receiver: Receiver<(MiningOwned, Option<ShareOrigin>)>,
    generation_events: Sender<GenerationEvent>,
    started: bool,
}

/// Connection attempts made to an upstream before moving on.
const MAX_UPSTREAM_RETRIES: usize = 3;

/// Longest pause between upstream walks while miners are held.
const MAX_WALK_PAUSE: Duration = Duration::from_secs(16);

/// The core coordinator of the Translator runtime, parameterized by its current bootstrap `State`.
///
/// It manages the lifecycle of essential sub-services and channels, ensuring resources
/// are correctly initialized, passed to background executors, and cleanly torn down.
pub(super) struct TranslatorRuntime<State> {
    tproxy_mode: TproxyMode,
    fallback_coordinator: FallbackCoordinator,
    task_manager: Arc<TaskManager>,
    translator: TranslatorSv2,
    upstreams: UpstreamWalk,
    protocol_reconnect: SharedLock<ProtocolReconnect>,
    listen_addr: SocketAddr,
    sv1: Sv1Side,
    state: State,
}

pub(super) struct Init;

struct IoReady {
    io: Io,
}

pub(super) struct ChannelManagerReady {
    io: Io,
    channel_manager: Arc<ChannelManager>,
}

pub(super) struct UpstreamReady {
    channel_manager: Arc<ChannelManager>,
}

pub(super) struct Running;

pub(super) struct Failed;

#[must_use = "bootstrap errors include a partially initialized runtime that must be shut down"]
pub(super) struct BootstrapError {
    kind: TproxyErrorKind,
    runtime: TranslatorRuntime<Failed>,
}

impl BootstrapError {
    pub(super) fn into_parts(self) -> (TproxyErrorKind, TranslatorRuntime<Failed>) {
        (self.kind, self.runtime)
    }
}

impl<State> From<(TproxyErrorKind, TranslatorRuntime<State>)> for BootstrapError {
    fn from((kind, runtime): (TproxyErrorKind, TranslatorRuntime<State>)) -> Self {
        Self {
            kind,
            runtime: runtime.into_failed(),
        }
    }
}

impl<State> TranslatorRuntime<State> {
    fn into_failed(self) -> TranslatorRuntime<Failed> {
        TranslatorRuntime {
            tproxy_mode: self.tproxy_mode,
            fallback_coordinator: self.fallback_coordinator,
            task_manager: self.task_manager,
            translator: self.translator,
            upstreams: self.upstreams,
            protocol_reconnect: self.protocol_reconnect,
            listen_addr: self.listen_addr,
            sv1: self.sv1,
            state: Failed,
        }
    }

    /// Performs a coordinated, graceful shutdown of the runtime.
    ///
    /// Signals cancellation to all active sub-services and background tasks, awaiting
    /// their clean termination up to a configured graceful timeout, and finally marks the
    /// owning [`TranslatorSv2`] as stopped — callers do not need to do so themselves.
    pub(super) async fn shutdown(self) {
        self.translator.cancellation_token.cancel();

        warn!(
            "Graceful shutdown: waiting {} seconds for tasks to finish",
            GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS
        );
        match tokio::time::timeout(
            Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS),
            self.task_manager.join_all(),
        )
        .await
        {
            Ok(_) => {
                info!("All tasks joined cleanly");
            }
            Err(_) => {
                warn!(
                    "Tasks did not finish within {} seconds, aborting",
                    GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS
                );
                self.task_manager.abort_all().await;
                info!("Joining aborted tasks...");
                self.task_manager.join_all().await;
                warn!("Forced shutdown complete");
            }
        }
        self.translator.mark_stopped();
        info!("TranslatorSv2 shutdown complete.");
    }

    /// Asks every held miner to reconnect, spread over the drain window, and waits for them to
    /// leave. Called before shutting down because no upstream connected within the hold window.
    pub(super) async fn release_held_miners(&self) {
        let was_holding = self.upstreams.holding_until().is_some();
        self.upstreams.stop_holding();
        if !was_holding || !self.sv1.started {
            return;
        }
        let _ = self
            .sv1
            .generation_events
            .try_send(GenerationEvent::Released);
        let wait = self.translator.config.drain_window() + Duration::from_secs(2);
        let server = self.sv1.server.clone();
        if tokio::time::timeout(wait, async move {
            while !server.downstreams.is_empty() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .is_err()
        {
            warn!("Held miners did not all leave within {wait:?}");
        }
    }

    fn payout_mode(&self, user_identity: &str) -> Result<Option<PayoutMode>, TproxyErrorKind> {
        let expected_payout_distribution = self
            .translator
            .config
            .expected_payout_distribution(user_identity)
            .map_err(|e| {
                TproxyErrorKind::InvalidUserIdentity(format!(
                    "invalid payout user_identity `{user_identity}`: {e}"
                ))
            })?;
        if let Some(distribution) = &expected_payout_distribution {
            info!(
                "Payout verification enabled for configured user_identity: {}",
                distribution
            );
        }
        Ok(expected_payout_distribution)
    }
}

/// `pause` scaled by a random factor in [0.8, 1.2), so translators that lost the same upstream
/// do not retry in lockstep.
fn jittered(pause: Duration) -> Duration {
    let unit = (std::hash::BuildHasher::hash_one(
        &std::collections::hash_map::RandomState::new(),
        Instant::now(),
    ) >> 11) as f64
        / (1u64 << 53) as f64;
    pause.mul_f64(0.8 + 0.4 * unit)
}

/// How long a fallback waits for the SV1 server to acknowledge [`GenerationEvent::Ended`].
const GENERATION_END_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether the SV1 server acknowledged [`GenerationEvent::Ended`] within `within`. An event
/// dropped unhandled, because the server is gone, is not an acknowledgement.
async fn generation_end_acknowledged(
    drained: tokio::sync::oneshot::Receiver<()>,
    within: Duration,
) -> bool {
    matches!(tokio::time::timeout(within, drained).await, Ok(Ok(())))
}

/// Ends the upstream generation for the SV1 server and waits until it has acknowledged that
/// nothing of the generation is left queued for the next channel manager.
///
/// Fails with [`TproxyErrorKind::GenerationEndUnacknowledged`] when the server does not answer
/// in time, since the next channel manager would otherwise act on messages meant for the old
/// one, and with [`TproxyErrorKind::CouldNotInitiateSystem`] when shutdown comes first.
async fn await_generation_end(
    sv1: &Sv1Side,
    shutdown: &CancellationToken,
) -> Result<(), TproxyErrorKind> {
    let (drained, drained_receiver) = tokio::sync::oneshot::channel();
    let _ = sv1.generation_events.try_send(GenerationEvent::Ended {
        drained: Some(drained),
    });
    if !sv1.started {
        return Ok(());
    }
    tokio::select! {
        acknowledged = generation_end_acknowledged(
            drained_receiver,
            GENERATION_END_ACK_TIMEOUT,
        ) => {
            if acknowledged {
                Ok(())
            } else {
                error!("SV1 server did not acknowledge the end of the upstream generation");
                Err(TproxyErrorKind::GenerationEndUnacknowledged)
            }
        }
        // The SV1 server stops without handling `Ended`, and no channel manager follows.
        _ = shutdown.cancelled() => Err(TproxyErrorKind::CouldNotInitiateSystem),
    }
}

impl TranslatorRuntime<Init> {
    pub(super) fn new(translator: TranslatorSv2) -> Result<Self, TproxyErrorKind> {
        let tproxy_mode = TproxyMode::from(translator.config.aggregate_channels);

        let downstream_addr_ip = translator
            .config
            .downstream_address
            .parse::<std::net::IpAddr>()
            .map_err(|e| TproxyErrorKind::General(format!("Invalid downstream address: {e}")))?;

        let listen_addr = SocketAddr::new(downstream_addr_ip, translator.config.downstream_port);

        let upstream_entries = translator
            .config
            .upstreams
            .iter()
            .map(|u| UpstreamEntry {
                host: u.address.clone(),
                port: u.port,
                authority_pubkey: u.authority_pubkey,
                user_identity: u.user_identity.clone(),
            })
            .collect();
        let upstreams = UpstreamWalk::new(
            upstream_entries,
            translator.config.max_fallbacks,
            translator.config.fallback_window(),
        );

        let (channel_manager_to_sv1_server_sender, channel_manager_to_sv1_server_receiver) =
            unbounded();
        let (sv1_server_to_channel_manager_sender, sv1_server_to_channel_manager_receiver) =
            unbounded();
        let (generation_events, generation_events_receiver) = unbounded();
        let server = Arc::new(
            Sv1Server::new(
                listen_addr,
                channel_manager_to_sv1_server_receiver,
                sv1_server_to_channel_manager_sender,
                translator.config.clone(),
                tproxy_mode,
            )
            .with_generation_control(
                generation_events_receiver,
                sv1_server_to_channel_manager_receiver.clone(),
            ),
        );
        let _ = translator.sv1_server.set(server.clone());
        #[cfg(feature = "monitoring")]
        translator.current_generation.set_sv1_server(server.clone());

        Ok(TranslatorRuntime {
            tproxy_mode,
            fallback_coordinator: FallbackCoordinator::new(),
            task_manager: Arc::new(TaskManager::new()),
            translator,
            upstreams,
            protocol_reconnect: SharedLock::new(ProtocolReconnect::default()),
            listen_addr,
            sv1: Sv1Side {
                server,
                channel_manager_to_sv1_server_sender,
                sv1_server_to_channel_manager_receiver,
                generation_events,
                started: false,
            },
            state: Init,
        })
    }

    /// Drives the linear bootstrap sequence of the translator proxy, transitioning the runtime
    /// from [`Init`] to the active [`Running`] state.
    ///
    /// If an intermediate phase fails, the caller receives the partially initialized
    /// runtime and is responsible for shutting down any resources that were already started.
    pub(super) async fn bootstrap(self) -> Result<TranslatorRuntime<Running>, BootstrapError> {
        let runtime = self.bootstrap_io();
        let runtime = runtime.bootstrap_channel_manager();
        #[cfg(feature = "monitoring")]
        if let Err(e) = runtime.start_monitoring_server() {
            error!("Failed to initialize monitoring server: {e}");
            return Err(BootstrapError::from((e, runtime)));
        }
        let runtime = runtime.try_upstream().await?;
        runtime.start_services().await
    }

    fn bootstrap_io(self) -> TranslatorRuntime<IoReady> {
        let (channel_manager_to_upstream_sender, channel_manager_to_upstream_receiver) =
            unbounded();
        let (upstream_to_channel_manager_sender, upstream_to_channel_manager_receiver) =
            unbounded();

        TranslatorRuntime {
            tproxy_mode: self.tproxy_mode,
            fallback_coordinator: self.fallback_coordinator,
            task_manager: self.task_manager,
            translator: self.translator,
            upstreams: self.upstreams,
            protocol_reconnect: self.protocol_reconnect,
            listen_addr: self.listen_addr,
            sv1: self.sv1,
            state: IoReady {
                io: Io {
                    channel_manager_to_upstream_sender,
                    channel_manager_to_upstream_receiver,
                    upstream_to_channel_manager_sender,
                    upstream_to_channel_manager_receiver,
                },
            },
        }
    }
}

impl TranslatorRuntime<IoReady> {
    fn bootstrap_channel_manager(self) -> TranslatorRuntime<ChannelManagerReady> {
        let channel_manager = Arc::new(ChannelManager::new(
            self.state.io.channel_manager_to_upstream_sender.clone(),
            self.state.io.upstream_to_channel_manager_receiver.clone(),
            self.sv1.channel_manager_to_sv1_server_sender.clone(),
            self.sv1.sv1_server_to_channel_manager_receiver.clone(),
            self.translator.config.supported_extensions.clone(),
            self.translator.config.required_extensions.clone(),
            self.tproxy_mode,
            self.translator.config.max_past_jobs,
            #[cfg(feature = "monitoring")]
            self.translator
                .config
                .downstream_difficulty_config
                .enable_vardiff,
            #[cfg(feature = "monitoring")]
            self.sv1.server.share_drops.clone(),
            #[cfg(feature = "monitoring")]
            self.sv1.server.blocks_found.clone(),
        ));

        TranslatorRuntime {
            tproxy_mode: self.tproxy_mode,
            fallback_coordinator: self.fallback_coordinator,
            task_manager: self.task_manager,
            translator: self.translator,
            upstreams: self.upstreams,
            protocol_reconnect: self.protocol_reconnect,
            listen_addr: self.listen_addr,
            sv1: self.sv1,
            state: ChannelManagerReady {
                io: self.state.io,
                channel_manager,
            },
        }
    }
}

impl TranslatorRuntime<ChannelManagerReady> {
    pub(super) async fn try_upstream(
        mut self,
    ) -> Result<TranslatorRuntime<UpstreamReady>, BootstrapError> {
        if let Err(kind) = self.initialize_upstream().await {
            return Err(BootstrapError::from((kind, self)));
        }
        Ok(self.into_upstream_ready())
    }

    /// Follows a reconnect the upstream asked for, or moves on to the configured upstreams if its
    /// endpoint cannot be used.
    ///
    /// The endpoint keeps the authority key and user identity of the upstream that requested it,
    /// so it can only be another server of the same pool. It is tried on its own and does not join
    /// the configured list.
    pub(super) async fn try_reconnect(
        mut self,
        request: ReconnectRequest,
    ) -> Result<TranslatorRuntime<UpstreamReady>, BootstrapError> {
        let upstream = request.into_upstream();
        info!("Reconnecting to {}:{}", upstream.host, upstream.port);
        match self.connect_with_retries(&upstream).await {
            Ok(true) => {
                self.upstreams.mark_reconnected();
                Ok(self.into_upstream_ready())
            }
            Ok(false) => {
                warn!(
                    "Could not reconnect to {}:{}; trying the configured upstreams",
                    upstream.host, upstream.port
                );
                self.try_upstream().await
            }
            Err(kind) => Err(BootstrapError::from((kind, self))),
        }
    }

    fn into_upstream_ready(self) -> TranslatorRuntime<UpstreamReady> {
        TranslatorRuntime {
            tproxy_mode: self.tproxy_mode,
            fallback_coordinator: self.fallback_coordinator,
            task_manager: self.task_manager,
            translator: self.translator,
            upstreams: self.upstreams,
            protocol_reconnect: self.protocol_reconnect,
            listen_addr: self.listen_addr,
            sv1: self.sv1,
            state: UpstreamReady {
                channel_manager: self.state.channel_manager,
            },
        }
    }

    /// Walks the configured upstreams until one accepts the connection.
    ///
    /// Each entry gets [`MAX_UPSTREAM_RETRIES`] attempts before the walk moves on. See
    /// [`UpstreamWalk`] for the order entries are visited in. Returns
    /// [`TproxyErrorKind::CouldNotInitiateSystem`] once every entry has been tried in this walk.
    ///
    /// While miners are held after a fallback, walks repeat with a growing pause (1 s doubling to
    /// [`MAX_WALK_PAUSE`], with jitter) until one connects or the hold window would end before
    /// the next walk.
    async fn initialize_upstream(&mut self) -> Result<(), TproxyErrorKind> {
        let mut pause = Duration::from_secs(1);
        loop {
            if self.walk_upstreams().await? {
                return Ok(());
            }
            let Some(until) = self.upstreams.holding_until() else {
                break;
            };
            let delay = jittered(pause);
            if Instant::now() + delay >= until {
                break;
            }
            warn!("No upstream accepted the connection; holding miners, next attempt in {delay:?}");
            tokio::select! {
                _ = self.translator.cancellation_token.cancelled() => {
                    return Err(TproxyErrorKind::CouldNotInitiateSystem);
                }
                _ = tokio::time::sleep(delay) => {}
            }
            pause = (pause * 2).min(MAX_WALK_PAUSE);
        }

        tracing::error!(
            "All upstreams failed after {} retries each",
            MAX_UPSTREAM_RETRIES
        );
        Err(TproxyErrorKind::CouldNotInitiateSystem)
    }

    /// One walk over the configured upstreams. Returns whether one connected.
    async fn walk_upstreams(&mut self) -> Result<bool, TproxyErrorKind> {
        let upstream_len = self.upstreams.len();

        for i in self.upstreams.order() {
            let upstream = self.upstreams.entry(i).clone();
            info!(
                "Trying upstream {} of {}: {}:{}",
                i + 1,
                upstream_len,
                upstream.host,
                upstream.port
            );

            if self.connect_with_retries(&upstream).await? {
                self.upstreams.mark_connected(i);
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Tries `upstream` up to [`MAX_UPSTREAM_RETRIES`] times and, once connected, configures the
    /// channel manager and SV1 server for it.
    ///
    /// Returns `false` if every attempt failed.
    async fn connect_with_retries(
        &self,
        upstream: &UpstreamEntry,
    ) -> Result<bool, TproxyErrorKind> {
        for attempt in 1..=MAX_UPSTREAM_RETRIES {
            info!("Connection attempt {}/{}...", attempt, MAX_UPSTREAM_RETRIES);
            tokio::time::sleep(Duration::from_secs(1)).await;

            match self.try_initialize_upstream_single(upstream).await {
                Ok(()) => {
                    let user_identity = upstream.user_identity.to_string();
                    let payout_mode = self.payout_mode(&user_identity)?;

                    self.state
                        .channel_manager
                        .set_expected_payout_distribution(payout_mode);
                    self.sv1.server.set_user_identity(user_identity);
                    return Ok(true);
                }
                Err(e) => {
                    if e.action.is_shutdown() {
                        error!("Fatal shutdown signal during upstream setup: {:?}", e.kind);
                        return Err(e.kind);
                    }

                    warn!(
                        "Attempt {}/{} failed for {}:{}: {:?}",
                        attempt, MAX_UPSTREAM_RETRIES, upstream.host, upstream.port, e
                    );
                }
            }
        }
        warn!(
            "Max retries reached for {}:{}, moving to next upstream",
            upstream.host, upstream.port
        );
        Ok(false)
    }

    // Attempts to initialize a single upstream.
    #[cfg_attr(not(test), hotpath::measure)]
    async fn try_initialize_upstream_single(
        &self,
        upstream_addr: &UpstreamEntry,
    ) -> Result<(), TproxyError<crate::error::Upstream>> {
        let upstream = Upstream::new(
            upstream_addr,
            self.state.io.upstream_to_channel_manager_sender.clone(),
            self.state.io.channel_manager_to_upstream_receiver.clone(),
            self.translator.cancellation_token.clone(),
            self.fallback_coordinator.clone(),
            self.task_manager.clone(),
            self.translator.config.required_extensions.clone(),
            self.protocol_reconnect.clone(),
            UpstreamConnectOptions::from(&self.translator.config),
        )
        .await?;

        upstream
            .start(
                self.translator.cancellation_token.clone(),
                self.fallback_coordinator.clone(),
                self.task_manager.clone(),
            )
            .await?;
        Ok(())
    }
}

impl TranslatorRuntime<UpstreamReady> {
    /// Activates the background execution loops of the SV1 server, [`ChannelManager`], and
    /// monitoring server, transitioning the runtime to [`Running`].
    pub(super) async fn start_services(
        mut self,
    ) -> Result<TranslatorRuntime<Running>, BootstrapError> {
        if !self.sv1.started {
            info!("Launching SV1 server...");
            if let Err(e) = self
                .sv1
                .server
                .clone()
                .start_with_drain(
                    self.translator.cancellation_token.clone(),
                    self.translator.drain_token.clone(),
                    self.task_manager.clone(),
                )
                .await
            {
                error!("SV1 server startup failed: {e:?}");
                return Err(BootstrapError::from((e.kind, self)));
            }
            self.sv1.started = true;
        }

        // Queued ahead of anything the new channel manager sends the SV1 server.
        let _ = self
            .sv1
            .generation_events
            .try_send(GenerationEvent::Connected {
                fallback_token: self.fallback_coordinator.token(),
            });

        info!("Launching ChannelManager tasks...");
        ChannelManager::run_channel_manager_tasks(
            self.state.channel_manager.clone(),
            self.translator.cancellation_token.clone(),
            self.fallback_coordinator.clone(),
            self.task_manager.clone(),
        )
        .await;

        #[cfg(feature = "monitoring")]
        {
            self.translator
                .current_generation
                .set_channel_manager(self.state.channel_manager.clone());
            if self.translator.config.monitoring_address().is_some() {
                self.start_miner_telemetry();
            }
        }

        Ok(TranslatorRuntime {
            tproxy_mode: self.tproxy_mode,
            fallback_coordinator: self.fallback_coordinator,
            task_manager: self.task_manager,
            translator: self.translator,
            upstreams: self.upstreams,
            protocol_reconnect: self.protocol_reconnect,
            listen_addr: self.listen_addr,
            sv1: self.sv1,
            state: Running,
        })
    }

    /// Runs the SV1 miner telemetry loop for this upstream generation.
    #[cfg(feature = "monitoring")]
    fn start_miner_telemetry(&self) {
        let refresh_interval = self.monitoring_refresh_interval();
        self.task_manager.spawn(self.fallback_coordinator.track({
            let cancellation_token = self.translator.cancellation_token.clone();
            let fallback_token = self.fallback_coordinator.token();
            let sv1_server = self.sv1.server.clone();
            async move {
                sv1_server
                    .run_miner_telemetry_loop(refresh_interval, cancellation_token, fallback_token)
                    .await;

                info!("SV1 miner telemetry task exited");
            }
        }));
    }
}

impl<State> TranslatorRuntime<State> {
    #[cfg(feature = "monitoring")]
    fn monitoring_refresh_interval(&self) -> Duration {
        Duration::from_secs(
            self.translator
                .config
                .monitoring_cache_refresh_secs()
                .unwrap_or(15),
        )
    }
}

impl TranslatorRuntime<ChannelManagerReady> {
    /// Starts the monitoring HTTP server for the lifetime of the process.
    ///
    /// It runs before the first upstream connection and across fallbacks, reporting the upstream
    /// generation through [`crate::monitoring::CurrentGeneration`]. `/api/v1/health` returns 503
    /// whenever no upstream is connected, so a load balancer stops routing miners to a translator
    /// that cannot serve them.
    ///
    /// "Connected" means the upstream accepted `SetupConnection`. Health does not prove that a
    /// mining channel is open (an upstream that never answers `OpenExtendedMiningChannel` keeps
    /// it at 200), nor that the SV1 listener accepts miners: it stays 200 while draining.
    #[cfg(feature = "monitoring")]
    fn start_monitoring_server(&self) -> Result<(), TproxyErrorKind> {
        let Some(monitoring_addr) = self.translator.config.monitoring_address() else {
            return Ok(());
        };
        info!("Initializing monitoring server on http://{monitoring_addr}");
        let refresh_interval = self.monitoring_refresh_interval();
        let current_generation = self.translator.current_generation.clone();

        let monitoring_server = stratum_apps::monitoring::MonitoringServer::new(
            monitoring_addr,
            Some(current_generation.clone()),
            None,
            refresh_interval,
        )
        .map_err(|e| {
            TproxyErrorKind::General(format!("failed to initialize monitoring server: {e}"))
        })?
        .with_sv1_monitoring(current_generation)
        .map_err(|e| TproxyErrorKind::General(format!("failed to add SV1 monitoring: {e}")))?
        .with_upstream_monitoring(self.upstreams.status())
        .map_err(|e| TproxyErrorKind::General(format!("failed to add upstream monitoring: {e}")))?;

        let cancellation_token = self.translator.cancellation_token.clone();
        self.task_manager.spawn(async move {
            let shutdown_signal = {
                let cancellation_token = cancellation_token.clone();
                async move {
                    cancellation_token.cancelled().await;
                    info!("Monitoring server: received shutdown signal.");
                }
            };
            if let Err(e) = monitoring_server.run(shutdown_signal).await {
                error!("Monitoring server error: {:?}", e);
                cancellation_token.cancel();
            }
            info!("Monitoring server task exited");
        });

        Ok(())
    }
}

pub(super) enum RuntimeEvent {
    Shutdown,
    Fallback,
    /// The upstream asked tProxy to reconnect.
    Reconnect(ReconnectRequest),
}

impl TranslatorRuntime<Running> {
    pub(super) async fn wait(&self) -> RuntimeEvent {
        let fallback_token = self.fallback_coordinator.token();
        let cancellation_token = self.translator.cancellation_token.clone();

        tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => RuntimeEvent::Shutdown,
            _ = fallback_token.cancelled() => {
                // Taken as soon as the teardown starts. A request the upstream recorded after
                // another component started it, before this point, is followed too: it can only
                // point to another server of the same pool. One recorded later is dropped by
                // `cleanup_for_fallback`.
                match self
                    .protocol_reconnect
                    .with(|protocol_reconnect| protocol_reconnect.take(Instant::now()))
                {
                    Ok(Some(request)) => RuntimeEvent::Reconnect(request),
                    _ => RuntimeEvent::Fallback,
                }
            }
        }
    }

    /// Tears down the current upstream generation and prepares a fresh one.
    ///
    /// Fails with [`TproxyErrorKind::FallbackLoop`] when this fallback exceeds the configured
    /// budget, or with [`TproxyErrorKind::GenerationEndUnacknowledged`] when the SV1 server does
    /// not confirm it discarded the ended generation's messages; the caller then shuts the
    /// translator down. When the translator is already shutting down it fails with
    /// [`TproxyErrorKind::CouldNotInitiateSystem`] before recording the fallback.
    pub(super) async fn cleanup_for_fallback(
        mut self,
    ) -> Result<TranslatorRuntime<ChannelManagerReady>, BootstrapError> {
        // A shutdown or drain that has already cancelled the translator is not an upstream
        // failure: it must not hold miners or count toward the fallback metric and budget.
        if self.translator.cancellation_token.is_cancelled() {
            return Err(BootstrapError::from((
                TproxyErrorKind::CouldNotInitiateSystem,
                self,
            )));
        }
        info!("Preparing fallback");
        // Set before the teardown so health does not report the held miners' pod as down.
        if let Some(hold) = self.translator.config.upstream_loss_hold() {
            info!("Holding SV1 miners for up to {}s", hold.as_secs());
            self.upstreams.hold_until(Instant::now() + hold);
        }
        let fallback_budget = self.upstreams.record_fallback(Instant::now());
        #[cfg(feature = "monitoring")]
        self.translator.current_generation.clear_channel_manager();
        self.fallback_coordinator.trigger_fallback_and_wait().await;
        info!("All components finished fallback cleanup");
        // `wait` already took the request that caused this teardown. One recorded while it was
        // under way, after another component triggered it, is dropped rather than followed later.
        let _ = self
            .protocol_reconnect
            .with(|protocol_reconnect| protocol_reconnect.discard());

        // The old channel manager has exited, so the `Ended` event lands behind everything it
        // sent. The next one must not start before the SV1 server has discarded what is left of
        // this generation in the queue they share.
        if let Err(kind) =
            await_generation_end(&self.sv1, &self.translator.cancellation_token).await
        {
            // The SV1 server would not act on a release either; shutting down disconnects the
            // miners.
            self.upstreams.stop_holding();
            return Err(BootstrapError::from((kind, self)));
        }

        if let Err(fallbacks) = fallback_budget {
            let window_secs = self.translator.config.fallback_window().as_secs();
            error!(
                "{fallbacks} fallbacks within {window_secs}s exceed max_fallbacks = {}",
                self.translator.config.max_fallbacks
            );
            return Err(BootstrapError::from((
                TproxyErrorKind::FallbackLoop {
                    fallbacks,
                    window_secs,
                },
                self,
            )));
        }

        let fresh_fallback_coordinator = FallbackCoordinator::new();

        Ok(TranslatorRuntime {
            tproxy_mode: self.tproxy_mode,
            fallback_coordinator: fresh_fallback_coordinator,
            task_manager: self.task_manager,
            translator: self.translator,
            upstreams: self.upstreams,
            protocol_reconnect: self.protocol_reconnect,
            listen_addr: self.listen_addr,
            sv1: self.sv1,
            state: Init,
        }
        .bootstrap_io()
        .bootstrap_channel_manager())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DownstreamDifficultyConfig, TranslatorConfig};
    use tokio::sync::oneshot;

    #[test]
    fn walk_pauses_are_jittered_within_twenty_percent() {
        let pause = Duration::from_secs(10);
        for _ in 0..100 {
            let jittered = jittered(pause);
            assert!(jittered >= Duration::from_secs(8) && jittered < Duration::from_secs(12));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_unanswered_generation_end_is_not_acknowledged() {
        let (_drained, receiver) = oneshot::channel();
        assert!(!generation_end_acknowledged(receiver, GENERATION_END_ACK_TIMEOUT).await);
    }

    #[tokio::test]
    async fn a_generation_end_dropped_unhandled_is_not_acknowledged() {
        let (drained, receiver) = oneshot::channel();
        drop(drained);
        assert!(!generation_end_acknowledged(receiver, GENERATION_END_ACK_TIMEOUT).await);
    }

    #[tokio::test]
    async fn an_answered_generation_end_is_acknowledged() {
        let (drained, receiver) = oneshot::channel();
        drained.send(()).unwrap();
        assert!(generation_end_acknowledged(receiver, GENERATION_END_ACK_TIMEOUT).await);
    }

    /// An SV1 side whose server never handles the events sent to it, unless `answer` is set.
    fn sv1_side(started: bool, answer: bool) -> Sv1Side {
        let (channel_manager_to_sv1_server_sender, channel_manager_to_sv1_server_receiver) =
            unbounded();
        let (sv1_server_to_channel_manager_sender, sv1_server_to_channel_manager_receiver) =
            unbounded();
        let (generation_events, generation_events_receiver) = unbounded();
        if answer {
            tokio::spawn({
                let generation_events_receiver = generation_events_receiver.clone();
                async move {
                    while let Ok(event) = generation_events_receiver.recv().await {
                        if let GenerationEvent::Ended {
                            drained: Some(drained),
                        } = event
                        {
                            let _ = drained.send(());
                        }
                    }
                }
            });
        }
        let config = TranslatorConfig::new(
            vec![],
            "127.0.0.1".into(),
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
        let server = Arc::new(
            Sv1Server::new(
                "127.0.0.1:3333".parse().unwrap(),
                channel_manager_to_sv1_server_receiver,
                sv1_server_to_channel_manager_sender,
                config,
                TproxyMode::Aggregated,
            )
            .with_generation_control(
                generation_events_receiver,
                sv1_server_to_channel_manager_receiver.clone(),
            ),
        );
        Sv1Side {
            server,
            channel_manager_to_sv1_server_sender,
            sv1_server_to_channel_manager_receiver,
            generation_events,
            started,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_unacknowledged_generation_end_fails_the_fallback() {
        let sv1 = sv1_side(true, false);
        let started = tokio::time::Instant::now();
        let result = await_generation_end(&sv1, &CancellationToken::new()).await;
        assert!(matches!(
            result,
            Err(TproxyErrorKind::GenerationEndUnacknowledged)
        ));
        assert!(started.elapsed() >= GENERATION_END_ACK_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_the_generation_end_wait_stops_the_translator() {
        let sv1 = sv1_side(true, false);
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let result = await_generation_end(&sv1, &shutdown).await;
        assert!(matches!(
            result,
            Err(TproxyErrorKind::CouldNotInitiateSystem)
        ));
    }

    #[tokio::test]
    async fn an_acknowledged_generation_end_lets_the_next_channel_manager_start() {
        let sv1 = sv1_side(true, true);
        assert!(
            await_generation_end(&sv1, &CancellationToken::new())
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_server_that_never_started_has_nothing_to_acknowledge() {
        let sv1 = sv1_side(false, false);
        assert!(
            await_generation_end(&sv1, &CancellationToken::new())
                .await
                .is_ok()
        );
    }

    fn running_runtime() -> TranslatorRuntime<Running> {
        let config = TranslatorConfig::new(
            vec![],
            "127.0.0.1".into(),
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
        let runtime = TranslatorRuntime::<Init>::new(TranslatorSv2::new(config)).unwrap();
        TranslatorRuntime {
            tproxy_mode: runtime.tproxy_mode,
            fallback_coordinator: runtime.fallback_coordinator,
            task_manager: runtime.task_manager,
            translator: runtime.translator,
            upstreams: runtime.upstreams,
            protocol_reconnect: runtime.protocol_reconnect,
            listen_addr: runtime.listen_addr,
            sv1: runtime.sv1,
            state: Running,
        }
    }

    #[tokio::test]
    async fn a_fallback_during_shutdown_is_not_counted() {
        let runtime = running_runtime();
        let status = runtime.upstreams.status();
        runtime.translator.cancellation_token.cancel();

        let Err(bootstrap_err) = runtime.cleanup_for_fallback().await else {
            panic!("a translator that is shutting down does not fall back");
        };

        let (kind, runtime) = bootstrap_err.into_parts();
        assert!(matches!(kind, TproxyErrorKind::CouldNotInitiateSystem));
        assert_eq!(status.fallbacks(), 0);
        assert!(runtime.upstreams.holding_until().is_none());
    }

    #[tokio::test]
    async fn a_fallback_is_counted_while_the_translator_runs() {
        let runtime = running_runtime();
        let status = runtime.upstreams.status();

        assert!(runtime.cleanup_for_fallback().await.is_ok());

        assert_eq!(status.fallbacks(), 1);
    }
}
