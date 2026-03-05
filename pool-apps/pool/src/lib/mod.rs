use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use async_channel::{unbounded, Receiver, Sender};

use bitcoin_core_sv2::CancellationToken;
use stratum_apps::{
    fallback_coordinator::FallbackCoordinator,
    stratum_core::{bitcoin::consensus::Encodable, parsers_sv2::TemplateDistribution},
    task_manager::TaskManager,
    tp_type::{TemplateProviderEntry, TemplateProviderType},
    utils::types::GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
};
use tokio::sync::{broadcast, Notify};
use tracing::{debug, error, info, warn};

use crate::{
    channel_manager::ChannelManager,
    config::PoolConfig,
    error::PoolErrorKind,
    status::{State, Status},
    template_receiver::{
        bitcoin_core::{connect_to_bitcoin_core, BitcoinCoreSv2Config},
        sv2_tp::Sv2Tp,
    },
};

pub mod channel_manager;
pub mod config;
pub mod downstream;
pub mod error;
mod io_task;
#[cfg(feature = "monitoring")]
mod monitoring;
pub mod status;
pub mod template_receiver;
pub mod utils;

#[derive(Debug, Clone)]
pub struct PoolSv2 {
    config: PoolConfig,
    cancellation_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl PoolSv2 {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            cancellation_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Starts the Pool main loop.
    pub async fn start(&self) -> Result<(), PoolErrorKind> {
        let coinbase_outputs = vec![self.config.get_txout()];
        let mut encoded_outputs = vec![];

        coinbase_outputs
            .consensus_encode(&mut encoded_outputs)
            .expect("Invalid coinbase output in config");

        let cancellation_token = self.cancellation_token.clone();

        let task_manager = Arc::new(TaskManager::new());
        let mut fallback_coordinator = FallbackCoordinator::new();

        let (status_sender, status_receiver) = unbounded();

        let (channel_manager_to_downstream_sender, _channel_manager_to_downstream_receiver) =
            broadcast::channel(10);
        let (downstream_to_channel_manager_sender, downstream_to_channel_manager_receiver) =
            unbounded();

        let (channel_manager_to_tp_sender, channel_manager_to_tp_receiver) = unbounded();
        let (tp_to_channel_manager_sender, tp_to_channel_manager_receiver) = unbounded();

        debug!("Channels initialized.");

        let mut channel_manager = ChannelManager::new(
            self.config.clone(),
            channel_manager_to_tp_sender.clone(),
            tp_to_channel_manager_receiver.clone(),
            channel_manager_to_downstream_sender.clone(),
            downstream_to_channel_manager_receiver,
            encoded_outputs.clone(),
        )
        .await?;

        // Start monitoring server if configured
        #[cfg(feature = "monitoring")]
        if let Some(monitoring_addr) = self.config.monitoring_address() {
            info!(
                "Initializing monitoring server on http://{}",
                monitoring_addr
            );

            let monitoring_server = stratum_apps::monitoring::MonitoringServer::new(
                monitoring_addr,
                None, // Pool doesn't have channels opened with servers
                Some(Arc::new(channel_manager.clone())), // channels opened with clients
                std::time::Duration::from_secs(self.config.monitoring_cache_refresh_secs()),
            )
            .expect("Failed to initialize monitoring server");

            let cancellation_token_clone = cancellation_token.clone();
            let fallback_coordinator_token = fallback_coordinator.token();
            let shutdown_signal = async move {
                tokio::select! {
                    _ = cancellation_token_clone.cancelled() => {}
                    _ = fallback_coordinator_token.cancelled() => {}
                }
            };

            let fallback_coordinator_clone = fallback_coordinator.clone();
            task_manager.spawn(async move {
                let fallback_handler = fallback_coordinator_clone.register();
                if let Err(e) = monitoring_server.run(shutdown_signal).await {
                    error!("Monitoring server error: {}", e);
                }
                fallback_handler.done();
            });
        }

        let channel_manager_clone = channel_manager.clone();
        let channel_manager_for_cleanup = channel_manager.clone();

        let mut tp_entries =
            TemplateProviderEntry::from_config(self.config.template_provider_types());

        let mut bitcoin_core_sv2_join_handle: Option<JoinHandle<()>> = self
            .initialize_tp(
                &mut tp_entries,
                channel_manager_to_tp_receiver.clone(),
                tp_to_channel_manager_sender.clone(),
                cancellation_token.clone(),
                fallback_coordinator.clone(),
                task_manager.clone(),
                status_sender.clone(),
            )
            .await?;

        channel_manager
            .start(
                cancellation_token.clone(),
                status_sender.clone(),
                task_manager.clone(),
                coinbase_outputs.clone(),
            )
            .await?;

        channel_manager_clone
            .start_downstream_server(
                *self.config.authority_public_key(),
                *self.config.authority_secret_key(),
                self.config.cert_validity_sec(),
                *self.config.listen_address(),
                task_manager.clone(),
                cancellation_token.clone(),
                status_sender.clone(),
                downstream_to_channel_manager_sender.clone(),
                channel_manager_to_downstream_sender.clone(),
            )
            .await?;

        info!("Spawning status listener task...");
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    info!("Ctrl+C received — initiating graceful shutdown...");
                    cancellation_token.cancel();
                    break;
                }
                _ = cancellation_token.cancelled() => {
                    break;
                }
                message = status_receiver.recv() => {
                    if let Ok(status) = message {
                        match status.state {
                            State::DownstreamShutdown{downstream_id,..} => {
                                warn!("Downstream {downstream_id:?} disconnected — cleaning up channel manager.");
                                if let Err(e) = channel_manager_for_cleanup.remove_downstream(downstream_id) {
                                    error!("Failed to remove downstream {downstream_id:?}: {e:?}");
                                    cancellation_token.cancel();
                                    break;
                                }
                            }
                            State::TemplateReceiverShutdown(_) => {
                                warn!("Template Receiver shutdown requested — initiating full shutdown.");
                                cancellation_token.cancel();
                                break;
                            }
                            State::TemplateReceiverShutdownFallback(_) => {
                                warn!("Template Provider connection dropped — attempting fallback...");

                                fallback_coordinator.trigger_fallback_and_wait().await;
                                info!("All components finished fallback cleanup");

                                // Drain buffered status messages from old components
                                while let Ok(old_status) = status_receiver.try_recv() {
                                    debug!("Draining buffered status message: {:?}", old_status.state);
                                }

                                // Create fresh FallbackCoordinator for the reconnection attempt
                                fallback_coordinator = FallbackCoordinator::new();

                                // Recreate TP channels (old ones closed during fallback)
                                let (channel_manager_to_tp_sender_new, channel_manager_to_tp_receiver_new) = unbounded();
                                let (tp_to_channel_manager_sender_new, tp_to_channel_manager_receiver_new) = unbounded();

                                let (channel_manager_to_downstream_sender_new, _) = broadcast::channel(10);
                                let (downstream_to_channel_manager_sender_new, downstream_to_channel_manager_receiver_new) = unbounded();

                                // Recreate ChannelManager with new TP channels
                                channel_manager = ChannelManager::new(
                                    self.config.clone(),
                                    channel_manager_to_tp_sender_new.clone(),
                                    tp_to_channel_manager_receiver_new.clone(),
                                    channel_manager_to_downstream_sender_new.clone(),
                                    downstream_to_channel_manager_receiver_new.clone(),
                                    encoded_outputs.clone(),
                                )
                                .await?;

                                // Try connecting to the next template provider
                                match self.initialize_tp(
                                    &mut tp_entries,
                                    channel_manager_to_tp_receiver_new,
                                    tp_to_channel_manager_sender_new,
                                    cancellation_token.clone(),
                                    fallback_coordinator.clone(),
                                    task_manager.clone(),
                                    status_sender.clone(),
                                ).await {
                                    Ok(join_handle) => {
                                        bitcoin_core_sv2_join_handle = join_handle;

                                        let channel_manager_for_downstream = channel_manager.clone();
                                        channel_manager
                                            .start(
                                                cancellation_token.clone(),
                                                status_sender.clone(),
                                                task_manager.clone(),
                                                coinbase_outputs.clone(),
                                            )
                                            .await?;

                                        channel_manager_for_downstream
                                            .start_downstream_server(
                                                *self.config.authority_public_key(),
                                                *self.config.authority_secret_key(),
                                                self.config.cert_validity_sec(),
                                                *self.config.listen_address(),
                                                task_manager.clone(),
                                                cancellation_token.clone(),
                                                status_sender.clone(),
                                                downstream_to_channel_manager_sender_new,
                                                channel_manager_to_downstream_sender_new,
                                            )
                                            .await?;

                                        info!("Successfully reconnected to backup template provider");
                                    }
                                    Err(e) => {
                                        error!("All template providers exhausted: {e:?}");
                                        cancellation_token.cancel();
                                        break;
                                    }
                                }
                            }
                            State::ChannelManagerShutdown(_) => {
                                warn!("Channel Manager shutdown requested — initiating full shutdown.");
                                cancellation_token.cancel();
                                break;
                            }
                        }
                    }
                }
            }
        }

        if let Some(bitcoin_core_sv2_join_handle) = bitcoin_core_sv2_join_handle {
            info!("Waiting for BitcoinCoreSv2 dedicated thread to shutdown...");
            match bitcoin_core_sv2_join_handle.join() {
                Ok(_) => info!("BitcoinCoreSv2 dedicated thread shutdown complete."),
                Err(e) => error!("BitcoinCoreSv2 dedicated thread error: {e:?}"),
            }
        }

        warn!(
            "Graceful shutdown: waiting {} seconds for tasks to finish",
            GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS
        );

        match tokio::time::timeout(
            std::time::Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS),
            task_manager.join_all(),
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
                task_manager.abort_all().await;
                info!("Joining aborted tasks...");
                task_manager.join_all().await;
                warn!("Forced shutdown complete");
            }
        }
        self.shutdown_notify.notify_waiters();
        self.is_alive.store(false, Ordering::Relaxed);
        info!("Pool shutdown complete.");
        Ok(())
    }

    pub async fn shutdown(&self) {
        if !self.is_alive.load(Ordering::Relaxed) {
            return;
        }
        // The Notified future is guaranteed to receive wakeups from notify_waiters()
        // as soon as it has been created, even if it has not yet been polled.
        let notified = self.shutdown_notify.notified();
        self.cancellation_token.cancel();
        notified.await;
    }

    /// Iterates through template providers in priority order, trying each with retries.
    ///
    /// Returns `Ok(Some(JoinHandle))` for BitcoinCoreIpc connections (dedicated thread),
    /// or `Ok(None)` for Sv2Tp connections (async task).
    #[allow(clippy::too_many_arguments)]
    async fn initialize_tp(
        &self,
        tp_entries: &mut [TemplateProviderEntry],
        channel_manager_to_tp_receiver: Receiver<TemplateDistribution<'static>>,
        tp_to_channel_manager_sender: Sender<TemplateDistribution<'static>>,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        task_manager: Arc<TaskManager>,
        status_sender: Sender<Status>,
    ) -> Result<Option<JoinHandle<()>>, PoolErrorKind> {
        const MAX_RETRIES: usize = 3;
        let tp_count = tp_entries.len();

        for (i, entry) in tp_entries.iter_mut().enumerate() {
            if entry.tried_or_flagged {
                info!(
                    "Template provider {} of {} previously tried, skipping",
                    i + 1,
                    tp_count
                );
                continue;
            }

            info!(
                "Trying template provider {} of {}: {:?}",
                i + 1,
                tp_count,
                entry.tp_type
            );

            for attempt in 1..=MAX_RETRIES {
                info!("Connection attempt {}/{}...", attempt, MAX_RETRIES);

                match self
                    .try_connect_tp(
                        &entry.tp_type,
                        channel_manager_to_tp_receiver.clone(),
                        tp_to_channel_manager_sender.clone(),
                        cancellation_token.clone(),
                        fallback_coordinator.clone(),
                        task_manager.clone(),
                        status_sender.clone(),
                    )
                    .await
                {
                    Ok(join_handle) => {
                        entry.tried_or_flagged = true;
                        return Ok(join_handle);
                    }
                    Err(e) => {
                        warn!(
                            "Attempt {}/{} failed for TP {} of {}: {:?}",
                            attempt,
                            MAX_RETRIES,
                            i + 1,
                            tp_count,
                            e
                        );
                        if attempt < MAX_RETRIES {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }

            warn!(
                "Max retries reached for template provider {} of {}, moving to next",
                i + 1,
                tp_count
            );
            entry.tried_or_flagged = true;
        }

        error!(
            "All template providers failed after {} retries each",
            MAX_RETRIES
        );
        Err(PoolErrorKind::CouldNotInitiateSystem)
    }

    /// Attempt to connect to a single template provider.
    #[allow(clippy::too_many_arguments)]
    async fn try_connect_tp(
        &self,
        tp_type: &TemplateProviderType,
        channel_manager_to_tp_receiver: Receiver<TemplateDistribution<'static>>,
        tp_to_channel_manager_sender: Sender<TemplateDistribution<'static>>,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        task_manager: Arc<TaskManager>,
        status_sender: Sender<Status>,
    ) -> Result<Option<JoinHandle<()>>, PoolErrorKind> {
        match tp_type.clone() {
            TemplateProviderType::Sv2Tp {
                address,
                public_key,
            } => {
                let sv2_tp = Sv2Tp::new(
                    address.clone(),
                    public_key,
                    channel_manager_to_tp_receiver,
                    tp_to_channel_manager_sender,
                    cancellation_token.clone(),
                    fallback_coordinator.clone(),
                    task_manager.clone(),
                )
                .await?;

                sv2_tp
                    .start(
                        address,
                        cancellation_token,
                        fallback_coordinator,
                        status_sender,
                        task_manager,
                    )
                    .await?;

                info!("Sv2 Template Provider setup done");
                Ok(None)
            }
            TemplateProviderType::BitcoinCoreIpc {
                network,
                data_dir,
                fee_threshold,
                min_interval,
            } => {
                let unix_socket_path =
                    stratum_apps::tp_type::resolve_ipc_socket_path(&network, data_dir)
                        .ok_or_else(|| {
                            PoolErrorKind::Configuration(
                                "Could not determine Bitcoin data directory. Please set data_dir in config.".to_string(),
                            )
                        })?;

                info!(
                    "Using Bitcoin Core IPC socket at: {}",
                    unix_socket_path.display()
                );

                let bitcoin_core_config = BitcoinCoreSv2Config {
                    unix_socket_path,
                    fee_threshold,
                    min_interval,
                    incoming_tdp_receiver: channel_manager_to_tp_receiver,
                    outgoing_tdp_sender: tp_to_channel_manager_sender,
                    cancellation_token: CancellationToken::new(),
                };

                let join_handle = connect_to_bitcoin_core(
                    bitcoin_core_config,
                    cancellation_token,
                    task_manager,
                    status_sender,
                )
                .await;

                info!("Bitcoin Core IPC Template Provider setup done");
                Ok(Some(join_handle))
            }
        }
    }
}

impl Drop for PoolSv2 {
    fn drop(&mut self) {
        info!("PoolSv2 dropped");
        self.cancellation_token.cancel();
    }
}
