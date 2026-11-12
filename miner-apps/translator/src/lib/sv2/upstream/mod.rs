pub mod common_message_handler;

use crate::{
    config::TranslatorConfig,
    error::{self, Action, LoopControl, TproxyError, TproxyErrorKind, TproxyResult},
    io_task::spawn_io_tasks,
    utils::{ReconnectRequest, UpstreamEntry},
};
use async_channel::{Receiver, Sender, unbounded};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use stratum_apps::{
    channel_utils::ReceiverCleanup,
    fallback_coordinator::FallbackCoordinator,
    network_helpers::{self, TCP_CONNECT_TIMEOUT, connect_with_noise, resolve_host},
    stratum_core::{
        binary_sv2::Seq064KOwned,
        common_messages_sv2::{
            MESSAGE_TYPE_SETUP_CONNECTION_ERROR, MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS, Protocol,
            SetupConnectionOwned,
        },
        extensions_sv2::RequestExtensionsOwned,
        handlers_sv2::HandleCommonMessagesFromServerOwnedAsync,
        parsers_sv2::AnyMessageOwned,
    },
    sync::SharedLock,
    task_manager::TaskManager,
    utils::{
        protocol_message_type::{MessageType, protocol_message_type},
        types::{InboundFrame, Message, OutboundFrame},
    },
};

use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Minimum time between two reconnects requested by the upstream that tProxy follows.
///
/// Every reconnect disconnects all SV1 miners. A request arriving sooner after the last followed
/// one is ignored, and tProxy stays on the current upstream.
const PROTOCOL_RECONNECT_MIN_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// Reconnects requested by the upstream with `Reconnect` or `ChannelEndpointChanged`.
///
/// Shared by the runtime and every upstream connection, so the interval between followed requests
/// holds across reconnects.
#[derive(Debug, Default)]
pub struct ProtocolReconnect {
    /// The recorded request, taken by the runtime once the teardown starts.
    requested: Option<ReconnectRequest>,
    /// When the runtime last took a request to follow it.
    last_followed: Option<Instant>,
}

impl ProtocolReconnect {
    /// Records a request made at `now` for the runtime to follow, unless it arrives within
    /// [`PROTOCOL_RECONNECT_MIN_INTERVAL`] of the last followed one. Returns whether it is
    /// recorded.
    fn record(&mut self, request: ReconnectRequest, now: Instant) -> bool {
        if self
            .last_followed
            .is_some_and(|last| now.duration_since(last) < PROTOCOL_RECONNECT_MIN_INTERVAL)
        {
            return false;
        }
        self.requested = Some(request);
        true
    }

    /// Takes the recorded request to follow it at `now`, which starts the interval before the
    /// next one can be followed.
    pub(crate) fn take(&mut self, now: Instant) -> Option<ReconnectRequest> {
        let request = self.requested.take()?;
        self.last_followed = Some(now);
        Some(request)
    }

    /// Drops the recorded request without following it.
    pub(crate) fn discard(&mut self) {
        self.requested = None;
    }
}

#[derive(Debug, Clone)]
struct UpstreamIo {
    /// Receiver for the SV2 Upstream role
    upstream_receiver: Receiver<InboundFrame>,
    /// Sender for the SV2 Upstream role
    upstream_sender: Sender<OutboundFrame>,
    /// Sender for the ChannelManager to send SV2 frames
    channel_manager_sender: Sender<InboundFrame>,
    /// Receiver for the ChannelManager to receive SV2 frames
    channel_manager_receiver: Receiver<OutboundFrame>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl UpstreamIo {
    fn new(
        upstream_receiver: Receiver<InboundFrame>,
        upstream_sender: Sender<OutboundFrame>,
        channel_manager_sender: Sender<InboundFrame>,
        channel_manager_receiver: Receiver<OutboundFrame>,
    ) -> Self {
        Self {
            upstream_receiver,
            upstream_sender,
            channel_manager_sender,
            channel_manager_receiver,
        }
    }

    fn close(&self) {
        debug!("Closing all upstream channels");
        self.upstream_sender.close();
        self.channel_manager_sender.close();
        self.upstream_receiver.close_and_drain();
        self.channel_manager_receiver.close_and_drain();
    }
}

/// Per-connection settings for [`Upstream::new`], derived from the [`TranslatorConfig`].
#[derive(Debug, Clone)]
pub struct UpstreamConnectOptions {
    /// How long to wait for the upstream to answer `SetupConnection`.
    pub setup_connection_timeout: Duration,
}

impl From<&TranslatorConfig> for UpstreamConnectOptions {
    fn from(config: &TranslatorConfig) -> Self {
        Self {
            setup_connection_timeout: config.setup_connection_timeout(),
        }
    }
}

/// Manages the upstream SV2 connection to a mining pool or proxy.
///
/// This struct handles the SV2 protocol communication with upstream servers,
/// including:
/// - Connection establishment with multiple upstream fallbacks
/// - SV2 handshake and setup procedures
/// - Message routing between channel manager and upstream
/// - Connection monitoring and error handling
/// - Graceful shutdown coordination
///
/// The upstream connection supports automatic failover between multiple
/// configured upstream servers and implements retry logic for connection
/// establishment.
#[derive(Debug, Clone)]
pub struct Upstream {
    upstream_io: UpstreamIo,
    /// Extensions that the translator requires (must be supported by server)
    required_extensions: Vec<u16>,
    address: SocketAddr,
    /// The configured upstream this connection reaches. A reconnect it requests keeps its
    /// authority key and user identity.
    entry: UpstreamEntry,
    /// Hands a reconnect requested by the upstream to the runtime, which follows it once the
    /// components have shut down.
    protocol_reconnect: SharedLock<ProtocolReconnect>,
    setup_connection_timeout: Duration,
    /// Stops the reader and writer tasks of this connection without affecting the rest of the
    /// translator. It is cancelled when setup fails, because nothing else would wake them.
    io_cancellation: CancellationToken,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl Upstream {
    fn handle_error_action(
        &self,
        context: &str,
        e: &TproxyError<error::Upstream>,
        cancellation_token: &CancellationToken,
        fallback_token: &CancellationToken,
    ) -> LoopControl {
        if cancellation_token.is_cancelled() {
            debug!(
                error_kind = ?e.kind,
                "{context} returned an error after shutdown was requested"
            );
            return LoopControl::Continue;
        }

        if fallback_token.is_cancelled() {
            debug!(
                error_kind = ?e.kind,
                "{context} returned an error during fallback"
            );
            return LoopControl::Continue;
        }

        match e.action {
            Action::Log => {
                warn!(
                    error_kind = ?e.kind,
                    "{context} returned a log-only error"
                );
                LoopControl::Continue
            }
            Action::Fallback => {
                warn!(
                    error_kind = ?e.kind,
                    "{context} requested fallback"
                );
                fallback_token.cancel();
                LoopControl::Break
            }
            Action::Reconnect => {
                let TproxyErrorKind::ReconnectRequested(request) = &e.kind else {
                    warn!(
                        error_kind = ?e.kind,
                        "{context} requested a reconnect without an endpoint; falling back"
                    );
                    fallback_token.cancel();
                    return LoopControl::Break;
                };
                match self.protocol_reconnect.with(|protocol_reconnect| {
                    protocol_reconnect.record((**request).clone(), Instant::now())
                }) {
                    // The runtime follows the request once every component has shut down,
                    // through the same coordinated teardown as a fallback.
                    Ok(true) => {
                        info!("{context}: {request}");
                        fallback_token.cancel();
                        LoopControl::Break
                    }
                    Ok(false) => {
                        warn!(
                            "{context}: {request}, ignored because the last reconnect was followed less than {:?} ago",
                            PROTOCOL_RECONNECT_MIN_INTERVAL
                        );
                        LoopControl::Continue
                    }
                    Err(_) => {
                        error!("{context}: failed to record the requested reconnect");
                        cancellation_token.cancel();
                        LoopControl::Break
                    }
                }
            }
            Action::Shutdown => {
                warn!(
                    error_kind = ?e.kind,
                    "{context} requested shutdown"
                );
                cancellation_token.cancel();
                LoopControl::Break
            }
            other => {
                warn!(
                    action = ?other,
                    error_kind = ?e.kind,
                    "{context} returned an unhandled action"
                );
                LoopControl::Continue
            }
        }
    }

    /// Creates a new upstream connection by attempting to connect to configured servers.
    ///
    /// This method tries to establish a connection to one of the provided upstream
    /// servers, implementing retry logic and fallback behavior. It will attempt
    /// to connect to each server multiple times before giving up.
    ///
    /// # Arguments
    /// * `upstreams` - A single `UpstreamEntry` representing the upstream candidate currently being
    ///   attempted.
    /// * `channel_manager_sender` - Channel to send messages to the channel manager
    /// * `channel_manager_receiver` - Channel to receive messages from the channel manager
    /// * `cancellation_token` - Global application cancellation token
    /// * `fallback_coordinator` - Coordinator for upstream fallback
    /// * `protocol_reconnect` - Where a reconnect requested by the upstream is handed to the
    ///   runtime
    /// * `options` - Per-connection settings such as the `SetupConnection` timeout
    ///
    /// # Returns
    /// * `Ok(Upstream)` - Successfully connected to an upstream server
    /// * `Err(TproxyError)` - Failed to connect to any upstream server
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        upstream: &UpstreamEntry,
        channel_manager_sender: Sender<InboundFrame>,
        channel_manager_receiver: Receiver<OutboundFrame>,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        task_manager: Arc<TaskManager>,
        required_extensions: Vec<u16>,
        protocol_reconnect: SharedLock<ProtocolReconnect>,
        options: UpstreamConnectOptions,
    ) -> TproxyResult<Self, error::Upstream> {
        info!(
            "Trying to connect to upstream at {}:{}",
            upstream.host, upstream.port
        );

        if cancellation_token.is_cancelled() {
            info!("Shutdown signal received during upstream connection attempt. Aborting.");
            return Err(TproxyError::shutdown(
                TproxyErrorKind::CouldNotInitiateSystem,
            ));
        }

        let candidate_addrs = resolve_host(&upstream.host, upstream.port)
            .await
            .map_err(|e| {
                error!(
                    "Failed to resolve upstream address {}:{}: {e}",
                    upstream.host, upstream.port
                );
                TproxyError::fallback(TproxyErrorKind::NetworkHelpersError(e.into()))
            })?;

        // Each resolved address gets an attempt, so a pool that publishes both an IPv6 and an
        // IPv4 address is still reachable when only one of the two accepts connections.
        match tokio::time::timeout(
            TCP_CONNECT_TIMEOUT,
            TcpStream::connect(&candidate_addrs[..]),
        )
        .await
        .map_err(TproxyError::fallback)?
        {
            Ok(socket) => {
                let resolved_addr = socket.peer_addr().map_err(TproxyError::fallback)?;
                info!("Connected to upstream at {}", resolved_addr);

                tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        info!("Shutdown received during handshake, dropping connection");
                        Err(TproxyError::shutdown(TproxyErrorKind::CouldNotInitiateSystem))
                    }
                    result = connect_with_noise(socket, Some(upstream.authority_pubkey)) => {
                        match result {
                            Ok(stream) => {
                                let (reader, writer) = stream.into_split();

                                let io_cancellation = cancellation_token.child_token();
                                let (outbound_tx, outbound_rx) = unbounded();
                                let (inbound_tx, inbound_rx) = unbounded();

                                spawn_io_tasks(
                                    task_manager,
                                    reader,
                                    writer,
                                    outbound_rx,
                                    inbound_tx,
                                    io_cancellation.clone(),
                                    fallback_coordinator.clone(),
                                );

                                let upstream_io = UpstreamIo::new(
                                    inbound_rx,
                                    outbound_tx,
                                    channel_manager_sender,
                                    channel_manager_receiver,
                                );
                                debug!(
                                    "Successfully initialized upstream channel with {}",
                                    resolved_addr
                                );

                                Ok(Self {
                                    upstream_io,
                                    required_extensions: required_extensions.clone(),
                                    address: resolved_addr,
                                    entry: upstream.clone(),
                                    protocol_reconnect,
                                    setup_connection_timeout: options.setup_connection_timeout,
                                    io_cancellation,
                                })
                            }
                            Err(network_helpers::Error::InvalidKey) => {
                                Err(TproxyError::fallback(TproxyErrorKind::InvalidKey))
                            }
                            Err(e) => {
                                error!(
                                    "Failed Noise handshake with {}: {e}.",
                                    resolved_addr
                                );
                                Err(TproxyError::fallback(
                                    TproxyErrorKind::NetworkHelpersError(e),
                                ))
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to any of {:?}: {e}.", candidate_addrs);
                Err(TproxyError::fallback(e))
            }
        }
    }

    /// Starts the upstream connection and begins message processing.
    ///
    /// This method:
    /// - Completes the SV2 handshake with the upstream server
    /// - Spawns the main message processing task
    /// - Handles graceful shutdown coordination
    ///
    /// The method will first attempt to complete the SV2 setup connection
    /// handshake. If successful, it spawns a task to handle bidirectional
    /// message flow between the channel manager and upstream server.
    pub async fn start(
        mut self,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        task_manager: Arc<TaskManager>,
    ) -> TproxyResult<(), error::Upstream> {
        let fallback_token: CancellationToken = fallback_coordinator.token();

        // wait for connection setup or cancellation signal
        tokio::select! {
            biased;

            _ = cancellation_token.cancelled() => {
                info!("Upstream: shutdown signal received during connection setup.");
                self.upstream_io.close();
                return Ok(());
            }
            _ = fallback_token.cancelled() => {
                info!("Upstream: fallback signal received during connection setup.");
                self.upstream_io.close();
                return Ok(());
            }
            result = self.setup_connection() => {
                if let Err(e) = result {
                    error!("Upstream: failed to set up SV2 connection: {e:?}");
                    self.io_cancellation.cancel();
                    return Err(e);
                }
            }
        }

        self.run_upstream_task(cancellation_token, fallback_coordinator, task_manager)?;

        Ok(())
    }

    /// Performs the SV2 handshake setup with the upstream server.
    ///
    /// This method handles the initial SV2 protocol handshake by:
    /// - Creating and sending a SetupConnection message
    /// - Waiting for the handshake response
    /// - Validating and processing the response
    ///
    /// The handshake establishes the protocol version, capabilities, and
    /// other connection parameters needed for SV2 communication.
    async fn setup_connection(&mut self) -> TproxyResult<(), error::Upstream> {
        debug!("Upstream: initiating SV2 handshake...");
        // Build SetupConnection message
        let setup_conn_msg = Self::get_setup_connection_message(2, 2, &self.address, false)
            .map_err(TproxyError::shutdown)?;
        let sv2_frame = OutboundFrame::from_message(Message::Common(setup_conn_msg.into()))
            .map_err(|error| {
                error!("Failed to serialize SetupConnection message: {error:?}");
                TproxyError::shutdown(error)
            })?;

        // Send SetupConnection message to upstream
        self.upstream_io
            .upstream_sender
            .send(sv2_frame)
            .await
            .map_err(|e| {
                error!("Failed to send SetupConnection to upstream: {:?}", e);
                TproxyError::fallback(TproxyErrorKind::ChannelErrorSender)
            })?;

        let response = tokio::time::timeout(
            self.setup_connection_timeout,
            self.upstream_io.upstream_receiver.recv(),
        )
        .await
        .map_err(|_| {
            error!(
                "Upstream {} did not answer SetupConnection within {:?}",
                self.address, self.setup_connection_timeout
            );
            TproxyError::fallback(TproxyErrorKind::SetupConnectionTimeout)
        })?;

        let mut incoming: InboundFrame = match response {
            Ok(frame) => {
                debug!("Received handshake response from upstream.");
                frame
            }
            Err(e) => {
                error!("Failed to receive handshake response from upstream: {}", e);
                return Err(TproxyError::fallback(e));
            }
        };

        let header = incoming.header();

        if header.ext_type() != 0
            || !matches!(
                header.msg_type(),
                MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS | MESSAGE_TYPE_SETUP_CONNECTION_ERROR
            )
        {
            return Err(TproxyError::fallback(TproxyErrorKind::UnexpectedMessage(
                header.ext_type(),
                header.msg_type(),
            )));
        }

        let payload = incoming.payload();

        self.handle_common_message_frame_from_server(None, header, payload)
            .await?;
        debug!("Upstream: handshake completed successfully.");

        // Send RequestExtensions message if there are any required extensions
        if !self.required_extensions.is_empty() {
            let require_extensions = RequestExtensionsOwned {
                request_id: 1,
                requested_extensions: Seq064KOwned::new(self.required_extensions.clone())
                    .map_err(TproxyError::shutdown)?,
            };

            info!(
                "Sending RequestExtensions message to upstream: {:?}",
                require_extensions
            );

            let sv2_frame =
                OutboundFrame::from_message(AnyMessageOwned::Extensions(require_extensions.into()))
                    .map_err(TproxyError::shutdown)?;

            self.upstream_io
                .upstream_sender
                .send(sv2_frame)
                .await
                .map_err(|e| {
                    error!("Failed to send message to upstream: {:?}", e);
                    TproxyError::fallback(TproxyErrorKind::ChannelErrorSender)
                })?;
        }
        Ok(())
    }

    /// Handles one SV2 frame received from upstream.
    async fn handle_upstream_message(mut self) -> TproxyResult<(), error::Upstream> {
        let mut sv2_frame = self
            .upstream_io
            .upstream_receiver
            .recv()
            .await
            .map_err(TproxyError::fallback)?;

        debug!("Upstream: received frame.");
        let header = sv2_frame.header();

        match protocol_message_type(header.ext_type(), header.msg_type()) {
            MessageType::Common => {
                info!(
                    extension_type = header.ext_type(),
                    message_type = header.msg_type(),
                    "Handling common message from Upstream."
                );
                self.handle_common_message_frame_from_server(None, header, sv2_frame.payload())
                    .await?;
            }
            MessageType::Mining | MessageType::Extensions => {
                self.upstream_io
                    .channel_manager_sender
                    .send(sv2_frame)
                    .await
                    .map_err(|e| {
                        error!("Failed to send mining message to channel manager: {:?}", e);
                        TproxyError::shutdown(TproxyErrorKind::ChannelErrorSender)
                    })?;
            }
            _ => {
                warn!(
                    extension_type = header.ext_type(),
                    message_type = header.msg_type(),
                    "Received unsupported message type from upstream."
                );
                return Err(TproxyError::fallback(TproxyErrorKind::UnexpectedMessage(
                    header.ext_type(),
                    header.msg_type(),
                )));
            }
        }

        Ok(())
    }

    /// Handles one SV2 frame received from the channel manager and forwards it upstream.
    async fn handle_channel_manager_message(&self) -> TproxyResult<(), error::Upstream> {
        let sv2_frame = self
            .upstream_io
            .channel_manager_receiver
            .recv()
            .await
            .map_err(TproxyError::shutdown)?;

        debug!(
            "Upstream: sending sv2 frame from channel manager: {:?}",
            sv2_frame
        );
        self.upstream_io
            .upstream_sender
            .send(sv2_frame)
            .await
            .map_err(|e| {
                error!("Upstream: failed to send sv2 frame: {e:?}");
                TproxyError::fallback(TproxyErrorKind::ChannelErrorSender)
            })?;

        Ok(())
    }

    /// Spawns a unified task to handle upstream message I/O and shutdown logic.
    #[allow(clippy::result_large_err)]
    fn run_upstream_task(
        self,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        task_manager: Arc<TaskManager>,
    ) -> TproxyResult<(), error::Upstream> {
        let fallback_token = fallback_coordinator.token();
        task_manager.spawn(fallback_coordinator.track(async move {
            loop {
                tokio::select! {
                    biased;

                    // Handle app shutdown signal
                    _ = cancellation_token.cancelled() => {
                        info!("Upstream: received shutdown signal. Exiting loop.");
                        break;
                    }

                    // Handle fallback trigger
                    _ = fallback_token.cancelled() => {
                        info!("Upstream: fallback triggered");
                        break;
                    }

                    res = self.clone().handle_upstream_message() => {
                        if let Err(e) = res {
                            if let LoopControl::Break = self.handle_error_action(
                                "Upstream::handle_upstream_message",
                                &e,
                                &cancellation_token,
                                &fallback_token,
                            ) {
                                break;
                            }
                        }
                    }
                    res = self.handle_channel_manager_message() => {
                        if let Err(e) = res {
                            if let LoopControl::Break = self.handle_error_action(
                                "Upstream::handle_channel_manager_message",
                                &e,
                                &cancellation_token,
                                &fallback_token,
                            ) {
                                break;
                            }
                        }
                    }
                }
            }

            self.upstream_io.close();
            warn!("Upstream: task shutting down cleanly.");
        }));

        Ok(())
    }

    /// Constructs the `SetupConnection` message.
    #[allow(clippy::result_large_err)]
    fn get_setup_connection_message(
        min_version: u16,
        max_version: u16,
        address: &SocketAddr,
        is_work_selection_enabled: bool,
    ) -> Result<SetupConnectionOwned, TproxyErrorKind> {
        let endpoint_host = address.ip().to_string().try_into()?;
        let vendor = "SRI".try_into()?;
        let hardware_version = "Translator Proxy".try_into()?;
        let firmware = "".try_into()?;
        let device_id = "".try_into()?;
        let flags = if is_work_selection_enabled {
            0b110
        } else {
            0b100
        };

        Ok(SetupConnectionOwned {
            protocol: Protocol::MiningProtocol,
            min_version,
            max_version,
            flags,
            endpoint_host,
            endpoint_port: address.port(),
            vendor,
            hardware_version,
            firmware,
            device_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratum_apps::network_helpers::accept_noise_connection;

    const TIMEOUT: Duration = Duration::from_secs(7);
    const AUTHORITY_PUBLIC_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
    const AUTHORITY_SECRET_KEY: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";

    /// An `Upstream` whose peer never sends anything. The returned receiver shows what the
    /// translator sent and the returned sender keeps the peer's channel open.
    fn silent_upstream() -> (Upstream, Receiver<OutboundFrame>, Sender<InboundFrame>) {
        let (outbound_tx, outbound_rx) = unbounded();
        let (inbound_tx, inbound_rx) = unbounded::<InboundFrame>();
        let (cm_sender, _cm_inbound) = unbounded();
        let (_cm_outbound, cm_receiver) = unbounded();
        let upstream = Upstream {
            upstream_io: UpstreamIo::new(inbound_rx, outbound_tx, cm_sender, cm_receiver),
            required_extensions: vec![],
            address: "127.0.0.1:3333".parse().unwrap(),
            entry: UpstreamEntry {
                host: "127.0.0.1".to_string(),
                port: 3333,
                authority_pubkey: "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan"
                    .parse()
                    .unwrap(),
                user_identity: "test".to_string(),
            },
            protocol_reconnect: SharedLock::new(ProtocolReconnect::default()),
            setup_connection_timeout: TIMEOUT,
            io_cancellation: CancellationToken::new(),
        };
        (upstream, outbound_rx, inbound_tx)
    }

    #[tokio::test(start_paused = true)]
    async fn setup_connection_times_out_when_upstream_stays_silent() {
        let (mut upstream, sent, _peer) = silent_upstream();

        let started = tokio::time::Instant::now();
        let err = tokio::time::timeout(TIMEOUT * 2, upstream.setup_connection())
            .await
            .expect("setup_connection should give up on its own")
            .unwrap_err();

        assert!(matches!(err.kind, TproxyErrorKind::SetupConnectionTimeout));
        assert!(matches!(err.action, Action::Fallback));
        assert_eq!(started.elapsed(), TIMEOUT);
        assert!(sent.try_recv().is_ok(), "SetupConnection was not sent");
    }

    /// A peer that completes the Noise handshake, then reads and answers nothing. The receiver
    /// fires once the translator has closed its write side of the connection.
    async fn silent_noise_peer() -> (SocketAddr, tokio::sync::oneshot::Receiver<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (closed_tx, closed) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = accept_noise_connection(
                stream,
                AUTHORITY_PUBLIC_KEY.parse().unwrap(),
                AUTHORITY_SECRET_KEY.parse().unwrap(),
                3600,
            )
            .await
            .unwrap();
            let (mut reader, _writer) = stream.into_split();
            while reader.read_frame().await.is_ok() {}
            let _ = closed_tx.send(());
            // Keeps its end of the socket open, so the translator's reader never sees EOF.
            std::future::pending::<()>().await;
        });
        (address, closed)
    }

    #[tokio::test]
    async fn failed_setup_stops_the_connection_io_tasks() {
        let (address, peer_closed) = silent_noise_peer().await;
        let entry = UpstreamEntry {
            host: address.ip().to_string(),
            port: address.port(),
            authority_pubkey: AUTHORITY_PUBLIC_KEY.parse().unwrap(),
            user_identity: "test".to_string(),
        };
        let (cm_sender, _cm_inbound) = unbounded();
        let (_cm_outbound, cm_receiver) = unbounded();
        let cancellation_token = CancellationToken::new();
        let fallback_coordinator = FallbackCoordinator::new();
        let task_manager = Arc::new(TaskManager::new());
        let upstream = Upstream::new(
            &entry,
            cm_sender,
            cm_receiver,
            cancellation_token.clone(),
            fallback_coordinator.clone(),
            task_manager.clone(),
            vec![],
            SharedLock::new(ProtocolReconnect::default()),
            UpstreamConnectOptions {
                setup_connection_timeout: Duration::from_millis(200),
            },
        )
        .await
        .unwrap();

        let result = upstream
            .start(
                cancellation_token.clone(),
                fallback_coordinator,
                task_manager.clone(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err().kind,
            TproxyErrorKind::SetupConnectionTimeout
        ));
        assert!(!cancellation_token.is_cancelled());
        // The reader would otherwise wait for the silent peer forever.
        tokio::time::timeout(Duration::from_secs(5), task_manager.join_all())
            .await
            .expect("the connection's io tasks are still running");
        tokio::time::timeout(Duration::from_secs(5), peer_closed)
            .await
            .expect("the failed connection is still open")
            .unwrap();
    }
}
