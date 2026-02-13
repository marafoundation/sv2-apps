use crate::{
    error::{self, TproxyError, TproxyErrorKind, TproxyResult},
    io_task::spawn_io_tasks,
    status::{handle_error, Status, StatusSender},
    sv2::upstream::channel::UpstreamChannelState,
    utils::UpstreamEntry,
};
use async_channel::{unbounded, Receiver, Sender};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use stratum_apps::{
    fallback_coordinator::FallbackCoordinator,
    network_helpers::{self, connect_with_noise},
    stratum_core::{
        binary_sv2::{self, Seq064K},
        codec_sv2::HandshakeRole,
        common_messages_sv2::{Protocol, SetupConnection},
        extensions_sv2::{RequestExtensions, RequestExtensionsError, RequestExtensionsSuccess},
        handlers_sv2::HandleCommonMessagesFromServerAsync,
        parsers_sv2::{AnyMessage, Mining},
    },
    task_manager::TaskManager,
    utils::{
        protocol_message_type::{protocol_message_type, MessageType},
        types::{Message, Sv2Frame},
    },
};

use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Timeout for extension negotiation response (30 seconds)
const EXTENSION_NEGOTIATION_TIMEOUT_SECS: u64 = 30;

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
    pub upstream_channel_state: UpstreamChannelState,
    /// Extensions that the translator requires (must be supported by server)
    pub required_extensions: Vec<u16>,
    address: SocketAddr,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl Upstream {
    /// Creates a new upstream connection by attempting to connect to configured servers.
    ///
    /// This method tries to establish a connection to one of the provided upstream
    /// servers, implementing retry logic and fallback behavior. It will attempt
    /// to connect to each server multiple times before giving up.
    ///
    /// # Arguments
    /// * `upstreams` - A single `UpstreamEntry` representing the upstream candidate currently being
    ///   attempted. The `tried_or_flagged` is set once the upstream has either been connected to
    ///   successfully or marked as malicious. Because `new` is only called from
    ///   `try_initialize_upstream`, we can treat this flag as the definitive state for that
    ///   upstream.
    /// * `channel_manager_sender` - Channel to send messages to the channel manager
    /// * `channel_manager_receiver` - Channel to receive messages from the channel manager
    /// * `cancellation_token` - Global application cancellation token
    /// * `fallback_coordinator` - Coordinator for upstream fallback
    ///
    /// # Returns
    /// * `Ok(Upstream)` - Successfully connected to an upstream server
    /// * `Err(TproxyError)` - Failed to connect to any upstream server
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        upstream: &UpstreamEntry,
        channel_manager_sender: Sender<Sv2Frame>,
        channel_manager_receiver: Receiver<Sv2Frame>,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        task_manager: Arc<TaskManager>,
        required_extensions: Vec<u16>,
    ) -> TproxyResult<Self, error::Upstream> {
        info!("Trying to connect to upstream at {}", upstream.addr);

        if cancellation_token.is_cancelled() {
            info!("Shutdown signal received during upstream connection attempt. Aborting.");
            return Err(TproxyError::shutdown(
                TproxyErrorKind::CouldNotInitiateSystem,
            ));
        }

        match TcpStream::connect(upstream.addr).await {
            Ok(socket) => {
                info!("Connected to upstream at {}", upstream.addr);

                tokio::select! {
                    result = connect_with_noise(socket, Some(upstream.authority_pubkey)) => {
                        match result {
                            Ok(stream) => {
                                let (reader, writer) = stream.into_split();

                                let (outbound_tx, outbound_rx) = unbounded();
                                let (inbound_tx, inbound_rx) = unbounded();

                                spawn_io_tasks(
                                    task_manager,
                                    reader,
                                    writer,
                                    outbound_rx,
                                    inbound_tx,
                                    cancellation_token.clone(),
                                    fallback_coordinator.clone(),
                                );

                                let upstream_channel_state = UpstreamChannelState::new(
                                    inbound_rx,
                                    outbound_tx,
                                    channel_manager_sender,
                                    channel_manager_receiver,
                                );
                                debug!(
                                    "Successfully initialized upstream channel with {}",
                                    upstream.addr
                                );

                                return Ok(Self {
                                    upstream_channel_state,
                                    required_extensions: required_extensions.clone(),
                                    address: upstream.addr,
                                });
                            }
                            Err(network_helpers::Error::InvalidKey) => {
                                return Err(TproxyError::fallback(TproxyErrorKind::InvalidKey));
                            }
                            Err(e) => {
                                error!(
                                    "Failed Noise handshake with {}: {e}. Retrying...",
                                    upstream.addr
                                );
                            }
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        info!("Shutdown received during handshake, dropping connection");
                        return Err(TproxyError::shutdown(TproxyErrorKind::CouldNotInitiateSystem));
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to {}: {e}.", upstream.addr);
            }
        }

        error!("Failed to connect to any configured upstream.");
        Err(TproxyError::shutdown(
            TproxyErrorKind::CouldNotInitiateSystem,
        ))
    }

    /// Starts the upstream connection and begins message processing.
    ///
    /// This method:
    /// - Completes the SV2 handshake with the upstream server
    /// - Negotiates extensions synchronously (waits for response)
    /// - Spawns the main message processing task
    /// - Handles graceful shutdown coordination
    ///
    /// The method will first attempt to complete the SV2 setup connection
    /// handshake. If successful, it spawns a task to handle bidirectional
    /// message flow between the channel manager and upstream server.
    ///
    /// # Returns
    /// * `Ok(Vec<u16>)` - Successfully started, returns negotiated extensions
    /// * `Err(TproxyError)` - Failed to start or negotiate extensions
    pub async fn start(
        mut self,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        status_sender: Sender<Status>,
        task_manager: Arc<TaskManager>,
    ) -> TproxyResult<Vec<u16>, error::Upstream> {
        let fallback_token: CancellationToken = fallback_coordinator.token();
        let negotiated_extensions;

        // Wait for connection setup or cancellation signal
        tokio::select! {
            result = self.setup_connection() => {
                match result {
                    Ok(extensions) => {
                        negotiated_extensions = extensions;
                    }
                    Err(e) => {
                        error!("Upstream: failed to set up SV2 connection: {e:?}");
                        return Err(e);
                    }
                }
            }
            _ = cancellation_token.cancelled() => {
                info!("Upstream: shutdown signal received during connection setup.");
                return Ok(vec![]);
            }
            _ = fallback_token.cancelled() => {
                info!("Upstream: fallback signal received during connection setup.");
                return Ok(vec![]);
            }
        }

        // Wrap status sender and start upstream task
        let wrapped_status_sender = StatusSender::Upstream(status_sender);

        self.run_upstream_task(
            cancellation_token,
            fallback_coordinator,
            wrapped_status_sender,
            task_manager,
        )?;

        Ok(negotiated_extensions)
    }

    /// Performs the SV2 handshake setup with the upstream server.
    ///
    /// This method handles the initial SV2 protocol handshake by:
    /// - Creating and sending a SetupConnection message
    /// - Waiting for the handshake response
    /// - Validating and processing the response
    /// - Sending RequestExtensions if required extensions are configured
    /// - **Waiting for RequestExtensionsSuccess/Error response** before returning
    ///
    /// The handshake establishes the protocol version, capabilities, and
    /// other connection parameters needed for SV2 communication.
    ///
    /// # Returns
    /// * `Ok(Vec<u16>)` - The list of negotiated extensions (empty if none were requested)
    /// * `Err(TproxyError)` - Error during handshake or extension negotiation
    pub async fn setup_connection(&mut self) -> TproxyResult<Vec<u16>, error::Upstream> {
        debug!("Upstream: initiating SV2 handshake...");
        // Build SetupConnection message
        let setup_conn_msg = Self::get_setup_connection_message(2, 2, &self.address, false)
            .map_err(TproxyError::shutdown)?;
        let sv2_frame: Sv2Frame =
            Message::Common(setup_conn_msg.into())
                .try_into()
                .map_err(|error| {
                    error!("Failed to serialize SetupConnection message: {error:?}");
                    TproxyError::shutdown(error)
                })?;

        // Send SetupConnection message to upstream
        self.upstream_channel_state
            .upstream_sender
            .send(sv2_frame)
            .await
            .map_err(|e| {
                error!("Failed to send SetupConnection to upstream: {:?}", e);
                TproxyError::fallback(TproxyErrorKind::ChannelErrorSender)
            })?;

        let mut incoming: Sv2Frame =
            match self.upstream_channel_state.upstream_receiver.recv().await {
                Ok(frame) => {
                    debug!("Received handshake response from upstream.");
                    frame
                }
                Err(e) => {
                    error!("Failed to receive handshake response from upstream: {}", e);
                    return Err(TproxyError::fallback(e));
                }
            };

        let header = incoming.get_header().ok_or_else(|| {
            error!("Expected handshake frame but no header found.");
            TproxyError::fallback(TproxyErrorKind::UnexpectedMessage(0, 0))
        })?;

        let payload = incoming.payload();

        self.handle_common_message_frame_from_server(None, header, payload)
            .await?;
        debug!("Upstream: handshake completed successfully.");

        // Send RequestExtensions message if there are any required extensions
        // and wait for the response before returning
        if !self.required_extensions.is_empty() {
            let negotiated = self.negotiate_extensions().await?;
            return Ok(negotiated);
        }

        Ok(vec![])
    }

    /// Sends RequestExtensions and waits for the response.
    ///
    /// This method handles the extension negotiation flow:
    /// 1. Sends RequestExtensions with required extensions
    /// 2. Waits for RequestExtensionsSuccess or RequestExtensionsError
    /// 3. Validates that all required extensions are supported
    /// 4. Handles retry if server requires additional extensions we support
    ///
    /// # Returns
    /// * `Ok(Vec<u16>)` - The list of successfully negotiated extensions
    /// * `Err(TproxyError)` - Extension negotiation failed
    async fn negotiate_extensions(&mut self) -> TproxyResult<Vec<u16>, error::Upstream> {
        let request_extensions = RequestExtensions {
            request_id: 1,
            requested_extensions: Seq064K::new(self.required_extensions.clone()).unwrap(),
        };

        let sv2_frame: Sv2Frame = AnyMessage::Extensions(request_extensions.into_static().into())
            .try_into()
            .map_err(TproxyError::shutdown)?;

        info!(
            "Sending RequestExtensions to upstream with required extensions: {:?}",
            self.required_extensions
        );

        self.upstream_channel_state
            .upstream_sender
            .send(sv2_frame)
            .await
            .map_err(|e| {
                error!("Failed to send RequestExtensions to upstream: {:?}", e);
                TproxyError::fallback(TproxyErrorKind::ChannelErrorSender)
            })?;

        // Wait for extension negotiation response with timeout
        let response = tokio::time::timeout(
            Duration::from_secs(EXTENSION_NEGOTIATION_TIMEOUT_SECS),
            self.upstream_channel_state.upstream_receiver.recv(),
        )
        .await
        .map_err(|_| {
            error!(
                "Extension negotiation timed out after {} seconds",
                EXTENSION_NEGOTIATION_TIMEOUT_SECS
            );
            TproxyError::fallback(TproxyErrorKind::ExtensionNegotiationTimeout)
        })?
        .map_err(|e| {
            error!("Failed to receive extension negotiation response: {}", e);
            TproxyError::fallback(e)
        })?;

        self.handle_extension_response(response).await
    }

    /// Handles the extension negotiation response (Success or Error).
    async fn handle_extension_response(
        &mut self,
        mut response: Sv2Frame,
    ) -> TproxyResult<Vec<u16>, error::Upstream> {
        let header = response.get_header().ok_or_else(|| {
            error!("Extension response frame missing header");
            TproxyError::fallback(TproxyErrorKind::UnexpectedMessage(0, 0))
        })?;

        let msg_type = header.msg_type();
        let payload = response.payload();

        // Message types for extension negotiation:
        // 0x00 = RequestExtensions
        // 0x01 = RequestExtensionsSuccess
        // 0x02 = RequestExtensionsError
        match msg_type {
            0x01 => {
                // RequestExtensionsSuccess
                let msg: RequestExtensionsSuccess =
                    binary_sv2::from_bytes(payload).map_err(|e| {
                        error!("Failed to parse RequestExtensionsSuccess: {:?}", e);
                        TproxyError::fallback(TproxyErrorKind::BinarySv2(e))
                    })?;

                let supported: Vec<u16> = msg.supported_extensions.into_inner();
                info!("Extension negotiation success: supported={:?}", supported);

                // Check if all required extensions are supported
                let missing_required: Vec<u16> = self
                    .required_extensions
                    .iter()
                    .filter(|ext| !supported.contains(ext))
                    .copied()
                    .collect();

                if !missing_required.is_empty() {
                    error!(
                        "Server does not support required extensions: {:?}",
                        missing_required
                    );
                    return Err(TproxyError::fallback(
                        TproxyErrorKind::RequiredExtensionsNotSupported(missing_required),
                    ));
                }

                info!("Successfully negotiated extensions: {:?}", supported);
                Ok(supported)
            }
            0x02 => {
                // RequestExtensionsError
                let msg: RequestExtensionsError = binary_sv2::from_bytes(payload).map_err(|e| {
                    error!("Failed to parse RequestExtensionsError: {:?}", e);
                    TproxyError::fallback(TproxyErrorKind::BinarySv2(e))
                })?;

                let unsupported: Vec<u16> = msg.unsupported_extensions.into_inner();
                let required_by_server: Vec<u16> = msg.required_extensions.into_inner();

                error!(
                    "Extension negotiation error: unsupported={:?}, required_by_server={:?}",
                    unsupported, required_by_server
                );

                // Check if any of our required extensions were not supported
                let missing_required: Vec<u16> = self
                    .required_extensions
                    .iter()
                    .filter(|ext| unsupported.contains(ext))
                    .copied()
                    .collect();

                if !missing_required.is_empty() {
                    error!(
                        "Server does not support required extensions: {:?}",
                        missing_required
                    );
                    return Err(TproxyError::fallback(
                        TproxyErrorKind::RequiredExtensionsNotSupported(missing_required),
                    ));
                }

                // If server requires extensions we don't support, fail
                if !required_by_server.is_empty() {
                    error!(
                        "Server requires extensions that we don't support: {:?}",
                        required_by_server
                    );
                    return Err(TproxyError::fallback(
                        TproxyErrorKind::ServerRequiresUnsupportedExtensions(required_by_server),
                    ));
                }

                // No required extensions failed, return empty (negotiation succeeded with no
                // extensions)
                Ok(vec![])
            }
            _ => {
                error!(
                    "Unexpected message type during extension negotiation: {}",
                    msg_type
                );
                Err(TproxyError::fallback(TproxyErrorKind::UnexpectedMessage(
                    header.ext_type(),
                    msg_type,
                )))
            }
        }
    }

    /// Processes incoming messages from the upstream SV2 server.
    ///
    /// This method handles different types of frames received from upstream:
    /// - SV2 frames: Parses and routes mining/common messages appropriately
    /// - Handshake frames: Logs for debugging (shouldn't occur during normal operation)
    ///
    /// Common messages are handled directly, while mining messages are forwarded
    /// to the channel manager for processing and distribution to downstream connections.
    pub async fn on_upstream_message(
        &mut self,
        mut sv2_frame: Sv2Frame,
    ) -> TproxyResult<(), error::Upstream> {
        debug!("Received SV2 frame from upstream.");
        let Some(header) = sv2_frame.get_header() else {
            return Err(TproxyError::fallback(TproxyErrorKind::UnexpectedMessage(
                0, 0,
            )));
        };

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
                self.upstream_channel_state
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

    /// Spawns a unified task to handle upstream message I/O and shutdown logic.
    #[allow(clippy::result_large_err)]
    fn run_upstream_task(
        mut self,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        status_sender: StatusSender,
        task_manager: Arc<TaskManager>,
    ) -> TproxyResult<(), error::Upstream> {
        task_manager.spawn(async move {
            // we just spawned a new task that's relevant to fallback coordination
            // so register it with the fallback coordinator
            let fallback_handler = fallback_coordinator.register();

            // get the cancellation token that signals fallback
            let fallback_token = fallback_coordinator.token();

            loop {
                tokio::select! {
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

                    // Handle incoming SV2 messages from upstream
                    result = self.upstream_channel_state.upstream_receiver.recv() => {
                        match result {
                            Ok(frame) => {
                                debug!("Upstream: received frame.");
                                if let Err(e) = self.on_upstream_message(frame).await {
                                    error!("Upstream: error while processing message: {e:?}");
                                    handle_error(&status_sender, e).await;
                                }
                            }
                            Err(e) => {
                                error!("Upstream: receiver channel closed unexpectedly: {e}");
                                handle_error(&status_sender, TproxyError::<error::Upstream>::fallback(e)).await;
                                break;
                            }
                        }
                    }

                    // Handle messages from channel manager to send upstream
                    result = self.upstream_channel_state.channel_manager_receiver.recv() => {
                        match result {
                            Ok(sv2_frame) => {
                                debug!("Upstream: sending sv2 frame from channel manager: {:?}", sv2_frame);
                                if let Err(e) = self
                                    .upstream_channel_state
                                    .upstream_sender
                                    .send(sv2_frame)
                                    .await
                                    .map_err(|e| {
                                        error!("Upstream: failed to send sv2 frame: {e:?}");
                                        TproxyError::<error::Upstream>::fallback(TproxyErrorKind::ChannelErrorSender)
                                    })
                                {
                                    handle_error(&status_sender, e).await;
                                }
                            }
                            Err(e) => {
                                error!("Upstream: channel manager receiver closed: {e}");
                                handle_error(&status_sender, TproxyError::<error::Upstream>::shutdown(e)).await;
                                break;
                            }
                        }
                    }
                }
            }

            self.upstream_channel_state.drop();
            warn!("Upstream: task shutting down cleanly.");

            // signal fallback coordinator that this task has completed its cleanup
            fallback_handler.done();
        });

        Ok(())
    }

    /// Sends a message to the upstream SV2 server.
    ///
    /// This method forwards messages from the channel manager to the upstream
    /// server. Messages are typically mining-related (share submissions, channel
    /// requests, etc.) that need to be sent upstream.
    ///
    /// # Arguments
    /// * `sv2_frame` - The SV2 frame to send to the upstream server
    ///
    /// # Returns
    /// * `Ok(())` - Message sent successfully
    /// * `Err(TproxyError)` - Error sending the message
    pub async fn send_upstream(
        &self,
        message: Mining<'static>,
    ) -> TproxyResult<(), error::Upstream> {
        debug!("Sending message to upstream.");
        let message = AnyMessage::Mining(message);
        let sv2_frame: Sv2Frame = message.try_into().map_err(TproxyError::shutdown)?;

        self.upstream_channel_state
            .upstream_sender
            .send(sv2_frame)
            .await
            .map_err(|e| {
                error!("Failed to send message to upstream: {:?}", e);
                TproxyError::fallback(TproxyErrorKind::ChannelErrorSender)
            })?;

        Ok(())
    }

    /// Constructs the `SetupConnection` message.
    #[allow(clippy::result_large_err)]
    fn get_setup_connection_message(
        min_version: u16,
        max_version: u16,
        address: &SocketAddr,
        is_work_selection_enabled: bool,
    ) -> Result<SetupConnection<'static>, TproxyErrorKind> {
        let endpoint_host = address.ip().to_string().into_bytes().try_into()?;
        let vendor = "SRI".to_string().try_into()?;
        let hardware_version = "Translator Proxy".to_string().try_into()?;
        let firmware = String::new().try_into()?;
        let device_id = String::new().try_into()?;
        let flags = if is_work_selection_enabled {
            0b110
        } else {
            0b100
        };

        Ok(SetupConnection {
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
