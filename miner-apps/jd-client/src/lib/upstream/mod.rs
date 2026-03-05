//! Upstream module
//!
//! This module defines the [`Upstream`] struct, which manages communication
//! with an upstream SV2 server (e.g., pool).
//!
//! Responsibilities:
//! - Establish a TCP + Noise encrypted connection to upstream
//! - Perform `SetupConnection` handshake
//! - Negotiate extensions synchronously before returning
//! - Forward SV2 mining messages between upstream and channel manager
//! - Handle common messages from upstream

use std::{net::SocketAddr, sync::Arc, time::Duration};

use async_channel::{unbounded, Receiver, Sender};
use bitcoin_core_sv2::template_distribution_protocol::CancellationToken;
use stratum_apps::{
    custom_mutex::Mutex,
    fallback_coordinator::FallbackCoordinator,
    key_utils::Secp256k1PublicKey,
    network_helpers::{connect_with_noise, noise_stream::NoiseTcpStream, resolve_host},
    stratum_core::{
        binary_sv2::{self, Seq064K},
        codec_sv2::HandshakeRole,
        extensions_sv2::{
            RequestExtensions, RequestExtensionsError, RequestExtensionsSuccess,
            MESSAGE_TYPE_REQUEST_EXTENSIONS_ERROR, MESSAGE_TYPE_REQUEST_EXTENSIONS_SUCCESS,
        },
        framing_sv2,
        handlers_sv2::HandleCommonMessagesFromServerAsync,
        noise_sv2::Initiator,
        parsers_sv2::AnyMessage,
    },
    task_manager::TaskManager,
    utils::{
        protocol_message_type::{protocol_message_type, MessageType},
        types::{Message, Sv2Frame},
    },
};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use crate::{
    error::{self, JDCError, JDCErrorKind, JDCResult},
    io_task::spawn_io_tasks,
    status::{handle_error, Status, StatusSender},
    utils::{get_setup_connection_message, UpstreamEntry},
};

/// Timeout for extension negotiation response (30 seconds)
const EXTENSION_NEGOTIATION_TIMEOUT_SECS: u64 = 10;

mod message_handler;

/// Placeholder for future upstream-specific data/state.
pub struct UpstreamData;

/// Holds channels for communication between upstream and channel manager.
///
/// - `channel_manager_sender` → sends frames to channel manager
/// - `channel_manager_receiver` → receives frames from channel manager
/// - `outbound_tx` → sends frames outbound to upstream
/// - `inbound_rx` → receives frames inbound from upstream
#[derive(Clone)]
pub struct UpstreamChannel {
    channel_manager_sender: Sender<Sv2Frame>,
    channel_manager_receiver: Receiver<Sv2Frame>,
    upstream_sender: Sender<Sv2Frame>,
    upstream_receiver: Receiver<Sv2Frame>,
}

/// Represents an upstream connection (e.g., a pool).
#[derive(Clone)]
pub struct Upstream {
    #[allow(dead_code)]
    /// Internal state
    upstream_data: Arc<Mutex<UpstreamData>>,
    /// Messaging channels to/from the channel manager and Upstream.
    upstream_channel: UpstreamChannel,
    /// Protocol extensions that the JDC requires
    required_extensions: Vec<u16>,
    /// Upstream address
    address: SocketAddr,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl Upstream {
    /// Create a new [`Upstream`] connection to the given address.
    ///
    /// - Resolves hostname to IP address via DNS (if not already an IP)
    /// - Establishes TCP + Noise connection
    /// - Spawns IO tasks to handle inbound/outbound traffic
    pub async fn new(
        upstream_entry: &UpstreamEntry,
        channel_manager_sender: Sender<Sv2Frame>,
        channel_manager_receiver: Receiver<Sv2Frame>,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        task_manager: Arc<TaskManager>,
        required_extensions: Vec<u16>,
    ) -> JDCResult<Self, error::Upstream> {
        let addr = resolve_host(&upstream_entry.pool_host, upstream_entry.pool_port)
            .await
            .map_err(|e| {
                error!(
                    "Failed to resolve pool address {}:{}: {e}",
                    upstream_entry.pool_host, upstream_entry.pool_port
                );
                JDCError::fallback(JDCErrorKind::NetworkHelpersError(e.into()))
            })?;

        let stream = tokio::time::timeout(
            tokio::time::Duration::from_secs(5),
            TcpStream::connect(addr),
        )
        .await
        .map_err(JDCError::fallback)?
        .map_err(JDCError::fallback)?;
        info!("Connected to upstream at {}", addr);
        debug!("Begin with noise setup in upstream connection");

        let (noise_stream_reader, noise_stream_writer) = tokio::select! {
            result = connect_with_noise(stream, Some(upstream_entry.authority_pubkey)) => {
                match result {
                    Ok(noise_stream) => Ok(noise_stream.into_split()),
                    Err(e) => Err(JDCError::fallback(e))
                }
            }
            _ = cancellation_token.cancelled() => {
                info!("Shutdown received during handshake, dropping connection");
                Err(JDCError::shutdown(JDCErrorKind::CouldNotInitiateSystem))
            }
        }?;

        let (inbound_tx, inbound_rx) = unbounded::<Sv2Frame>();
        let (outbound_tx, outbound_rx) = unbounded::<Sv2Frame>();

        spawn_io_tasks(
            task_manager,
            noise_stream_reader,
            noise_stream_writer,
            outbound_rx,
            inbound_tx,
            cancellation_token.clone(),
            fallback_coordinator.clone(),
        );

        debug!("Noise setup done in upstream connection");
        let upstream_data = Arc::new(Mutex::new(UpstreamData));
        let upstream_channel = UpstreamChannel {
            channel_manager_receiver,
            channel_manager_sender,
            upstream_sender: outbound_tx,
            upstream_receiver: inbound_rx,
        };
        Ok(Upstream {
            upstream_data,
            upstream_channel,
            required_extensions,
            address: addr,
        })
    }

    /// Perform `SetupConnection` handshake with upstream.
    ///
    /// Sends [`SetupConnection`] and awaits response.
    /// If required extensions are configured, negotiates them synchronously
    /// before returning.
    ///
    /// # Returns
    /// * `Ok(Vec<u16>)` - The list of negotiated extensions (empty if none were requested)
    /// * `Err(JDCError)` - Error during handshake or extension negotiation
    pub async fn setup_connection(
        &mut self,
        min_version: u16,
        max_version: u16,
    ) -> JDCResult<Vec<u16>, error::Upstream> {
        info!("Upstream: initiating SV2 handshake...");
        let setup_connection =
            get_setup_connection_message(min_version, max_version, &self.address)
                .map_err(JDCError::shutdown)?;
        debug!(?setup_connection, "Prepared `SetupConnection` message");
        let sv2_frame: Sv2Frame = Message::Common(setup_connection.into())
            .try_into()
            .map_err(JDCError::shutdown)?;
        debug!(?sv2_frame, "Encoded `SetupConnection` frame");

        // Send SetupConnection
        if let Err(e) = self.upstream_channel.upstream_sender.send(sv2_frame).await {
            error!(?e, "Failed to send `SetupConnection` frame to upstream");
            return Err(JDCError::fallback(JDCErrorKind::ChannelErrorSender));
        }
        info!("Sent `SetupConnection` to upstream, awaiting response...");

        let incoming_frame = match self.upstream_channel.upstream_receiver.recv().await {
            Ok(frame) => {
                debug!(?frame, "Received raw inbound frame during handshake");
                frame
            }
            Err(e) => {
                error!(?e, "Upstream closed connection during handshake");
                return Err(JDCError::fallback(e));
            }
        };

        let mut incoming: Sv2Frame = incoming_frame;
        debug!(?incoming, "Decoded inbound handshake frame");

        let header = incoming.get_header().ok_or_else(|| {
            error!("Handshake frame missing header");
            JDCError::fallback(framing_sv2::Error::MissingHeader)
        })?;

        info!(ext_type = ?header.ext_type(), msg_type = ?header.msg_type(), "Dispatching inbound handshake message");
        self.handle_common_message_frame_from_server(None, header, incoming.payload())
            .await?;

        // Send RequestExtensions after successful SetupConnection if there are required extensions
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
    ///
    /// # Returns
    /// * `Ok(Vec<u16>)` - The list of successfully negotiated extensions
    /// * `Err(JDCError)` - Extension negotiation failed
    async fn negotiate_extensions(&mut self) -> JDCResult<Vec<u16>, error::Upstream> {
        let requested_extensions =
            Seq064K::new(self.required_extensions.clone()).map_err(JDCError::shutdown)?;

        let request_extensions = RequestExtensions {
            request_id: 0,
            requested_extensions,
        };

        info!(
            "Sending RequestExtensions to upstream with required extensions: {:?}",
            self.required_extensions
        );

        let sv2_frame: Sv2Frame = AnyMessage::Extensions(request_extensions.into_static().into())
            .try_into()
            .map_err(JDCError::shutdown)?;

        self.upstream_channel
            .upstream_sender
            .send(sv2_frame)
            .await
            .map_err(|e| {
                error!(?e, "Failed to send RequestExtensions to upstream");
                JDCError::fallback(JDCErrorKind::ChannelErrorSender)
            })?;

        // Wait for extension negotiation response with timeout
        let response = tokio::time::timeout(
            Duration::from_secs(EXTENSION_NEGOTIATION_TIMEOUT_SECS),
            self.upstream_channel.upstream_receiver.recv(),
        )
        .await
        .map_err(|_| {
            error!(
                "Extension negotiation timed out after {} seconds",
                EXTENSION_NEGOTIATION_TIMEOUT_SECS
            );
            JDCError::fallback(JDCErrorKind::ExtensionNegotiationTimeout)
        })?
        .map_err(|e| {
            error!("Failed to receive extension negotiation response: {}", e);
            JDCError::fallback(e)
        })?;

        self.handle_extension_response(response).await
    }

    /// Handles the extension negotiation response (Success or Error).
    async fn handle_extension_response(
        &mut self,
        mut response: Sv2Frame,
    ) -> JDCResult<Vec<u16>, error::Upstream> {
        let header = response.get_header().ok_or_else(|| {
            error!("Extension response frame missing header");
            JDCError::fallback(JDCErrorKind::UnexpectedMessage(0, 0))
        })?;

        let msg_type = header.msg_type();
        let payload = response.payload();

        match msg_type {
            MESSAGE_TYPE_REQUEST_EXTENSIONS_SUCCESS => {
                let msg: RequestExtensionsSuccess =
                    binary_sv2::from_bytes(payload).map_err(|e| {
                        error!("Failed to parse RequestExtensionsSuccess: {:?}", e);
                        JDCError::fallback(JDCErrorKind::BinarySv2(e))
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
                    return Err(JDCError::fallback(
                        JDCErrorKind::RequiredExtensionsNotSupported(missing_required),
                    ));
                }

                info!("Successfully negotiated extensions: {:?}", supported);
                Ok(supported)
            }
            MESSAGE_TYPE_REQUEST_EXTENSIONS_ERROR => {
                let msg: RequestExtensionsError = binary_sv2::from_bytes(payload).map_err(|e| {
                    error!("Failed to parse RequestExtensionsError: {:?}", e);
                    JDCError::fallback(JDCErrorKind::BinarySv2(e))
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
                    return Err(JDCError::fallback(
                        JDCErrorKind::RequiredExtensionsNotSupported(missing_required),
                    ));
                }

                // If server requires extensions we don't support, fail
                if !required_by_server.is_empty() {
                    error!(
                        "Server requires extensions that we don't support: {:?}",
                        required_by_server
                    );
                    return Err(JDCError::fallback(
                        JDCErrorKind::ServerRequiresUnsupportedExtensions(required_by_server),
                    ));
                }

                // No required extensions failed, return empty
                Ok(vec![])
            }
            _ => {
                error!(
                    "Unexpected message type during extension negotiation: {}",
                    msg_type
                );
                Err(JDCError::fallback(JDCErrorKind::UnexpectedMessage(
                    header.ext_type(),
                    msg_type,
                )))
            }
        }
    }

    /// Start unified upstream loop.
    ///
    /// Responsibilities:
    /// - Run `setup_connection` (including extension negotiation)
    /// - Handle messages from upstream (pool) and channel manager
    /// - React to shutdown signals
    ///
    /// This function spawns an async task and returns the negotiated extensions.
    ///
    /// # Returns
    /// * `Vec<u16>` - The list of negotiated extensions (empty if none were requested or setup
    ///   failed)
    #[allow(clippy::too_many_arguments)]
    pub async fn start(
        mut self,
        min_version: u16,
        max_version: u16,
        cancellation_token: CancellationToken,
        fallback_coordinator: FallbackCoordinator,
        status_sender: Sender<Status>,
        task_manager: Arc<TaskManager>,
    ) -> Vec<u16> {
        let status_sender = StatusSender::Upstream(status_sender);

        let negotiated_extensions = match self.setup_connection(min_version, max_version).await {
            Ok(extensions) => {
                info!(
                    "Upstream: extension negotiation complete. Extensions: {:?}",
                    extensions
                );
                extensions
            }
            Err(e) => {
                error!(error = ?e, "Upstream: connection setup failed.");
                return vec![];
            }
        };

        task_manager.spawn(async move {
            // we just spawned a new task that's relevant to fallback coordination
            // so register it with the fallback coordinator
            let fallback_handler = fallback_coordinator.register();

            // get the cancellation token that signals fallback
            let fallback_token = fallback_coordinator.token();

            let mut self_clone_1 = self.clone();
            let mut self_clone_2 = self.clone();
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        info!("Upstream: received shutdown signal");
                        break;
                    }
                    _ = fallback_token.cancelled() => {
                        info!("Upstream: fallback triggered");
                        break;
                    }
                    res = self_clone_1.handle_pool_message_frame() => {
                        if let Err(e) = res {
                            error!(error = ?e, "Upstream: error handling pool message.");
                            if handle_error(&status_sender, e).await {
                                break;
                            }
                        }
                    }
                    res = self_clone_2.handle_channel_manager_message_frame() => {
                        if let Err(e) = res {
                            error!(error = ?e, "Upstream: error handling channel manager message.");
                            if handle_error(&status_sender, e).await {
                                break;
                            }
                        }
                    }

                }
            }
            warn!("Upstream: unified message loop exited.");

            // signal fallback coordinator that this task has completed its cleanup
            fallback_handler.done();
        });

        negotiated_extensions
    }

    // Handle incoming frames from upstream (pool).
    //
    // Routes:
    // - `Common` messages → handled locally
    // - `Mining` messages → forwarded to channel manager
    // - Unsupported → error
    async fn handle_pool_message_frame(&mut self) -> JDCResult<(), error::Upstream> {
        debug!("Received SV2 frame from upstream.");
        let mut sv2_frame = self
            .upstream_channel
            .upstream_receiver
            .recv()
            .await
            .map_err(JDCError::fallback)?;
        let header = sv2_frame.get_header().ok_or_else(|| {
            error!("SV2 frame missing header");
            JDCError::fallback(framing_sv2::Error::MissingHeader)
        })?;
        let message_type = header.msg_type();
        let extension_type = header.ext_type();

        match protocol_message_type(extension_type, message_type) {
            MessageType::Common => {
                info!(ext_type = ?extension_type, msg_type = ?message_type, "Handling common message from Upstream.");
                self.handle_common_message_frame_from_server(None, header, sv2_frame.payload())
                    .await?;
            }
            MessageType::Mining | MessageType::Extensions => {
                self.upstream_channel
                    .channel_manager_sender
                    .send(sv2_frame)
                    .await
                    .map_err(|e| {
                        error!(error=?e, "Failed to send mining message to channel manager.");
                        JDCError::shutdown(JDCErrorKind::ChannelErrorSender)
                    })?;
            }
            _ => {
                warn!("Received unsupported message type from upstream: {message_type}");
            }
        }
        Ok(())
    }

    // Handle outbound frames from channel manager → upstream.
    //
    // Forwards messages upstream.
    async fn handle_channel_manager_message_frame(&mut self) -> JDCResult<(), error::Upstream> {
        match self.upstream_channel.channel_manager_receiver.recv().await {
            Ok(sv2_frame) => {
                debug!("Received sv2 frame from channel manager, forwarding upstream.");
                self.upstream_channel
                    .upstream_sender
                    .send(sv2_frame)
                    .await
                    .map_err(|e| {
                        error!(error=?e, "Failed to send sv2 frame to upstream.");
                        JDCError::fallback(JDCErrorKind::ChannelErrorSender)
                    })?;
            }
            Err(e) => {
                warn!(error=?e, "Channel manager receiver closed or errored.");
            }
        }
        Ok(())
    }
}
