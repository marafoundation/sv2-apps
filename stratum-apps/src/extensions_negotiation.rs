//! Extension negotiation utilities shared by JDC and Translator
//!
//! This module provides the shared logic for negotiating SV2 extensions with
//! an upstream server, handling the RequestExtensions/Response flow.

use async_channel::{Receiver, Sender};
use std::time::Duration;
use tracing::{error, info};

use stratum_core::{
    binary_sv2::Seq064K, codec_sv2::StandardSv2Frame, extensions_sv2::RequestExtensions,
    handlers_sv2::HandleExtensionsFromServerAsync, parsers_sv2::AnyMessage,
};

use crate::utils::types::Message;

const EXTENSION_NEGOTIATION_TIMEOUT_SECS: u64 = 30;

#[derive(Debug)]
pub enum ExtensionNegotiationError {
    SendError,
    ReceiveError(async_channel::RecvError),
    Timeout,
    UnexpectedMessage(u16, u16),
    HandlerError(String),
}

impl std::fmt::Display for ExtensionNegotiationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtensionNegotiationError::SendError => write!(f, "Failed to send RequestExtensions"),
            ExtensionNegotiationError::ReceiveError(e) => {
                write!(f, "Failed to receive extension response: {}", e)
            }
            ExtensionNegotiationError::Timeout => write!(f, "Extension negotiation timed out"),
            ExtensionNegotiationError::UnexpectedMessage(ext, msg) => {
                write!(f, "Unexpected message: ext={}, msg={}", ext, msg)
            }
            ExtensionNegotiationError::HandlerError(s) => {
                write!(f, "Handler error: {}", s)
            }
        }
    }
}

impl std::error::Error for ExtensionNegotiationError {}

pub async fn negotiate_extensions<CM, E>(
    required_extensions: Vec<u16>,
    upstream_sender: Sender<StandardSv2Frame<Message>>,
    upstream_receiver: Receiver<StandardSv2Frame<Message>>,
    channel_manager_receiver: Receiver<StandardSv2Frame<Message>>,
    channel_manager: &mut CM,
) -> Result<Vec<u16>, ExtensionNegotiationError>
where
    CM: HandleExtensionsFromServerAsync<Error = E> + Send,
    E: std::fmt::Debug,
{
    let requested_extensions = Seq064K::new(required_extensions.clone()).map_err(|e| {
        ExtensionNegotiationError::HandlerError(format!("Failed to create Seq064K: {:?}", e))
    })?;

    let request_extensions = RequestExtensions {
        request_id: 0,
        requested_extensions,
    };

    let sv2_frame: StandardSv2Frame<Message> =
        AnyMessage::Extensions(request_extensions.into_static().into())
            .try_into()
            .map_err(|e| {
                ExtensionNegotiationError::HandlerError(format!("Failed to frame: {:?}", e))
            })?;

    info!(
        "Sending RequestExtensions to upstream with required extensions: {:?}",
        required_extensions
    );

    upstream_sender
        .send(sv2_frame)
        .await
        .map_err(|_| ExtensionNegotiationError::SendError)?;

    loop {
        let mut response = tokio::time::timeout(
            Duration::from_secs(EXTENSION_NEGOTIATION_TIMEOUT_SECS),
            upstream_receiver.recv(),
        )
        .await
        .map_err(|_| {
            error!(
                "Extension negotiation timed out after {} seconds",
                EXTENSION_NEGOTIATION_TIMEOUT_SECS
            );
            ExtensionNegotiationError::Timeout
        })?
        .map_err(ExtensionNegotiationError::ReceiveError)?;

        let header = response.get_header().ok_or_else(|| {
            error!("Extension response frame missing header");
            ExtensionNegotiationError::UnexpectedMessage(0, 0)
        })?;

        channel_manager
            .handle_extensions_message_frame_from_server(None, header, response.payload())
            .await
            .map_err(|e| ExtensionNegotiationError::HandlerError(format!("{:?}", e)))?;

        if let Ok(retry_frame) = channel_manager_receiver.try_recv() {
            info!("Forwarding retry RequestExtensions to upstream pool...");
            upstream_sender
                .send(retry_frame)
                .await
                .map_err(|_| ExtensionNegotiationError::SendError)?;
            continue;
        }

        return channel_manager
            .get_negotiated_extensions_with_server(None)
            .map_err(|e| ExtensionNegotiationError::HandlerError(format!("{:?}", e)));
    }
}
