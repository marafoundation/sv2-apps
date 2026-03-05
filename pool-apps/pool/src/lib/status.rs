//! Status reporting and error propagation Utility.
//!
//! This module provides mechanisms for communicating shutdown events and
//! component state changes across the system. Each component (downstream,
//! upstream, job declarator, template receiver, channel manager) can send
//! and receive status updates via typed channels. Errors are automatically
//! converted into shutdown signals, allowing coordinated teardown of tasks.

use stratum_apps::utils::types::DownstreamId;
use tracing::{debug, warn};

use crate::error::{Action, PoolError, PoolErrorKind};

/// Sender type for propagating status updates from different system components.
#[derive(Debug, Clone)]
pub enum StatusSender {
    /// Status updates from a specific downstream connection.
    Downstream {
        downstream_id: DownstreamId,
        tx: async_channel::Sender<Status>,
    },
    /// Status updates from the template receiver.
    TemplateReceiver(async_channel::Sender<Status>),
    /// Status updates from the channel manager.
    ChannelManager(async_channel::Sender<Status>),
}

/// High-level identifier of a component type that can send status updates.
#[derive(Debug, PartialEq, Eq)]
pub enum StatusType {
    /// A downstream connection identified by its ID.
    Downstream(DownstreamId),
    /// The template receiver component.
    TemplateReceiver,
    /// The channel manager component.
    ChannelManager,
}

impl From<&StatusSender> for StatusType {
    fn from(value: &StatusSender) -> Self {
        match value {
            StatusSender::ChannelManager(_) => StatusType::ChannelManager,
            StatusSender::Downstream {
                downstream_id,
                tx: _,
            } => StatusType::Downstream(*downstream_id),
            StatusSender::TemplateReceiver(_) => StatusType::TemplateReceiver,
        }
    }
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl StatusSender {
    /// Sends a status update for the associated component.
    pub async fn send(&self, status: Status) -> Result<(), async_channel::SendError<Status>> {
        match self {
            Self::Downstream { downstream_id, tx } => {
                debug!(
                    "Sending status from Downstream [{}]: {:?}",
                    downstream_id, status.state
                );
                tx.send(status).await
            }
            Self::TemplateReceiver(tx) => {
                debug!("Sending status from TemplateReceiver: {:?}", status.state);
                tx.send(status).await
            }
            Self::ChannelManager(tx) => {
                debug!("Sending status from ChannelManager: {:?}", status.state);
                tx.send(status).await
            }
        }
    }
}

/// Represents the state of a component, typically triggered by an error or shutdown event.
#[derive(Debug)]
pub enum State {
    /// A downstream connection has shut down with a reason.
    DownstreamShutdown {
        downstream_id: DownstreamId,
        reason: PoolErrorKind,
    },
    /// Template receiver has shut down with a reason.
    TemplateReceiverShutdown(PoolErrorKind),
    /// Template receiver requests fallback to next template provider.
    TemplateReceiverShutdownFallback(PoolErrorKind),
    /// Channel manager has shut down with a reason.
    ChannelManagerShutdown(PoolErrorKind),
}

/// Wrapper around a component’s state, sent as status updates across the system.
#[derive(Debug)]
pub struct Status {
    /// The current state being reported.
    pub state: State,
}

#[cfg_attr(not(test), hotpath::measure)]
async fn send_status<O>(sender: &StatusSender, error: PoolError<O>) -> bool {
    use Action::*;

    match error.action {
        Log => {
            warn!("Log-only error from {:?}: {:?}", sender, error.kind);
            false
        }

        Disconnect(downstream_id) => {
            let state = State::DownstreamShutdown {
                downstream_id,
                reason: error.kind,
            };

            if let Err(e) = sender.send(Status { state }).await {
                tracing::error!(
                    "Failed to send downstream shutdown status from {:?}: {:?}",
                    sender,
                    e
                );
                std::process::abort();
            }
            matches!(sender, StatusSender::Downstream { .. })
        }
        Fallback => {
            let state = State::TemplateReceiverShutdownFallback(error.kind);

            if let Err(e) = sender.send(Status { state }).await {
                tracing::error!("Failed to send fallback status from {:?}: {:?}", sender, e);
                std::process::abort();
            }
            matches!(sender, StatusSender::TemplateReceiver(_))
        }
        Shutdown => {
            let state = match sender {
                StatusSender::ChannelManager(_) => {
                    warn!(
                        "Channel Manager shutdown requested due to error: {:?}",
                        error.kind
                    );
                    State::ChannelManagerShutdown(error.kind)
                }
                StatusSender::TemplateReceiver(_) => {
                    warn!(
                        "Template Receiver shutdown requested due to error: {:?}",
                        error.kind
                    );
                    State::TemplateReceiverShutdown(error.kind)
                }
                _ => State::ChannelManagerShutdown(error.kind),
            };

            if let Err(e) = sender.send(Status { state }).await {
                tracing::error!("Failed to send shutdown status from {:?}: {:?}", sender, e);
                std::process::abort();
            }
            true
        }
    }
}

pub async fn handle_error<O>(sender: &StatusSender, e: PoolError<O>) -> bool {
    send_status(sender, e).await
}
