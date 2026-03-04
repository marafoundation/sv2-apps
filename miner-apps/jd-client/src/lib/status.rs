//! Status reporting and error propagation Utility.
//!
//! This module provides mechanisms for communicating shutdown events and
//! component state changes across the system. Each component (downstream,
//! upstream, job declarator, template receiver, channel manager) can send
//! and receive status updates via typed channels. Errors are automatically
//! converted into shutdown signals, allowing coordinated teardown of tasks.

use stratum_apps::utils::types::DownstreamId;
use tracing::{debug, warn};

use crate::error::{Action, JDCError, JDCErrorKind};

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
    /// Status updates from the upstream.
    Upstream(async_channel::Sender<Status>),
    /// Status updates from the job declarator.
    JobDeclarator(async_channel::Sender<Status>),
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
            Self::Upstream(tx) => {
                debug!("Sending status from Upstream: {:?}", status.state);
                tx.send(status).await
            }
            Self::JobDeclarator(tx) => {
                debug!("Sending status from JobDeclarator: {:?}", status.state);
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
        reason: JDCErrorKind,
    },
    /// Template receiver has shut down with a reason.
    TemplateReceiverShutdown(JDCErrorKind),
    /// Template receiver requests fallback to next template provider.
    TemplateReceiverShutdownFallback(JDCErrorKind),
    /// Job declarator has shut down during fallback with a reason.
    JobDeclaratorShutdownFallback(JDCErrorKind),
    /// Channel manager has shut down with a reason.
    ChannelManagerShutdown(JDCErrorKind),
    /// Upstream has shut down during fallback with a reason.
    UpstreamShutdownFallback(JDCErrorKind),
}

/// Wrapper around a component’s state, sent as status updates across the system.
#[derive(Debug)]
pub struct Status {
    /// The current state being reported.
    pub state: State,
}

#[cfg_attr(not(test), hotpath::measure)]
async fn send_status<O>(sender: &StatusSender, error: JDCError<O>) -> bool {
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
            let state = match sender {
                StatusSender::TemplateReceiver(_) => {
                    warn!(
                        "Template Receiver fallback requested due to error: {:?}",
                        error.kind
                    );
                    State::TemplateReceiverShutdownFallback(error.kind)
                }
                _ => State::UpstreamShutdownFallback(error.kind),
            };

            if let Err(e) = sender.send(Status { state }).await {
                tracing::error!("Failed to send fallback status from {:?}: {:?}", sender, e);
                std::process::abort();
            }
            matches!(
                sender,
                StatusSender::Upstream { .. } | StatusSender::TemplateReceiver(_)
            )
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
                _ => {
                    tracing::error!("Shutdown action received from invalid sender: {:?}", sender);
                    State::ChannelManagerShutdown(error.kind)
                }
            };

            if let Err(e) = sender.send(Status { state }).await {
                tracing::error!("Failed to send shutdown status from {:?}: {:?}", sender, e);
                std::process::abort();
            }
            true
        }
    }
}

#[cfg_attr(not(test), hotpath::measure)]
pub async fn handle_error<O>(sender: &StatusSender, e: JDCError<O>) -> bool {
    send_status(sender, e).await
}
