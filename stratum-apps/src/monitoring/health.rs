//! Health reporting for the upstream block-template source.
//!
//! Apps that depend on a bitcoin node / Template Provider to hand out work
//! (e.g. the Pool) implement [`HealthMonitoring`] so the `/api/v1/health`
//! endpoint can return a non-`200` status code when the node is unavailable —
//! including while the node is still performing its initial block download and
//! has not yet produced a block template.
//!
//! Apps that have no bitcoin node (e.g. the Translator Proxy) simply don't
//! provide a source, in which case `/health` keeps reporting `200 OK`.

/// Availability of the upstream block-template source (the bitcoin node /
/// Template Provider) that an app depends on to produce mining work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeHealth {
    /// `true` when the node is currently usable as a source of block templates.
    pub healthy: bool,
    /// Human-readable explanation, primarily useful when `healthy` is `false`.
    pub reason: String,
}

impl NodeHealth {
    /// The node is available and producing block templates.
    pub fn healthy(reason: impl Into<String>) -> Self {
        Self {
            healthy: true,
            reason: reason.into(),
        }
    }

    /// The node is unavailable for some reason (not connected, still syncing,
    /// or no longer producing templates).
    pub fn unavailable(reason: impl Into<String>) -> Self {
        Self {
            healthy: false,
            reason: reason.into(),
        }
    }
}

/// Implemented by apps that depend on a bitcoin node / Template Provider so the
/// monitoring server can report node availability through `/api/v1/health`.
pub trait HealthMonitoring {
    /// Report the current availability of the upstream bitcoin node.
    fn node_health(&self) -> NodeHealth;
}
