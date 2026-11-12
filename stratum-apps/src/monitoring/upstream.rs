//! Upstream connection monitoring types
//!
//! These types describe the connection state of the endpoints an app can use as its upstream,
//! independently of the channels opened on it (see `server`). Apps that opt in also have their
//! health endpoint report whether an upstream is currently connected.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Connection state of one configured upstream endpoint
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct UpstreamEndpointInfo {
    /// Address as configured, in `host:port` form
    pub address: String,
    /// Whether the app is currently connected to this endpoint
    pub connected: bool,
}

/// Connection state of every configured upstream endpoint
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct UpstreamInfo {
    /// All configured upstream endpoints, connected or not
    pub upstreams: Vec<UpstreamEndpointInfo>,
    /// Number of fallbacks to another upstream since the process started
    pub fallbacks: u64,
}

/// Trait for monitoring the upstream connection state
pub trait UpstreamMonitoring: Send + Sync {
    /// Get the state of every configured upstream endpoint
    fn get_upstream_info(&self) -> UpstreamInfo;

    /// Whether an upstream is currently connected.
    ///
    /// Called by the health endpoint on every request, so it must be cheap and must not take
    /// locks shared with business logic.
    fn is_connected(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upstream_info_default_is_empty() {
        let info = UpstreamInfo::default();
        assert!(info.upstreams.is_empty());
        assert_eq!(info.fallbacks, 0);
    }

    #[test]
    fn upstream_info_serde_round_trip() {
        let info = UpstreamInfo {
            upstreams: vec![
                UpstreamEndpointInfo {
                    address: "pool.example.com:34254".into(),
                    connected: true,
                },
                UpstreamEndpointInfo {
                    address: "backup.example.com:34254".into(),
                    connected: false,
                },
            ],
            fallbacks: 2,
        };
        let json = serde_json::to_string(&info).unwrap();
        assert_eq!(serde_json::from_str::<UpstreamInfo>(&json).unwrap(), info);
    }
}
