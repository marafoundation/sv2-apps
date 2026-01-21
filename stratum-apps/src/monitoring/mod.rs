//! Monitoring system for SV2 applications.
//!
//! Provides HTTP JSON API and Prometheus metrics for monitoring.
//! Read-only - does not modify any state.
//!
//! ## Architecture
//!
//! - **Server**: The upstream connection (pool, JDS) - typically one per app
//! - **Clients**: Downstream connections (miners) - multiple per app
//! - **SV1 clients**: Legacy SV1 connections (Translator only)

pub mod client;
pub mod event_metrics;
pub mod http_server;
pub mod server;
pub mod snapshot_cache;
pub mod snapshot_metrics;
pub mod sv1;

pub use client::{
    ClientInfo, ClientMetadata, ClientsMonitoring, ClientsSummary, ExtendedChannelInfo,
    StandardChannelInfo,
};
pub use http_server::MonitoringServer;
pub use server::{
    ServerExtendedChannelInfo, ServerInfo, ServerMonitoring, ServerStandardChannelInfo,
    ServerSummary,
};
pub use snapshot_cache::{CachedMonitoring, MonitoringSnapshot, SnapshotCache};
pub use sv1::{Sv1ClientInfo, Sv1ClientsMonitoring, Sv1ClientsSummary};

use utoipa::ToSchema;

/// Global statistics from `/api/v1/global` endpoint
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct GlobalInfo {
    pub server: ServerSummary,
    pub clients: ClientsSummary,
    pub uptime_secs: u64,
}
