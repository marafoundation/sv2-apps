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
pub mod http_server;
pub mod prometheus_metrics;
pub mod server;
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
pub use sv1::{Sv1ClientInfo, Sv1ClientsMonitoring, Sv1ClientsSummary};

use utoipa::ToSchema;

/// Common interface for extracting metrics from channel types.
///
/// This trait provides a unified way to access common channel data across
/// different channel types (server/client, extended/standard), enabling
/// generic metric collection without code duplication.
pub trait ChannelMetrics {
    fn channel_id(&self) -> u32;
    fn user_identity(&self) -> &str;
    fn nominal_hashrate(&self) -> f32;
    fn shares_accepted(&self) -> u32;
    fn shares_per_minute(&self) -> f32;
}

impl ChannelMetrics for ServerExtendedChannelInfo {
    fn channel_id(&self) -> u32 {
        self.channel_id
    }
    fn user_identity(&self) -> &str {
        &self.user_identity
    }
    fn nominal_hashrate(&self) -> f32 {
        self.nominal_hashrate
    }
    fn shares_accepted(&self) -> u32 {
        self.shares_accepted
    }
    fn shares_per_minute(&self) -> f32 {
        0.0 // Not tracked for server
    }
}

impl ChannelMetrics for ServerStandardChannelInfo {
    fn channel_id(&self) -> u32 {
        self.channel_id
    }
    fn user_identity(&self) -> &str {
        &self.user_identity
    }
    fn nominal_hashrate(&self) -> f32 {
        self.nominal_hashrate
    }
    fn shares_accepted(&self) -> u32 {
        self.shares_accepted
    }
    fn shares_per_minute(&self) -> f32 {
        0.0 // Not tracked for server
    }
}

impl ChannelMetrics for ExtendedChannelInfo {
    fn channel_id(&self) -> u32 {
        self.channel_id
    }
    fn user_identity(&self) -> &str {
        &self.user_identity
    }
    fn nominal_hashrate(&self) -> f32 {
        self.nominal_hashrate
    }
    fn shares_accepted(&self) -> u32 {
        self.shares_accepted
    }
    fn shares_per_minute(&self) -> f32 {
        self.shares_per_minute
    }
}

impl ChannelMetrics for StandardChannelInfo {
    fn channel_id(&self) -> u32 {
        self.channel_id
    }
    fn user_identity(&self) -> &str {
        &self.user_identity
    }
    fn nominal_hashrate(&self) -> f32 {
        self.nominal_hashrate
    }
    fn shares_accepted(&self) -> u32 {
        self.shares_accepted
    }
    fn shares_per_minute(&self) -> f32 {
        self.shares_per_minute
    }
}

/// Global statistics from `/api/v1/global` endpoint
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct GlobalInfo {
    pub server: ServerSummary,
    pub clients: ClientsSummary,
    pub uptime_secs: u64,
}
