//! Event-driven metrics for real-time Prometheus counter increments.
//!
//! This module provides metrics that are incremented at the point where events occur
//! (e.g., share acceptance, share rejection, block discovery) rather than being
//! sampled periodically from snapshots.
//!
//! ## Architecture
//!
//! - **EventMetrics** is passed to business logic components (e.g., ChannelManager)
//! - Events trigger immediate counter increments (e.g., `shares_accepted_total.inc()`)
//! - Prometheus scrapes these counters directly from the registry
//! - No snapshot cache needed for event metrics
//!
//! ## Metric Types
//!
//! - **Counters**: Monotonically increasing values (shares accepted, blocks found)
//! - **Histograms**: Distribution tracking (share validation latency) - future
//! - **Gauges**: Use snapshot-based collection in `SnapshotMetrics` (channel counts, hashrate)

use prometheus::{CounterVec, Opts, Registry};

/// Event-driven metrics that are incremented at the point where events occur.
///
/// These metrics are passed to business logic components and incremented
/// immediately when events happen, providing real-time data to Prometheus.
#[derive(Clone)]
pub struct EventMetrics {
    /// Total shares accepted per server channel (upstream)
    pub sv2_server_shares_accepted_total: Option<CounterVec>,

    /// Total shares accepted per client channel (downstream)
    pub sv2_client_shares_accepted_total: Option<CounterVec>,
}

impl EventMetrics {
    /// Create new EventMetrics and register them with the provided Prometheus registry.
    ///
    /// # Arguments
    ///
    /// * `registry` - Prometheus registry to register metrics with
    /// * `enable_server_metrics` - Whether to enable server (upstream) metrics
    /// * `enable_clients_metrics` - Whether to enable client (downstream) metrics
    pub fn new(
        registry: &Registry,
        enable_server_metrics: bool,
        enable_clients_metrics: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let sv2_server_shares_accepted_total = if enable_server_metrics {
            let counter = CounterVec::new(
                Opts::new(
                    "sv2_server_shares_accepted_total",
                    "Total shares accepted per server channel",
                ),
                &["channel_id", "user_identity"],
            )?;
            registry.register(Box::new(counter.clone()))?;
            Some(counter)
        } else {
            None
        };

        let sv2_client_shares_accepted_total = if enable_clients_metrics {
            let counter = CounterVec::new(
                Opts::new(
                    "sv2_client_shares_accepted_total",
                    "Total shares accepted per client channel",
                ),
                &["client_id", "channel_id", "user_identity"],
            )?;
            registry.register(Box::new(counter.clone()))?;
            Some(counter)
        } else {
            None
        };

        Ok(Self {
            sv2_server_shares_accepted_total,
            sv2_client_shares_accepted_total,
        })
    }

    /// Increment server shares accepted counter for a specific channel.
    ///
    /// This should be called immediately when a share is accepted from an upstream server.
    pub fn inc_server_shares_accepted(&self, channel_id: u32, user_identity: &str) {
        if let Some(ref counter) = self.sv2_server_shares_accepted_total {
            counter
                .with_label_values(&[&channel_id.to_string(), user_identity])
                .inc();
        }
    }

    /// Increment client shares accepted counter for a specific channel.
    ///
    /// This should be called immediately when a share is accepted from a downstream client.
    pub fn inc_client_shares_accepted(
        &self,
        client_id: usize,
        channel_id: u32,
        user_identity: &str,
    ) {
        if let Some(ref counter) = self.sv2_client_shares_accepted_total {
            counter
                .with_label_values(&[
                    &client_id.to_string(),
                    &channel_id.to_string(),
                    user_identity,
                ])
                .inc();
        }
    }
}
