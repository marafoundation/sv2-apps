//! Snapshot-based Prometheus metrics for SV2 monitoring.
//!
//! These metrics are populated from the snapshot cache during periodic refreshes
//! and Prometheus scrapes. They represent point-in-time state (gauges) rather than
//! real-time event counts (counters/histograms in EventMetrics).

use prometheus::{CounterVec, Gauge, GaugeVec, Opts, Registry};

/// Snapshot-based metrics populated from cached monitoring state.
///
/// These are primarily gauges that represent current state (hashrate, channel counts)
/// sampled periodically from business logic. Counters in this struct are legacy and
/// being migrated to EventMetrics for proper event-driven collection.
///
/// Metrics are optional - only registered when the corresponding monitoring type is enabled.
#[derive(Clone)]
pub struct SnapshotMetrics {
    pub registry: Registry,
    // System metrics
    pub sv2_uptime_seconds: Gauge,
    // Server metrics (upstream connection)
    pub sv2_server_channels_total: Option<Gauge>,
    pub sv2_server_channels_extended: Option<Gauge>,
    pub sv2_server_channels_standard: Option<Gauge>,
    pub sv2_server_hashrate_total: Option<Gauge>,
    pub sv2_server_channel_hashrate: Option<GaugeVec>,
    pub sv2_server_shares_accepted_total: Option<CounterVec>,
    // Clients metrics (downstream connections)
    pub sv2_clients_total: Option<Gauge>,
    pub sv2_client_channels_total: Option<Gauge>,
    pub sv2_client_channels_extended: Option<Gauge>,
    pub sv2_client_channels_standard: Option<Gauge>,
    pub sv2_client_hashrate_total: Option<Gauge>,
    pub sv2_client_channel_hashrate: Option<GaugeVec>,
    pub sv2_client_shares_accepted_total: Option<CounterVec>,
    pub sv2_client_channel_shares_per_minute: Option<GaugeVec>,
    // SV1 metrics
    pub sv1_clients_total: Option<Gauge>,
    pub sv1_hashrate_total: Option<Gauge>,
}

impl SnapshotMetrics {
    pub fn new(
        enable_server_metrics: bool,
        enable_clients_metrics: bool,
        enable_sv1_metrics: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let registry = Registry::new();

        // System metrics (always enabled)
        let sv2_uptime_seconds = Gauge::new("sv2_uptime_seconds", "Server uptime in seconds")?;
        registry.register(Box::new(sv2_uptime_seconds.clone()))?;

        // Server metrics (upstream connection)
        let (
            sv2_server_channels_total,
            sv2_server_channels_extended,
            sv2_server_channels_standard,
            sv2_server_hashrate_total,
            sv2_server_channel_hashrate,
            sv2_server_shares_accepted_total,
        ) = if enable_server_metrics {
            let total = Gauge::new(
                "sv2_server_channels_total",
                "Total number of channels opened with the server",
            )?;
            registry.register(Box::new(total.clone()))?;

            let extended = Gauge::new(
                "sv2_server_channels_extended",
                "Number of extended channels opened with the server",
            )?;
            registry.register(Box::new(extended.clone()))?;

            let standard = Gauge::new(
                "sv2_server_channels_standard",
                "Number of standard channels opened with the server",
            )?;
            registry.register(Box::new(standard.clone()))?;

            let hashrate = Gauge::new(
                "sv2_server_hashrate_total",
                "Total hashrate for channels opened with the server",
            )?;
            registry.register(Box::new(hashrate.clone()))?;

            let channel_hashrate = GaugeVec::new(
                Opts::new(
                    "sv2_server_channel_hashrate",
                    "Hashrate for individual server channels",
                ),
                &["channel_id", "user_identity"],
            )?;
            registry.register(Box::new(channel_hashrate.clone()))?;

            let shares_accepted = CounterVec::new(
                Opts::new(
                    "sv2_server_shares_accepted_total",
                    "Total shares accepted per server channel",
                ),
                &["channel_id", "user_identity"],
            )?;
            registry.register(Box::new(shares_accepted.clone()))?;

            (
                Some(total),
                Some(extended),
                Some(standard),
                Some(hashrate),
                Some(channel_hashrate),
                Some(shares_accepted),
            )
        } else {
            (None, None, None, None, None, None)
        };

        // Clients metrics (downstream connections)
        let (
            sv2_clients_total,
            sv2_client_channels_total,
            sv2_client_channels_extended,
            sv2_client_channels_standard,
            sv2_client_hashrate_total,
            sv2_client_channel_hashrate,
            sv2_client_shares_accepted_total,
            sv2_client_channel_shares_per_minute,
        ) = if enable_clients_metrics {
            let clients_total =
                Gauge::new("sv2_clients_total", "Total number of connected clients")?;
            registry.register(Box::new(clients_total.clone()))?;

            let total = Gauge::new(
                "sv2_client_channels_total",
                "Total number of channels opened with clients",
            )?;
            registry.register(Box::new(total.clone()))?;

            let extended = Gauge::new(
                "sv2_client_channels_extended",
                "Number of extended channels opened with clients",
            )?;
            registry.register(Box::new(extended.clone()))?;

            let standard = Gauge::new(
                "sv2_client_channels_standard",
                "Number of standard channels opened with clients",
            )?;
            registry.register(Box::new(standard.clone()))?;

            let hashrate = Gauge::new(
                "sv2_client_hashrate_total",
                "Total hashrate for channels opened with clients",
            )?;
            registry.register(Box::new(hashrate.clone()))?;

            let channel_hashrate = GaugeVec::new(
                Opts::new(
                    "sv2_client_channel_hashrate",
                    "Hashrate for individual client channels",
                ),
                &["client_id", "channel_id", "user_identity"],
            )?;
            registry.register(Box::new(channel_hashrate.clone()))?;

            let shares_accepted = CounterVec::new(
                Opts::new(
                    "sv2_client_shares_accepted_total",
                    "Total shares accepted per client channel",
                ),
                &["client_id", "channel_id", "user_identity"],
            )?;
            registry.register(Box::new(shares_accepted.clone()))?;

            let shares_per_minute = GaugeVec::new(
                Opts::new(
                    "sv2_client_channel_shares_per_minute",
                    "Shares per minute for client channels",
                ),
                &["client_id", "channel_id", "user_identity"],
            )?;
            registry.register(Box::new(shares_per_minute.clone()))?;

            (
                Some(clients_total),
                Some(total),
                Some(extended),
                Some(standard),
                Some(hashrate),
                Some(channel_hashrate),
                Some(shares_accepted),
                Some(shares_per_minute),
            )
        } else {
            (None, None, None, None, None, None, None, None)
        };

        // SV1 metrics
        let (sv1_clients_total, sv1_hashrate_total) = if enable_sv1_metrics {
            let clients = Gauge::new("sv1_clients_total", "Total number of SV1 clients")?;
            registry.register(Box::new(clients.clone()))?;

            let hashrate = Gauge::new("sv1_hashrate_total", "Total hashrate from SV1 clients")?;
            registry.register(Box::new(hashrate.clone()))?;

            (Some(clients), Some(hashrate))
        } else {
            (None, None)
        };

        Ok(Self {
            registry,
            sv2_uptime_seconds,
            sv2_server_channels_total,
            sv2_server_channels_extended,
            sv2_server_channels_standard,
            sv2_server_hashrate_total,
            sv2_server_channel_hashrate,
            sv2_server_shares_accepted_total,
            sv2_clients_total,
            sv2_client_channels_total,
            sv2_client_channels_extended,
            sv2_client_channels_standard,
            sv2_client_hashrate_total,
            sv2_client_channel_hashrate,
            sv2_client_shares_accepted_total,
            sv2_client_channel_shares_per_minute,
            sv1_clients_total,
            sv1_hashrate_total,
        })
    }
}
