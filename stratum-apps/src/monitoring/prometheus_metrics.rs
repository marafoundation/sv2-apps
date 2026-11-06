//! Prometheus metrics definitions for SV2 monitoring

use prometheus::{Gauge, GaugeVec, Opts, Registry};

use super::sv1::{Sv1ShareDropReason, Sv1ShareRejection};

/// Prometheus metrics for the monitoring server.
/// Metrics are optional - only registered when the corresponding monitoring type is enabled.
#[derive(Clone)]
pub struct PrometheusMetrics {
    pub registry: Registry,
    // System metrics
    pub sv2_uptime_seconds: Gauge,
    pub sv2_monitoring_snapshot_age_seconds: Gauge,
    /// Allocator heap usage; present only where the allocator reports it (glibc).
    pub sv2_heap_in_use_bytes: Option<Gauge>,
    pub sv2_heap_free_bytes: Option<Gauge>,
    pub sv2_heap_mapped_bytes: Option<Gauge>,
    // Server metrics (upstream connection)
    pub sv2_server_channels: Option<GaugeVec>,
    pub sv2_server_hashrate_total: Option<Gauge>,
    pub sv2_server_channel_hashrate: Option<GaugeVec>,
    pub sv2_server_shares_accepted_total: Option<GaugeVec>,
    pub sv2_server_shares_rejected_total: Option<GaugeVec>,
    pub sv2_server_blocks_found_total: Option<Gauge>,
    pub sv2_coinbase_output_info: Option<GaugeVec>,
    // Clients metrics (downstream connections)
    pub sv2_clients_total: Option<Gauge>,
    pub sv2_client_channels: Option<GaugeVec>,
    pub sv2_client_hashrate_total: Option<Gauge>,
    pub sv2_client_channel_hashrate: Option<GaugeVec>,
    pub sv2_client_shares_accepted_total: Option<GaugeVec>,
    pub sv2_client_shares_rejected_total: Option<GaugeVec>,
    pub sv2_client_blocks_found_total: Option<Gauge>,
    // SV1 metrics
    pub sv1_clients_total: Option<Gauge>,
    pub sv1_hashrate_total: Option<Gauge>,
    pub sv1_shares_accepted_total: Option<Gauge>,
    pub sv1_shares_rejected_total: Option<GaugeVec>,
    pub sv1_shares_below_upstream_target_total: Option<Gauge>,
    pub sv1_shares_dropped_total: Option<GaugeVec>,
    /// One series per recent block-finding share, valued at the Unix time it was submitted.
    pub sv1_block_found_info: Option<GaugeVec>,
    pub sv1_blocks_found_total: Option<Gauge>,
    // Upstream metrics
    pub sv2_upstream_connected: Option<GaugeVec>,
    pub sv2_upstream_fallbacks_total: Option<Gauge>,
}

impl PrometheusMetrics {
    pub fn new(
        enable_server_metrics: bool,
        enable_clients_metrics: bool,
        enable_sv1_metrics: bool,
        enable_upstream_metrics: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let registry = Registry::new();

        // System metrics (always enabled)
        let sv2_uptime_seconds = Gauge::new("sv2_uptime_seconds", "Server uptime in seconds")?;
        registry.register(Box::new(sv2_uptime_seconds.clone()))?;
        // Set at scrape time: every other metric keeps its last value if refreshes stop.
        let sv2_monitoring_snapshot_age_seconds = Gauge::new(
            "sv2_monitoring_snapshot_age_seconds",
            "Seconds since the monitoring snapshot was last refreshed",
        )?;
        registry.register(Box::new(sv2_monitoring_snapshot_age_seconds.clone()))?;
        let (sv2_heap_in_use_bytes, sv2_heap_free_bytes, sv2_heap_mapped_bytes) =
            if super::heap::heap_stats().is_some() {
                let in_use = Gauge::new(
                    "sv2_heap_in_use_bytes",
                    "Heap bytes allocated and not yet freed",
                )?;
                registry.register(Box::new(in_use.clone()))?;
                let free = Gauge::new(
                    "sv2_heap_free_bytes",
                    "Heap bytes freed but still held by the allocator",
                )?;
                registry.register(Box::new(free.clone()))?;
                let mapped = Gauge::new(
                    "sv2_heap_mapped_bytes",
                    "Heap bytes obtained from the OS, in use or free",
                )?;
                registry.register(Box::new(mapped.clone()))?;
                (Some(in_use), Some(free), Some(mapped))
            } else {
                (None, None, None)
            };

        // Server metrics (upstream connection)
        let (
            sv2_server_channels,
            sv2_server_hashrate_total,
            sv2_server_channel_hashrate,
            sv2_server_shares_accepted_total,
            sv2_server_shares_rejected_total,
            sv2_server_blocks_found_total,
            sv2_coinbase_output_info,
        ) = if enable_server_metrics {
            let channels = GaugeVec::new(
                Opts::new("sv2_server_channels", "Number of server channels by type"),
                &["channel_type"],
            )?;
            registry.register(Box::new(channels.clone()))?;

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

            let shares_accepted = GaugeVec::new(
                Opts::new(
                    "sv2_server_shares_accepted_total",
                    "Total shares accepted per server channel",
                ),
                &["channel_id", "user_identity"],
            )?;
            registry.register(Box::new(shares_accepted.clone()))?;

            let shares_rejected = GaugeVec::new(
                Opts::new(
                    "sv2_server_shares_rejected_total",
                    "Total shares rejected per server channel by error code",
                ),
                &["channel_id", "user_identity", "error_code"],
            )?;
            registry.register(Box::new(shares_rejected.clone()))?;

            let blocks_found = Gauge::new(
                "sv2_server_blocks_found_total",
                "Total blocks found across all current server channels",
            )?;
            registry.register(Box::new(blocks_found.clone()))?;

            let coinbase_output_info = GaugeVec::new(
                Opts::new(
                    "sv2_coinbase_output_info",
                    "Scripts paid by the coinbase outputs the app builds its jobs from (always 1)",
                ),
                &["script_hex", "address"],
            )?;
            registry.register(Box::new(coinbase_output_info.clone()))?;

            (
                Some(channels),
                Some(hashrate),
                Some(channel_hashrate),
                Some(shares_accepted),
                Some(shares_rejected),
                Some(blocks_found),
                Some(coinbase_output_info),
            )
        } else {
            (None, None, None, None, None, None, None)
        };

        // Clients metrics (downstream connections)
        let (
            sv2_clients_total,
            sv2_client_channels,
            sv2_client_hashrate_total,
            sv2_client_channel_hashrate,
            sv2_client_shares_accepted_total,
            sv2_client_shares_rejected_total,
            sv2_client_blocks_found_total,
        ) = if enable_clients_metrics {
            let clients_total =
                Gauge::new("sv2_clients_total", "Total number of connected clients")?;
            registry.register(Box::new(clients_total.clone()))?;

            let channels = GaugeVec::new(
                Opts::new("sv2_client_channels", "Number of client channels by type"),
                &["channel_type"],
            )?;
            registry.register(Box::new(channels.clone()))?;

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

            let shares_accepted = GaugeVec::new(
                Opts::new(
                    "sv2_client_shares_accepted_total",
                    "Total shares accepted per client channel",
                ),
                &["client_id", "channel_id", "user_identity"],
            )?;
            registry.register(Box::new(shares_accepted.clone()))?;

            let shares_rejected = GaugeVec::new(
                Opts::new(
                    "sv2_client_shares_rejected_total",
                    "Total shares rejected per client channel by error code",
                ),
                &["client_id", "channel_id", "user_identity", "error_code"],
            )?;
            registry.register(Box::new(shares_rejected.clone()))?;

            let blocks_found = Gauge::new(
                "sv2_client_blocks_found_total",
                "Total blocks found across all current client channels",
            )?;
            registry.register(Box::new(blocks_found.clone()))?;

            (
                Some(clients_total),
                Some(channels),
                Some(hashrate),
                Some(channel_hashrate),
                Some(shares_accepted),
                Some(shares_rejected),
                Some(blocks_found),
            )
        } else {
            (None, None, None, None, None, None, None)
        };

        // SV1 metrics
        let (
            sv1_clients_total,
            sv1_hashrate_total,
            sv1_shares_accepted_total,
            sv1_shares_rejected_total,
            sv1_shares_below_upstream_target_total,
            sv1_shares_dropped_total,
            sv1_block_found_info,
            sv1_blocks_found_total,
        ) = if enable_sv1_metrics {
            let clients = Gauge::new("sv1_clients_total", "Total number of SV1 clients")?;
            registry.register(Box::new(clients.clone()))?;

            let hashrate = Gauge::new("sv1_hashrate_total", "Total hashrate from SV1 clients")?;
            registry.register(Box::new(hashrate.clone()))?;

            let accepted = Gauge::new(
                "sv1_shares_accepted_total",
                "SV1 shares accepted by the SV1 server since the process started",
            )?;
            registry.register(Box::new(accepted.clone()))?;

            let rejected = GaugeVec::new(
                Opts::new(
                    "sv1_shares_rejected_total",
                    "SV1 shares rejected by the SV1 server since the process started, by reason",
                ),
                &["reason"],
            )?;
            registry.register(Box::new(rejected.clone()))?;
            // Every reason is exported from the start, so a rate over it never starts mid-way.
            for rejection in Sv1ShareRejection::ALL {
                rejected.with_label_values(&[rejection.label()]).set(0.0);
            }

            let block_found = GaugeVec::new(
                Opts::new(
                    "sv1_block_found_info",
                    "Recent SV1 shares that met the network target (block candidates, before the \
                     pool confirms them), valued at the Unix time they were submitted",
                ),
                &[
                    "sv1_username",
                    "worker",
                    "user_identity",
                    "share_hash",
                    "height",
                ],
            )?;
            registry.register(Box::new(block_found.clone()))?;

            let blocks_found = Gauge::new(
                "sv1_blocks_found_total",
                "SV1 shares that met the network target since the process started (block \
                 candidates, before the pool confirms them)",
            )?;
            registry.register(Box::new(blocks_found.clone()))?;

            let below_target = Gauge::new(
                "sv1_shares_below_upstream_target_total",
                "Accepted SV1 shares not sent upstream because they do not meet the upstream \
                 channel's target, since the process started",
            )?;
            registry.register(Box::new(below_target.clone()))?;

            let dropped = GaugeVec::new(
                Opts::new(
                    "sv1_shares_dropped_total",
                    "Accepted SV1 shares not sent upstream for any other reason, since the process \
                     started, by reason",
                ),
                &["reason"],
            )?;
            registry.register(Box::new(dropped.clone()))?;
            for reason in Sv1ShareDropReason::ALL {
                dropped.with_label_values(&[reason.label()]).set(0.0);
            }

            (
                Some(clients),
                Some(hashrate),
                Some(accepted),
                Some(rejected),
                Some(below_target),
                Some(dropped),
                Some(block_found),
                Some(blocks_found),
            )
        } else {
            (None, None, None, None, None, None, None, None)
        };

        // Upstream metrics
        let (sv2_upstream_connected, sv2_upstream_fallbacks_total) = if enable_upstream_metrics {
            let connected = GaugeVec::new(
                Opts::new(
                    "sv2_upstream_connected",
                    "Whether the app is connected to each configured upstream (1) or not (0)",
                ),
                &["pool_address"],
            )?;
            registry.register(Box::new(connected.clone()))?;

            let fallbacks = Gauge::new(
                "sv2_upstream_fallbacks_total",
                "Number of fallbacks to another upstream since the process started",
            )?;
            registry.register(Box::new(fallbacks.clone()))?;

            (Some(connected), Some(fallbacks))
        } else {
            (None, None)
        };

        Ok(Self {
            registry,
            sv2_uptime_seconds,
            sv2_monitoring_snapshot_age_seconds,
            sv2_heap_in_use_bytes,
            sv2_heap_free_bytes,
            sv2_heap_mapped_bytes,
            sv2_server_channels,
            sv2_server_hashrate_total,
            sv2_server_channel_hashrate,
            sv2_server_shares_accepted_total,
            sv2_server_shares_rejected_total,
            sv2_server_blocks_found_total,
            sv2_coinbase_output_info,
            sv2_clients_total,
            sv2_client_channels,
            sv2_client_hashrate_total,
            sv2_client_channel_hashrate,
            sv2_client_shares_accepted_total,
            sv2_client_shares_rejected_total,
            sv2_client_blocks_found_total,
            sv1_clients_total,
            sv1_hashrate_total,
            sv1_shares_accepted_total,
            sv1_shares_rejected_total,
            sv1_shares_below_upstream_target_total,
            sv1_shares_dropped_total,
            sv1_block_found_info,
            sv1_blocks_found_total,
            sv2_upstream_connected,
            sv2_upstream_fallbacks_total,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::Encoder;

    #[test]
    fn uptime_metric_always_registered() {
        let m = PrometheusMetrics::new(false, false, false, false).unwrap();
        m.sv2_uptime_seconds.set(42.0);

        let families = m.registry.gather();
        let names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();
        assert!(names.contains(&"sv2_uptime_seconds"));
    }

    #[test]
    fn server_metrics_enabled() {
        let m = PrometheusMetrics::new(true, false, false, false).unwrap();
        assert!(m.sv2_server_channels.is_some());
        assert!(m.sv2_server_hashrate_total.is_some());
        assert!(m.sv2_server_channel_hashrate.is_some());
        assert!(m.sv2_server_shares_accepted_total.is_some());
        assert!(m.sv2_server_shares_rejected_total.is_some());
        assert!(m.sv2_coinbase_output_info.is_some());
        // clients and sv1 should be None
        assert!(m.sv2_clients_total.is_none());
        assert!(m.sv1_clients_total.is_none());
    }

    #[test]
    fn clients_metrics_enabled() {
        let m = PrometheusMetrics::new(false, true, false, false).unwrap();
        assert!(m.sv2_clients_total.is_some());
        assert!(m.sv2_client_channels.is_some());
        assert!(m.sv2_client_hashrate_total.is_some());
        assert!(m.sv2_client_channel_hashrate.is_some());
        assert!(m.sv2_client_shares_accepted_total.is_some());
        assert!(m.sv2_client_shares_rejected_total.is_some());
        // server and sv1 should be None
        assert!(m.sv2_server_channels.is_none());
        assert!(m.sv1_clients_total.is_none());
    }

    #[test]
    fn sv1_metrics_enabled() {
        let m = PrometheusMetrics::new(false, false, true, false).unwrap();
        assert!(m.sv1_clients_total.is_some());
        assert!(m.sv1_hashrate_total.is_some());
        // server and clients should be None
        assert!(m.sv2_server_channels.is_none());
        assert!(m.sv2_clients_total.is_none());
    }

    #[test]
    fn upstream_metrics_enabled() {
        let m = PrometheusMetrics::new(false, false, false, true).unwrap();
        assert!(m.sv2_upstream_connected.is_some());
        assert!(m.sv2_upstream_fallbacks_total.is_some());
        // server, clients and sv1 should be None
        assert!(m.sv2_server_channels.is_none());
        assert!(m.sv2_clients_total.is_none());
        assert!(m.sv1_clients_total.is_none());
    }

    #[test]
    fn all_metrics_disabled_only_system_metrics() {
        let m = PrometheusMetrics::new(false, false, false, false).unwrap();
        assert!(m.sv2_server_channels.is_none());
        assert!(m.sv2_clients_total.is_none());
        assert!(m.sv1_clients_total.is_none());
        assert!(m.sv2_upstream_connected.is_none());
        assert!(m.sv2_upstream_fallbacks_total.is_none());

        let families = m.registry.gather();
        // Only the system metrics should be registered (with default value 0)
        let names: Vec<&str> = families.iter().map(|f| f.get_name()).collect();
        let mut expected = vec!["sv2_monitoring_snapshot_age_seconds", "sv2_uptime_seconds"];
        if super::super::heap::heap_stats().is_some() {
            expected.extend([
                "sv2_heap_free_bytes",
                "sv2_heap_in_use_bytes",
                "sv2_heap_mapped_bytes",
            ]);
        }
        expected.sort_unstable();
        assert_eq!(names, expected);
    }

    #[test]
    fn metrics_encode_to_prometheus_text_format() {
        let m = PrometheusMetrics::new(true, true, true, true).unwrap();
        m.sv2_uptime_seconds.set(123.0);
        m.sv2_server_hashrate_total.as_ref().unwrap().set(1000.0);
        m.sv2_clients_total.as_ref().unwrap().set(5.0);
        m.sv1_clients_total.as_ref().unwrap().set(3.0);

        let encoder = prometheus::TextEncoder::new();
        let families = m.registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).unwrap();
        let output = String::from_utf8(buf).unwrap();

        assert!(output.contains("sv2_uptime_seconds 123"));
        assert!(output.contains("sv2_server_hashrate_total 1000"));
        assert!(output.contains("sv2_clients_total 5"));
        assert!(output.contains("sv1_clients_total 3"));
    }
}
