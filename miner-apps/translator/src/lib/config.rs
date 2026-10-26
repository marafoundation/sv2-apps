//! ## Translator Configuration Module
//!
//! Defines [`TranslatorConfig`], the primary configuration structure for the Translator.
//!
//! This module provides the necessary structures to configure the Translator,
//! managing connections and settings for both upstream and downstream interfaces.
//!
//! This module handles:
//! - Upstream server address, port, and authentication key ([`Upstream`])
//! - Downstream interface address and port ([`TranslatorConfig::downstream_address`],
//!   [`TranslatorConfig::downstream_port`])
//! - Supported protocol versions
//! - Downstream difficulty adjustment parameters ([`DownstreamDifficultyConfig`])
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use serde::Deserialize;
use std::net::SocketAddr;
use stratum_apps::{
    config_helpers::opt_path_from_toml,
    key_utils::Secp256k1PublicKey,
    payout::{MissingMinerPayoutMode, PayoutMode, PayoutModeError},
    utils::types::{Hashrate, SharesPerMinute},
};
use tracing::warn;

/// How long to wait for an upstream to answer `SetupConnection` when
/// [`TranslatorConfig::setup_connection_timeout_secs`] is unset. It matches the Noise handshake
/// timeout, so an upstream gets the same patience at both steps of connecting.
pub const DEFAULT_SETUP_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// TCP keepalive parameters for the upstream socket, see
/// [`TranslatorConfig::upstream_tcp_keepalive`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpKeepaliveConfig {
    /// How long the connection must be idle before the first probe is sent.
    pub idle: Duration,
    /// Time between probes. `None` keeps the operating system default.
    pub interval: Option<Duration>,
    /// Unanswered probes after which the connection is dropped. `None` keeps the operating
    /// system default.
    pub probes: Option<u32>,
}

/// Configuration for the Translator.
#[derive(Debug, Deserialize, Clone)]
pub struct TranslatorConfig {
    pub upstreams: Vec<Upstream>,
    /// The address for the downstream interface.
    pub downstream_address: String,
    /// The port for the downstream interface.
    pub downstream_port: u16,
    /// The maximum supported protocol version for communication.
    pub max_supported_version: u16,
    /// The minimum supported protocol version for communication.
    pub min_supported_version: u16,
    /// The size of the extranonce2 field for downstream mining connections.
    pub downstream_extranonce2_size: u16,
    /// Whether to verify upstream coinbase outputs against a payout address encoded in each
    /// upstream `user_identity`. Defaults to `false` for standard pool-mining configurations,
    /// which trust the upstream's payout policy.
    #[serde(default)]
    pub verify_payout: bool,
    /// Configuration settings for managing difficulty on the downstream connection.
    pub downstream_difficulty_config: DownstreamDifficultyConfig,
    /// Whether to aggregate all downstream connections into a single upstream channel.
    /// If true, all miners share one channel. If false, each miner gets its own channel.
    pub aggregate_channels: bool,
    /// Protocol extensions that the translator supports (will request if supported by server).
    #[serde(default)]
    pub supported_extensions: Vec<u16>,
    /// Past jobs retained per channel for late-share validation.
    ///
    /// `None` and `Some(0)` both select the `channels_sv2` default. It is a retention window —
    /// `cap / job rate` — and here the upstream sets the rate; see `channels_sv2` for sizing.
    #[serde(default)]
    pub max_past_jobs: Option<usize>,
    /// Seconds to wait for the upstream to answer `SetupConnection` before giving up on it and
    /// moving to the next attempt or upstream.
    ///
    /// `None` and `Some(0)` both select [`DEFAULT_SETUP_CONNECTION_TIMEOUT`]; a zero timeout
    /// would fail every connection attempt.
    #[serde(default)]
    pub setup_connection_timeout_secs: Option<u64>,
    /// Seconds an upstream connection must be idle before TCP keepalive probes start.
    ///
    /// This is the switch: unset (or 0) leaves keepalive off, so a peer that silently disappears
    /// is only noticed when the operating system gives up retransmitting, which can take hours.
    /// The interval and probe count below only apply when this is set.
    #[serde(default)]
    pub upstream_tcp_keepalive_idle_secs: Option<u64>,
    /// Seconds between TCP keepalive probes. Unset or 0 keeps the operating system default.
    #[serde(default)]
    pub upstream_tcp_keepalive_interval_secs: Option<u64>,
    /// Unanswered TCP keepalive probes before the connection is dropped. Unset or 0 keeps the
    /// operating system default.
    #[serde(default)]
    pub upstream_tcp_keepalive_probes: Option<u32>,
    /// Protocol extensions that the translator requires (server must support these).
    /// If the upstream server doesn't support these, the translator will fail over to another
    /// upstream.
    #[serde(default)]
    pub required_extensions: Vec<u16>,
    /// The path to the log file for the Translator.
    #[serde(default, deserialize_with = "opt_path_from_toml")]
    log_file: Option<PathBuf>,
    /// Optional monitoring server bind address
    #[serde(default)]
    monitoring_address: Option<SocketAddr>,
    #[serde(default)]
    monitoring_cache_refresh_secs: Option<u64>,
    #[serde(default)]
    miner_telemetry: MinerTelemetryConfig,
    /// Seconds to drain SV1 downstreams for on SIGTERM/SIGINT before shutting down.
    ///
    /// When non-zero, a shutdown signal closes the SV1 listener and then disconnects the
    /// connected miners one at a time, spread evenly (with jitter) across this window, while the
    /// upstream SV2 connection keeps serving the miners that are still connected. The translator
    /// exits once the last miner is gone or the window elapses, whichever comes first. `0` (the
    /// default) shuts down immediately and drops every miner at once.
    #[serde(default)]
    pub drain_seconds: u64,
    /// Whether to send SV1 `client.reconnect` (no parameters, i.e. "reconnect to the same
    /// host and port") to each miner just before closing its connection during a drain.
    #[serde(default)]
    pub drain_send_reconnect: bool,
    /// Fallbacks tolerated within `fallback_window_secs` before the translator gives up and
    /// exits. Upstreams that served miners stay eligible for later fallbacks, so this bounds an
    /// upstream that accepts the connection and then keeps failing. `0` exits on the first
    /// fallback.
    #[serde(default = "default_max_fallbacks")]
    pub max_fallbacks: usize,
    /// Window, in seconds, over which `max_fallbacks` is counted.
    #[serde(default = "default_fallback_window_secs")]
    pub fallback_window_secs: u64,
}

fn default_max_fallbacks() -> usize {
    5
}

fn default_fallback_window_secs() -> u64 {
    600
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MinerTelemetryConfig {
    /// Private IPv4 CIDRs to scan for miner management interfaces.
    #[serde(default)]
    pub cidrs: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Upstream {
    /// The address of the upstream server.
    pub address: String,
    /// The port of the upstream server.
    pub port: u16,
    /// The Secp256k1 public key used to authenticate the upstream authority.
    pub authority_pubkey: Secp256k1PublicKey,
    /// The user identity/username to use when connecting to the pool.
    /// This will be appended with a counter for each mining channel (e.g., username.miner1,
    /// username.miner2).
    pub user_identity: String,
}

impl Upstream {
    /// Creates a new `UpstreamConfig` instance.
    pub fn new(
        address: String,
        port: u16,
        authority_pubkey: Secp256k1PublicKey,
        user_identity: String,
    ) -> Self {
        Self {
            address,
            port,
            authority_pubkey,
            user_identity,
        }
    }
}

impl TranslatorConfig {
    /// `drain_seconds` as a window, capped at one day so the drain deadline cannot overflow
    /// `Instant`.
    pub fn drain_window(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.drain_seconds.min(86_400))
    }

    /// `fallback_window_secs` as a window, capped at one day like [`Self::drain_window`].
    pub fn fallback_window(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.fallback_window_secs.min(86_400))
    }

    /// How long to wait for the upstream's answer to `SetupConnection`.
    pub fn setup_connection_timeout(&self) -> Duration {
        match self.setup_connection_timeout_secs {
            Some(secs) if secs > 0 => Duration::from_secs(secs),
            _ => DEFAULT_SETUP_CONNECTION_TIMEOUT,
        }
    }

    /// TCP keepalive to apply to upstream sockets, or `None` when it is off.
    pub fn upstream_tcp_keepalive(&self) -> Option<TcpKeepaliveConfig> {
        let idle = self.upstream_tcp_keepalive_idle_secs.filter(|&s| s > 0)?;
        Some(TcpKeepaliveConfig {
            idle: Duration::from_secs(idle),
            interval: self
                .upstream_tcp_keepalive_interval_secs
                .filter(|&s| s > 0)
                .map(Duration::from_secs),
            probes: self.upstream_tcp_keepalive_probes.filter(|&p| p > 0),
        })
    }

    /// Warns about settings that are present but have no effect.
    pub(crate) fn warn_about_ignored_settings(&self) {
        if self.upstream_tcp_keepalive().is_none()
            && (self.upstream_tcp_keepalive_interval_secs.is_some()
                || self.upstream_tcp_keepalive_probes.is_some())
        {
            warn!(
                "upstream_tcp_keepalive_interval_secs and upstream_tcp_keepalive_probes are \
                 ignored because upstream_tcp_keepalive_idle_secs is not set"
            );
        }
    }

    /// Creates a new `TranslatorConfig` instance with the specified upstream and downstream
    /// configurations and version constraints.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        upstreams: Vec<Upstream>,
        downstream_address: String,
        downstream_port: u16,
        downstream_difficulty_config: DownstreamDifficultyConfig,
        max_supported_version: u16,
        min_supported_version: u16,
        downstream_extranonce2_size: u16,
        verify_payout: bool,
        aggregate_channels: bool,
        supported_extensions: Vec<u16>,
        required_extensions: Vec<u16>,
        monitoring_address: Option<SocketAddr>,
        monitoring_cache_refresh_secs: Option<u64>,
    ) -> Self {
        Self {
            upstreams,
            // Programmatic construction takes the library default; the file-based path
            // sets it via serde, and callers can assign the field directly.
            max_past_jobs: None,
            setup_connection_timeout_secs: None,
            upstream_tcp_keepalive_idle_secs: None,
            upstream_tcp_keepalive_interval_secs: None,
            upstream_tcp_keepalive_probes: None,
            downstream_address,
            downstream_port,
            max_supported_version,
            min_supported_version,
            downstream_extranonce2_size,
            verify_payout,
            downstream_difficulty_config,
            aggregate_channels,
            supported_extensions,
            required_extensions,
            log_file: None,
            monitoring_address,
            monitoring_cache_refresh_secs,
            miner_telemetry: MinerTelemetryConfig::default(),
            drain_seconds: 0,
            drain_send_reconnect: false,
            max_fallbacks: default_max_fallbacks(),
            fallback_window_secs: default_fallback_window_secs(),
        }
    }

    /// Returns the monitoring server bind address (if enabled)
    pub fn monitoring_address(&self) -> Option<SocketAddr> {
        self.monitoring_address
    }

    /// Returns the monitoring cache refresh interval in seconds.
    pub fn monitoring_cache_refresh_secs(&self) -> Option<u64> {
        self.monitoring_cache_refresh_secs
    }

    /// Returns the miner management CIDRs used for telemetry discovery.
    pub fn miner_telemetry_cidrs(&self) -> &[String] {
        &self.miner_telemetry.cidrs
    }

    pub(crate) fn expected_payout_distribution(
        &self,
        user_identity: &str,
    ) -> Result<Option<PayoutMode>, PayoutModeError> {
        if !self.verify_payout {
            return Ok(None);
        }

        match PayoutMode::try_from(user_identity) {
            Ok(
                payout_mode @ (PayoutMode::Solo { .. }
                | PayoutMode::LegacySolo { .. }
                | PayoutMode::Donate { .. }),
            ) => Ok(Some(payout_mode)),
            Ok(PayoutMode::FullDonation) => Err(PayoutModeError::MissingMinerPayout {
                user_identity: user_identity.to_string(),
                mode: MissingMinerPayoutMode::FullDonation,
            }),
            Err(PayoutModeError::NoPayoutMode(_)) => Err(PayoutModeError::MissingMinerPayout {
                user_identity: user_identity.to_string(),
                mode: MissingMinerPayoutMode::NoPayoutMode,
            }),
            Err(e) => Err(e),
        }
    }

    pub fn set_log_dir(&mut self, log_dir: Option<PathBuf>) {
        if let Some(dir) = log_dir {
            self.log_file = Some(dir);
        }
    }
    pub fn log_dir(&self) -> Option<&Path> {
        self.log_file.as_deref()
    }
}

/// Configuration settings for managing difficulty adjustments on the downstream connection.
#[derive(Debug, Deserialize, Clone)]
pub struct DownstreamDifficultyConfig {
    /// The minimum hashrate expected from an individual miner on the downstream connection.
    pub min_individual_miner_hashrate: Hashrate,
    /// The target number of shares per minute for difficulty adjustment.
    pub shares_per_minute: SharesPerMinute,
    /// Whether to enable variable difficulty adjustment mechanism.
    /// If false, difficulty will be managed by upstream (useful with JDC).
    pub enable_vardiff: bool,
    /// Minimum idle interval in seconds before sending a miner a keepalive job.
    /// The translator will send periodic mining.notify messages with updated time
    /// to prevent SV1 miners from timing out when the upstream doesn't send new jobs
    /// frequently enough (e.g., due to low Bitcoin mempool activity).
    /// Only miners whose individual interval has elapsed receive a keepalive. Aggregated
    /// keepalive generation advances nTime at most once per interval; other overdue miners
    /// reuse the latest shared keepalive. Normal upstream jobs are not delayed by this interval.
    /// Set to 0 to disable keepalive jobs.
    pub job_keepalive_interval_secs: u16,
}

impl DownstreamDifficultyConfig {
    /// Creates a new `DownstreamDifficultyConfig` instance.
    pub fn new(
        min_individual_miner_hashrate: Hashrate,
        shares_per_minute: SharesPerMinute,
        enable_vardiff: bool,
        job_keepalive_interval_secs: u16,
    ) -> Self {
        Self {
            min_individual_miner_hashrate,
            shares_per_minute,
            enable_vardiff,
            job_keepalive_interval_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn create_test_upstream() -> Upstream {
        // Use a valid base58-encoded public key from the key-utils test cases
        let pubkey_str = "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan";
        let pubkey = Secp256k1PublicKey::from_str(pubkey_str).unwrap();
        Upstream::new("127.0.0.1".to_string(), 4444, pubkey, "IT_TEST".to_string())
    }

    fn create_test_difficulty_config() -> DownstreamDifficultyConfig {
        DownstreamDifficultyConfig::new(100.0, 5.0, true, 60)
    }

    #[test]
    fn test_upstream_creation() {
        let upstream = create_test_upstream();
        assert_eq!(upstream.address, "127.0.0.1");
        assert_eq!(upstream.port, 4444);
    }

    #[test]
    fn test_downstream_difficulty_config_creation() {
        let config = create_test_difficulty_config();
        assert_eq!(config.min_individual_miner_hashrate, 100.0);
        assert_eq!(config.shares_per_minute, 5.0);
        assert!(config.enable_vardiff);
    }

    #[test]
    fn test_translator_config_creation() {
        let upstreams = vec![create_test_upstream()];
        let difficulty_config = create_test_difficulty_config();

        let config = TranslatorConfig::new(
            upstreams,
            "0.0.0.0".to_string(),
            3333,
            difficulty_config,
            2,
            1,
            4,
            false,
            true,
            vec![],
            vec![],
            None,
            None,
        );

        assert_eq!(config.upstreams.len(), 1);
        assert_eq!(config.downstream_address, "0.0.0.0");
        assert_eq!(config.downstream_port, 3333);
        assert_eq!(config.max_supported_version, 2);
        assert_eq!(config.min_supported_version, 1);
        assert_eq!(config.downstream_extranonce2_size, 4);
        assert!(!config.verify_payout);
        assert!(config.aggregate_channels);
        assert!(config.supported_extensions.is_empty());
        assert!(config.required_extensions.is_empty());
        assert!(config.log_file.is_none());
    }

    #[test]
    fn payout_verification_requires_explicit_opt_in() {
        let upstreams = vec![create_test_upstream()];
        let difficulty_config = create_test_difficulty_config();
        let payout_address = "bc1qtzqxqaxyy6lda2fhdtp5dp0v56vlf6g0tljy2x";

        let disabled_config = TranslatorConfig::new(
            upstreams.clone(),
            "0.0.0.0".to_string(),
            3333,
            difficulty_config.clone(),
            2,
            1,
            4,
            false,
            true,
            vec![],
            vec![],
            None,
            None,
        );

        assert!(
            disabled_config
                .expected_payout_distribution(payout_address)
                .unwrap()
                .is_none()
        );

        let enabled_config = TranslatorConfig::new(
            upstreams,
            "0.0.0.0".to_string(),
            3333,
            difficulty_config,
            2,
            1,
            4,
            true,
            true,
            vec![],
            vec![],
            None,
            None,
        );

        assert!(matches!(
            enabled_config
                .expected_payout_distribution(payout_address)
                .unwrap(),
            Some(PayoutMode::LegacySolo { .. })
        ));
    }

    #[test]
    fn payout_verification_requires_miner_payout_identity() {
        let config = TranslatorConfig::new(
            vec![create_test_upstream()],
            "0.0.0.0".to_string(),
            3333,
            create_test_difficulty_config(),
            2,
            1,
            4,
            true,
            true,
            vec![],
            vec![],
            None,
            None,
        );

        assert!(matches!(
            config
                .expected_payout_distribution("sri/donate/worker")
                .unwrap_err(),
            PayoutModeError::MissingMinerPayout {
                mode: MissingMinerPayoutMode::FullDonation,
                ..
            }
        ));

        assert!(matches!(
            config
                .expected_payout_distribution("invalid_address.worker")
                .unwrap_err(),
            PayoutModeError::MissingMinerPayout {
                mode: MissingMinerPayoutMode::NoPayoutMode,
                ..
            }
        ));
    }

    #[test]
    fn payout_verification_rejects_address_like_typos() {
        let config = TranslatorConfig::new(
            vec![create_test_upstream()],
            "0.0.0.0".to_string(),
            3333,
            create_test_difficulty_config(),
            2,
            1,
            4,
            true,
            true,
            vec![],
            vec![],
            None,
            None,
        );

        assert!(matches!(
            config
                .expected_payout_distribution("bc1q_typo.worker")
                .unwrap_err(),
            PayoutModeError::MissingMinerPayout {
                mode: MissingMinerPayoutMode::NoPayoutMode,
                ..
            }
        ));
    }

    #[test]
    fn test_translator_config_log_dir() {
        let upstreams = vec![create_test_upstream()];
        let difficulty_config = create_test_difficulty_config();

        let mut config = TranslatorConfig::new(
            upstreams,
            "0.0.0.0".to_string(),
            3333,
            difficulty_config,
            2,
            1,
            4,
            false,
            false,
            vec![],
            vec![],
            None,
            None,
        );

        assert!(config.log_dir().is_none());

        let log_path = PathBuf::from("/tmp/logs");
        config.set_log_dir(Some(log_path.clone()));
        assert_eq!(config.log_dir(), Some(log_path.as_path()));

        config.set_log_dir(None);
        assert_eq!(config.log_dir(), Some(log_path.as_path())); // Should remain unchanged
    }

    #[test]
    fn test_multiple_upstreams() {
        let upstream1 = create_test_upstream();
        let mut upstream2 = create_test_upstream();
        upstream2.address = "192.168.1.1".to_string();
        upstream2.port = 5555;

        let upstreams = vec![upstream1, upstream2];
        let difficulty_config = create_test_difficulty_config();

        let config = TranslatorConfig::new(
            upstreams,
            "0.0.0.0".to_string(),
            3333,
            difficulty_config,
            2,
            1,
            4,
            false,
            true,
            vec![],
            vec![],
            None,
            None,
        );

        assert_eq!(config.upstreams.len(), 2);
        assert_eq!(config.upstreams[0].address, "127.0.0.1");
        assert_eq!(config.upstreams[0].port, 4444);
        assert_eq!(config.upstreams[1].address, "192.168.1.1");
        assert_eq!(config.upstreams[1].port, 5555);
    }

    #[test]
    fn test_vardiff_disabled_config() {
        let mut difficulty_config = create_test_difficulty_config();
        difficulty_config.enable_vardiff = false;

        let upstreams = vec![create_test_upstream()];
        let config = TranslatorConfig::new(
            upstreams,
            "0.0.0.0".to_string(),
            3333,
            difficulty_config,
            2,
            1,
            4,
            false,
            false,
            vec![],
            vec![],
            None,
            None,
        );

        assert!(!config.downstream_difficulty_config.enable_vardiff);
        assert!(!config.aggregate_channels);
    }

    #[test]
    fn test_upstream_user_identity_toml_present() {
        let toml = r#"
            address = "127.0.0.1"
            port = 4444
            authority_pubkey = "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan"
            user_identity = "sri/solo/bc1qtest"
        "#;
        let upstream: Upstream = toml::from_str(toml).unwrap();
        assert_eq!(upstream.user_identity, "sri/solo/bc1qtest");
    }

    #[test]
    fn test_upstream_user_identity_toml_absent_is_rejected() {
        let toml = r#"
            address = "127.0.0.1"
            port = 4444
            authority_pubkey = "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan"
        "#;
        assert!(toml::from_str::<Upstream>(toml).is_err());
    }

    #[test]
    fn test_upstream_user_identity_toml_empty_string() {
        let toml = r#"
            address = "127.0.0.1"
            port = 4444
            authority_pubkey = "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan"
            user_identity = ""
        "#;
        let upstream: Upstream = toml::from_str(toml).unwrap();
        assert_eq!(upstream.user_identity, "");
    }

    /// Parses a minimal config file, with `extra` as additional top-level keys.
    fn parse_config(extra: &str) -> TranslatorConfig {
        let toml = format!(
            r#"
            {extra}
            downstream_address = "0.0.0.0"
            downstream_port = 34255
            max_supported_version = 2
            min_supported_version = 2
            downstream_extranonce2_size = 8
            aggregate_channels = false

            [downstream_difficulty_config]
            min_individual_miner_hashrate = 10000000.0
            shares_per_minute = 6.0
            enable_vardiff = false
            job_keepalive_interval_secs = 60

            [[upstreams]]
            address = "127.0.0.1"
            port = 3333
            authority_pubkey = "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan"
            user_identity = "user"
            "#
        );
        toml::from_str(&toml).unwrap()
    }

    #[test]
    fn setup_connection_timeout_defaults_when_absent() {
        let config = parse_config("");
        assert_eq!(config.setup_connection_timeout_secs, None);
        assert_eq!(
            config.setup_connection_timeout(),
            DEFAULT_SETUP_CONNECTION_TIMEOUT
        );
    }

    #[test]
    fn setup_connection_timeout_uses_configured_value() {
        let config = parse_config("setup_connection_timeout_secs = 3");
        assert_eq!(config.setup_connection_timeout(), Duration::from_secs(3));
    }

    #[test]
    fn setup_connection_timeout_zero_selects_the_default() {
        let config = parse_config("setup_connection_timeout_secs = 0");
        assert_eq!(
            config.setup_connection_timeout(),
            DEFAULT_SETUP_CONNECTION_TIMEOUT
        );
    }

    #[test]
    fn tcp_keepalive_is_off_when_absent() {
        let config = parse_config("");
        assert_eq!(config.upstream_tcp_keepalive_idle_secs, None);
        assert_eq!(config.upstream_tcp_keepalive_interval_secs, None);
        assert_eq!(config.upstream_tcp_keepalive_probes, None);
        assert_eq!(config.upstream_tcp_keepalive(), None);
    }

    #[test]
    fn tcp_keepalive_uses_configured_values() {
        let config = parse_config(
            "upstream_tcp_keepalive_idle_secs = 30
             upstream_tcp_keepalive_interval_secs = 10
             upstream_tcp_keepalive_probes = 3",
        );
        assert_eq!(
            config.upstream_tcp_keepalive(),
            Some(TcpKeepaliveConfig {
                idle: Duration::from_secs(30),
                interval: Some(Duration::from_secs(10)),
                probes: Some(3),
            })
        );
    }

    #[test]
    fn tcp_keepalive_idle_alone_keeps_os_defaults_for_the_rest() {
        let config = parse_config("upstream_tcp_keepalive_idle_secs = 30");
        assert_eq!(
            config.upstream_tcp_keepalive(),
            Some(TcpKeepaliveConfig {
                idle: Duration::from_secs(30),
                interval: None,
                probes: None,
            })
        );
    }

    #[test]
    fn tcp_keepalive_needs_idle_to_be_enabled() {
        let config = parse_config(
            "upstream_tcp_keepalive_interval_secs = 10
             upstream_tcp_keepalive_probes = 3",
        );
        assert_eq!(config.upstream_tcp_keepalive(), None);
        let zero_idle = parse_config("upstream_tcp_keepalive_idle_secs = 0");
        assert_eq!(zero_idle.upstream_tcp_keepalive(), None);
    }

    #[test]
    fn test_multi_upstream_identity_toml() {
        let pubkey = "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan";
        let toml = format!(
            r#"
            downstream_address = "0.0.0.0"
            downstream_port = 34255
            max_supported_version = 2
            min_supported_version = 2
            downstream_extranonce2_size = 8
            aggregate_channels = false

            [downstream_difficulty_config]
            min_individual_miner_hashrate = 10000000.0
            shares_per_minute = 6.0
            enable_vardiff = false
            job_keepalive_interval_secs = 60

            [[upstreams]]
            address = "127.0.0.1"
            port = 3333
            authority_pubkey = "{pubkey}"
            user_identity = "sri/solo/bc1qprimary"

            [[upstreams]]
            address = "192.168.1.1"
            port = 3333
            authority_pubkey = "{pubkey}"
            user_identity = "bc1qbackup.worker"
            "#
        );
        let config: TranslatorConfig = toml::from_str(&toml).unwrap();
        assert_eq!(config.upstreams.len(), 2);
        assert_eq!(config.upstreams[0].user_identity, "sri/solo/bc1qprimary");
        assert_eq!(config.upstreams[1].user_identity, "bc1qbackup.worker");
        assert!(!config.verify_payout);
        assert_eq!(config.max_fallbacks, 5);
        assert_eq!(config.fallback_window_secs, 600);
    }

    #[test]
    fn test_fallback_budget_toml() {
        let pubkey = "9bDuixKmZqAJnrmP746n8zU1wyAQRrus7th9dxnkPg6RzQvCnan";
        let toml = format!(
            r#"
            downstream_address = "0.0.0.0"
            downstream_port = 34255
            max_supported_version = 2
            min_supported_version = 2
            downstream_extranonce2_size = 8
            aggregate_channels = false
            max_fallbacks = 0
            fallback_window_secs = 120

            [downstream_difficulty_config]
            min_individual_miner_hashrate = 10000000.0
            shares_per_minute = 6.0
            enable_vardiff = false
            job_keepalive_interval_secs = 60

            [[upstreams]]
            address = "127.0.0.1"
            port = 3333
            authority_pubkey = "{pubkey}"
            user_identity = "user"
            "#
        );
        let config: TranslatorConfig = toml::from_str(&toml).unwrap();
        assert_eq!(config.max_fallbacks, 0);
        assert_eq!(
            config.fallback_window(),
            std::time::Duration::from_secs(120)
        );
    }
}

#[cfg(test)]
mod max_past_jobs_tests {
    /// Every shipped example must document the setting, commented out.
    ///
    /// Commented out matters: it keeps the library default in force for existing
    /// deployments. Absent-means-default is also why `Some(0)` is treated as "no
    /// opinion" rather than a literal zero — a zero cap would evict the job that just
    /// retired and reject the most common late share as `invalid-job-id`.
    ///
    /// The test walks the directory rather than naming files, and asserts it found
    /// some: an earlier version named one file, guessed the name wrong, and passed by
    /// silently doing nothing.
    #[test]
    fn every_example_documents_the_cap_commented_out() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config-examples");
        let mut checked = 0;
        let mut stack = vec![dir];
        while let Some(d) = stack.pop() {
            for entry in std::fs::read_dir(&d).expect("read config-examples") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                } else if path.extension().is_some_and(|e| e == "toml") {
                    let text = std::fs::read_to_string(&path).expect("read example");
                    assert!(
                        text.contains("# max_past_jobs"),
                        "{} does not document max_past_jobs (commented out)",
                        path.display()
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 0, "no examples found — the test would be vacuous");
    }
}
