//! Server monitoring types
//!
//! These types are for monitoring the **server** (upstream connection).
//! An app typically has one server connection with one or more channels.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use stratum_core::bitcoin::{Address, Network, ScriptBuf};
use utoipa::ToSchema;

/// Information about an extended channel opened with the server
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ServerExtendedChannelInfo {
    pub channel_id: u32,
    pub user_identity: String,
    /// None when vardiff is disabled and hashrate cannot be reliably tracked
    pub nominal_hashrate: Option<f32>,
    pub target_hex: String,
    pub extranonce_prefix_hex: String,
    pub full_extranonce_size: usize,
    pub rollable_extranonce_size: u16,
    pub version_rolling: bool,
    pub shares_acknowledged: u32,
    pub shares_submitted: u32,
    pub shares_rejected: u32,
    pub shares_rejected_by_reason: HashMap<String, u32>,
    /// Work acknowledged by upstream via `SubmitSharesSuccess.new_shares_sum`.
    pub acknowledged_work_sum: u64,
    /// Work locally validated by the client channel.
    pub validated_work_sum: f64,
    pub best_diff: f64,
    pub blocks_found: u32,
}

/// Information about a standard channel opened with the server
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ServerStandardChannelInfo {
    pub channel_id: u32,
    pub user_identity: String,
    /// None when vardiff is disabled and hashrate cannot be reliably tracked
    pub nominal_hashrate: Option<f32>,
    pub target_hex: String,
    pub extranonce_prefix_hex: String,
    pub shares_acknowledged: u32,
    pub shares_submitted: u32,
    pub shares_rejected: u32,
    pub shares_rejected_by_reason: HashMap<String, u32>,
    /// Work acknowledged by upstream via `SubmitSharesSuccess.new_shares_sum`.
    pub acknowledged_work_sum: u64,
    /// Work locally validated by the client channel.
    pub validated_work_sum: f64,
    pub best_diff: f64,
    pub blocks_found: u32,
}

/// A script the app's own coinbase outputs pay to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CoinbaseOutputInfo {
    /// Hex-encoded `script_pubkey`.
    pub script_hex: String,
    /// The script as an address on the app's network, or empty if the network is unknown or
    /// the script has no address form.
    pub address: String,
}

impl CoinbaseOutputInfo {
    /// One entry per distinct script, ordered by script.
    pub fn from_scripts(
        scripts: impl IntoIterator<Item = ScriptBuf>,
        network: Option<Network>,
    ) -> Vec<Self> {
        scripts
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|script| Self {
                address: network
                    .and_then(|network| Address::from_script(&script, network).ok())
                    .map(|address| address.to_string())
                    .unwrap_or_default(),
                script_hex: script.to_hex_string(),
            })
            .collect()
    }
}

/// Information about the server (upstream connection)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ServerInfo {
    pub extended_channels: Vec<ServerExtendedChannelInfo>,
    pub standard_channels: Vec<ServerStandardChannelInfo>,
    /// Scripts paid by the coinbase outputs this app builds its jobs from (the Pool's reward
    /// script and connected miners' payout modes, the JDC's coinbase outputs). Read from that
    /// state, not from each job sent. Empty for apps that build no coinbase.
    pub coinbase_outputs: Vec<CoinbaseOutputInfo>,
}

impl ServerInfo {
    /// Get total number of channels with the server
    pub fn total_channels(&self) -> usize {
        self.extended_channels.len() + self.standard_channels.len()
    }

    /// Get total hashrate across all server channels
    pub fn total_hashrate(&self) -> f32 {
        self.extended_channels
            .iter()
            .filter_map(|c| c.nominal_hashrate)
            .sum::<f32>()
            + self
                .standard_channels
                .iter()
                .filter_map(|c| c.nominal_hashrate)
                .sum::<f32>()
    }
}

/// Aggregate information about the server connection
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ServerSummary {
    pub total_channels: usize,
    pub extended_channels: usize,
    pub standard_channels: usize,
    pub total_hashrate: f32,
}

/// Trait for monitoring the server (upstream connection)
pub trait ServerMonitoring: Send + Sync {
    /// Get server connection info with all its channels
    fn get_server(&self) -> ServerInfo;

    /// Get summary of server connection
    fn get_server_summary(&self) -> ServerSummary {
        let server = self.get_server();

        ServerSummary {
            total_channels: server.total_channels(),
            extended_channels: server.extended_channels.len(),
            standard_channels: server.standard_channels.len(),
            total_hashrate: server.total_hashrate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratum_core::mining_sv2::ERROR_CODE_SUBMIT_SHARES_DUPLICATE_SHARE;

    // ── helpers ──────────────────────────────────────────────────────

    fn create_server_extended_channel_info(
        channel_id: u32,
        hashrate: Option<f32>,
    ) -> ServerExtendedChannelInfo {
        ServerExtendedChannelInfo {
            channel_id,
            user_identity: format!("pool-ext-{}", channel_id),
            nominal_hashrate: hashrate,
            target_hex: "00ff".into(),
            extranonce_prefix_hex: "aa".into(),
            full_extranonce_size: 16,
            rollable_extranonce_size: 4,
            version_rolling: true,
            shares_acknowledged: 10,
            shares_rejected: 0,
            shares_rejected_by_reason: HashMap::new(),
            acknowledged_work_sum: 100,
            validated_work_sum: 100.0,
            shares_submitted: 12,
            best_diff: 50.0,
            blocks_found: 0,
        }
    }

    fn create_server_standard_channel_info(
        channel_id: u32,
        hashrate: Option<f32>,
    ) -> ServerStandardChannelInfo {
        ServerStandardChannelInfo {
            channel_id,
            user_identity: format!("pool-std-{}", channel_id),
            nominal_hashrate: hashrate,
            target_hex: "00ff".into(),
            extranonce_prefix_hex: "bb".into(),
            shares_acknowledged: 20,
            shares_submitted: 22,
            shares_rejected: 1,
            shares_rejected_by_reason: HashMap::from([(
                ERROR_CODE_SUBMIT_SHARES_DUPLICATE_SHARE.to_string(),
                1,
            )]),
            acknowledged_work_sum: 200,
            validated_work_sum: 200.0,
            best_diff: 80.0,
            blocks_found: 0,
        }
    }

    // ── ServerInfo unit tests ───────────────────────────────────────

    #[test]
    fn server_info_empty() {
        let server = ServerInfo {
            coinbase_outputs: vec![],
            extended_channels: vec![],
            standard_channels: vec![],
        };
        assert_eq!(server.total_channels(), 0);
        assert_eq!(server.total_hashrate(), 0.0);
    }

    #[test]
    fn server_info_aggregates_both_channel_types() {
        let server = ServerInfo {
            coinbase_outputs: vec![],
            extended_channels: vec![create_server_extended_channel_info(1, Some(100.0))],
            standard_channels: vec![
                create_server_standard_channel_info(2, Some(50.0)),
                create_server_standard_channel_info(3, Some(75.0)),
            ],
        };
        assert_eq!(server.total_channels(), 3);
        assert_eq!(server.total_hashrate(), 225.0);
    }

    #[test]
    fn server_info_hashrate_skips_none_values() {
        let server = ServerInfo {
            coinbase_outputs: vec![],
            extended_channels: vec![
                create_server_extended_channel_info(1, Some(100.0)),
                create_server_extended_channel_info(2, None),
            ],
            standard_channels: vec![
                create_server_standard_channel_info(3, Some(50.0)),
                create_server_standard_channel_info(4, None),
            ],
        };
        assert_eq!(server.total_channels(), 4);
        assert_eq!(server.total_hashrate(), 150.0);
    }

    // ── ServerMonitoring trait default implementations ───────────────

    struct MockServer(ServerInfo);
    impl ServerMonitoring for MockServer {
        fn get_server(&self) -> ServerInfo {
            self.0.clone()
        }
    }

    #[test]
    fn server_monitoring_summary_empty() {
        let monitor = MockServer(ServerInfo {
            coinbase_outputs: vec![],
            extended_channels: vec![],
            standard_channels: vec![],
        });
        let summary = monitor.get_server_summary();

        assert_eq!(summary.total_channels, 0);
        assert_eq!(summary.extended_channels, 0);
        assert_eq!(summary.standard_channels, 0);
        assert_eq!(summary.total_hashrate, 0.0);
    }

    #[test]
    fn server_monitoring_summary_aggregates_correctly() {
        let monitor = MockServer(ServerInfo {
            coinbase_outputs: vec![],
            extended_channels: vec![
                create_server_extended_channel_info(1, Some(100.0)),
                create_server_extended_channel_info(2, Some(200.0)),
            ],
            standard_channels: vec![create_server_standard_channel_info(3, Some(50.0))],
        });
        let summary = monitor.get_server_summary();

        assert_eq!(summary.total_channels, 3);
        assert_eq!(summary.extended_channels, 2);
        assert_eq!(summary.standard_channels, 1);
        assert_eq!(summary.total_hashrate, 350.0);
    }

    #[test]
    fn coinbase_outputs_render_canonical_addresses_for_the_network() {
        use std::str::FromStr;
        // a bech32 address supplied in upper case still renders canonically (lower case)
        let script = |address: &str| {
            Address::from_str(address)
                .unwrap()
                .assume_checked()
                .script_pubkey()
        };
        let p2sh = script("32i1m6gNcSHwiPX9nfTNXVjme9j5DU8y5g");
        let p2wpkh = script("BC1QAR0SRRR7XFKVY5L643LYDNW9RE59GTZZWF5MDQ");
        let burn = ScriptBuf::from_hex("6a0101").unwrap();

        let outputs = CoinbaseOutputInfo::from_scripts(
            [p2wpkh.clone(), p2sh.clone(), burn.clone(), p2sh.clone()],
            Some(Network::Bitcoin),
        );
        let addresses: Vec<&str> = outputs.iter().map(|o| o.address.as_str()).collect();
        // one entry per script, ordered by script bytes
        assert_eq!(
            addresses,
            [
                "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
                "",
                "32i1m6gNcSHwiPX9nfTNXVjme9j5DU8y5g"
            ]
        );
        assert_eq!(outputs[1].script_hex, "6a0101");

        let testnet = CoinbaseOutputInfo::from_scripts([p2wpkh.clone()], Some(Network::Testnet4));
        assert!(testnet[0].address.starts_with("tb1q"));
        let round_trip = Address::from_str(&testnet[0].address)
            .unwrap()
            .require_network(Network::Testnet4)
            .unwrap();
        assert_eq!(round_trip.script_pubkey(), p2wpkh);

        let unknown = CoinbaseOutputInfo::from_scripts([p2wpkh.clone()], None);
        assert_eq!(unknown[0].address, "");
        assert_eq!(unknown[0].script_hex, p2wpkh.to_hex_string());
    }
}
