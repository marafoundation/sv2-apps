//! SV1 client monitoring types
//!
//! These types are specific to SV1 protocol client connections.
//! Used by Translator Proxy (tProxy) that accepts SV1 miner connections.

use std::net::IpAddr;

#[cfg(feature = "asic-rs-telemetry")]
pub use super::miner_telemetry::{MinerTelemetry, MinerTelemetryStatus};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Information about a single SV1 client connection
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Sv1ClientInfo {
    pub client_id: usize,
    pub channel_id: Option<u32>,
    #[schema(value_type = String)]
    pub connection_ip: IpAddr,
    #[cfg(feature = "asic-rs-telemetry")]
    /// Miner management IP used for matched telemetry, if discovery found one.
    #[schema(value_type = Option<String>)]
    pub management_ip: Option<IpAddr>,
    pub sv1_username: String,
    pub sv1_worker_name: String,
    pub target_hex: String,
    pub hashrate: Option<f32>,
    pub stable_hashrate: bool,
    pub extranonce1_hex: String,
    pub extranonce2_len: usize,
    pub version_rolling_mask: Option<String>,
    pub version_rolling_min_bit: Option<String>,
    /// Outcomes of the shares this connection submitted, as answered to the miner.
    #[serde(default)]
    pub shares: Sv1ShareCounts,
    #[cfg(feature = "asic-rs-telemetry")]
    /// Latest telemetry fetched from the matched miner management interface.
    pub miner_telemetry: Option<MinerTelemetry>,
    #[cfg(feature = "asic-rs-telemetry")]
    /// Current discovery and fetch status for miner telemetry matching.
    pub miner_telemetry_status: Option<MinerTelemetryStatus>,
}

/// Aggregate information about SV1 client connections
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Sv1ClientsSummary {
    pub total_clients: usize,
    pub total_hashrate: f32,
    /// Outcomes of every share submitted since the process started, including by miners that
    /// have since disconnected.
    #[serde(default)]
    pub shares: Sv1ShareCounts,
    /// Accepted shares the translator did not send upstream, since the process started.
    #[serde(default)]
    pub dropped: Sv1ShareDrops,
}

/// Why the translator dropped a share it had accepted from an SV1 miner instead of sending it
/// upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sv1ShareDropReason {
    /// Its job was replaced by a new chain tip.
    Stale,
    /// Its job is unknown to the upstream channel.
    InvalidJobId,
    /// The share does not form a valid block header.
    Invalid,
    /// The upstream channel already has the share.
    Duplicate,
    /// Its extranonce does not have the size the upstream channel expects.
    BadExtranonceSize,
    /// It rolls version bits the upstream channel does not allow.
    VersionRollingNotAllowed,
    /// The upstream channel has no chain tip yet.
    NoChainTip,
    /// The upstream channel it belongs to no longer exists.
    UnknownChannel,
    /// The upstream it was meant for went away, in a fallback, before it could be sent.
    UpstreamGone,
}

impl Sv1ShareDropReason {
    pub const ALL: [Self; 9] = [
        Self::Stale,
        Self::InvalidJobId,
        Self::Invalid,
        Self::Duplicate,
        Self::BadExtranonceSize,
        Self::VersionRollingNotAllowed,
        Self::NoChainTip,
        Self::UnknownChannel,
        Self::UpstreamGone,
    ];

    /// The `reason` label of `sv1_shares_dropped_total`.
    pub fn label(self) -> &'static str {
        match self {
            Self::Stale => "stale",
            Self::InvalidJobId => "invalid-job-id",
            Self::Invalid => "invalid",
            Self::Duplicate => "duplicate",
            Self::BadExtranonceSize => "bad-extranonce-size",
            Self::VersionRollingNotAllowed => "version-rolling-not-allowed",
            Self::NoChainTip => "no-chain-tip",
            Self::UnknownChannel => "unknown-channel",
            Self::UpstreamGone => "upstream-gone",
        }
    }
}

/// Shares the translator accepted from SV1 miners but did not send upstream.
///
/// Every one of them is also counted as accepted in [`Sv1ShareCounts`], since that is how the
/// miner was answered.
///
/// A share below the upstream channel's target is expected where a miner's own difficulty is
/// easier than the channel's, as in aggregated mode, so those are counted apart from the rest.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Sv1ShareDrops {
    pub below_upstream_target: u64,
    pub stale: u64,
    pub invalid_job_id: u64,
    pub invalid: u64,
    pub duplicate: u64,
    pub bad_extranonce_size: u64,
    pub version_rolling_not_allowed: u64,
    pub no_chain_tip: u64,
    pub unknown_channel: u64,
    pub upstream_gone: u64,
}

impl Sv1ShareDrops {
    /// Counts one dropped share: below the upstream target when `reason` is `None`.
    pub fn record(&mut self, reason: Option<Sv1ShareDropReason>) {
        let count = match reason {
            None => &mut self.below_upstream_target,
            Some(reason) => self.count_mut(reason),
        };
        *count = count.saturating_add(1);
    }

    pub fn dropped(&self, reason: Sv1ShareDropReason) -> u64 {
        match reason {
            Sv1ShareDropReason::Stale => self.stale,
            Sv1ShareDropReason::InvalidJobId => self.invalid_job_id,
            Sv1ShareDropReason::Invalid => self.invalid,
            Sv1ShareDropReason::Duplicate => self.duplicate,
            Sv1ShareDropReason::BadExtranonceSize => self.bad_extranonce_size,
            Sv1ShareDropReason::VersionRollingNotAllowed => self.version_rolling_not_allowed,
            Sv1ShareDropReason::NoChainTip => self.no_chain_tip,
            Sv1ShareDropReason::UnknownChannel => self.unknown_channel,
            Sv1ShareDropReason::UpstreamGone => self.upstream_gone,
        }
    }

    fn count_mut(&mut self, reason: Sv1ShareDropReason) -> &mut u64 {
        match reason {
            Sv1ShareDropReason::Stale => &mut self.stale,
            Sv1ShareDropReason::InvalidJobId => &mut self.invalid_job_id,
            Sv1ShareDropReason::Invalid => &mut self.invalid,
            Sv1ShareDropReason::Duplicate => &mut self.duplicate,
            Sv1ShareDropReason::BadExtranonceSize => &mut self.bad_extranonce_size,
            Sv1ShareDropReason::VersionRollingNotAllowed => &mut self.version_rolling_not_allowed,
            Sv1ShareDropReason::NoChainTip => &mut self.no_chain_tip,
            Sv1ShareDropReason::UnknownChannel => &mut self.unknown_channel,
            Sv1ShareDropReason::UpstreamGone => &mut self.upstream_gone,
        }
    }
}

/// Why an SV1 share was rejected, by the `mining.submit` error code sent to the miner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sv1ShareRejection {
    /// 21: the job is not, or no longer, valid for this miner. Includes stale shares.
    JobNotFound,
    /// 22
    Duplicate,
    /// 23
    LowDifficulty,
    /// 24
    UnauthorizedWorker,
    /// 25
    NotSubscribed,
    /// 20, or any other code.
    Other,
}

impl Sv1ShareRejection {
    pub const ALL: [Self; 6] = [
        Self::JobNotFound,
        Self::Duplicate,
        Self::LowDifficulty,
        Self::UnauthorizedWorker,
        Self::NotSubscribed,
        Self::Other,
    ];

    pub fn from_code(code: i32) -> Self {
        match code {
            21 => Self::JobNotFound,
            22 => Self::Duplicate,
            23 => Self::LowDifficulty,
            24 => Self::UnauthorizedWorker,
            25 => Self::NotSubscribed,
            _ => Self::Other,
        }
    }

    /// The `reason` label of `sv1_shares_rejected_total`.
    pub fn label(self) -> &'static str {
        match self {
            Self::JobNotFound => "job-not-found",
            Self::Duplicate => "duplicate",
            Self::LowDifficulty => "low-difficulty",
            Self::UnauthorizedWorker => "unauthorized-worker",
            Self::NotSubscribed => "not-subscribed",
            Self::Other => "other",
        }
    }
}

/// Counts of SV1 share outcomes.
///
/// "Accepted" means accepted by the SV1 server and answered `true` to the miner; the upstream
/// may still reject the share, which is counted per upstream channel instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct Sv1ShareCounts {
    pub accepted: u64,
    pub rejected_job_not_found: u64,
    pub rejected_duplicate: u64,
    pub rejected_low_difficulty: u64,
    pub rejected_unauthorized_worker: u64,
    pub rejected_not_subscribed: u64,
    pub rejected_other: u64,
}

impl Sv1ShareCounts {
    /// Counts one share: accepted when `rejection` is `None`.
    pub fn record(&mut self, rejection: Option<Sv1ShareRejection>) {
        let count = match rejection {
            None => &mut self.accepted,
            Some(rejection) => self.rejected_mut(rejection),
        };
        *count = count.saturating_add(1);
    }

    pub fn rejected(&self, rejection: Sv1ShareRejection) -> u64 {
        match rejection {
            Sv1ShareRejection::JobNotFound => self.rejected_job_not_found,
            Sv1ShareRejection::Duplicate => self.rejected_duplicate,
            Sv1ShareRejection::LowDifficulty => self.rejected_low_difficulty,
            Sv1ShareRejection::UnauthorizedWorker => self.rejected_unauthorized_worker,
            Sv1ShareRejection::NotSubscribed => self.rejected_not_subscribed,
            Sv1ShareRejection::Other => self.rejected_other,
        }
    }

    fn rejected_mut(&mut self, rejection: Sv1ShareRejection) -> &mut u64 {
        match rejection {
            Sv1ShareRejection::JobNotFound => &mut self.rejected_job_not_found,
            Sv1ShareRejection::Duplicate => &mut self.rejected_duplicate,
            Sv1ShareRejection::LowDifficulty => &mut self.rejected_low_difficulty,
            Sv1ShareRejection::UnauthorizedWorker => &mut self.rejected_unauthorized_worker,
            Sv1ShareRejection::NotSubscribed => &mut self.rejected_not_subscribed,
            Sv1ShareRejection::Other => &mut self.rejected_other,
        }
    }
}

/// Trait for monitoring SV1 client connections
pub trait Sv1ClientsMonitoring: Send + Sync {
    /// Get all SV1 clients
    fn get_sv1_clients(&self) -> Vec<Sv1ClientInfo>;

    /// Get a single SV1 client by client_id
    ///
    /// Default implementation does O(n) scan. Override for O(1) lookup
    /// if your implementation uses a HashMap internally.
    fn get_sv1_client_by_id(&self, client_id: usize) -> Option<Sv1ClientInfo> {
        self.get_sv1_clients()
            .into_iter()
            .find(|c| c.client_id == client_id)
    }

    /// Outcomes of every share submitted since the process started.
    fn get_sv1_share_totals(&self) -> Sv1ShareCounts {
        Sv1ShareCounts::default()
    }

    /// Accepted shares not sent upstream since the process started.
    fn get_sv1_share_drops(&self) -> Sv1ShareDrops {
        Sv1ShareDrops::default()
    }

    /// Get summary of SV1 clients
    fn get_sv1_clients_summary(&self) -> Sv1ClientsSummary {
        Sv1ClientsSummary::new(
            &self.get_sv1_clients(),
            self.get_sv1_share_totals(),
            self.get_sv1_share_drops(),
        )
    }
}

impl Sv1ClientsSummary {
    /// Summarises `clients`, with `shares` and `dropped` counted since the process started.
    pub fn new(clients: &[Sv1ClientInfo], shares: Sv1ShareCounts, dropped: Sv1ShareDrops) -> Self {
        Self {
            total_clients: clients.len(),
            total_hashrate: clients.iter().filter_map(|c| c.hashrate).sum(),
            shares,
            dropped,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_sv1_client_info(id: usize, hashrate: Option<f32>) -> Sv1ClientInfo {
        Sv1ClientInfo {
            client_id: id,
            channel_id: Some(id as u32),
            connection_ip: format!("192.0.2.{}", id)
                .parse()
                .expect("test IP address must be valid"),
            #[cfg(feature = "asic-rs-telemetry")]
            management_ip: None,
            sv1_username: format!("account.worker-{}", id),
            sv1_worker_name: format!("worker-{}", id),
            target_hex: "00ff".into(),
            hashrate,
            stable_hashrate: false,
            extranonce1_hex: "aabb".into(),
            extranonce2_len: 8,
            version_rolling_mask: Some("ffffffff".into()),
            version_rolling_min_bit: Some("00000000".into()),
            shares: Default::default(),
            #[cfg(feature = "asic-rs-telemetry")]
            miner_telemetry: None,
            #[cfg(feature = "asic-rs-telemetry")]
            miner_telemetry_status: None,
        }
    }

    struct MockSv1Clients(Vec<Sv1ClientInfo>);
    impl Sv1ClientsMonitoring for MockSv1Clients {
        fn get_sv1_clients(&self) -> Vec<Sv1ClientInfo> {
            self.0.clone()
        }
    }

    #[test]
    fn sv1_get_client_by_id_found() {
        let monitor = MockSv1Clients(vec![
            create_sv1_client_info(1, Some(10.0)),
            create_sv1_client_info(2, Some(20.0)),
        ]);
        let found = monitor.get_sv1_client_by_id(2);
        assert!(found.is_some());
        assert_eq!(found.unwrap().client_id, 2);
    }

    #[test]
    fn sv1_get_client_by_id_not_found() {
        let monitor = MockSv1Clients(vec![create_sv1_client_info(1, Some(10.0))]);
        assert!(monitor.get_sv1_client_by_id(999).is_none());
    }

    #[test]
    fn sv1_summary_empty() {
        let monitor = MockSv1Clients(vec![]);
        let summary = monitor.get_sv1_clients_summary();
        assert_eq!(summary.total_clients, 0);
        assert_eq!(summary.total_hashrate, 0.0);
    }

    #[test]
    fn sv1_summary_skips_none_hashrate() {
        let monitor = MockSv1Clients(vec![
            create_sv1_client_info(1, Some(100.0)),
            create_sv1_client_info(2, None),
            create_sv1_client_info(3, Some(50.0)),
        ]);
        let summary = monitor.get_sv1_clients_summary();
        assert_eq!(summary.total_clients, 3);
        assert_eq!(summary.total_hashrate, 150.0);
    }

    #[test]
    fn share_counts_follow_the_submit_error_codes() {
        let mut counts = Sv1ShareCounts::default();
        counts.record(None);
        for code in [20, 21, 21, 22, 23, 24, 25, 99] {
            counts.record(Some(Sv1ShareRejection::from_code(code)));
        }
        assert_eq!(
            counts,
            Sv1ShareCounts {
                accepted: 1,
                rejected_job_not_found: 2,
                rejected_duplicate: 1,
                rejected_low_difficulty: 1,
                rejected_unauthorized_worker: 1,
                rejected_not_subscribed: 1,
                rejected_other: 2,
            }
        );
        for rejection in Sv1ShareRejection::ALL {
            assert_eq!(
                counts.rejected(rejection),
                match rejection {
                    Sv1ShareRejection::JobNotFound | Sv1ShareRejection::Other => 2,
                    _ => 1,
                }
            );
        }
    }

    #[test]
    fn summary_keeps_share_totals_of_departed_clients() {
        let totals = Sv1ShareCounts {
            accepted: 5,
            ..Default::default()
        };
        let summary = Sv1ClientsSummary::new(&[], totals, Sv1ShareDrops::default());
        assert_eq!(summary.total_clients, 0);
        assert_eq!(summary.shares, totals);
    }

    #[test]
    fn share_drops_are_counted_by_reason() {
        let mut drops = Sv1ShareDrops::default();
        drops.record(None);
        drops.record(None);
        for reason in Sv1ShareDropReason::ALL {
            drops.record(Some(reason));
        }
        drops.record(Some(Sv1ShareDropReason::Stale));
        assert_eq!(drops.below_upstream_target, 2);
        for reason in Sv1ShareDropReason::ALL {
            let expected = if reason == Sv1ShareDropReason::Stale {
                2
            } else {
                1
            };
            assert_eq!(drops.dropped(reason), expected, "{reason:?}");
        }
    }
}
