//! Sv2 client monitoring types
//!
//! These types are for monitoring **Sv2 clients** (downstream connections).
//! Each client can have multiple channels opened with the app.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Per-channel breakdown of all share submission outcomes.
///
/// Enables monitoring of rejection rates and root-cause analysis:
/// a high `stale` count may indicate slow job distribution, a high
/// `invalid_job_id` count may signal template propagation issues, etc.
///
/// All counters are monotonically increasing over the lifetime of the channel.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ShareResponseCounts {
    /// Shares that passed validation (includes block-found shares).
    pub accepted: u32,
    /// Blocks found (subset of accepted).
    pub blocks_found: u32,
    /// `invalid-share` — share failed hash validation.
    pub invalid: u32,
    /// `stale-share` — share references an outdated prev_hash / job.
    pub stale: u32,
    /// `invalid-job-id` — job_id in the share does not match any known job.
    pub invalid_job_id: u32,
    /// `difficulty-too-low` — share hash does not meet the channel's target.
    pub difficulty_too_low: u32,
    /// `duplicate-share` — share was already submitted.
    pub duplicate: u32,
    /// `bad-extranonce-size` — extranonce size mismatch (extended channels only).
    pub bad_extranonce_size: u32,
    /// `invalid-channel-id` — the channel_id in the share message was not found.
    pub invalid_channel_id: u32,
}

impl ShareResponseCounts {
    /// Merge a single-share outcome delta into the running totals.
    pub fn accumulate(&mut self, delta: &ShareResponseCounts) {
        self.accepted += delta.accepted;
        self.blocks_found += delta.blocks_found;
        self.invalid += delta.invalid;
        self.stale += delta.stale;
        self.invalid_job_id += delta.invalid_job_id;
        self.difficulty_too_low += delta.difficulty_too_low;
        self.duplicate += delta.duplicate;
        self.bad_extranonce_size += delta.bad_extranonce_size;
        self.invalid_channel_id += delta.invalid_channel_id;
    }
}

/// Information about an extended channel
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExtendedChannelInfo {
    pub channel_id: u32,
    pub user_identity: String,
    pub nominal_hashrate: f32,
    pub target_hex: String,
    pub requested_max_target_hex: String,
    pub extranonce_prefix_hex: String,
    pub full_extranonce_size: usize,
    pub rollable_extranonce_size: u16,
    pub expected_shares_per_minute: f32,
    pub shares_accepted: u32,
    pub share_work_sum: f64,
    pub last_share_sequence_number: u32,
    pub best_diff: f64,
    pub last_batch_accepted: u32,
    pub last_batch_work_sum: f64,
    pub share_batch_size: usize,
    pub blocks_found: u32,
    /// Per-outcome share response counters (only populated by the pool).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_responses: Option<ShareResponseCounts>,
}

/// Information about a standard channel
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StandardChannelInfo {
    pub channel_id: u32,
    pub user_identity: String,
    pub nominal_hashrate: f32,
    pub target_hex: String,
    pub requested_max_target_hex: String,
    pub extranonce_prefix_hex: String,
    pub expected_shares_per_minute: f32,
    pub shares_accepted: u32,
    pub share_work_sum: f64,
    pub last_share_sequence_number: u32,
    pub best_diff: f64,
    pub last_batch_accepted: u32,
    pub last_batch_work_sum: f64,
    pub share_batch_size: usize,
    pub blocks_found: u32,
    /// Per-outcome share response counters (only populated by the pool).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub share_responses: Option<ShareResponseCounts>,
}

/// Full information about a single Sv2 client including all channels
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Sv2ClientInfo {
    pub client_id: usize,
    pub extended_channels: Vec<ExtendedChannelInfo>,
    pub standard_channels: Vec<StandardChannelInfo>,
}

impl Sv2ClientInfo {
    /// Get total number of channels for this client
    pub fn total_channels(&self) -> usize {
        self.extended_channels.len() + self.standard_channels.len()
    }

    /// Get total hashrate for this client
    pub fn total_hashrate(&self) -> f32 {
        self.extended_channels
            .iter()
            .map(|c| c.nominal_hashrate)
            .sum::<f32>()
            + self
                .standard_channels
                .iter()
                .map(|c| c.nominal_hashrate)
                .sum::<f32>()
    }

    /// Convert to metadata (without channel arrays)
    pub fn to_metadata(&self) -> Sv2ClientMetadata {
        Sv2ClientMetadata {
            client_id: self.client_id,
            extended_channels_count: self.extended_channels.len(),
            standard_channels_count: self.standard_channels.len(),
            total_hashrate: self.total_hashrate(),
        }
    }
}

/// Sv2 client metadata without channel arrays (for listings)
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Sv2ClientMetadata {
    pub client_id: usize,
    pub extended_channels_count: usize,
    pub standard_channels_count: usize,
    pub total_hashrate: f32,
}

/// Aggregate information about all Sv2 clients
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Sv2ClientsSummary {
    pub total_clients: usize,
    pub total_channels: usize,
    pub extended_channels: usize,
    pub standard_channels: usize,
    pub total_hashrate: f32,
}

/// Trait for monitoring Sv2 clients (downstream connections)
pub trait Sv2ClientsMonitoring: Send + Sync {
    /// Get all Sv2 clients with their channels
    fn get_sv2_clients(&self) -> Vec<Sv2ClientInfo>;

    /// Get a single Sv2 client by client_id
    ///
    /// Default implementation does O(n) scan. Override for O(1) lookup
    /// if your implementation uses a HashMap internally.
    fn get_sv2_client_by_id(&self, client_id: usize) -> Option<Sv2ClientInfo> {
        self.get_sv2_clients()
            .into_iter()
            .find(|c| c.client_id == client_id)
    }

    /// Get summary of all Sv2 clients
    fn get_sv2_clients_summary(&self) -> Sv2ClientsSummary {
        let clients = self.get_sv2_clients();
        let extended: usize = clients.iter().map(|c| c.extended_channels.len()).sum();
        let standard: usize = clients.iter().map(|c| c.standard_channels.len()).sum();

        Sv2ClientsSummary {
            total_clients: clients.len(),
            total_channels: extended + standard,
            extended_channels: extended,
            standard_channels: standard,
            total_hashrate: clients.iter().map(|c| c.total_hashrate()).sum(),
        }
    }
}
