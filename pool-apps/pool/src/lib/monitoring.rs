//! Monitoring integration for Pool
//!
//! This module implements the Sv2ClientsMonitoring and HealthMonitoring traits
//! on `ChannelManager`. Pool only has clients (miners connecting to it), no
//! upstream server.

use std::time::Duration;

use stratum_apps::monitoring::{
    client::{ExtendedChannelInfo, StandardChannelInfo, Sv2ClientInfo, Sv2ClientsMonitoring},
    health::{HealthMonitoring, NodeHealth},
};

use crate::{channel_manager::ChannelManager, downstream::Downstream};

/// How long the pool may go without a fresh template or prev-hash from the
/// bitcoin node / Template Provider before it is treated as unavailable.
///
/// During normal operation the Template Provider pushes a new template on every
/// chain tip and whenever the mempool changes, so a healthy node updates far
/// more frequently than this. A gap this long indicates the node is unreachable
/// or stalled (e.g. a hung Template Provider whose TCP connection stays open).
const NODE_TEMPLATE_STALENESS_TIMEOUT: Duration = Duration::from_secs(120);

/// Helper to convert a Downstream to Sv2ClientInfo.
/// Returns None if the lock cannot be acquired (graceful degradation for monitoring).
fn downstream_to_sv2_client_info(client: &Downstream) -> Option<Sv2ClientInfo> {
    client
        .downstream_data
        .safe_lock(|dd| {
            let mut extended_channels = Vec::new();
            let mut standard_channels = Vec::new();

            for (_channel_id, extended_channel) in dd.extended_channels.iter() {
                let channel_id = extended_channel.get_channel_id();
                let target = extended_channel.get_target();
                let requested_max_target = extended_channel.get_requested_max_target();
                let user_identity = extended_channel.get_user_identity();
                let share_accounting = extended_channel.get_share_accounting();

                extended_channels.push(ExtendedChannelInfo {
                    channel_id,
                    user_identity: user_identity.to_string(),
                    nominal_hashrate: extended_channel.get_nominal_hashrate(),
                    stable_hashrate: extended_channel.get_stable_hashrate(),
                    target_hex: hex::encode(target.to_be_bytes()),
                    requested_max_target_hex: hex::encode(requested_max_target.to_be_bytes()),
                    extranonce_prefix_hex: hex::encode(extended_channel.get_extranonce_prefix()),
                    full_extranonce_size: extended_channel.get_full_extranonce_size(),
                    rollable_extranonce_size: extended_channel.get_rollable_extranonce_size(),
                    expected_shares_per_minute: extended_channel.get_shares_per_minute(),
                    shares_accepted: share_accounting.get_shares_accepted(),
                    shares_rejected: share_accounting.get_rejected_shares_count(),
                    shares_rejected_by_reason: share_accounting
                        .get_rejected_shares()
                        .map(|(reason, count)| (reason.to_string(), count))
                        .collect(),
                    share_work_sum: share_accounting.get_share_work_sum(),
                    last_share_sequence_number: share_accounting.get_last_share_sequence_number(),
                    best_diff: share_accounting.get_best_diff(),
                    last_batch_accepted: share_accounting.get_last_batch_accepted(),
                    last_batch_work_sum: share_accounting.get_last_batch_work_sum(),
                    share_batch_size: share_accounting.get_share_batch_size(),
                    blocks_found: share_accounting.get_blocks_found(),
                });
            }

            for (_channel_id, standard_channel) in dd.standard_channels.iter() {
                let channel_id = standard_channel.get_channel_id();
                let target = standard_channel.get_target();
                let requested_max_target = standard_channel.get_requested_max_target();
                let user_identity = standard_channel.get_user_identity();
                let share_accounting = standard_channel.get_share_accounting();

                standard_channels.push(StandardChannelInfo {
                    channel_id,
                    user_identity: user_identity.to_string(),
                    nominal_hashrate: standard_channel.get_nominal_hashrate(),
                    stable_hashrate: standard_channel.get_stable_hashrate(),
                    target_hex: hex::encode(target.to_be_bytes()),
                    requested_max_target_hex: hex::encode(requested_max_target.to_be_bytes()),
                    extranonce_prefix_hex: hex::encode(standard_channel.get_extranonce_prefix()),
                    expected_shares_per_minute: standard_channel.get_shares_per_minute(),
                    shares_accepted: share_accounting.get_shares_accepted(),
                    shares_rejected: share_accounting.get_rejected_shares_count(),
                    shares_rejected_by_reason: share_accounting
                        .get_rejected_shares()
                        .map(|(reason, count)| (reason.to_string(), count))
                        .collect(),
                    share_work_sum: share_accounting.get_share_work_sum(),
                    last_share_sequence_number: share_accounting.get_last_share_sequence_number(),
                    best_diff: share_accounting.get_best_diff(),
                    last_batch_accepted: share_accounting.get_last_batch_accepted(),
                    last_batch_work_sum: share_accounting.get_last_batch_work_sum(),
                    share_batch_size: share_accounting.get_share_batch_size(),
                    blocks_found: share_accounting.get_blocks_found(),
                });
            }

            Sv2ClientInfo {
                client_id: client.downstream_id,
                extended_channels,
                standard_channels,
            }
        })
        .ok()
}

impl Sv2ClientsMonitoring for ChannelManager {
    fn get_sv2_clients(&self) -> Vec<Sv2ClientInfo> {
        // Clone Downstream references and release lock immediately to avoid contention
        // with template distribution and message handling
        let downstream_refs: Vec<Downstream> = self
            .channel_manager_data
            .safe_lock(|data| data.downstream.values().cloned().collect())
            .unwrap_or_default();

        downstream_refs
            .iter()
            .filter_map(downstream_to_sv2_client_info)
            .collect()
    }

    fn get_sv2_client_by_id(&self, client_id: usize) -> Option<Sv2ClientInfo> {
        self.channel_manager_data
            .safe_lock(|d| {
                d.downstream
                    .get(&client_id)
                    .and_then(downstream_to_sv2_client_info)
            })
            .unwrap_or(None)
    }
}

impl HealthMonitoring for ChannelManager {
    /// Report the pool unhealthy whenever the bitcoin node / Template Provider
    /// is unavailable: before the first template arrives (the node is still
    /// performing its initial block download, or hasn't connected yet) and if
    /// templates stop arriving (the node went away or stalled).
    fn node_health(&self) -> NodeHealth {
        self.channel_manager_data
            .safe_lock(|data| match data.last_node_update {
                None => NodeHealth::unavailable(
                    "no block template received yet — bitcoin node unavailable or \
                     performing initial block download",
                ),
                Some(last) => {
                    let elapsed = last.elapsed();
                    if elapsed > NODE_TEMPLATE_STALENESS_TIMEOUT {
                        NodeHealth::unavailable(format!(
                            "no block template received for {}s — bitcoin node unavailable",
                            elapsed.as_secs()
                        ))
                    } else {
                        NodeHealth::healthy("receiving block templates from bitcoin node")
                    }
                }
            })
            // A poisoned lock means the channel manager panicked mid-update; we
            // can no longer vouch for the node, so report unhealthy.
            .unwrap_or_else(|_| NodeHealth::unavailable("unable to read channel manager state"))
    }
}
