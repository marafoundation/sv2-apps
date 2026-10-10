//! Monitoring integration for Pool
//!
//! This module implements the Sv2ClientsMonitoring and ServerMonitoring traits on
//! `ChannelManager`. Pool has clients (miners connecting to it) but no upstream server, so its
//! `ServerInfo` has no channels and only reports the coinbase outputs the pool pays.

use stratum_apps::{
    monitoring::{
        client::{ExtendedChannelInfo, StandardChannelInfo, Sv2ClientInfo, Sv2ClientsMonitoring},
        server::{CoinbaseOutputInfo, ServerInfo, ServerMonitoring},
    },
    stratum_core::channels_sv2::outputs::deserialize_outputs,
};

use crate::{channel_manager::ChannelManager, downstream::Downstream};

/// Helper to convert a Downstream to Sv2ClientInfo.
fn downstream_to_sv2_client_info(client: &Downstream) -> Option<Sv2ClientInfo> {
    let mut extended_channels = Vec::new();
    let mut standard_channels = Vec::new();

    client
        .extended_channels
        .for_each(|_channel_id, extended_channel| {
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
        });

    client
        .standard_channels
        .for_each(|_channel_id, standard_channel| {
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
        });

    Some(Sv2ClientInfo::new(
        client.downstream_id,
        extended_channels,
        standard_channels,
    ))
}

impl Sv2ClientsMonitoring for ChannelManager {
    fn get_sv2_clients(&self) -> Vec<Sv2ClientInfo> {
        // Clone Downstream references and release lock immediately to avoid contention
        // with template distribution and message handling
        let mut downstream_refs: Vec<Downstream> = Vec::new();
        self.downstreams
            .for_each(|_, downstream| downstream_refs.push(downstream.clone()));

        downstream_refs
            .iter()
            .filter_map(downstream_to_sv2_client_info)
            .collect()
    }

    fn get_sv2_client_by_id(&self, client_id: usize) -> Option<Sv2ClientInfo> {
        self.downstreams.with(&client_id, |downstream| {
            downstream_to_sv2_client_info(downstream)
        })?
    }
}

impl ServerMonitoring for ChannelManager {
    /// The scripts the pool's jobs pay: its loaded coinbase outputs, used for clients without a
    /// payout mode, and the outputs of each connected client's payout mode. This is the state
    /// jobs are built from, not a record of each job sent.
    fn get_server(&self) -> ServerInfo {
        let mut scripts: Vec<_> = deserialize_outputs(self.coinbase_outputs.clone())
            .unwrap_or_default()
            .into_iter()
            .map(|output| output.script_pubkey)
            .collect();
        self.downstreams.for_each(|_, downstream| {
            // a poisoned lock only drops this client's scripts from one refresh
            let _ = downstream.payout_mode.with(|payout_mode| {
                if let Some(payout_mode) = payout_mode {
                    // the value is irrelevant: only the scripts are reported
                    let outputs = payout_mode.coinbase_outputs(0, &self.coinbase_reward_script);
                    scripts.extend(outputs.into_iter().map(|output| output.script_pubkey));
                }
            });
        });
        ServerInfo {
            extended_channels: vec![],
            standard_channels: vec![],
            coinbase_outputs: CoinbaseOutputInfo::from_scripts(scripts, self.network),
        }
    }
}
