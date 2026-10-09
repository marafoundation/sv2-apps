//! Monitoring integration for Pool
//!
//! This module implements the Sv2ClientsMonitoring trait on `ChannelManager`.
//! Pool only has clients (miners connecting to it), no upstream server.

use std::collections::BTreeSet;

use stratum_apps::{
    monitoring::client::{
        CoinbaseOutputInfo, ExtendedChannelInfo, StandardChannelInfo, Sv2ClientInfo,
        Sv2ClientsMonitoring,
    },
    stratum_core::bitcoin::{Address, Network},
};

use crate::{
    channel_manager::{ChannelManager, SentCoinbaseOutputs},
    downstream::Downstream,
};

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

/// Converts the recorded outputs to their monitoring form. Addresses use `network`; without one,
/// or for a script with no address form, `address` is empty. An overflow of the recorded set is
/// reported as a `script_hex` of `overflow`, which no ledger can authorize.
fn coinbase_output_info(
    sent: &SentCoinbaseOutputs,
    network: Option<Network>,
) -> BTreeSet<CoinbaseOutputInfo> {
    let mut info: BTreeSet<CoinbaseOutputInfo> = sent
        .scripts
        .iter()
        .map(|script| CoinbaseOutputInfo {
            script_hex: script.to_hex_string(),
            address: network
                .and_then(|network| Address::from_script(script, network).ok())
                .map(|address| address.to_string())
                .unwrap_or_default(),
        })
        .collect();
    if sent.overflowed {
        info.insert(CoinbaseOutputInfo {
            script_hex: "overflow".to_string(),
            address: String::new(),
        });
    }
    info
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

    /// Every output the pool has put in a job since start-up, including its loaded outputs.
    /// That includes outputs no config file holds: per-client payout modes derived from
    /// `user_identity`, and custom jobs declared by JD clients. See [`SentCoinbaseOutputs`].
    fn get_coinbase_outputs(&self) -> BTreeSet<CoinbaseOutputInfo> {
        self.sent_coinbase_outputs
            .with(|sent| coinbase_output_info(sent, self.network))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use stratum_apps::stratum_core::bitcoin::{Amount, ScriptBuf, TxOut};

    fn script_for(address: &str, network: Network) -> ScriptBuf {
        Address::from_str(address)
            .unwrap()
            .require_network(network)
            .unwrap()
            .script_pubkey()
    }

    #[test]
    fn recorded_outputs_render_addresses_and_drop_commitments() {
        let pool_script = script_for("32i1m6gNcSHwiPX9nfTNXVjme9j5DU8y5g", Network::Bitcoin);
        let miner_script = script_for(
            "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq",
            Network::Bitcoin,
        );
        let witness_commitment = ScriptBuf::from_hex(
            "6a24aa21a9ed0000000000000000000000000000000000000000000000000000000000000000",
        )
        .unwrap();
        let output = |sats: u64, script: &ScriptBuf| TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: script.clone(),
        };

        let mut sent = SentCoinbaseOutputs::default();
        sent.record(&[output(1_000, &pool_script), output(0, &witness_commitment)]);
        // a later job paying the same script again, a miner, and an OP_RETURN that burns value
        sent.record(&[
            output(2_000, &pool_script),
            output(500, &miner_script),
            output(1, &witness_commitment),
        ]);

        let info = coinbase_output_info(&sent, Some(Network::Bitcoin));
        let expected = BTreeSet::from([
            CoinbaseOutputInfo {
                script_hex: pool_script.to_hex_string(),
                address: "32i1m6gNcSHwiPX9nfTNXVjme9j5DU8y5g".to_string(),
            },
            CoinbaseOutputInfo {
                script_hex: miner_script.to_hex_string(),
                address: "bc1qar0srrr7xfkvy5l643lydnw9re59gtzzwf5mdq".to_string(),
            },
            CoinbaseOutputInfo {
                script_hex: witness_commitment.to_hex_string(),
                address: String::new(),
            },
        ]);
        assert_eq!(info, expected);

        // without a known network every output is still reported, by script only
        let info = coinbase_output_info(&sent, None);
        assert_eq!(info.len(), 3);
        assert!(info.iter().all(|output| output.address.is_empty()));
    }

    #[test]
    fn recorded_outputs_are_bounded_and_report_overflow() {
        let mut sent = SentCoinbaseOutputs::default();
        let scripts: Vec<ScriptBuf> = (0..=crate::channel_manager::MAX_SENT_COINBASE_OUTPUTS)
            .map(|i| ScriptBuf::from_bytes((i as u32).to_le_bytes().to_vec()))
            .collect();
        let outputs: Vec<TxOut> = scripts
            .iter()
            .map(|script| TxOut {
                value: Amount::from_sat(1),
                script_pubkey: script.clone(),
            })
            .collect();

        sent.record(&outputs[..outputs.len() - 1]);
        assert!(!sent.overflowed);
        // a script already recorded is not an overflow
        sent.record(&outputs[..1]);
        assert!(!sent.overflowed);

        sent.record(&outputs[outputs.len() - 1..]);
        assert!(sent.overflowed);
        assert_eq!(
            sent.scripts.len(),
            crate::channel_manager::MAX_SENT_COINBASE_OUTPUTS
        );
        assert!(
            coinbase_output_info(&sent, None).contains(&CoinbaseOutputInfo {
                script_hex: "overflow".to_string(),
                address: String::new(),
            })
        );
    }
}
