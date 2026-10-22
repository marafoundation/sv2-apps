//! Monitoring integration for Translation Proxy (tProxy)
//!
//! This module implements the ServerMonitoring trait on `ChannelManager`.
//! tProxy has server channels (upstream to pool) but no SV2 clients
//! (SV1 clients are handled separately by the SV1 monitoring module).

use std::sync::{Arc, RwLock};

use stratum_apps::monitoring::{
    server::{ServerExtendedChannelInfo, ServerInfo, ServerMonitoring},
    sv1::{Sv1ClientInfo, Sv1ClientsMonitoring},
    upstream::{UpstreamEndpointInfo, UpstreamInfo, UpstreamMonitoring},
};

use crate::{
    TproxyMode, sv1::Sv1Server, sv2::ChannelManager, upstream_walk::UpstreamStatus,
    utils::AGGREGATED_CHANNEL_ID,
};

/// The [`ChannelManager`] and [`Sv1Server`] currently serving miners.
///
/// Both are rebuilt on every fallback while the monitoring server keeps running, so monitoring
/// reads them through this handle. Between a fallback and the next upstream connection it is
/// empty and reports no channels and no SV1 clients, which is what is being served.
#[derive(Default)]
pub(crate) struct CurrentGeneration {
    inner: RwLock<Option<(Arc<ChannelManager>, Arc<Sv1Server>)>>,
}

impl CurrentGeneration {
    pub(crate) fn set(&self, channel_manager: Arc<ChannelManager>, sv1_server: Arc<Sv1Server>) {
        *self.inner.write().unwrap_or_else(|e| e.into_inner()) =
            Some((channel_manager, sv1_server));
    }

    pub(crate) fn clear(&self) {
        *self.inner.write().unwrap_or_else(|e| e.into_inner()) = None;
    }

    fn get(&self) -> Option<(Arc<ChannelManager>, Arc<Sv1Server>)> {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl ServerMonitoring for CurrentGeneration {
    fn get_server(&self) -> ServerInfo {
        match self.get() {
            Some((channel_manager, _)) => channel_manager.get_server(),
            None => ServerInfo {
                extended_channels: vec![],
                standard_channels: vec![],
                coinbase_outputs: vec![],
            },
        }
    }
}

impl Sv1ClientsMonitoring for CurrentGeneration {
    fn get_sv1_clients(&self) -> Vec<Sv1ClientInfo> {
        self.get()
            .map(|(_, sv1_server)| sv1_server.get_sv1_clients())
            .unwrap_or_default()
    }

    fn get_sv1_client_by_id(&self, client_id: usize) -> Option<Sv1ClientInfo> {
        self.get()
            .and_then(|(_, sv1_server)| sv1_server.get_sv1_client_by_id(client_id))
    }
}

impl UpstreamMonitoring for UpstreamStatus {
    fn get_upstream_info(&self) -> UpstreamInfo {
        let connected = self.connected();
        UpstreamInfo {
            upstreams: self
                .addresses()
                .iter()
                .enumerate()
                .map(|(index, address)| UpstreamEndpointInfo {
                    address: address.clone(),
                    connected: connected == Some(index),
                })
                .collect(),
            fallbacks: self.fallbacks(),
        }
    }

    fn is_connected(&self) -> bool {
        self.connected().is_some()
    }
}

impl ServerMonitoring for ChannelManager {
    fn get_server(&self) -> ServerInfo {
        let mut extended_channels = Vec::new();
        let standard_channels = Vec::new(); // tProxy only uses extended channels
        let report_hashrate = self.report_hashrate;

        match self.mode {
            TproxyMode::Aggregated => {
                // In Aggregated mode: one shared channel to the server
                // stored under AGGREGATED_CHANNEL_ID
                self.extended_channels.with(
                    &AGGREGATED_CHANNEL_ID,
                    |aggregated_extended_channel| {
                        let channel_id = aggregated_extended_channel.get_channel_id();
                        let target = *aggregated_extended_channel.get_target();
                        let extranonce_prefix =
                            aggregated_extended_channel.get_extranonce_prefix().to_vec();
                        let user_identity =
                            aggregated_extended_channel.get_user_identity().to_string();
                        let full_extranonce_size =
                            aggregated_extended_channel.get_full_extranonce_size();
                        let rollable_extranonce_size =
                            aggregated_extended_channel.get_rollable_extranonce_size();
                        let version_rolling = aggregated_extended_channel.is_version_rolling();
                        let nominal_hashrate = aggregated_extended_channel.get_nominal_hashrate();
                        let share_accounting = aggregated_extended_channel.get_share_accounting();
                        let shares_rejected_by_reason = share_accounting
                            .get_rejected_shares()
                            .map(|(reason, count)| (reason.to_string(), count))
                            .collect();
                        let shares_rejected = share_accounting.get_rejected_shares_count();

                        extended_channels.push(ServerExtendedChannelInfo {
                            channel_id,
                            user_identity,
                            nominal_hashrate: report_hashrate.then_some(nominal_hashrate),
                            target_hex: hex::encode(target.to_be_bytes()),
                            extranonce_prefix_hex: hex::encode(extranonce_prefix),
                            full_extranonce_size,
                            rollable_extranonce_size,
                            version_rolling,
                            shares_acknowledged: share_accounting.get_acknowledged_shares(),
                            shares_submitted: share_accounting.get_validated_shares(),
                            shares_rejected,
                            shares_rejected_by_reason,
                            acknowledged_work_sum: share_accounting.get_acknowledged_work_sum(),
                            validated_work_sum: share_accounting.get_validated_work_sum(),
                            best_diff: share_accounting.get_best_diff(),
                            blocks_found: share_accounting.get_blocks_found(),
                        });
                    },
                );
            }
            TproxyMode::NonAggregated => {
                // In NonAggregated mode: each downstream Sv1 miner has its own upstream Sv2
                // channel to the server
                self.extended_channels.for_each(|_, extended_channel| {
                    let channel_id = extended_channel.get_channel_id();
                    let target = extended_channel.get_target();
                    let extranonce_prefix = extended_channel.get_extranonce_prefix();
                    let user_identity = extended_channel.get_user_identity();
                    let share_accounting = extended_channel.get_share_accounting();
                    let shares_rejected_by_reason = share_accounting
                        .get_rejected_shares()
                        .map(|(reason, count)| (reason.to_string(), count))
                        .collect();
                    let shares_rejected = share_accounting.get_rejected_shares_count();

                    extended_channels.push(ServerExtendedChannelInfo {
                        channel_id,
                        user_identity: user_identity.to_string(),
                        nominal_hashrate: if report_hashrate {
                            Some(extended_channel.get_nominal_hashrate())
                        } else {
                            None
                        },
                        target_hex: hex::encode(target.to_be_bytes()),
                        extranonce_prefix_hex: hex::encode(extranonce_prefix),
                        full_extranonce_size: extended_channel.get_full_extranonce_size(),
                        rollable_extranonce_size: extended_channel.get_rollable_extranonce_size(),
                        version_rolling: extended_channel.is_version_rolling(),
                        shares_acknowledged: share_accounting.get_acknowledged_shares(),
                        shares_submitted: share_accounting.get_validated_shares(),
                        shares_rejected,
                        shares_rejected_by_reason,
                        acknowledged_work_sum: share_accounting.get_acknowledged_work_sum(),
                        validated_work_sum: share_accounting.get_validated_work_sum(),
                        best_diff: share_accounting.get_best_diff(),
                        blocks_found: share_accounting.get_blocks_found(),
                    });
                });
            }
        }

        ServerInfo {
            extended_channels,
            standard_channels,
            // the translator builds no coinbase
            coinbase_outputs: vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_channel::unbounded;
    use stratum_apps::stratum_core::{
        bitcoin::Target,
        channels_sv2::{client::extended::ExtendedChannel, extranonce_manager::ExtranoncePrefix},
    };

    fn create_test_channel_manager() -> ChannelManager {
        let (upstream_sender, _upstream_receiver) = unbounded();
        let (_upstream_sender2, upstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (_sv1_server_sender2, sv1_server_receiver) = unbounded();

        ChannelManager::new(
            upstream_sender,
            upstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            vec![],
            vec![],
            TproxyMode::Aggregated,
            None,
            true,
        )
    }

    fn create_extended_channel(channel_id: u32, user_identity: &str) -> ExtendedChannel {
        let prefix = ExtranoncePrefix::from_wire(vec![0x01, channel_id as u8]).unwrap();
        ExtendedChannel::new(
            channel_id,
            user_identity.to_string(),
            prefix,
            Target::from_le_bytes([0xff; 32]),
            1.0,
            true,
            4,
            None,
        )
        .unwrap()
    }

    #[test]
    fn aggregated_monitoring_uses_only_upstream_channel_accounting() {
        let manager = create_test_channel_manager();

        manager.extended_channels.insert(
            AGGREGATED_CHANNEL_ID,
            create_extended_channel(42, "upstream"),
        );
        manager
            .extended_channels
            .insert(7, create_extended_channel(7, "downstream"));

        manager
            .extended_channels
            .with_mut(&AGGREGATED_CHANNEL_ID, |channel| {
                channel.on_share_acknowledgement(2, 10);
            })
            .unwrap();
        manager
            .extended_channels
            .with_mut(&7, |channel| {
                channel.on_share_acknowledgement(5, 25);
            })
            .unwrap();

        let server = manager.get_server();
        let aggregated = server.extended_channels.first().unwrap();

        assert_eq!(server.extended_channels.len(), 1);
        assert_eq!(aggregated.channel_id, 42);
        assert_eq!(aggregated.shares_acknowledged, 2);
        assert_eq!(aggregated.acknowledged_work_sum, 10);
        assert_eq!(aggregated.validated_work_sum, 0.0);
    }
}
