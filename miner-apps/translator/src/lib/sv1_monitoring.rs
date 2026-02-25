//! SV1 client monitoring integration for Sv1Server
//!
//! This module implements the Sv1ClientsMonitoring trait on `Sv1Server`.
use stratum_apps::monitoring::{
    client::ShareResponseCounts,
    sv1::{Sv1ClientInfo, Sv1ClientsMonitoring},
};

use crate::sv1::{downstream::downstream::Downstream, sv1_server::sv1_server::Sv1Server};

/// Helper to convert a Downstream to Sv1ClientInfo
fn downstream_to_sv1_client_info(downstream: &Downstream) -> Option<Sv1ClientInfo> {
    downstream
        .downstream_data
        .safe_lock(|dd| {
            let sc = &dd.share_counts;
            let share_responses = ShareResponseCounts {
                accepted: sc.accepted,
                blocks_found: 0,
                invalid: sc.failed_validation,
                stale: 0,
                invalid_job_id: sc.job_not_found,
                difficulty_too_low: 0,
                duplicate: 0,
                bad_extranonce_size: 0,
                invalid_channel_id: sc.channel_not_open,
            };

            Sv1ClientInfo {
                client_id: downstream.downstream_id,
                channel_id: dd.channel_id,
                authorized_worker_name: dd.authorized_worker_name.clone(),
                user_identity: dd.user_identity.clone(),
                target_hex: hex::encode(dd.target.to_be_bytes()),
                hashrate: dd.hashrate,
                extranonce1_hex: hex::encode(&dd.extranonce1),
                extranonce2_len: dd.extranonce2_len,
                version_rolling_mask: dd
                    .version_rolling_mask
                    .as_ref()
                    .map(|mask| format!("{:08x}", mask.0)),
                version_rolling_min_bit: dd
                    .version_rolling_min_bit
                    .as_ref()
                    .map(|bit| format!("{:08x}", bit.0)),
                share_responses: Some(share_responses),
            }
        })
        .ok()
}

impl Sv1ClientsMonitoring for Sv1Server {
    fn get_sv1_clients(&self) -> Vec<Sv1ClientInfo> {
        self.downstreams
            .iter()
            .filter_map(|downstream| downstream_to_sv1_client_info(downstream.value()))
            .collect()
    }

    fn get_sv1_client_by_id(&self, client_id: usize) -> Option<Sv1ClientInfo> {
        self.downstreams
            .get(&client_id)
            .and_then(|downstream| downstream_to_sv1_client_info(downstream.value()))
    }
}
