use std::{convert::TryFrom, net::SocketAddr};
use stratum_apps::{
    stratum_core::{
        binary_sv2::Str0255Owned,
        common_messages_sv2::{Protocol, SetupConnectionOwned},
        mining_sv2::CloseChannelOwned,
        parsers_sv2::{MiningOwned, Tlv},
    },
    utils::types::ChannelId,
};

use crate::{config::PayoutModes, error::PoolErrorKind};

pub use stratum_apps::payout::{PayoutMode, PayoutModeError};

/// Resolves the payout mode a channel's `user_identity` selects, as allowed by `payout_modes`.
///
/// An identity with no payout mode pays the pool ([`PayoutMode::FullDonation`]). On `Err`, the
/// channel must be refused with `invalid-user-identity`; the string says why, for the log.
pub(crate) fn resolve_payout_mode(
    user_identity: &str,
    payout_modes: PayoutModes,
) -> Result<PayoutMode, String> {
    match PayoutMode::try_from(user_identity) {
        Err(PayoutModeError::NoPayoutMode(_)) | Ok(PayoutMode::FullDonation) => {
            Ok(PayoutMode::FullDonation)
        }
        Err(_) => Err("does not match any supported identity format".to_string()),
        Ok(mode) if payout_modes == PayoutModes::Any => Ok(mode),
        Ok(mode) => Err(format!(
            "selects payout mode {mode}, which payout_modes = \"pool_only\" refuses"
        )),
    }
}

pub(crate) type DownstreamMessage = (MiningOwned, Option<Vec<Tlv>>);

/// Constructs a `SetupConnection` message for the mining protocol.
#[allow(clippy::result_large_err)]
pub fn get_setup_connection_message(
    min_version: u16,
    max_version: u16,
    address: &SocketAddr,
) -> Result<SetupConnectionOwned, PoolErrorKind> {
    let endpoint_host = address.ip().to_string().try_into()?;
    let vendor = "".try_into()?;
    let hardware_version = "".try_into()?;
    let firmware = "".try_into()?;
    let device_id = "".try_into()?;
    let flags = 0b0000_0000_0000_0000_0000_0000_0000_0110;
    Ok(SetupConnectionOwned {
        protocol: Protocol::MiningProtocol,
        min_version,
        max_version,
        flags,
        endpoint_host,
        endpoint_port: address.port(),
        vendor,
        hardware_version,
        firmware,
        device_id,
    })
}

/// Constructs a `SetupConnection` message for the Template Provider (TP).
#[allow(clippy::result_large_err)]
pub fn get_setup_connection_message_tp(
    address: SocketAddr,
) -> Result<SetupConnectionOwned, PoolErrorKind> {
    let endpoint_host = address.ip().to_string().try_into()?;
    let vendor = "".try_into()?;
    let hardware_version = "".try_into()?;
    let firmware = "".try_into()?;
    let device_id = "".try_into()?;
    Ok(SetupConnectionOwned {
        protocol: Protocol::TemplateDistributionProtocol,
        min_version: 2,
        max_version: 2,
        flags: 0b0000_0000_0000_0000_0000_0000_0000_0000,
        endpoint_host,
        endpoint_port: address.port(),
        vendor,
        hardware_version,
        firmware,
        device_id,
    })
}

/// Creates a [`CloseChannel`] message for the given channel ID and reason.
///
/// The `msg` is converted into a [`Str0255`] reason code.  
/// If conversion fails, this function will panic.
pub(crate) fn create_close_channel_msg(channel_id: ChannelId, msg: &str) -> CloseChannelOwned {
    CloseChannelOwned {
        channel_id,
        reason_code: Str0255Owned::try_from(msg).expect("Could not convert message."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ADDR: &str = "tb1qpusf5256yxv50qt0pm0tue8k952fsu5lzsphft";

    #[test]
    fn resolve_payout_mode_under_each_setting() {
        let solo = format!("sri/solo/{ADDR}/worker.1");
        let legacy = ADDR.to_string();
        let legacy_with_worker = format!("{ADDR}.worker.1");
        let donate = format!("sri/donate/10/{ADDR}/worker.1");
        // (user_identity, accepted under Any, accepted under PoolOnly)
        let cases: [(&str, bool, bool); 9] = [
            ("mara-staging-mainnet-translator", true, true),
            ("marapool.miner1", true, true),
            ("", true, true),
            ("sri/donate", true, true),
            ("sri/donate/worker.1", true, true),
            (&solo, true, false),
            (&legacy, true, false),
            (&legacy_with_worker, true, false),
            (&donate, true, false),
        ];
        for (identity, any, pool_only) in cases {
            assert_eq!(
                resolve_payout_mode(identity, PayoutModes::Any).is_ok(),
                any,
                "{identity:?} under Any"
            );
            let resolved = resolve_payout_mode(identity, PayoutModes::PoolOnly);
            assert_eq!(resolved.is_ok(), pool_only, "{identity:?} under PoolOnly");
            // whatever PoolOnly accepts pays the pool
            if let Ok(mode) = resolved {
                assert!(matches!(mode, PayoutMode::FullDonation), "{identity:?}");
            }
        }
        // Any keeps the mode the identity selects
        assert!(matches!(
            resolve_payout_mode(&solo, PayoutModes::Any),
            Ok(PayoutMode::Solo { .. })
        ));
        assert!(matches!(
            resolve_payout_mode(&donate, PayoutModes::Any),
            Ok(PayoutMode::Donate { percentage: 10, .. })
        ));
    }

    #[test]
    fn resolve_payout_mode_refuses_malformed_identities_under_both_settings() {
        for identity in [
            "sri/solo/tb1qbalieiro/worker.1",
            "sri/unknown/x",
            "sri/donate/0/x/y",
        ] {
            for payout_modes in [PayoutModes::Any, PayoutModes::PoolOnly] {
                assert!(
                    resolve_payout_mode(identity, payout_modes).is_err(),
                    "{identity:?} under {payout_modes:?}"
                );
            }
        }
    }
}
