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

use crate::error::PoolErrorKind;

pub use stratum_apps::payout::{PayoutMode, PayoutModeError};

pub(crate) type DownstreamMessage = (MiningOwned, Option<Vec<Tlv>>);

pub(crate) const ERROR_CODE_OPEN_MINING_CHANNEL_INCOMPATIBLE_PAYOUT_MODE: &str =
    "incompatible-payout-mode";

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

/// Applies the pool's nominal hashrate floor to a hashrate declared by a downstream.
///
/// A `floor` of `0.0` disables it. A declared rate that the target derivation refuses (NaN,
/// infinite or negative, including `-0.0`) is returned untouched, so it is still rejected with
/// the usual error rather than being papered over by the floor.
pub fn clamp_nominal_hash_rate(declared: f32, floor: f32) -> f32 {
    if floor > 0.0 && declared.is_finite() && !declared.is_sign_negative() {
        declared.max(floor)
    } else {
        declared
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_floor_leaves_the_declared_rate_unchanged() {
        for declared in [0.0, 1.0, 1e9, 2e14, f32::MAX] {
            assert_eq!(clamp_nominal_hash_rate(declared, 0.0), declared);
        }
    }

    #[test]
    fn declared_below_the_floor_gets_the_floor() {
        assert_eq!(clamp_nominal_hash_rate(1e9, 1e13), 1e13);
        assert_eq!(clamp_nominal_hash_rate(0.0, 1e13), 1e13);
    }

    #[test]
    fn declared_at_or_above_the_floor_is_kept() {
        assert_eq!(clamp_nominal_hash_rate(1e13, 1e13), 1e13);
        assert_eq!(clamp_nominal_hash_rate(2e14, 1e13), 2e14);
    }

    #[test]
    fn invalid_declared_rates_are_left_for_the_channel_to_reject() {
        for declared in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -1.0, -0.0] {
            let clamped = clamp_nominal_hash_rate(declared, 1e13);
            assert_eq!(clamped.to_bits(), declared.to_bits());
        }
    }
}
