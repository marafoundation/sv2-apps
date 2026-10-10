//! Shared payout-mode parsing and coinbase-output distribution helpers.
//!
//! This module is meant for applications that accept SRI-style mining identities and need a
//! single source of truth for reward distribution. Pool-like applications can use
//! [`crate::payout::PayoutMode::coinbase_outputs`] to build outputs, while proxy/client
//! applications can use [`crate::payout::PayoutMode::validate_coinbase_outputs`] or
//! [`crate::payout::PayoutMode::validate_coinbase_tx_parts`] to verify upstream jobs.

use std::fmt;

use crate::{
    config_helpers::CoinbaseRewardScript,
    stratum_core::bitcoin::{
        Amount, Script, ScriptBuf, Transaction, TxOut, VarInt,
        consensus::{Decodable, deserialize},
    },
};

// Legacy solo identities do not encode a fee policy, so allow at most a 10% service fee.
const MIN_LEGACY_SOLO_PAYOUT_PERCENTAGE: u8 = 90;

// Consensus limit on a coinbase scriptSig.
const MAX_COINBASE_SCRIPT_SIG_LEN: u64 = 100;

/// Which [`PayoutMode`]s a pool lets a `user_identity` select. Pool config key `payout_modes`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllowedPayoutModes {
    /// Every payout mode (default).
    #[default]
    Any,
    /// Only [`PayoutMode::FullDonation`]: the whole reward goes to the pool. Solo, legacy solo and
    /// donate identities are refused, and declared coinbases must pass
    /// [`validate_pool_only_outputs`].
    PoolOnly,
}

impl AllowedPayoutModes {
    /// Returns `mode` if this setting allows it, so it chains after [`PayoutMode::try_from`].
    pub fn check(self, mode: PayoutMode) -> Result<PayoutMode, PayoutModeError> {
        if self == Self::PoolOnly && !matches!(mode, PayoutMode::FullDonation) {
            return Err(PayoutModeError::NotAllowed(mode.to_string()));
        }
        Ok(mode)
    }
}

/// Verifies that a coinbase pays only `pool_script`: every output pays it or is a zero-value
/// OP_RETURN (a commitment), and at least one output pays it.
///
/// The total is not compared with the block reward, which the caller may not know: value left
/// unclaimed pays nobody.
pub fn validate_pool_only_outputs(
    outputs: &[TxOut],
    pool_script: &Script,
) -> Result<(), PayoutValidationError> {
    if let Some(index) = outputs.iter().position(|output| {
        output.script_pubkey.as_script() != pool_script
            && !(output.script_pubkey.is_op_return() && output.value == Amount::ZERO)
    }) {
        return Err(PayoutValidationError::NonPoolOutput(index));
    }
    if !outputs
        .iter()
        .any(|output| output.script_pubkey.as_script() == pool_script)
    {
        return Err(PayoutValidationError::NoSpendableOutputs);
    }
    Ok(())
}

/// Reassembles a coinbase from its SV2 split, `prefix | zeroed extranonce | suffix`, and decodes
/// it.
///
/// The extranonce bytes are zero-filled because callers only need the transaction to decode and
/// expose its outputs and input; the actual extranonce value does not affect them.
pub fn decode_coinbase_tx_parts(
    coinbase_tx_prefix: &[u8],
    coinbase_tx_suffix: &[u8],
    full_extranonce_size: usize,
) -> Result<Transaction, PayoutValidationError> {
    let mut coinbase = Vec::with_capacity(
        coinbase_tx_prefix.len() + full_extranonce_size + coinbase_tx_suffix.len(),
    );
    coinbase.extend_from_slice(coinbase_tx_prefix);
    coinbase.resize(coinbase.len() + full_extranonce_size, 0);
    coinbase.extend_from_slice(coinbase_tx_suffix);

    deserialize(&coinbase)
        .map_err(|e| PayoutValidationError::DecodeCoinbaseTransaction(e.to_string()))
}

/// [`decode_coinbase_tx_parts`] for a split that does not carry the extranonce size, such as
/// `DeclareMiningJob`'s: the prefix must end inside the scriptSig, and the extranonce fills the
/// rest of the scriptSig length the prefix declares.
///
/// Rejects, without panicking or allocating by it, a prefix that is too short or ends past the
/// scriptSig, and a scriptSig length over the consensus limit of 100 bytes.
pub fn decode_declared_coinbase_tx(
    coinbase_tx_prefix: &[u8],
    coinbase_tx_suffix: &[u8],
) -> Result<Transaction, PayoutValidationError> {
    let invalid = |reason: &str| PayoutValidationError::DecodeCoinbaseTransaction(reason.into());
    // version(4) [+ segwit marker and flag(2)] + input count(1) + outpoint(36)
    let script_sig_len_offset = if coinbase_tx_prefix.get(4..6) == Some(&[0, 1]) {
        43
    } else {
        41
    };
    let mut script_sig = coinbase_tx_prefix
        .get(script_sig_len_offset..)
        .ok_or_else(|| invalid("prefix ends before the scriptSig length"))?;
    let script_sig_len = VarInt::consensus_decode(&mut script_sig)
        .map_err(|e| PayoutValidationError::DecodeCoinbaseTransaction(e.to_string()))?
        .0;
    if script_sig_len > MAX_COINBASE_SCRIPT_SIG_LEN {
        return Err(invalid("scriptSig is longer than 100 bytes"));
    }
    // `script_sig` now holds the scriptSig bytes already in the prefix.
    let full_extranonce_size = (script_sig_len as usize)
        .checked_sub(script_sig.len())
        .ok_or_else(|| invalid("prefix ends past the scriptSig"))?;
    decode_coinbase_tx_parts(coinbase_tx_prefix, coinbase_tx_suffix, full_extranonce_size)
}

/// Represents the payout mode encoded by a mining `user_identity`.
///
/// Supported patterns:
/// - `sri/solo/<payout_address>/<worker_name>`: full reward goes to the miner.
/// - `<payout_address>` or `<payout_address>.<worker_name>`: legacy solo mode; payout verification
///   checks that the miner address receives at least 90% of spendable coinbase outputs.
/// - `sri/donate/<percentage>/<payout_address>/<worker_name>`: pool receives `percentage`, miner
///   receives the remainder.
/// - `sri/donate/<worker_name>`: full reward goes to the pool.
#[derive(Debug, Clone)]
pub enum PayoutMode {
    /// Solo mode: miner receives full block reward.
    Solo {
        /// Miner payout address as supplied in `user_identity`.
        address: String,
        /// Miner payout script.
        script: CoinbaseRewardScript,
    },
    /// Legacy solo mode: miner payout address must receive at least 90% of spendable coinbase
    /// outputs.
    LegacySolo {
        /// Miner payout address as supplied in `user_identity`.
        address: String,
        /// Miner payout script.
        script: CoinbaseRewardScript,
    },
    /// Donate mode: pool receives specified percentage, miner gets remainder.
    Donate {
        /// Pool's portion, from 1 to 99.
        percentage: u8,
        /// Miner payout address as supplied in `user_identity`.
        address: String,
        /// Miner payout script.
        script: CoinbaseRewardScript,
    },
    /// Full donation mode: full reward goes to the pool.
    FullDonation,
}

impl PayoutMode {
    /// Creates coinbase outputs for this payout mode.
    pub fn coinbase_outputs(
        &self,
        total_value: u64,
        pool_script: &CoinbaseRewardScript,
    ) -> Vec<TxOut> {
        match self {
            Self::Solo {
                script: coinbase_script,
                ..
            }
            | Self::LegacySolo {
                script: coinbase_script,
                ..
            } => {
                vec![TxOut {
                    value: Amount::from_sat(total_value),
                    script_pubkey: coinbase_script.script_pubkey(),
                }]
            }

            Self::Donate {
                percentage,
                script: miner_script,
                ..
            } => {
                let pool_value = (total_value * *percentage as u64) / 100;
                let miner_value = total_value.saturating_sub(pool_value);

                vec![
                    TxOut {
                        value: Amount::from_sat(pool_value),
                        script_pubkey: pool_script.script_pubkey(),
                    },
                    TxOut {
                        value: Amount::from_sat(miner_value),
                        script_pubkey: miner_script.script_pubkey(),
                    },
                ]
            }

            Self::FullDonation => {
                vec![TxOut {
                    value: Amount::from_sat(total_value),
                    script_pubkey: pool_script.script_pubkey(),
                }]
            }
        }
    }

    /// Verifies that spendable outputs match the miner-side payout encoded by this mode.
    ///
    /// OP_RETURN outputs are ignored. [`PayoutMode::FullDonation`] has no miner payout address, so
    /// it returns success without checking a miner output.
    pub fn validate_coinbase_outputs(
        &self,
        outputs: &[TxOut],
    ) -> Result<(), PayoutValidationError> {
        let Some(script_pubkey) = self.miner_script_pubkey() else {
            return Ok(());
        };

        let total_spendable_sats = outputs
            .iter()
            .filter(|output| !output.script_pubkey.is_op_return())
            .map(|output| output.value.to_sat())
            .sum();
        if total_spendable_sats == 0 {
            return Err(PayoutValidationError::NoSpendableOutputs);
        }

        let actual_miner_sats = outputs
            .iter()
            .filter(|output| !output.script_pubkey.is_op_return())
            .filter(|output| output.script_pubkey.as_bytes() == script_pubkey.as_bytes())
            .map(|output| output.value.to_sat())
            .sum();
        if matches!(self, Self::LegacySolo { .. }) {
            let expected_miner_sats = self.expected_legacy_solo_miner_sats(total_spendable_sats);
            if actual_miner_sats < expected_miner_sats {
                return Err(PayoutValidationError::PayoutMismatch {
                    address: self
                        .miner_address()
                        .expect("miner script exists only when miner address exists")
                        .to_string(),
                    expected_sats: expected_miner_sats,
                    expected_percentage: MIN_LEGACY_SOLO_PAYOUT_PERCENTAGE,
                    total_spendable_sats,
                    actual_sats: actual_miner_sats,
                });
            }

            return Ok(());
        }

        let expected_miner_sats = self.expected_miner_sats(total_spendable_sats);
        if actual_miner_sats != expected_miner_sats {
            return Err(PayoutValidationError::PayoutMismatch {
                address: self
                    .miner_address()
                    .expect("miner script exists only when miner address exists")
                    .to_string(),
                expected_sats: expected_miner_sats,
                expected_percentage: self.expected_miner_percentage(),
                total_spendable_sats,
                actual_sats: actual_miner_sats,
            });
        }

        Ok(())
    }

    /// Verifies `NewExtendedMiningJob` coinbase transaction parts against this payout mode.
    ///
    /// The SV2 split only guarantees that `coinbase_tx_suffix` is the part after the full
    /// extranonce. The suffix can still contain remaining coinbase scriptSig bytes before the input
    /// sequence, so this reconstructs and deserializes the full transaction (see
    /// [`decode_coinbase_tx_parts`]) before checking outputs.
    pub fn validate_coinbase_tx_parts(
        &self,
        coinbase_tx_prefix: &[u8],
        coinbase_tx_suffix: &[u8],
        full_extranonce_size: usize,
    ) -> Result<(), PayoutValidationError> {
        let coinbase =
            decode_coinbase_tx_parts(coinbase_tx_prefix, coinbase_tx_suffix, full_extranonce_size)?;
        self.validate_coinbase_outputs(&coinbase.output)
    }

    fn miner_address(&self) -> Option<&str> {
        match self {
            Self::Solo { address, .. }
            | Self::LegacySolo { address, .. }
            | Self::Donate { address, .. } => Some(address.as_str()),
            Self::FullDonation => None,
        }
    }

    fn miner_script_pubkey(&self) -> Option<ScriptBuf> {
        match self {
            Self::Solo { script, .. }
            | Self::LegacySolo { script, .. }
            | Self::Donate { script, .. } => Some(script.script_pubkey()),
            Self::FullDonation => None,
        }
    }

    fn expected_miner_percentage(&self) -> u8 {
        match self {
            Self::Solo { .. } | Self::LegacySolo { .. } => 100,
            Self::Donate { percentage, .. } => 100 - percentage,
            Self::FullDonation => 0,
        }
    }

    fn expected_miner_sats(&self, total_spendable_sats: u64) -> u64 {
        match self {
            Self::Solo { .. } | Self::LegacySolo { .. } => total_spendable_sats,
            Self::Donate { percentage, .. } => {
                let pool_sats = (total_spendable_sats * *percentage as u64) / 100;
                total_spendable_sats.saturating_sub(pool_sats)
            }
            Self::FullDonation => 0,
        }
    }

    fn expected_legacy_solo_miner_sats(&self, total_spendable_sats: u64) -> u64 {
        (total_spendable_sats * MIN_LEGACY_SOLO_PAYOUT_PERCENTAGE as u64).div_ceil(100)
    }
}

impl TryFrom<&str> for PayoutMode {
    type Error = PayoutModeError;

    fn try_from(user_identity: &str) -> Result<Self, Self::Error> {
        if user_identity.is_empty() {
            return Err(PayoutModeError::NoPayoutMode(user_identity.to_string()));
        }

        let addr = address_part_from_user_identity(user_identity);

        if let Ok(script) = script_from_address(addr) {
            return Ok(Self::LegacySolo {
                address: addr.to_string(),
                script,
            });
        }

        let mut parts = user_identity.split('/');

        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some("sri"), Some("solo"), Some(payout_address), _) => {
                let script = script_from_address(payout_address)?;
                Ok(Self::Solo {
                    address: payout_address.to_string(),
                    script,
                })
            }

            (Some("sri"), Some("donate"), None, _)
            | (Some("sri"), Some("donate"), Some(_), None) => Ok(Self::FullDonation),

            (Some("sri"), Some("donate"), Some(percentage), Some(payout_address)) => {
                let percentage = percentage.parse::<u8>().map_err(|_| {
                    PayoutModeError::InvalidDonationPercentage(percentage.to_string())
                })?;
                if !(1..100).contains(&percentage) {
                    return Err(PayoutModeError::InvalidDonationPercentage(
                        percentage.to_string(),
                    ));
                }

                let script = script_from_address(payout_address)?;
                Ok(Self::Donate {
                    percentage,
                    address: payout_address.to_string(),
                    script,
                })
            }

            (Some("sri"), Some(_), _, _) => Err(PayoutModeError::InvalidUserIdentity(
                user_identity.to_string(),
            )),

            _ => Err(PayoutModeError::NoPayoutMode(user_identity.to_string())),
        }
    }
}

impl fmt::Display for PayoutMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Solo { address, .. } => {
                write!(f, "100% miner payout to {address}")
            }
            Self::LegacySolo { address, .. } => write!(
                f,
                "at least {MIN_LEGACY_SOLO_PAYOUT_PERCENTAGE}% miner payout to {address}"
            ),
            Self::Donate {
                percentage,
                address,
                ..
            } => write!(
                f,
                "{}% miner payout to {} ({}% pool donation)",
                100 - percentage,
                address,
                percentage
            ),
            Self::FullDonation => write!(f, "100% pool payout"),
        }
    }
}

/// Errors produced while parsing a payout mode from a `user_identity`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayoutModeError {
    /// No payout mode was encoded in `user_identity`.
    NoPayoutMode(String),
    /// `sri/...` was used with an unsupported payout pattern.
    InvalidUserIdentity(String),
    /// A payout address was present but could not be converted into a script.
    InvalidPayoutAddress { address: String, error: String },
    /// Donation percentage was not an integer in the supported 1..100 range.
    InvalidDonationPercentage(String),
    /// The payout mode is valid but [`AllowedPayoutModes`] refuses it.
    NotAllowed(String),
    /// Payout verification was requested but no miner payout address is present.
    MissingMinerPayout {
        user_identity: String,
        mode: MissingMinerPayoutMode,
    },
}

/// Payout modes that cannot be verified because they do not include a miner payout address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MissingMinerPayoutMode {
    /// `sri/donate/<worker>` full donation mode: all reward goes to the pool.
    FullDonation,
    /// No SRI payout mode or legacy address payout was encoded.
    NoPayoutMode,
}

impl fmt::Display for PayoutModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoPayoutMode(user_identity) => {
                write!(
                    f,
                    "no payout mode encoded in user_identity: {user_identity}"
                )
            }
            Self::InvalidUserIdentity(user_identity) => {
                write!(
                    f,
                    "invalid user_identity pattern for payout mode: {user_identity}"
                )
            }
            Self::InvalidPayoutAddress { address, error } => {
                write!(f, "invalid payout address `{address}`: {error}")
            }
            Self::InvalidDonationPercentage(percentage) => {
                write!(f, "invalid donation percentage: {percentage}")
            }
            Self::NotAllowed(mode) => {
                write!(f, "payout mode not allowed, only pool payouts are: {mode}")
            }
            Self::MissingMinerPayout {
                user_identity,
                mode: MissingMinerPayoutMode::FullDonation,
            } => write!(
                f,
                "verify_payout is enabled, but user_identity `{user_identity}` opts into full donation mode (`sri/donate/<worker>`), which has no miner payout to verify; disable verify_payout or use sri/solo/<address>/<worker>, sri/donate/<percentage>/<address>/<worker>, <address>, or <address>.<worker>"
            ),
            Self::MissingMinerPayout {
                user_identity,
                mode: MissingMinerPayoutMode::NoPayoutMode,
            } => write!(
                f,
                "verify_payout is enabled, but user_identity `{user_identity}` does not opt into a payout mode, so there is no miner payout to verify; disable verify_payout for pool usernames or use sri/solo/<address>/<worker>, sri/donate/<percentage>/<address>/<worker>, <address>, or <address>.<worker>"
            ),
        }
    }
}

impl std::error::Error for PayoutModeError {}

/// Errors produced while verifying coinbase outputs against a payout mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayoutValidationError {
    /// The coinbase output set has no spendable outputs.
    NoSpendableOutputs,
    /// The miner payout did not match the expected distribution.
    PayoutMismatch {
        /// Address encoded by the payout mode.
        address: String,
        /// Expected miner payout in satoshis.
        expected_sats: u64,
        /// Expected miner payout percentage.
        expected_percentage: u8,
        /// Total spendable coinbase output value in satoshis.
        total_spendable_sats: u64,
        /// Actual amount paid to the miner script in satoshis.
        actual_sats: u64,
    },
    /// Failed to decode the reconstructed coinbase transaction.
    DecodeCoinbaseTransaction(String),
    /// The output at this index pays a script other than the pool's.
    NonPoolOutput(usize),
}

impl fmt::Display for PayoutValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoSpendableOutputs => write!(f, "coinbase has no spendable outputs"),
            Self::PayoutMismatch {
                address,
                expected_sats,
                expected_percentage,
                total_spendable_sats,
                actual_sats,
            } => write!(
                f,
                "coinbase payout mismatch for {address}: expected {expected_sats} sats ({expected_percentage}% of {total_spendable_sats} spendable sats), found {actual_sats} sats"
            ),
            Self::DecodeCoinbaseTransaction(e) => {
                write!(f, "failed to decode coinbase transaction: {e}")
            }
            Self::NonPoolOutput(index) => {
                write!(
                    f,
                    "coinbase output {index} pays a script other than the pool's"
                )
            }
        }
    }
}

impl std::error::Error for PayoutValidationError {}

fn script_from_address(address: &str) -> Result<CoinbaseRewardScript, PayoutModeError> {
    CoinbaseRewardScript::from_descriptor(&format!("addr({address})")).map_err(|e| {
        PayoutModeError::InvalidPayoutAddress {
            address: address.to_string(),
            error: e.to_string(),
        }
    })
}

fn address_part_from_user_identity(user_identity: &str) -> &str {
    user_identity
        .split_once('.')
        .map(|(address, _)| address)
        .unwrap_or(user_identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stratum_core::bitcoin::{
        Address,
        consensus::serialize,
        params::{MAINNET, TESTNET4},
    };

    const MINER_ADDRESS: &str = "bc1qtzqxqaxyy6lda2fhdtp5dp0v56vlf6g0tljy2x";
    const OTHER_ADDRESS: &str = "bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4";
    const TESTNET_ADDRESS: &str = "tb1qa0sm0hxzj0x25rh8gw5xlzwlsfvvyz8u96w3p8";
    const FULL_EXTRANONCE_SIZE: usize = 8;

    fn tx_out(value: u64, address: &str) -> TxOut {
        TxOut {
            value: Amount::from_sat(value),
            script_pubkey: script_from_address(address).unwrap().script_pubkey(),
        }
    }

    fn coinbase_tx_parts(outputs: Vec<TxOut>, script_sig_suffix: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let script_sig_prefix = [0x03, 0x01, 0x02, 0x03];
        let script_sig_len =
            script_sig_prefix.len() + FULL_EXTRANONCE_SIZE + script_sig_suffix.len();
        assert!(script_sig_len < 0xfd);

        let mut prefix = Vec::new();
        prefix.extend([0x02, 0x00, 0x00, 0x00]);
        prefix.push(0x01);
        prefix.extend([0; 32]);
        prefix.extend([0xff, 0xff, 0xff, 0xff]);
        prefix.push(script_sig_len as u8);
        prefix.extend(script_sig_prefix);

        let mut suffix = Vec::new();
        suffix.extend(script_sig_suffix);
        suffix.extend([0xff, 0xff, 0xff, 0xff]);
        suffix.extend(serialize(&outputs));
        suffix.extend([0, 0, 0, 0]);

        (prefix, suffix)
    }

    fn validate_tx_outputs(
        expected: &PayoutMode,
        outputs: Vec<TxOut>,
    ) -> Result<(), PayoutValidationError> {
        let (prefix, suffix) = coinbase_tx_parts(outputs, &[]);
        expected.validate_coinbase_tx_parts(&prefix, &suffix, FULL_EXTRANONCE_SIZE)
    }

    #[test]
    fn parses_full_donation_identities() {
        assert!(matches!(
            PayoutMode::try_from("sri/donate/worker"),
            Ok(PayoutMode::FullDonation)
        ));
        assert!(matches!(
            PayoutMode::try_from("sri/donate"),
            Ok(PayoutMode::FullDonation)
        ));
    }

    #[test]
    fn parses_solo_identities() {
        assert!(matches!(
            PayoutMode::try_from(format!("sri/solo/{TESTNET_ADDRESS}/worker").as_str()),
            Ok(PayoutMode::Solo { script, .. }) if Address::from_script(script.script_pubkey().as_script(), TESTNET4.clone()).unwrap().to_string() == TESTNET_ADDRESS
        ));
        assert!(matches!(
            PayoutMode::try_from(format!("sri/solo/{MINER_ADDRESS}/worker/subworker").as_str()),
            Ok(PayoutMode::Solo { script, .. }) if Address::from_script(script.script_pubkey().as_script(), MAINNET.clone()).unwrap().to_string() == MINER_ADDRESS
        ));
        assert!(matches!(
            PayoutMode::try_from(MINER_ADDRESS),
            Ok(PayoutMode::LegacySolo { script, .. }) if Address::from_script(script.script_pubkey().as_script(), MAINNET.clone()).unwrap().to_string() == MINER_ADDRESS
        ));
    }

    #[test]
    fn parses_legacy_address_identity_with_worker_suffix() {
        assert!(matches!(
            PayoutMode::try_from(format!("{MINER_ADDRESS}.worker1").as_str()),
            Ok(PayoutMode::LegacySolo { script, .. }) if Address::from_script(script.script_pubkey().as_script(), MAINNET.clone()).unwrap().to_string() == MINER_ADDRESS
        ));
        assert!(matches!(
            PayoutMode::try_from(format!("{MINER_ADDRESS}.worker1.subworker").as_str()),
            Ok(PayoutMode::LegacySolo { script, .. }) if Address::from_script(script.script_pubkey().as_script(), MAINNET.clone()).unwrap().to_string() == MINER_ADDRESS
        ));
    }

    #[test]
    fn arbitrary_pool_usernames_have_no_payout_mode() {
        assert!(matches!(
            PayoutMode::try_from("invalid_address.worker"),
            Err(PayoutModeError::NoPayoutMode(_))
        ));
        assert!(matches!(
            PayoutMode::try_from(""),
            Err(PayoutModeError::NoPayoutMode(_))
        ));
        assert!(matches!(
            PayoutMode::try_from("other/donate/worker"),
            Err(PayoutModeError::NoPayoutMode(_))
        ));
    }

    #[test]
    fn permissive_parser_treats_address_like_typos_as_no_payout_mode() {
        assert!(matches!(
            PayoutMode::try_from("bc1q_typo.worker"),
            Err(PayoutModeError::NoPayoutMode(_))
        ));
    }

    #[test]
    fn parses_partial_donation_identities() {
        assert!(matches!(
            PayoutMode::try_from(format!("sri/donate/50/{TESTNET_ADDRESS}/worker").as_str()).unwrap(),
            PayoutMode::Donate { percentage: 50, script, .. } if Address::from_script(script.script_pubkey().as_script(), TESTNET4.clone()).unwrap().to_string() == TESTNET_ADDRESS
        ));

        assert!(matches!(
            PayoutMode::try_from(format!("sri/donate/50/{TESTNET_ADDRESS}").as_str()).unwrap(),
            PayoutMode::Donate { percentage: 50, script, .. } if Address::from_script(script.script_pubkey().as_script(), TESTNET4.clone()).unwrap().to_string() == TESTNET_ADDRESS
        ));
    }

    #[test]
    fn rejects_invalid_sri_patterns() {
        assert!(PayoutMode::try_from("sri/invalid/worker").is_err());
        assert!(PayoutMode::try_from("sri/solo").is_err());
        assert!(PayoutMode::try_from("sri/solo/random_thing_here/worker").is_err());
        assert!(PayoutMode::try_from("sri/solo/").is_err());
        assert!(matches!(
            PayoutMode::try_from("sri/donate/abc/addr/worker"),
            Err(PayoutModeError::InvalidDonationPercentage(_))
        ));
        assert!(matches!(
            PayoutMode::try_from("sri/donate/101/addr/worker"),
            Err(PayoutModeError::InvalidDonationPercentage(_))
        ));
        assert!(matches!(
            PayoutMode::try_from("sri/"),
            Err(PayoutModeError::InvalidUserIdentity(_))
        ));
    }

    #[test]
    fn builds_pool_coinbase_outputs_for_all_modes() {
        let pool_script = script_from_address(OTHER_ADDRESS).unwrap();

        let solo = PayoutMode::try_from(MINER_ADDRESS).unwrap();
        let solo_outputs = solo.coinbase_outputs(1_000, &pool_script);
        assert_eq!(solo_outputs.len(), 1);
        assert_eq!(solo_outputs[0].value.to_sat(), 1_000);

        let donate =
            PayoutMode::try_from(format!("sri/donate/10/{MINER_ADDRESS}/w").as_str()).unwrap();
        let donate_outputs = donate.coinbase_outputs(1_000, &pool_script);
        assert_eq!(donate_outputs.len(), 2);
        assert_eq!(donate_outputs[0].value.to_sat(), 100);
        assert_eq!(donate_outputs[1].value.to_sat(), 900);

        let full_donation = PayoutMode::FullDonation;
        let full_donation_outputs = full_donation.coinbase_outputs(1_000, &pool_script);
        assert_eq!(full_donation_outputs.len(), 1);
        assert_eq!(full_donation_outputs[0].value.to_sat(), 1_000);
    }

    #[test]
    fn validates_full_solo_distribution() {
        let expected =
            PayoutMode::try_from(format!("sri/solo/{MINER_ADDRESS}/w1").as_str()).unwrap();

        validate_tx_outputs(&expected, vec![tx_out(1_000, MINER_ADDRESS)]).unwrap();
    }

    #[test]
    fn rejects_full_solo_distribution_with_other_spendable_output() {
        let expected =
            PayoutMode::try_from(format!("sri/solo/{MINER_ADDRESS}/w1").as_str()).unwrap();

        let err = validate_tx_outputs(
            &expected,
            vec![tx_out(900, MINER_ADDRESS), tx_out(100, OTHER_ADDRESS)],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            PayoutValidationError::PayoutMismatch {
                expected_sats: 1000,
                actual_sats: 900,
                ..
            }
        ));
    }

    #[test]
    fn validates_legacy_solo_distribution_with_service_fee_output() {
        let expected = PayoutMode::try_from(format!("{MINER_ADDRESS}.w1").as_str()).unwrap();

        validate_tx_outputs(
            &expected,
            vec![tx_out(991, MINER_ADDRESS), tx_out(9, OTHER_ADDRESS)],
        )
        .unwrap();
    }

    #[test]
    fn rejects_legacy_solo_distribution_below_minimum() {
        let expected = PayoutMode::try_from(format!("{MINER_ADDRESS}.w1").as_str()).unwrap();

        let err = validate_tx_outputs(
            &expected,
            vec![tx_out(899, MINER_ADDRESS), tx_out(101, OTHER_ADDRESS)],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            PayoutValidationError::PayoutMismatch {
                expected_sats: 900,
                expected_percentage: 90,
                actual_sats: 899,
                ..
            }
        ));
    }

    #[test]
    fn rejects_legacy_solo_distribution_without_miner_address() {
        let expected = PayoutMode::try_from(format!("{MINER_ADDRESS}.w1").as_str()).unwrap();

        let err = validate_tx_outputs(&expected, vec![tx_out(1_000, OTHER_ADDRESS)]).unwrap_err();

        assert!(matches!(
            err,
            PayoutValidationError::PayoutMismatch {
                expected_sats: 900,
                expected_percentage: 90,
                total_spendable_sats: 1000,
                actual_sats: 0,
                ..
            }
        ));
    }

    #[test]
    fn validates_partial_donation_distribution() {
        let expected =
            PayoutMode::try_from(format!("sri/donate/10/{MINER_ADDRESS}/w1").as_str()).unwrap();

        validate_tx_outputs(
            &expected,
            vec![tx_out(100, OTHER_ADDRESS), tx_out(900, MINER_ADDRESS)],
        )
        .unwrap();
    }

    #[test]
    fn rejects_wrong_partial_donation_distribution() {
        let expected =
            PayoutMode::try_from(format!("sri/donate/10/{MINER_ADDRESS}/w1").as_str()).unwrap();

        let err = validate_tx_outputs(
            &expected,
            vec![tx_out(200, OTHER_ADDRESS), tx_out(800, MINER_ADDRESS)],
        )
        .unwrap_err();

        assert!(matches!(
            err,
            PayoutValidationError::PayoutMismatch {
                expected_sats: 900,
                actual_sats: 800,
                ..
            }
        ));
    }

    #[test]
    fn full_donation_has_no_miner_payout_to_verify() {
        let expected = PayoutMode::FullDonation;

        validate_tx_outputs(&expected, vec![tx_out(1_000, OTHER_ADDRESS)]).unwrap();
    }

    #[test]
    fn validates_coinbase_with_remaining_scriptsig_bytes_after_extranonce() {
        let expected = PayoutMode::try_from(format!("{MINER_ADDRESS}.w1").as_str()).unwrap();
        let (prefix, suffix) = coinbase_tx_parts(
            vec![tx_out(1_000, MINER_ADDRESS), tx_out(1, OTHER_ADDRESS)],
            b"/NexusPool/",
        );

        expected
            .validate_coinbase_tx_parts(&prefix, &suffix, FULL_EXTRANONCE_SIZE)
            .unwrap();
    }

    #[test]
    fn pool_only_allows_only_full_donation() {
        let solo = format!("sri/solo/{MINER_ADDRESS}/worker");
        let legacy = format!("{MINER_ADDRESS}.worker");
        let donate = format!("sri/donate/10/{MINER_ADDRESS}/worker");
        for identity in [solo.as_str(), MINER_ADDRESS, &legacy, &donate] {
            let mode = PayoutMode::try_from(identity).unwrap();
            assert!(
                AllowedPayoutModes::Any.check(mode.clone()).is_ok(),
                "{identity}"
            );
            assert!(
                matches!(
                    AllowedPayoutModes::PoolOnly.check(mode),
                    Err(PayoutModeError::NotAllowed(_))
                ),
                "{identity}"
            );
        }
        for allowed in [AllowedPayoutModes::Any, AllowedPayoutModes::PoolOnly] {
            for identity in ["sri/donate", "sri/donate/worker"] {
                let mode = PayoutMode::try_from(identity).unwrap();
                assert!(
                    matches!(allowed.check(mode), Ok(PayoutMode::FullDonation)),
                    "{identity}"
                );
            }
        }
    }

    #[test]
    fn validates_pool_only_outputs() {
        let pool = script_from_address(OTHER_ADDRESS).unwrap().script_pubkey();
        let commitment = |sats| TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: ScriptBuf::new_op_return([0xaa; 36]),
        };
        let pays = |sats, address| tx_out(sats, address);
        for outputs in [
            vec![pays(1_000, OTHER_ADDRESS)],
            vec![pays(1_000, OTHER_ADDRESS), commitment(0)],
            vec![pays(600, OTHER_ADDRESS), pays(400, OTHER_ADDRESS)],
        ] {
            validate_pool_only_outputs(&outputs, &pool).unwrap();
        }
        for (outputs, expected) in [
            (
                vec![pays(900, OTHER_ADDRESS), pays(100, MINER_ADDRESS)],
                PayoutValidationError::NonPoolOutput(1),
            ),
            (
                vec![pays(1_000, MINER_ADDRESS), commitment(0)],
                PayoutValidationError::NonPoolOutput(0),
            ),
            (
                vec![pays(1_000, OTHER_ADDRESS), pays(0, MINER_ADDRESS)],
                PayoutValidationError::NonPoolOutput(1),
            ),
            (
                vec![pays(1_000, OTHER_ADDRESS), commitment(1)],
                PayoutValidationError::NonPoolOutput(1),
            ),
            (
                vec![commitment(0)],
                PayoutValidationError::NoSpendableOutputs,
            ),
            (vec![], PayoutValidationError::NoSpendableOutputs),
        ] {
            assert_eq!(
                validate_pool_only_outputs(&outputs, &pool),
                Err(expected),
                "{outputs:?}"
            );
        }
    }

    /// Splits `tx` as a `DeclareMiningJob` does: the last `FULL_EXTRANONCE_SIZE` scriptSig bytes
    /// are left out of both parts.
    fn declared_parts(tx: &Transaction) -> (Vec<u8>, Vec<u8>) {
        let bytes = serialize(tx);
        let script_sig = tx.input[0].script_sig.as_bytes();
        let script_sig_end = bytes
            .windows(script_sig.len())
            .position(|w| w == script_sig)
            .unwrap()
            + script_sig.len();
        (
            bytes[..script_sig_end - FULL_EXTRANONCE_SIZE].to_vec(),
            bytes[script_sig_end..].to_vec(),
        )
    }

    fn coinbase(witness: bool) -> Transaction {
        use crate::stratum_core::bitcoin::{
            OutPoint, Sequence, TxIn, Witness, absolute::LockTime, transaction::Version,
        };
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::from_bytes(vec![0x03, 1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0]),
                sequence: Sequence::MAX,
                witness: if witness {
                    Witness::from_slice(&[[0u8; 32]])
                } else {
                    Witness::new()
                },
            }],
            output: vec![tx_out(1_000, OTHER_ADDRESS)],
        }
    }

    #[test]
    fn decodes_declared_coinbase() {
        for witness in [true, false] {
            let tx = coinbase(witness);
            let (prefix, suffix) = declared_parts(&tx);
            assert_eq!(decode_declared_coinbase_tx(&prefix, &suffix).unwrap(), tx);
        }
    }

    #[test]
    fn rejects_malformed_declared_coinbase_without_panicking() {
        let (prefix, suffix) = declared_parts(&coinbase(true));
        // shorter than the fixed fields before the scriptSig length
        assert!(decode_declared_coinbase_tx(&prefix[..20], &suffix).is_err());
        // scriptSig length smaller than its bytes already in the prefix
        let mut short = prefix.clone();
        short[43] = 1;
        assert!(decode_declared_coinbase_tx(&short, &suffix).is_err());
        // garbage suffix
        assert!(decode_declared_coinbase_tx(&prefix, &[0xff; 3]).is_err());
        // a scriptSig length over 100 bytes, even 256 MiB, is refused before anything is sized
        // by it
        for len in [&[101][..], &[0xfe, 0x00, 0x00, 0x00, 0x10]] {
            let mut long = prefix[..43].to_vec();
            long.extend_from_slice(len);
            assert_eq!(
                decode_declared_coinbase_tx(&long, &suffix),
                Err(PayoutValidationError::DecodeCoinbaseTransaction(
                    "scriptSig is longer than 100 bytes".into()
                ))
            );
        }
    }
}
