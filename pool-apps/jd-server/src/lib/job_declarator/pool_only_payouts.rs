//! Fork patch (#383 A2): refuse declared coinbases that pay anyone but the pool.
//!
//! Used by [`super::JobDeclarator`] when built `with_pool_only_payouts(true)`. Kept in its own
//! file, with its own copy of the coinbase reconstruction, so it touches none of the upstream
//! validation code and rebases cleanly. The reconstruction mirrors
//! `DeclaredCustomJob::get_coinbase_tx` in `job_validation/bitcoin_core_ipc.rs`, and also
//! refuses (instead of panicking on) a prefix too short to hold the scriptSig length, and a
//! scriptSig length over the consensus limit (which would otherwise size an allocation).

use stratum_apps::stratum_core::bitcoin::{
    Amount, Script, Transaction, VarInt, consensus::Decodable,
};

/// Consensus limit on a coinbase scriptSig (2 to 100 bytes).
const MAX_COINBASE_SCRIPT_SIG_SIZE: usize = 100;

/// Reconstructs a declared coinbase transaction by concatenating prefix, extranonce (zeros) and
/// suffix.
///
/// The extranonce size is calculated from the scriptSig size in the coinbase_tx_prefix. A prefix
/// too short to hold the scriptSig length, or whose scriptSig is shorter than its own bytes in the
/// prefix, is an error rather than a panic.
///
/// Error type is () because we don't need extra granularity for error_code =
/// "invalid-coinbase-tx"
fn reconstruct_coinbase_tx(
    coinbase_tx_prefix: &[u8],
    coinbase_tx_suffix: &[u8],
) -> Result<Transaction, ()> {
    // Coinbase structure: version(4) + marker+flag(2) + input_count(1) + outpoint(32) +
    // index(4) = 43 bytes Then comes scriptSig length (VarInt) followed by scriptSig
    // data
    const COINBASE_PREFIX_LEN: usize = 43;
    let script_sig_size: usize = {
        let mut cursor = coinbase_tx_prefix.get(COINBASE_PREFIX_LEN..).ok_or(())?;
        match VarInt::consensus_decode(&mut cursor) {
            Ok(varint) if varint.0 as usize <= MAX_COINBASE_SCRIPT_SIG_SIZE => varint.0 as usize,
            Ok(varint) => {
                tracing::error!(
                    "Declared coinbase scriptSig size {} is over 100 bytes",
                    varint.0
                );
                return Err(());
            }
            Err(e) => {
                tracing::error!(
                    "Failed to decode scriptSig size from coinbase prefix: {}",
                    e
                );
                return Err(());
            }
        }
    };

    // Calculate the size of scriptSig bytes already in the prefix.
    let varint_size = VarInt(script_sig_size as u64).size();
    let script_sig_offset = COINBASE_PREFIX_LEN + varint_size;
    let script_sig_bytes_in_prefix = coinbase_tx_prefix
        .len()
        .checked_sub(script_sig_offset)
        .ok_or(())?;

    // The full extranonce fills the remaining space in scriptSig
    let full_extranonce_size: usize = script_sig_size
        .checked_sub(script_sig_bytes_in_prefix)
        .ok_or(())?;

    // Concatenate prefix + full extranonce (zeros) + suffix to form the complete transaction
    // bytes
    let mut coinbase_tx = coinbase_tx_prefix.to_vec();
    coinbase_tx.resize(coinbase_tx.len() + full_extranonce_size, 0);
    coinbase_tx.extend_from_slice(coinbase_tx_suffix);

    // Deserialize the transaction
    Transaction::consensus_decode(&mut &coinbase_tx[..]).map_err(|e| {
        tracing::error!("Failed to deserialize declared coinbase transaction: {}", e);
    })
}

/// Whether a declared coinbase pays only `pool_script`: at least one output pays it, and every
/// other output is a zero-value OP_RETURN (a commitment, such as the segwit witness commitment).
/// A coinbase that cannot be reconstructed does not.
///
/// The value is not checked against the block reward, which the JDS does not know here; a
/// declaration can still leave part of the reward unclaimed, which pays nobody.
pub(crate) fn coinbase_pays_only(
    coinbase_tx_prefix: &[u8],
    coinbase_tx_suffix: &[u8],
    pool_script: &Script,
) -> bool {
    let Ok(coinbase_tx) = reconstruct_coinbase_tx(coinbase_tx_prefix, coinbase_tx_suffix) else {
        return false;
    };
    let pays_pool = |script: &Script| script == pool_script;
    coinbase_tx
        .output
        .iter()
        .any(|output| pays_pool(&output.script_pubkey))
        && coinbase_tx.output.iter().all(|output| {
            pays_pool(&output.script_pubkey)
                || (output.script_pubkey.is_op_return() && output.value == Amount::ZERO)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use stratum_apps::stratum_core::bitcoin::{
        OutPoint, ScriptBuf, Sequence, TxIn, TxOut, Witness, absolute::LockTime,
        consensus::Encodable, transaction::Version,
    };

    const EXTRANONCE_SIZE: usize = 8;

    fn pool_script() -> ScriptBuf {
        ScriptBuf::from_hex("a9140b2867e31c2c5e8ebb55631eececd31da8e2510887").unwrap()
    }

    fn witness_commitment(value: u64) -> TxOut {
        TxOut {
            value: Amount::from_sat(value),
            script_pubkey: ScriptBuf::from_hex(
                "6a24aa21a9ed0000000000000000000000000000000000000000000000000000000000000000",
            )
            .unwrap(),
        }
    }

    fn pays(sats: u64, script: ScriptBuf) -> TxOut {
        TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: script,
        }
    }

    /// Splits a segwit coinbase into the (prefix, suffix) a JD client declares: the last
    /// `EXTRANONCE_SIZE` bytes of the scriptSig are the extranonce, left out of both.
    fn declared_parts(outputs: Vec<TxOut>) -> (Vec<u8>, Vec<u8>) {
        let script_sig =
            ScriptBuf::from_bytes(vec![0x03, 0x01, 0x02, 0x03, 0, 0, 0, 0, 0, 0, 0, 0]);
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: script_sig.clone(),
                sequence: Sequence::MAX,
                witness: Witness::from_slice(&[[0u8; 32]]),
            }],
            output: outputs,
        };
        let mut bytes = Vec::new();
        tx.consensus_encode(&mut bytes).unwrap();
        // 43 fixed bytes, then a one-byte scriptSig length, then the scriptSig
        let script_sig_end = 43 + 1 + script_sig.len();
        (
            bytes[..script_sig_end - EXTRANONCE_SIZE].to_vec(),
            bytes[script_sig_end..].to_vec(),
        )
    }

    #[test]
    fn reconstructs_the_declared_coinbase() {
        let (prefix, suffix) = declared_parts(vec![pays(1_000, pool_script())]);
        let tx = reconstruct_coinbase_tx(&prefix, &suffix).unwrap();
        assert_eq!(tx.output, vec![pays(1_000, pool_script())]);
    }

    #[test]
    fn coinbase_paying_only_the_pool_is_accepted() {
        for outputs in [
            vec![pays(1_000, pool_script())],
            vec![pays(1_000, pool_script()), witness_commitment(0)],
            vec![
                pays(600, pool_script()),
                pays(400, pool_script()),
                witness_commitment(0),
            ],
        ] {
            let (prefix, suffix) = declared_parts(outputs.clone());
            assert!(
                coinbase_pays_only(&prefix, &suffix, &pool_script()),
                "{outputs:?}"
            );
        }
    }

    #[test]
    fn coinbase_paying_anyone_else_is_refused() {
        let miner = ScriptBuf::from_hex("0014e8df018c7e326cc253faac7e46cdc51e68542c42").unwrap();
        for outputs in [
            // a miner output next to the pool
            vec![
                pays(900, pool_script()),
                pays(100, miner.clone()),
                witness_commitment(0),
            ],
            // a miner instead of the pool
            vec![pays(1_000, miner.clone()), witness_commitment(0)],
            // a zero-value output to someone else is still not the pool
            vec![pays(1_000, pool_script()), pays(0, miner)],
            // an OP_RETURN that burns value
            vec![pays(1_000, pool_script()), witness_commitment(1)],
            // nothing pays the pool
            vec![witness_commitment(0)],
        ] {
            let (prefix, suffix) = declared_parts(outputs.clone());
            assert!(
                !coinbase_pays_only(&prefix, &suffix, &pool_script()),
                "{outputs:?}"
            );
        }
    }

    #[test]
    fn malformed_declarations_are_refused_without_panicking() {
        let (prefix, suffix) = declared_parts(vec![pays(1_000, pool_script())]);
        // prefix shorter than the fixed 43 bytes
        assert!(!coinbase_pays_only(&prefix[..20], &suffix, &pool_script()));
        // scriptSig length smaller than its bytes already in the prefix
        let mut short_script_sig = prefix.clone();
        short_script_sig[43] = 1;
        assert!(!coinbase_pays_only(
            &short_script_sig,
            &suffix,
            &pool_script()
        ));
        // garbage suffix
        assert!(!coinbase_pays_only(&prefix, &[0xff; 3], &pool_script()));
        // a scriptSig length over the consensus limit is refused before anything is sized
        // by it: 0xfe + 4 bytes declares 256 MiB
        let mut huge = prefix[..43].to_vec();
        huge.extend_from_slice(&[0xfe, 0x00, 0x00, 0x00, 0x10]);
        assert!(!coinbase_pays_only(&huge, &suffix, &pool_script()));
    }
}
