//! `payout_modes` (pool config): which payout modes a `user_identity` may select, and whether
//! the embedded JDS accepts declared coinbases that pay anyone but the pool.

use std::time::{Duration, Instant};

use async_channel::Sender;
use integration_tests_sv2::{
    interceptor::MessageDirection,
    mock_roles::{MockDownstream, WithSetup},
    sniffer::Sniffer,
    template_provider::DifficultyLevel,
    *,
};
use stratum_apps::{
    payout::AllowedPayoutModes,
    stratum_core::{
        binary_sv2::Seq064KOwned,
        bitcoin::{
            Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness,
            absolute::LockTime,
            consensus::{deserialize, serialize},
            hashes::{Hash, sha256d},
            script::Builder,
            transaction::Version,
        },
        common_messages_sv2::{MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS, Protocol},
        job_declaration_sv2::*,
        mining_sv2::*,
        parsers_sv2::{AnyMessageOwned, JobDeclarationOwned, MiningOwned},
    },
};

use pool_sv2::{
    PoolSv2,
    config::{AuthorityConfig, ConnectionConfig, JDSPartialConfig, PoolConfig},
};
use std::net::SocketAddr;
use stratum_apps::{
    config_helpers::CoinbaseRewardScript,
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
    tp_type::TemplateProviderType,
};

/// A pool like `start_pool` / `start_pool_with_jds` (lib/mod.rs) builds, with `payout_modes`
/// set. Kept here rather than in lib/mod.rs so this fork-only test touches no shared helper.
async fn start_pool_with_payout_modes(
    template_provider: TemplateProviderType,
    jds_address: Option<SocketAddr>,
    payout_modes: AllowedPayoutModes,
) -> (PoolSv2, SocketAddr) {
    let pool_address = utils::get_available_address();
    let mut config = PoolConfig::new(
        ConnectionConfig::new(pool_address, 3600, "Stratum V2 SRI Pool".to_string()),
        template_provider,
        AuthorityConfig::new(
            Secp256k1PublicKey::try_from(
                "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72".to_string(),
            )
            .unwrap(),
            Secp256k1SecretKey::try_from(
                "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n".to_string(),
            )
            .unwrap(),
        ),
        CoinbaseRewardScript::from_descriptor("addr(tb1qa0sm0hxzj0x25rh8gw5xlzwlsfvvyz8u96w3p8)")
            .unwrap(),
        120.0,
        1,
        1,
        vec![],
        vec![],
        None,
        None,
        jds_address.map(JDSPartialConfig::new),
    );
    config.set_payout_modes(payout_modes);
    let pool = PoolSv2::new(config);
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        _ = pool_clone.start().await;
    });
    tokio::time::sleep(utils::ROLE_STARTUP_DELAY).await;
    (pool, pool_address)
}

const MINER_ADDR: &str = "tb1qpusf5256yxv50qt0pm0tue8k952fsu5lzsphft";
const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

/// Identities that select a payout to the miner, and identities that pay the pool.
fn payout_identities() -> Vec<String> {
    vec![
        format!("sri/solo/{MINER_ADDR}/worker.1"),
        MINER_ADDR.to_string(),
        format!("{MINER_ADDR}.worker.1"),
        format!("sri/donate/10/{MINER_ADDR}/worker.1"),
    ]
}
const POOL_IDENTITIES: [&str; 3] = ["worker.1", "sri/donate", "sri/donate/worker.1"];

/// Waits for the next message from upstream that `pick` selects, skipping the rest.
async fn next_from_upstream<T>(
    sniffer: &Sniffer<'_>,
    mut pick: impl FnMut(AnyMessageOwned) -> Option<T>,
) -> T {
    let deadline = Instant::now() + REPLY_TIMEOUT;
    loop {
        if let Some((_, msg)) = sniffer.next_message_from_upstream() {
            if let Some(picked) = pick(msg) {
                return picked;
            }
            continue;
        }
        assert!(
            Instant::now() < deadline,
            "no matching reply within {REPLY_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Opens an extended and a standard channel with `user_identity` and returns, for each, the
/// `OpenMiningChannelError` code or `None` on success.
async fn open_both(
    send: &Sender<AnyMessageOwned>,
    sniffer: &Sniffer<'_>,
    request_id: u32,
    user_identity: &str,
) -> [Option<String>; 2] {
    let reply = |request_id: u32| {
        move |msg: AnyMessageOwned| match msg {
            AnyMessageOwned::Mining(MiningOwned::OpenMiningChannelError(m))
                if m.request_id == request_id =>
            {
                Some(Some(m.error_code.as_utf8_or_hex()))
            }
            AnyMessageOwned::Mining(MiningOwned::OpenExtendedMiningChannelSuccess(m))
                if m.request_id == request_id =>
            {
                Some(None)
            }
            AnyMessageOwned::Mining(MiningOwned::OpenStandardMiningChannelSuccess(m))
                if m.request_id == request_id =>
            {
                Some(None)
            }
            _ => None,
        }
    };

    send.send(AnyMessageOwned::Mining(
        MiningOwned::OpenExtendedMiningChannel(OpenExtendedMiningChannelOwned {
            request_id,
            user_identity: user_identity.to_string().try_into().unwrap(),
            nominal_hash_rate: 1000.0,
            max_target: vec![0xff; 32].try_into().unwrap(),
            min_extranonce_size: 8,
        }),
    ))
    .await
    .unwrap();
    let extended = next_from_upstream(sniffer, reply(request_id)).await;

    send.send(AnyMessageOwned::Mining(
        MiningOwned::OpenStandardMiningChannel(OpenStandardMiningChannelOwned {
            request_id: request_id + 1,
            user_identity: user_identity.to_string().try_into().unwrap(),
            nominal_hash_rate: 1000.0,
            max_target: vec![0xff; 32].try_into().unwrap(),
        }),
    ))
    .await
    .unwrap();
    let standard = next_from_upstream(sniffer, reply(request_id + 1)).await;

    [extended, standard]
}

async fn channel_replies(payout_modes: AllowedPayoutModes) -> Vec<(String, [Option<String>; 2])> {
    let (_tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (pool, pool_addr) =
        start_pool_with_payout_modes(sv2_tp_config(tp_addr), None, payout_modes).await;
    let mut replies = Vec::new();
    let identities = payout_identities()
        .into_iter()
        .chain(POOL_IDENTITIES.iter().map(|s| s.to_string()));
    // One connection per identity, so the test also holds where a connection keeps the payout
    // mode of its first channel (upstream stratum-mining/sv2-apps#857).
    for (i, identity) in identities.enumerate() {
        let name = format!("payout_modes_{i}");
        let (sniffer, sniffer_addr) = start_sniffer(&name, pool_addr, false, vec![], None);
        let send = MockDownstream::new(
            sniffer_addr,
            WithSetup::yes_with_defaults(Protocol::MiningProtocol, 0),
        )
        .start()
        .await;
        sniffer
            .wait_for_message_type_and_clean_queue(
                MessageDirection::ToDownstream,
                MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
            )
            .await;
        let reply = open_both(&send, &sniffer, 10, &identity).await;
        replies.push((identity, reply));
    }
    shutdown_all!(pool);
    replies
}

#[tokio::test]
async fn pool_only_refuses_channels_whose_identity_pays_the_miner() {
    start_tracing();
    let refused = Some(ERROR_CODE_OPEN_MINING_CHANNEL_INVALID_USER_IDENTITY.to_string());
    for (identity, reply) in channel_replies(AllowedPayoutModes::PoolOnly).await {
        let pays_pool = POOL_IDENTITIES.contains(&identity.as_str());
        let expected = if pays_pool { None } else { refused.clone() };
        assert_eq!(reply, [expected.clone(), expected], "{identity:?}");
    }
}

#[tokio::test]
async fn any_accepts_every_payout_identity() {
    start_tracing();
    for (identity, reply) in channel_replies(AllowedPayoutModes::Any).await {
        assert_eq!(reply, [None, None], "{identity:?}");
    }
}

/// The witness commitment of a block holding only its coinbase: the witness root of a lone
/// coinbase is all zeros, and the coinbase witness reserved value is 32 zero bytes too.
fn coinbase_only_witness_commitment() -> TxOut {
    let commitment = sha256d::Hash::hash(&[0u8; 64]);
    let mut script = vec![0x6a, 0x24, 0xaa, 0x21, 0xa9, 0xed];
    script.extend_from_slice(commitment.as_byte_array());
    TxOut {
        value: Amount::ZERO,
        script_pubkey: ScriptBuf::from_bytes(script),
    }
}

/// A segwit coinbase for block `height`, split the way a JD client declares it, with an 8-byte
/// extranonce left out.
fn declared_coinbase(height: i64, outputs: Vec<TxOut>) -> (Vec<u8>, Vec<u8>) {
    const EXTRANONCE_SIZE: usize = 8;
    let script_sig = Builder::new()
        .push_int(height)
        .push_slice([0u8; EXTRANONCE_SIZE])
        .into_script();
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
    let bytes = serialize(&tx);
    // 43 fixed bytes, then a one-byte scriptSig length, then the scriptSig, whose last
    // EXTRANONCE_SIZE bytes are the extranonce
    let script_sig_end = 43 + 1 + script_sig.len();
    (
        bytes[..script_sig_end - EXTRANONCE_SIZE].to_vec(),
        bytes[script_sig_end..].to_vec(),
    )
}

/// Allocates a token, declares a job on top of the current tip whose coinbase has
/// `outputs(pool_outputs)` plus the witness commitment, and returns the DeclareMiningJobError
/// code, or `None` if the JDS accepted it (DeclareMiningJobSuccess or ProvideMissingTransactions).
async fn declare(
    send: &Sender<AnyMessageOwned>,
    sniffer: &Sniffer<'_>,
    request_id: u32,
    height: i64,
    outputs: impl FnOnce(Vec<TxOut>) -> Vec<TxOut>,
) -> Option<String> {
    send.send(AnyMessageOwned::JobDeclaration(
        JobDeclarationOwned::AllocateMiningJobToken(AllocateMiningJobTokenOwned {
            user_identifier: "jd-client".to_string().try_into().unwrap(),
            request_id,
        }),
    ))
    .await
    .unwrap();
    let (token, pool_outputs) = next_from_upstream(sniffer, |msg| match msg {
        AnyMessageOwned::JobDeclaration(JobDeclarationOwned::AllocateMiningJobTokenSuccess(m))
            if m.request_id == request_id =>
        {
            let pool_outputs: Vec<TxOut> = deserialize(m.coinbase_outputs.as_bytes()).unwrap();
            Some((m.mining_job_token, pool_outputs))
        }
        _ => None,
    })
    .await;

    let mut outputs = outputs(pool_outputs);
    outputs.push(coinbase_only_witness_commitment());
    let (prefix, suffix) = declared_coinbase(height, outputs);
    send.send(AnyMessageOwned::JobDeclaration(
        JobDeclarationOwned::DeclareMiningJob(DeclareMiningJobOwned {
            request_id,
            mining_job_token: token,
            version: 0x2000_0000,
            coinbase_tx_prefix: prefix.try_into().unwrap(),
            coinbase_tx_suffix: suffix.try_into().unwrap(),
            wtxid_list: Seq064KOwned::new(Vec::new()).unwrap(),
            excess_data: Vec::<u8>::new().try_into().unwrap(),
        }),
    ))
    .await
    .unwrap();
    next_from_upstream(sniffer, |msg| match msg {
        AnyMessageOwned::JobDeclaration(JobDeclarationOwned::DeclareMiningJobError(m))
            if m.request_id == request_id =>
        {
            Some(Some(m.error_code.as_utf8_or_hex()))
        }
        AnyMessageOwned::JobDeclaration(JobDeclarationOwned::DeclareMiningJobSuccess(m))
            if m.request_id == request_id =>
        {
            Some(None)
        }
        AnyMessageOwned::JobDeclaration(JobDeclarationOwned::ProvideMissingTransactions(m))
            if m.request_id == request_id =>
        {
            Some(None)
        }
        _ => None,
    })
    .await
}

fn pays_miner(sats: u64) -> TxOut {
    TxOut {
        value: Amount::from_sat(sats),
        script_pubkey: ScriptBuf::from_hex("00140f209a2a9a219947816f0edebe64f62d1498729f").unwrap(),
    }
}

/// Error codes for a declaration paying (pool only, pool and miner, miner only).
async fn declaration_replies(payout_modes: AllowedPayoutModes) -> [Option<String>; 3] {
    let (tp, _tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let jds_addr = utils::get_available_address();
    let (pool, _pool_addr) = start_pool_with_payout_modes(
        ipc_config(
            tp.bitcoin_core().data_dir().clone(),
            tp.bitcoin_core().is_signet(),
            None,
        ),
        Some(jds_addr),
        payout_modes,
    )
    .await;
    let (sniffer, sniffer_addr) = start_sniffer("payout_modes_jds", jds_addr, false, vec![], None);
    let send = MockDownstream::new(
        sniffer_addr,
        WithSetup::yes_with_defaults(Protocol::JobDeclarationProtocol, 0b0001),
    )
    .start()
    .await;
    sniffer
        .wait_for_message_type_and_clean_queue(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    let height = tp.get_blockchain_info().unwrap().blocks + 1;
    let pool_only = declare(&send, &sniffer, 1, height, |pool| pool).await;
    let pool_and_miner = declare(&send, &sniffer, 2, height, |mut pool| {
        pool.push(pays_miner(1));
        pool
    })
    .await;
    let miner_only = declare(&send, &sniffer, 3, height, |_| vec![pays_miner(1)]).await;
    shutdown_all!(pool);
    [pool_only, pool_and_miner, miner_only]
}

#[tokio::test]
async fn pool_only_jds_refuses_declared_coinbases_that_pay_anyone_else() {
    start_tracing();
    let refused = Some(ERROR_CODE_DECLARE_MINING_JOB_INVALID_COINBASE_TX.to_string());
    let [pool_only, pool_and_miner, miner_only] =
        declaration_replies(AllowedPayoutModes::PoolOnly).await;
    assert_eq!(
        pool_only, None,
        "a coinbase paying only the pool is accepted"
    );
    assert_eq!(pool_and_miner, refused);
    assert_eq!(miner_only, refused);
}

#[tokio::test]
async fn any_jds_does_not_check_who_a_declared_coinbase_pays() {
    start_tracing();
    // the same declarations pool_only refuses are accepted
    assert_eq!(
        declaration_replies(AllowedPayoutModes::Any).await,
        [None, None, None]
    );
}

// A real JD client declares coinbases built from the pool's AllocateMiningJobTokenSuccess
// outputs, so pool_only must not get in its way.
#[tokio::test]
async fn pool_only_accepts_a_jd_client_mining_for_the_pool() {
    start_tracing();
    let (tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let jds_addr = utils::get_available_address();
    let (pool, pool_addr) = start_pool_with_payout_modes(
        ipc_config(
            tp.bitcoin_core().data_dir().clone(),
            tp.bitcoin_core().is_signet(),
            None,
        ),
        Some(jds_addr),
        AllowedPayoutModes::PoolOnly,
    )
    .await;
    let (jdc_pool_sniffer, jdc_pool_sniffer_addr) =
        start_sniffer("jdc-pool", pool_addr, false, vec![], None);
    let (jdc_jds_sniffer, jdc_jds_sniffer_addr) =
        start_sniffer("jdc-jds", jds_addr, false, vec![], None);
    let (jdc, jdc_addr, _) = start_jdc(
        &[(jdc_pool_sniffer_addr, jdc_jds_sniffer_addr)],
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        false,
        None,
    );
    let (translator, tproxy_addr, _) =
        start_sv2_translator(&[jdc_addr], false, vec![], vec![], None, false).await;
    let (_minerd_process, _minerd_addr) = start_minerd(tproxy_addr, None, None, false).await;

    jdc_jds_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_DECLARE_MINING_JOB_SUCCESS,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SET_CUSTOM_MINING_JOB_SUCCESS,
        )
        .await;
    assert!(!jdc_jds_sniffer.has_message_type(
        MessageDirection::ToDownstream,
        MESSAGE_TYPE_DECLARE_MINING_JOB_ERROR
    ));

    shutdown_all!(translator, jdc, pool);
}
