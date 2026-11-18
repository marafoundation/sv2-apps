//! Tests for `min_channel_nominal_hash_rate`: the floor applied to a channel's declared nominal
//! hashrate on open and on `UpdateChannel`, and the vardiff state seeded alongside it.

use std::{
    net::SocketAddr,
    sync::{Arc, atomic::AtomicU32},
};

use async_channel::{Receiver, unbounded};
use stratum_apps::{
    config_helpers::CoinbaseRewardScript,
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
    network_helpers::{accept_noise_connection, connect_with_noise},
    stratum_core::{
        binary_sv2::U256Owned,
        bitcoin::Target,
        channels_sv2::{
            Vardiff, seen_shares_budget,
            target::{hash_rate_from_target, hash_rate_to_target},
        },
        handlers_sv2::HandleMiningMessagesFromClientOwnedAsync,
        mining_sv2::{
            OpenExtendedMiningChannelOwned, OpenStandardMiningChannelOwned, UpdateChannelOwned,
        },
        parsers_sv2::MiningOwned,
        template_distribution_sv2::{NewTemplateOwned, SetNewPrevHashOwned},
    },
    task_manager::TaskManager,
    tp_type::TemplateProviderType,
};
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::{
    config::{AuthorityConfig, ConnectionConfig, PoolConfig},
    utils::DownstreamMessage,
};

const SHARES_PER_MINUTE: f32 = 6.0;
const DECLARED: f32 = 1e9;
const ACTUAL: f64 = 2e14;
const FLOOR: f32 = 1e13;
const DOWNSTREAM_ID: DownstreamId = 7;
const PUBLIC_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
const SECRET_KEY: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";

fn target_for(hash_rate: f32) -> Target {
    hash_rate_to_target(hash_rate.into(), SHARES_PER_MINUTE.into()).expect("valid hashrate")
}

fn max_target() -> U256Owned {
    Target::MAX.to_le_bytes().into()
}

fn target_of(bytes: &U256Owned) -> Target {
    Target::from_le_bytes(bytes.to_array())
}

/// Seconds a miner hashing at `actual` H/s needs to fill the seen-shares budget of a channel
/// whose target was derived for `effective` H/s, as `budget / (shares per second)`.
fn seconds_to_exhaust_budget(effective: f32, actual: f64) -> f64 {
    let target = target_for(effective);
    let hash_rate_at_target = hash_rate_from_target(target.to_le_bytes().into(), 1.0)
        .expect("target converts back to a hashrate");
    let shares_per_minute_at_actual = actual / hash_rate_at_target;
    seen_shares_budget(SHARES_PER_MINUTE.into()) as f64 / (shares_per_minute_at_actual / 60.0)
}

#[test]
fn declaring_a_thousandth_of_a_miners_rate_exhausts_the_budget_within_a_second() {
    assert_eq!(seen_shares_budget(SHARES_PER_MINUTE.into()), 7_200);
    assert!(
        seconds_to_exhaust_budget(clamp_nominal_hash_rate(DECLARED, 0.0), ACTUAL) < 1.0,
        "without a floor the declared rate drives the flood"
    );
}

#[test]
fn the_floor_keeps_the_budget_alive_past_the_vardiff_tick() {
    let effective = clamp_nominal_hash_rate(DECLARED, FLOOR);
    assert_eq!(effective, FLOOR);
    assert_eq!(target_for(effective), target_for(FLOOR));
    assert_ne!(target_for(effective), target_for(DECLARED));
    assert!(
        seconds_to_exhaust_budget(effective, ACTUAL) > 60.0,
        "vardiff ticks every 60 s and must get to correct the target before the budget runs out"
    );
}

fn new_template() -> NewTemplateOwned {
    NewTemplateOwned {
        template_id: 1,
        future_template: true,
        version: 536870912,
        coinbase_tx_version: 2,
        coinbase_prefix: vec![2, 159, 0, 0].try_into().unwrap(),
        coinbase_tx_input_sequence: 4294967294,
        coinbase_tx_value_remaining: 5_000_000_000,
        coinbase_tx_outputs_count: 0,
        coinbase_tx_outputs: vec![].try_into().unwrap(),
        coinbase_tx_locktime: 158,
        merkle_path: vec![].try_into().unwrap(),
    }
}

fn new_prev_hash() -> SetNewPrevHashOwned {
    SetNewPrevHashOwned {
        template_id: 1,
        prev_hash: [7u8; 32].into(),
        ntime_start: 1747092633,
        n_bits: 503543726,
        target: Target::MAX.to_le_bytes().into(),
    }
}

/// A channel manager with one registered downstream, whose outgoing messages arrive on `rx`.
struct Fixture {
    channel_manager: ChannelManager,
    rx: Receiver<DownstreamMessage>,
    // Keep the peer's end of the connection and the I/O tasks alive for the test.
    _peer: stratum_apps::network_helpers::noise_stream::NoiseTcpStream,
    _tasks: Arc<TaskManager>,
}

impl Fixture {
    async fn new(floor: f32) -> Self {
        let address: SocketAddr = "127.0.0.1:34254".parse().unwrap();
        let mut config = PoolConfig::new(
            ConnectionConfig::new(address, 3600, "test".to_string()),
            TemplateProviderType::Sv2Tp {
                address: "127.0.0.1:8442".to_string(),
                public_key: None,
            },
            AuthorityConfig::new(
                Secp256k1PublicKey::try_from(PUBLIC_KEY.to_string()).unwrap(),
                Secp256k1SecretKey::try_from(SECRET_KEY.to_string()).unwrap(),
            ),
            CoinbaseRewardScript::from_descriptor(
                "addr(tb1qa0sm0hxzj0x25rh8gw5xlzwlsfvvyz8u96w3p8)",
            )
            .unwrap(),
            SHARES_PER_MINUTE,
            1,
            1,
            vec![],
            vec![],
            None,
            None,
            None,
        );
        config.set_min_channel_nominal_hash_rate(floor);

        let (tp_sender, tp_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let channel_manager = ChannelManager::new(
            config,
            tp_sender,
            tp_receiver,
            downstream_receiver,
            vec![],
            None,
        )
        .await
        .expect("channel manager");
        channel_manager
            .last_future_template
            .set(Some(new_template()))
            .unwrap();
        channel_manager
            .last_new_prev_hash
            .set(Some(new_prev_hash()))
            .unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listen_address = listener.local_addr().unwrap();
        let public_key = Secp256k1PublicKey::try_from(PUBLIC_KEY.to_string()).unwrap();
        let secret_key = Secp256k1SecretKey::try_from(SECRET_KEY.to_string()).unwrap();
        let (server, client) = tokio::join!(
            async {
                let (stream, _) = listener.accept().await.unwrap();
                accept_noise_connection(stream, public_key, secret_key, 3600)
                    .await
                    .unwrap()
            },
            async {
                let stream = TcpStream::connect(listen_address).await.unwrap();
                connect_with_noise(stream, Some(public_key)).await.unwrap()
            }
        );

        let tasks = Arc::new(TaskManager::new());
        let channel_id_factory = AtomicU32::new(1);
        let group_channel_id = channel_id_factory.fetch_add(1, Ordering::SeqCst);
        let group_channel = channel_manager
            .bootstrap_group_channel(group_channel_id)
            .unwrap()
            .expect("group channel");
        let (cm_sender, _cm_inbox) = unbounded();
        let (_cm_outbox, cm_receiver) = unbounded();
        let downstream = Downstream::new(
            DOWNSTREAM_ID,
            channel_id_factory,
            group_channel,
            cm_sender,
            cm_receiver,
            server,
            stratum_apps::bitcoin_core_sv2::CancellationToken::new(),
            tasks.clone(),
            vec![],
            vec![],
        );
        let (tx, rx) = unbounded();
        channel_manager
            .channel_manager_io
            .downstream_sender
            .insert(DOWNSTREAM_ID, tx);
        channel_manager
            .downstreams
            .insert(DOWNSTREAM_ID, downstream);

        Self {
            channel_manager,
            rx,
            _peer: client,
            _tasks: tasks,
        }
    }

    async fn open_standard(&mut self, declared: f32) -> (u32, Target) {
        let msg = OpenStandardMiningChannelOwned {
            request_id: 1,
            user_identity: "miner".to_string().try_into().unwrap(),
            nominal_hash_rate: declared,
            max_target: max_target(),
        };
        self.channel_manager
            .handle_open_standard_mining_channel(Some(DOWNSTREAM_ID), msg, None)
            .await
            .unwrap();
        self.drain()
            .into_iter()
            .find_map(|m| match m {
                MiningOwned::OpenStandardMiningChannelSuccess(s) => {
                    Some((s.channel_id, target_of(&s.target)))
                }
                _ => None,
            })
            .expect("OpenStandardMiningChannelSuccess")
    }

    async fn open_extended(&mut self, declared: f32) -> (u32, Target) {
        let msg = OpenExtendedMiningChannelOwned {
            request_id: 2,
            user_identity: "miner".to_string().try_into().unwrap(),
            nominal_hash_rate: declared,
            max_target: max_target(),
            min_extranonce_size: 8,
        };
        self.channel_manager
            .handle_open_extended_mining_channel(Some(DOWNSTREAM_ID), msg, None)
            .await
            .unwrap();
        self.drain()
            .into_iter()
            .find_map(|m| match m {
                MiningOwned::OpenExtendedMiningChannelSuccess(s) => {
                    Some((s.channel_id, target_of(&s.target)))
                }
                _ => None,
            })
            .expect("OpenExtendedMiningChannelSuccess")
    }

    /// Everything the pool has queued for the downstream so far.
    fn drain(&self) -> Vec<MiningOwned> {
        std::iter::from_fn(|| self.rx.try_recv().ok().map(|(message, _)| message)).collect()
    }

    /// Sends an `UpdateChannel` and returns the messages the pool answers with.
    async fn update(&mut self, channel_id: u32, declared: f32) -> Vec<MiningOwned> {
        let msg = UpdateChannelOwned {
            channel_id,
            nominal_hash_rate: declared,
            max_target: max_target(),
        };
        self.channel_manager
            .handle_update_channel(Some(DOWNSTREAM_ID), msg, None)
            .await
            .unwrap();
        self.drain()
    }

    fn vardiff_min(&self, channel_id: u32) -> f32 {
        self.channel_manager
            .vardiff
            .with(&(DOWNSTREAM_ID, channel_id).into(), |v| {
                v.min_allowed_hashrate()
            })
            .expect("vardiff state exists")
    }
}

fn set_target(messages: Vec<MiningOwned>) -> Target {
    messages
        .into_iter()
        .find_map(|m| match m {
            MiningOwned::SetTarget(s) => Some(target_of(&s.target)),
            _ => None,
        })
        .expect("SetTarget")
}

#[tokio::test]
async fn standard_open_below_the_floor_gets_the_floors_target() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (channel_id, target) = fixture.open_standard(DECLARED).await;
    assert_eq!(target, target_for(FLOOR));
    assert_ne!(target, target_for(DECLARED));
    assert_eq!(
        fixture
            .channel_manager
            .downstreams
            .with(&DOWNSTREAM_ID, |d| d
                .standard_channels
                .with(&channel_id, |c| c.get_nominal_hashrate())
                .unwrap())
            .unwrap(),
        FLOOR
    );
}

#[tokio::test]
async fn extended_open_below_the_floor_gets_the_floors_target() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (channel_id, target) = fixture.open_extended(DECLARED).await;
    assert_eq!(target, target_for(FLOOR));
    assert_eq!(
        fixture
            .channel_manager
            .downstreams
            .with(&DOWNSTREAM_ID, |d| d
                .extended_channels
                .with(&channel_id, |c| c.get_nominal_hashrate())
                .unwrap())
            .unwrap(),
        FLOOR
    );
}

#[tokio::test]
async fn opens_at_or_above_the_floor_keep_the_declared_rate() {
    let mut fixture = Fixture::new(FLOOR).await;
    assert_eq!(fixture.open_standard(2e14).await.1, target_for(2e14));
    assert_eq!(fixture.open_extended(FLOOR).await.1, target_for(FLOOR));
}

#[tokio::test]
async fn without_a_floor_opens_use_the_declared_rate() {
    let mut fixture = Fixture::new(0.0).await;
    assert_eq!(
        fixture.open_standard(DECLARED).await.1,
        target_for(DECLARED)
    );
    assert_eq!(
        fixture.open_extended(DECLARED).await.1,
        target_for(DECLARED)
    );
}

#[tokio::test]
async fn update_channel_below_the_floor_is_clamped_for_standard_channels() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (channel_id, _) = fixture.open_standard(2e14).await;
    let answer = fixture.update(channel_id, DECLARED).await;
    assert_eq!(set_target(answer), target_for(FLOOR));
    let answer = fixture.update(channel_id, 5e13).await;
    assert_eq!(set_target(answer), target_for(5e13));
}

#[tokio::test]
async fn update_channel_below_the_floor_is_clamped_for_extended_channels() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (channel_id, _) = fixture.open_extended(2e14).await;
    let answer = fixture.update(channel_id, DECLARED).await;
    assert_eq!(set_target(answer), target_for(FLOOR));
    let answer = fixture.update(channel_id, 5e13).await;
    assert_eq!(set_target(answer), target_for(5e13));
}

#[tokio::test]
async fn without_a_floor_update_channel_uses_the_declared_rate() {
    let mut fixture = Fixture::new(0.0).await;
    let (channel_id, _) = fixture.open_standard(2e14).await;
    let answer = fixture.update(channel_id, DECLARED).await;
    assert_eq!(set_target(answer), target_for(DECLARED));
}

#[tokio::test]
async fn an_invalid_declared_rate_is_still_rejected_with_a_floor() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (channel_id, _) = fixture.open_standard(2e14).await;
    for declared in [f32::NAN, f32::INFINITY, -1.0] {
        let error = fixture
            .update(channel_id, declared)
            .await
            .into_iter()
            .find_map(|m| match m {
                MiningOwned::UpdateChannelError(e) => Some(e),
                _ => None,
            })
            .unwrap_or_else(|| panic!("expected UpdateChannelError for {declared}"));
        assert_eq!(
            error.error_code.as_utf8_or_hex(),
            "invalid-nominal-hashrate",
            "declared {declared}"
        );
    }
}

#[tokio::test]
async fn vardiff_is_seeded_with_the_floor_and_cannot_lower_a_channel_below_it() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (std_id, _) = fixture.open_standard(DECLARED).await;
    let (ext_id, _) = fixture.open_extended(DECLARED).await;
    assert_eq!(fixture.vardiff_min(std_id), FLOOR);
    assert_eq!(fixture.vardiff_min(ext_id), FLOOR);

    // An idle channel two minutes into its window is cut to a third of its rate; vardiff
    // runs on the channel's own (clamped) rate and must not go under the floor.
    let key = (DOWNSTREAM_ID, std_id).into();
    let proposed = fixture
        .channel_manager
        .vardiff
        .with_mut(&key, |v| {
            v.timestamp_of_last_update -= 120;
            v.try_vardiff(FLOOR, &target_for(FLOOR), SHARES_PER_MINUTE)
                .unwrap()
        })
        .unwrap();
    assert_eq!(proposed, Some(FLOOR));
}

#[tokio::test]
async fn without_a_floor_vardiff_keeps_its_default_minimum() {
    let mut fixture = Fixture::new(0.0).await;
    let (channel_id, _) = fixture.open_standard(DECLARED).await;
    assert_eq!(fixture.vardiff_min(channel_id), 1.0);
}

impl Fixture {
    /// Runs one vardiff tick as if a full window had passed with no shares submitted.
    async fn idle_vardiff_tick(&self) -> Vec<MiningOwned> {
        for key in self.channel_manager.vardiff.keys() {
            self.channel_manager
                .vardiff
                .with_mut(&key, |v| v.timestamp_of_last_update -= 120);
        }
        self.channel_manager.run_vardiff().await.unwrap();
        self.drain()
    }
}

fn set_targets(messages: &[MiningOwned]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m, MiningOwned::SetTarget(_)))
        .count()
}

#[tokio::test]
async fn standard_channel_held_at_the_floor_gets_no_further_set_target() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (channel_id, _) = fixture.open_standard(4e13).await;

    // 4e13 -> 1.33e13 (above the floor), then the next idle window would go below it.
    assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 1);
    assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 1);
    let nominal = |f: &Fixture| {
        f.channel_manager
            .downstreams
            .with(&DOWNSTREAM_ID, |d| {
                d.standard_channels
                    .with(&channel_id, |c| c.get_nominal_hashrate())
                    .unwrap()
            })
            .unwrap()
    };
    assert_eq!(nominal(&fixture), FLOOR);

    for _ in 0..2 {
        assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 0);
        assert_eq!(nominal(&fixture), FLOOR);
    }
}

#[tokio::test]
async fn extended_channel_held_at_the_floor_gets_no_further_set_target() {
    let mut fixture = Fixture::new(FLOOR).await;
    let (channel_id, _) = fixture.open_extended(4e13).await;

    assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 1);
    assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 1);
    let nominal = |f: &Fixture| {
        f.channel_manager
            .downstreams
            .with(&DOWNSTREAM_ID, |d| {
                d.extended_channels
                    .with(&channel_id, |c| c.get_nominal_hashrate())
                    .unwrap()
            })
            .unwrap()
    };
    assert_eq!(nominal(&fixture), FLOOR);

    for _ in 0..2 {
        assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 0);
        assert_eq!(nominal(&fixture), FLOOR);
    }
}

#[tokio::test]
async fn without_a_floor_vardiff_still_sends_set_target_at_its_default_minimum() {
    let mut fixture = Fixture::new(0.0).await;
    fixture.open_standard(1.0).await;
    assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 1);
    assert_eq!(set_targets(&fixture.idle_vardiff_tick().await), 1);
}
