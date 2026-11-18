// A miner that declares far less hashrate than it has gets a channel target that is far too easy,
// and floods the Pool with shares until the accepted-share dedup budget of the chain tip runs out
// and the Pool closes the connection. `min_channel_nominal_hash_rate` floors the declared rate.
//
// The topology: Sv2 mining device -> sniffer -> Pool -> high-difficulty Template Provider (so the
// device never finds a block). The Pool wants 6 shares per minute, which gives a budget of 7,200
// accepted shares per chain tip, and the device declares a ten-thousandth of what it hashes.
use integration_tests_sv2::{
    mining_device::{measure_hashrate, set_cores},
    prometheus_metrics_assertions::MonitoringApi,
    template_provider::DifficultyLevel,
    *,
};
use std::time::{Duration, Instant};
use stratum_apps::stratum_core::{
    channels_sv2::seen_shares_budget,
    mining_sv2::{
        MESSAGE_TYPE_SUBMIT_SHARES_ERROR, MESSAGE_TYPE_SUBMIT_SHARES_STANDARD,
        MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
    },
};

const POOL_SHARES_PER_MINUTE: f32 = 6.0;
const DECLARED_FRACTION: f32 = 1e-4;
const HANDICAP: u32 = 1;
const OBSERVATION_WINDOW: Duration = Duration::from_secs(30);

struct Outcome {
    pool_closed_the_connection: bool,
    shares_submitted: usize,
    shares_accepted: usize,
    shares_rejected: usize,
}

fn clients_total(metrics: &str) -> Option<f64> {
    metrics
        .lines()
        .find_map(|line| line.strip_prefix("sv2_clients_total "))
        .and_then(|value| value.trim().parse().ok())
}

/// Runs the scenario with a floor derived from the device's measured hashrate.
async fn run(floor: impl FnOnce(f32) -> f32) -> Outcome {
    start_tracing();
    set_cores(1);
    let device_hash_rate = measure_hashrate(5, HANDICAP) as f32;
    let floor = floor(device_hash_rate);

    let (_tp, tp_addr) = start_template_provider(None, DifficultyLevel::High);
    let (pool, pool_addr, pool_monitoring) = start_pool_with_floor(
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        true,
        1,
        POOL_SHARES_PER_MINUTE,
        floor,
    )
    .await;
    let monitoring =
        MonitoringApi::builder(pool_monitoring.expect("pool monitoring should be enabled")).build();
    let (sniffer, sniffer_addr) = start_sniffer("min-hash-rate", pool_addr, false, vec![], None);

    start_mining_device_sv2(
        sniffer_addr,
        None,
        None,
        None,
        HANDICAP,
        Some(DECLARED_FRACTION),
        false,
    );
    monitoring
        .poll_metric_gte("sv2_clients_total", 1.0, Duration::from_secs(30))
        .await;

    let mut outcome = Outcome {
        pool_closed_the_connection: false,
        shares_submitted: 0,
        shares_accepted: 0,
        shares_rejected: 0,
    };
    let drain_sniffer = |outcome: &mut Outcome| {
        while let Some((message_type, _)) = sniffer.next_message_from_downstream() {
            if message_type == MESSAGE_TYPE_SUBMIT_SHARES_STANDARD {
                outcome.shares_submitted += 1;
            }
        }
        while let Some((message_type, _)) = sniffer.next_message_from_upstream() {
            match message_type {
                MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS => outcome.shares_accepted += 1,
                MESSAGE_TYPE_SUBMIT_SHARES_ERROR => outcome.shares_rejected += 1,
                _ => {}
            }
        }
    };
    let started = Instant::now();
    while started.elapsed() < OBSERVATION_WINDOW && !outcome.pool_closed_the_connection {
        drain_sniffer(&mut outcome);
        outcome.pool_closed_the_connection =
            clients_total(&monitoring.fetch_metrics().await).is_some_and(|clients| clients < 1.0);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drain_sniffer(&mut outcome);
    pool.shutdown().await;
    outcome
}

#[tokio::test]
async fn without_a_floor_the_pool_closes_a_miner_that_declares_too_little() {
    let outcome = run(|_| 0.0).await;
    assert!(
        outcome.pool_closed_the_connection,
        "the Pool should close the connection once the share budget is exhausted ({} shares \
         submitted)",
        outcome.shares_submitted
    );
    // The Pool does not log to the test, so the budget disconnect is identified from the wire:
    // it closes the connection without a SubmitSharesError, and only once it has accepted a
    // full budget of shares (each accepted share is answered with its own success, as the test
    // Pool's batch size is 1). An unrelated failure would not line up with both.
    assert_eq!(outcome.shares_rejected, 0);
    assert!(
        outcome.shares_accepted >= seen_shares_budget(POOL_SHARES_PER_MINUTE.into()),
        "only {} shares accepted before the close",
        outcome.shares_accepted
    );
}

#[tokio::test]
async fn with_a_floor_the_pool_keeps_a_miner_that_declares_too_little() {
    let outcome = run(|device_hash_rate| device_hash_rate).await;
    assert!(
        !outcome.pool_closed_the_connection,
        "the Pool must not close the connection"
    );
    assert!(
        outcome.shares_submitted < 1000,
        "{} shares submitted in {OBSERVATION_WINDOW:?}",
        outcome.shares_submitted
    );
}
