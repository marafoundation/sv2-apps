use integration_tests_sv2::{
    interceptor::MessageDirection, metrics_assert::*, template_provider::DifficultyLevel, *,
};
use stratum_apps::stratum_core::common_messages_sv2::*;

#[tokio::test]
async fn sv2_mining_device_and_pool_success() {
    start_tracing();
    let (_tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (_pool, pool_addr, pool_monitoring, _pool_task) =
        start_pool_with_monitoring(sv2_tp_config(tp_addr), vec![], vec![], true).await;
    let (sniffer, sniffer_addr) = start_sniffer("A", pool_addr, false, vec![], None);
    start_mining_device_sv2(sniffer_addr, None, None, None, 1, None, true);
    sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;

    // -- Metrics validation --
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let pool_mon = pool_monitoring.expect("pool monitoring should be enabled");
    assert_api_health(pool_mon).await;
    let pool_metrics = fetch_metrics(pool_mon).await;
    assert_uptime(&pool_metrics);
    assert_metric_gte(&pool_metrics, "sv2_clients_total", 1.0);
}
