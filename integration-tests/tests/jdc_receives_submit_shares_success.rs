use integration_tests_sv2::{
    interceptor::MessageDirection, metrics_assert::*, template_provider::DifficultyLevel, *,
};
use stratum_apps::stratum_core::mining_sv2::*;

#[tokio::test]
async fn jdc_submit_shares_success() {
    start_tracing();
    let (tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (_pool, pool_addr, pool_monitoring, _pool_task) =
        start_pool_with_monitoring(sv2_tp_config(tp_addr), vec![], vec![], true).await;
    let (sniffer, sniffer_addr) = start_sniffer("0", pool_addr, false, vec![], None);
    let (_jds, jds_addr) = start_jds(tp.rpc_info());
    let (_jdc, jdc_addr) = start_jdc(
        &[(sniffer_addr, jds_addr)],
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
    );
    let (_translator, tproxy_addr) =
        start_sv2_translator(&[jdc_addr], false, vec![], vec![], None).await;
    let (_minerd_process, _minerd_addr) = start_minerd(tproxy_addr, None, None, false).await;

    // make sure sure JDC gets a share acknowledgement
    sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;

    // -- Metrics validation --
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let pool_mon = pool_monitoring.expect("pool monitoring should be enabled");
    assert_api_health(pool_mon).await;
    let pool_metrics = fetch_metrics(pool_mon).await;
    assert_uptime(&pool_metrics);
    assert_metric_gte(&pool_metrics, "sv2_clients_total", 1.0);
    assert_metric_present(&pool_metrics, "sv2_client_shares_accepted_total");
}
