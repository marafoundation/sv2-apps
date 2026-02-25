use integration_tests_sv2::{
    interceptor::MessageDirection, metrics_assert::*, template_provider::DifficultyLevel, *,
};
use stratum_apps::stratum_core::{common_messages_sv2::*, mining_sv2::*};

// Also validates metrics across the full JD topology: tProxy, JDC, and Pool.
#[tokio::test]
async fn jd_non_aggregated_tproxy_integration() {
    start_tracing();
    let (tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (_pool, pool_addr, pool_monitoring, _pool_task) =
        start_pool_with_monitoring(sv2_tp_config(tp_addr), vec![], vec![], true).await;
    let (jdc_pool_sniffer, jdc_pool_sniffer_addr) =
        start_sniffer("0", pool_addr, false, vec![], None);
    let (_jds, jds_addr) = start_jds(tp.rpc_info());
    let (_jdc, jdc_addr, jdc_monitoring, _jdc_task) = start_jdc_with_monitoring(
        &[(jdc_pool_sniffer_addr, jds_addr)],
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        true,
    );
    let (tproxy_jdc_sniffer, tproxy_jdc_sniffer_addr) =
        start_sniffer("1", jdc_addr, false, vec![], None);
    let (_translator, tproxy_addr, tproxy_monitoring, _tproxy_task) =
        start_sv2_translator_with_monitoring(
            &[tproxy_jdc_sniffer_addr],
            false,
            vec![],
            vec![],
            None,
            true,
        )
        .await;

    // start two minerd processes
    let (_minerd_process, _minerd_addr) = start_minerd(tproxy_addr, None, None, false).await;
    let (_minerd_process, _minerd_addr) = start_minerd(tproxy_addr, None, None, false).await;

    // assert that two OpenExtendedMiningChannel messages are present in the queue
    // because two minerd processes are started
    {
        tproxy_jdc_sniffer
            .wait_for_message_type_and_clean_queue(
                MessageDirection::ToUpstream,
                MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
            )
            .await;
        tproxy_jdc_sniffer
            .wait_for_message_type_and_clean_queue(
                MessageDirection::ToUpstream,
                MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
            )
            .await;
    }

    jdc_pool_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToUpstream,
            MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCESS,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToUpstream,
            MESSAGE_TYPE_SUBMIT_SHARES_EXTENDED,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;

    // -- Metrics validation for full JD non-aggregated topology --
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // tProxy: non-aggregated = 2 upstream extended channels (one per miner), 2 SV1 clients
    let tproxy_mon = tproxy_monitoring.expect("tproxy monitoring should be enabled");
    assert_api_health(tproxy_mon).await;
    let tproxy_metrics = fetch_metrics(tproxy_mon).await;
    assert_uptime(&tproxy_metrics);
    assert_metric_present(&tproxy_metrics, "sv2_server_channels");
    assert_metric_gte(&tproxy_metrics, "sv1_clients_total", 2.0);
    assert_metric_not_present(&tproxy_metrics, "sv2_clients_total");

    // JDC: sees tProxy's channels as SV2 clients, has 1 upstream extended channel to Pool
    let jdc_mon = jdc_monitoring.expect("jdc monitoring should be enabled");
    assert_api_health(jdc_mon).await;
    let jdc_metrics = fetch_metrics(jdc_mon).await;
    assert_uptime(&jdc_metrics);
    assert_metric_present(&jdc_metrics, "sv2_server_channels");
    assert_metric_gte(&jdc_metrics, "sv2_clients_total", 1.0);

    // Pool: sees JDC as 1 SV2 client, no server channels (pool has no upstream)
    let pool_mon = pool_monitoring.expect("pool monitoring should be enabled");
    assert_api_health(pool_mon).await;
    let pool_metrics = fetch_metrics(pool_mon).await;
    assert_uptime(&pool_metrics);
    assert_metric_gte(&pool_metrics, "sv2_clients_total", 1.0);
    assert_metric_not_present(&pool_metrics, "sv2_server_channels");
    assert_metric_present(&pool_metrics, "sv2_client_shares_accepted_total");
}

// Also validates aggregated topology metrics: single upstream channel despite multiple miners.
#[tokio::test]
async fn jd_aggregated_tproxy_integration() {
    start_tracing();
    let (tp, tp_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (_pool, pool_addr, pool_monitoring, _pool_task) =
        start_pool_with_monitoring(sv2_tp_config(tp_addr), vec![], vec![], true).await;
    let (jdc_pool_sniffer, jdc_pool_sniffer_addr) =
        start_sniffer("0", pool_addr, false, vec![], None);
    let (_jds, jds_addr) = start_jds(tp.rpc_info());
    let (_jdc, jdc_addr, _jdc_monitoring, _jdc_task) = start_jdc_with_monitoring(
        &[(jdc_pool_sniffer_addr, jds_addr)],
        sv2_tp_config(tp_addr),
        vec![],
        vec![],
        true,
    );
    let (tproxy_jdc_sniffer, tproxy_jdc_sniffer_addr) =
        start_sniffer("1", jdc_addr, false, vec![], None);
    let (_translator, tproxy_addr, tproxy_monitoring, _tproxy_task) =
        start_sv2_translator_with_monitoring(
            &[tproxy_jdc_sniffer_addr],
            true,
            vec![],
            vec![],
            None,
            true,
        )
        .await;

    // start two minerd processes
    let (_minerd_process, _minerd_addr) = start_minerd(tproxy_addr, None, None, false).await;
    let (_minerd_process, _minerd_addr) = start_minerd(tproxy_addr, None, None, false).await;

    // assert that only one OpenExtendedMiningChannel message is present in the queue
    {
        tproxy_jdc_sniffer
            .wait_for_message_type_and_clean_queue(
                MessageDirection::ToUpstream,
                MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
            )
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        tproxy_jdc_sniffer
            .assert_message_not_present(
                MessageDirection::ToUpstream,
                MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
            )
            .await;
    }

    jdc_pool_sniffer
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToUpstream,
            MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_OPEN_EXTENDED_MINING_CHANNEL_SUCCESS,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToUpstream,
            MESSAGE_TYPE_SUBMIT_SHARES_EXTENDED,
        )
        .await;
    jdc_pool_sniffer
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SUBMIT_SHARES_SUCCESS,
        )
        .await;

    // -- Metrics validation for aggregated topology --
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // tProxy aggregated: 2 SV1 clients but only 1 upstream extended channel
    let tproxy_mon = tproxy_monitoring.expect("tproxy monitoring should be enabled");
    assert_api_health(tproxy_mon).await;
    let tproxy_metrics = fetch_metrics(tproxy_mon).await;
    assert_uptime(&tproxy_metrics);
    assert_metric_present(&tproxy_metrics, "sv2_server_channels");
    assert_metric_gte(&tproxy_metrics, "sv1_clients_total", 2.0);
    assert_metric_not_present(&tproxy_metrics, "sv2_clients_total");

    // Pool: sees 1 SV2 client (JDC), shares accepted
    let pool_mon = pool_monitoring.expect("pool monitoring should be enabled");
    assert_api_health(pool_mon).await;
    let pool_metrics = fetch_metrics(pool_mon).await;
    assert_uptime(&pool_metrics);
    assert_metric_gte(&pool_metrics, "sv2_clients_total", 1.0);
    assert_metric_not_present(&pool_metrics, "sv2_server_channels");
    assert_metric_present(&pool_metrics, "sv2_client_shares_accepted_total");
}
