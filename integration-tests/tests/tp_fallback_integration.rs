// Integration tests for Template Provider fallback behavior.
//
// These tests verify that Pool and JDC correctly fall back to the next
// Template Provider in priority order when the active TP connection is lost.
use integration_tests_sv2::{interceptor::MessageDirection, template_provider::DifficultyLevel, *};
use stratum_apps::stratum_core::{
    common_messages_sv2::{MESSAGE_TYPE_SETUP_CONNECTION, MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS},
    template_distribution_sv2::MESSAGE_TYPE_NEW_TEMPLATE,
};

/// Pool falls back from TP1 to TP2 when TP1's process is killed.
///
/// Setup:
///   TP1 -> Sniffer1 <- Pool -> Sniffer2 -> TP2
///
/// 1. Start two Template Providers (TP1 and TP2).
/// 2. Place a sniffer in front of each TP.
/// 3. Start Pool configured with [sniffer1_addr, sniffer2_addr] as ordered TP list.
/// 4. Verify Pool connects to TP1 (sniffer1 sees SetupConnection exchange).
/// 5. Kill TP1 by dropping it.
/// 6. Verify Pool falls back to TP2 (sniffer2 sees SetupConnection exchange).
#[tokio::test]
async fn pool_falls_back_to_second_tp_on_first_tp_shutdown() {
    start_tracing();

    // Start two independent Template Providers, each with their own Bitcoin Core.
    let (tp1, tp1_addr) = start_template_provider(None, DifficultyLevel::Low);
    let (_tp2, tp2_addr) = start_template_provider(None, DifficultyLevel::Low);

    // Sniffers between Pool and each TP.
    // check_on_drop=false because sniffer1 will have its upstream killed mid-test.
    let (sniffer1, sniffer1_addr) = start_sniffer("TP1", tp1_addr, false, vec![], None);
    let (sniffer2, sniffer2_addr) = start_sniffer("TP2", tp2_addr, false, vec![], None);

    // Pool configured with two TPs: TP1 (primary) then TP2 (fallback).
    let (_pool, _pool_addr) = start_pool_with_tp_configs(
        vec![sv2_tp_config(sniffer1_addr), sv2_tp_config(sniffer2_addr)],
        vec![],
        vec![],
    )
    .await;

    // Verify Pool connects to TP1 successfully.
    sniffer1
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    sniffer1
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;
    // Wait for NewTemplate to confirm the TP connection is fully operational.
    sniffer1
        .wait_for_message_type(MessageDirection::ToDownstream, MESSAGE_TYPE_NEW_TEMPLATE)
        .await;

    // Kill TP1 — this terminates sv2-tp and Bitcoin Core, breaking the connection.
    drop(tp1);

    // Pool should detect the connection loss and fall back to TP2.
    // Verify sniffer2 sees the Pool connecting to TP2.
    sniffer2
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    sniffer2
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;
    // Confirm Pool receives templates from TP2.
    sniffer2
        .wait_for_message_type(MessageDirection::ToDownstream, MESSAGE_TYPE_NEW_TEMPLATE)
        .await;
}

/// JDC falls back from TP1 to TP2 when TP1's process is killed.
///
/// Setup (only 2 Bitcoin Core instances to stay within file descriptor limits):
///   TP1 (dedicated, will be killed) -> Sniffer1 <- JDC -> Sniffer2 -> TP_infra (shared)
///   Pool (connected to TP_infra)
///   JDS (connected to TP_infra's RPC)
///
/// TP_infra serves triple duty: Pool's TP, JDS's RPC source, and JDC's fallback TP2.
///
/// 1. Start TP_infra (shared) and TP1 (dedicated, will be killed).
/// 2. Place a sniffer in front of TP1 and TP_infra (for JDC's connections).
/// 3. Start Pool and JDS connected to TP_infra.
/// 4. Start JDC configured with [sniffer1_addr, sniffer2_addr] as ordered TP list.
/// 5. Verify JDC connects to TP1 (sniffer1 sees SetupConnection exchange).
/// 6. Kill TP1 by dropping it.
/// 7. Verify JDC falls back to TP_infra via sniffer2 (SetupConnection exchange).
#[tokio::test]
async fn jdc_falls_back_to_second_tp_on_first_tp_shutdown() {
    start_tracing();

    // TP_infra: shared by Pool, JDS, and JDC's fallback (stays alive the entire test).
    let (tp_infra, tp_infra_addr) = start_template_provider(None, DifficultyLevel::Low);

    // TP1: JDC's primary TP (will be killed to trigger fallback).
    let (tp1, tp1_addr) = start_template_provider(None, DifficultyLevel::Low);

    // Start Pool connected to infra TP.
    let (_pool, pool_addr) = start_pool(sv2_tp_config(tp_infra_addr), vec![], vec![]).await;

    // Start JDS connected to infra TP's RPC.
    let (_jds, jds_addr) = start_jds(tp_infra.rpc_info());

    // Sniffers between JDC and each TP.
    // Sniffer1 proxies to TP1 (will break when TP1 is killed).
    // Sniffer2 proxies to TP_infra (JDC's fallback destination).
    let (sniffer1, sniffer1_addr) = start_sniffer("JDC-TP1", tp1_addr, false, vec![], None);
    let (sniffer2, sniffer2_addr) = start_sniffer("JDC-TP2", tp_infra_addr, false, vec![], None);

    // JDC configured with two TPs: TP1 (primary) then TP_infra (fallback).
    let (_jdc, _jdc_addr) = start_jdc_with_tp_configs(
        &[(pool_addr, jds_addr)],
        vec![sv2_tp_config(sniffer1_addr), sv2_tp_config(sniffer2_addr)],
        vec![],
        vec![],
    );

    // Verify JDC connects to TP1 successfully.
    sniffer1
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    sniffer1
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;
    // Wait for NewTemplate to confirm the TP connection is fully operational.
    sniffer1
        .wait_for_message_type(MessageDirection::ToDownstream, MESSAGE_TYPE_NEW_TEMPLATE)
        .await;

    // Kill TP1 — this terminates sv2-tp and Bitcoin Core, breaking the connection.
    drop(tp1);

    // JDC should detect the connection loss and fall back to TP_infra via sniffer2.
    sniffer2
        .wait_for_message_type(MessageDirection::ToUpstream, MESSAGE_TYPE_SETUP_CONNECTION)
        .await;
    sniffer2
        .wait_for_message_type(
            MessageDirection::ToDownstream,
            MESSAGE_TYPE_SETUP_CONNECTION_SUCCESS,
        )
        .await;
    // Confirm JDC receives templates from TP_infra.
    sniffer2
        .wait_for_message_type(MessageDirection::ToDownstream, MESSAGE_TYPE_NEW_TEMPLATE)
        .await;
}
