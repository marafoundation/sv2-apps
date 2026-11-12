#![cfg(feature = "monitoring")]

use std::time::Duration;

use stratum_apps::monitoring::{
    SnapshotCache,
    heap::{self, HeapAllocator},
    prometheus_metrics::PrometheusMetrics,
};

#[test]
fn an_allocator_without_stats_replaces_the_default_gauges() {
    heap::use_allocator(HeapAllocator {
        stats: || None,
        release: || false,
    })
    .unwrap();
    let second = HeapAllocator {
        stats: || None,
        release: || true,
    };
    assert!((heap::use_allocator(second).unwrap_err().release)());

    assert_eq!(heap::heap_stats(), None);
    assert!(!heap::release_free_heap());

    let metrics = PrometheusMetrics::new(false, false, false, false).unwrap();
    SnapshotCache::new(Duration::from_secs(5), None, None)
        .with_metrics(metrics.clone())
        .refresh();
    assert!(metrics.sv2_heap_in_use_bytes.is_none());
    assert!(
        !metrics
            .registry
            .gather()
            .iter()
            .any(|family| family.get_name().starts_with("sv2_heap_"))
    );
}
