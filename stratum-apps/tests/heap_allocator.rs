#![cfg(feature = "monitoring")]

use std::time::Duration;

use stratum_apps::monitoring::{
    SnapshotCache,
    heap::{self, HeapAllocator, HeapStats},
    prometheus_metrics::PrometheusMetrics,
};

const STATS: HeapStats = HeapStats {
    in_use: 100,
    free: 20,
    mapped: 120,
    retained: Some(7),
};

#[test]
fn a_registered_allocator_provides_the_heap_figures() {
    heap::use_allocator(HeapAllocator {
        name: "test-allocator",
        stats: || Some(STATS),
        release: || true,
    })
    .unwrap();
    let second = HeapAllocator {
        name: "second",
        stats: || None,
        release: || false,
    };
    assert_eq!(heap::use_allocator(second).unwrap_err().name, "second");

    assert_eq!(heap::allocator_name(), Some("test-allocator"));
    assert_eq!(heap::heap_stats(), Some(STATS));
    assert!(heap::release_free_heap());

    let metrics = PrometheusMetrics::new(false, false, false, false).unwrap();
    SnapshotCache::new(Duration::from_secs(5), None, None)
        .with_metrics(metrics.clone())
        .refresh();

    let value = |name: &str| {
        metrics
            .registry
            .gather()
            .into_iter()
            .find(|family| family.get_name() == name)
            .map(|family| family.get_metric()[0].get_gauge().get_value())
    };
    assert_eq!(value("sv2_heap_in_use_bytes"), Some(100.0));
    assert_eq!(value("sv2_heap_free_bytes"), Some(20.0));
    assert_eq!(value("sv2_heap_mapped_bytes"), Some(120.0));
    assert_eq!(value("sv2_heap_retained_bytes"), Some(7.0));

    let families = metrics.registry.gather();
    let info = families
        .iter()
        .find(|family| family.get_name() == "sv2_heap_allocator_info")
        .unwrap();
    let label = &info.get_metric()[0].get_label()[0];
    assert_eq!(
        (label.get_name(), label.get_value()),
        ("allocator", "test-allocator")
    );
    assert_eq!(info.get_metric()[0].get_gauge().get_value(), 1.0);
}
