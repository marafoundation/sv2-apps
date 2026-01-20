//! Snapshot cache for monitoring data
//!
//! This module provides a cache layer that decouples monitoring API requests
//! from the business logic locks (e.g., `ChannelManagerData`).
//!
//! ## Problem
//!
//! Without caching, every monitoring request acquires the same lock used by
//! share validation and job distribution. An attacker can spam monitoring
//! endpoints to cause lock contention, degrading mining performance.
//!
//! ## Solution
//!
//! The `SnapshotCache` periodically copies monitoring data from the source
//! (via the monitoring traits) into a cache. API requests read from the cache
//! without acquiring the business logic lock.
//!
//! ```text
//! Business Logic                    Monitoring
//! ──────────────                    ──────────
//!     │                                  │
//!     │ (holds lock for                  │
//!     │  share validation)               │
//!     │                                  │
//!     └──────────────────────────────────┤
//!                                        │
//!                              ┌─────────▼─────────┐
//!                              │  SnapshotCache    │
//!                              │  (RwLock, fast)   │
//!                              └─────────┬─────────┘
//!                                        │
//!                    ┌───────────────────┼───────────────────┐
//!                    │                   │                   │
//!              ┌─────▼─────┐       ┌─────▼─────┐       ┌─────▼─────┐
//!              │ /metrics  │       │ /api/v1/* │       │ /health   │
//!              └───────────┘       └───────────┘       └───────────┘
//! ```

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use super::client::{ClientInfo, ClientsMonitoring, ClientsSummary};
use super::server::{ServerInfo, ServerMonitoring, ServerSummary};
use super::sv1::{Sv1ClientInfo, Sv1ClientsMonitoring, Sv1ClientsSummary};

/// Cached snapshot of monitoring data.
///
/// This struct holds a point-in-time copy of all monitoring data,
/// allowing API requests to read without acquiring business logic locks.
#[derive(Debug, Clone, Default)]
pub struct MonitoringSnapshot {
    /// When this snapshot was taken
    pub timestamp: Option<Instant>,

    /// Server (upstream) data
    pub server_info: Option<ServerInfo>,
    pub server_summary: Option<ServerSummary>,

    /// Clients (downstream) data
    pub clients: Vec<ClientInfo>,
    pub clients_summary: Option<ClientsSummary>,

    /// SV1 clients data (Tproxy only)
    pub sv1_clients: Vec<Sv1ClientInfo>,
    pub sv1_summary: Option<Sv1ClientsSummary>,
}

impl MonitoringSnapshot {
    /// Check if this snapshot is stale (older than the given duration)
    pub fn is_stale(&self, max_age: Duration) -> bool {
        match self.timestamp {
            None => true,
            Some(ts) => ts.elapsed() > max_age,
        }
    }

    /// Get the age of this snapshot
    pub fn age(&self) -> Option<Duration> {
        self.timestamp.map(|ts| ts.elapsed())
    }
}

/// A cache that holds monitoring snapshots and refreshes them periodically.
///
/// This is the core component that fixes the DoS vulnerability by decoupling
/// monitoring reads from business logic locks.
pub struct SnapshotCache {
    /// The cached snapshot, protected by a RwLock for fast concurrent reads
    snapshot: RwLock<MonitoringSnapshot>,

    /// How often the background task checks for refresh (e.g., 60s)
    refresh_interval: Duration,

    /// Maximum acceptable staleness for API requests (e.g., 30s)
    /// Defaults to refresh_interval / 2
    freshness_threshold: Duration,

    /// Data sources (trait objects that acquire the business logic lock)
    server_source: Option<Arc<dyn ServerMonitoring + Send + Sync>>,
    clients_source: Option<Arc<dyn ClientsMonitoring + Send + Sync>>,
    sv1_source: Option<Arc<dyn Sv1ClientsMonitoring + Send + Sync>>,
}

impl Clone for SnapshotCache {
    fn clone(&self) -> Self {
        // Clone creates a new cache with the same sources and current snapshot
        let current_snapshot = self.snapshot.read().unwrap().clone();
        Self {
            snapshot: RwLock::new(current_snapshot),
            refresh_interval: self.refresh_interval,
            freshness_threshold: self.freshness_threshold,
            server_source: self.server_source.clone(),
            clients_source: self.clients_source.clone(),
            sv1_source: self.sv1_source.clone(),
        }
    }
}

impl SnapshotCache {
    /// Create a new snapshot cache with the given refresh interval.
    ///
    /// # Arguments
    ///
    /// * `refresh_interval` - How often to refresh the cache (e.g., 60 seconds)
    /// * `server_source` - Optional server monitoring trait object
    /// * `clients_source` - Optional clients monitoring trait object
    ///
    /// The freshness threshold defaults to `refresh_interval / 2`.
    pub fn new(
        refresh_interval: Duration,
        server_source: Option<Arc<dyn ServerMonitoring + Send + Sync>>,
        clients_source: Option<Arc<dyn ClientsMonitoring + Send + Sync>>,
    ) -> Self {
        Self {
            snapshot: RwLock::new(MonitoringSnapshot::default()),
            refresh_interval,
            freshness_threshold: refresh_interval / 2,
            server_source,
            clients_source,
            sv1_source: None,
        }
    }

    /// Add SV1 monitoring source (for Tproxy)
    pub fn with_sv1_source(
        mut self,
        sv1_source: Arc<dyn Sv1ClientsMonitoring + Send + Sync>,
    ) -> Self {
        self.sv1_source = Some(sv1_source);
        self
    }

    /// Get the current snapshot.
    ///
    /// This is a fast read that does NOT acquire any business logic locks.
    /// The returned snapshot may be up to `refresh_interval` old.
    pub fn get_snapshot(&self) -> MonitoringSnapshot {
        self.snapshot.read().unwrap().clone()
    }

    /// Check if the cache needs to be refreshed (used by background task).
    pub fn needs_refresh(&self) -> bool {
        self.snapshot
            .read()
            .unwrap()
            .is_stale(self.refresh_interval)
    }

    /// Check if the cache is stale beyond the freshness threshold (used by API handlers).
    fn is_stale(&self) -> bool {
        self.snapshot
            .read()
            .unwrap()
            .is_stale(self.freshness_threshold)
    }

    /// Refresh the cache if it's stale beyond the freshness threshold.
    ///
    /// This is called by API handlers to ensure fresh data. It only refreshes
    /// if the cache is older than `freshness_threshold`, preventing excessive
    /// refreshes from concurrent requests.
    pub fn refresh_if_stale(&self) {
        if self.is_stale() {
            self.refresh();
        }
    }

    /// Refresh the cache by reading from the data sources.
    ///
    /// This method DOES acquire the business logic locks (via the trait methods),
    /// but it's only called periodically by a background task, not on every request.
    pub fn refresh(&self) {
        let mut new_snapshot = MonitoringSnapshot {
            timestamp: Some(Instant::now()),
            ..Default::default()
        };

        // Collect server data
        if let Some(ref source) = self.server_source {
            new_snapshot.server_info = Some(source.get_server());
            new_snapshot.server_summary = Some(source.get_server_summary());
        }

        // Collect clients data
        if let Some(ref source) = self.clients_source {
            new_snapshot.clients = source.get_clients();
            new_snapshot.clients_summary = Some(source.get_clients_summary());
        }

        // Collect SV1 data
        if let Some(ref source) = self.sv1_source {
            new_snapshot.sv1_clients = source.get_sv1_clients();
            new_snapshot.sv1_summary = Some(source.get_sv1_clients_summary());
        }

        // Update the cache
        *self.snapshot.write().unwrap() = new_snapshot;
    }

    /// Get the refresh interval
    pub fn refresh_interval(&self) -> Duration {
        self.refresh_interval
    }
}

/// A wrapper that implements the monitoring traits by reading from a cache.
///
/// This allows the HTTP server to use the same trait-based API while
/// actually reading from the cache instead of acquiring locks.
pub struct CachedMonitoring {
    cache: Arc<SnapshotCache>,
}

impl CachedMonitoring {
    pub fn new(cache: Arc<SnapshotCache>) -> Self {
        Self { cache }
    }
}

impl ServerMonitoring for CachedMonitoring {
    fn get_server(&self) -> ServerInfo {
        self.cache
            .get_snapshot()
            .server_info
            .unwrap_or_else(|| ServerInfo {
                extended_channels: vec![],
                standard_channels: vec![],
            })
    }

    fn get_server_summary(&self) -> ServerSummary {
        self.cache
            .get_snapshot()
            .server_summary
            .unwrap_or(ServerSummary {
                total_channels: 0,
                extended_channels: 0,
                standard_channels: 0,
                total_hashrate: 0.0,
            })
    }
}

impl ClientsMonitoring for CachedMonitoring {
    fn get_clients(&self) -> Vec<ClientInfo> {
        self.cache.get_snapshot().clients
    }

    fn get_client_by_id(&self, client_id: usize) -> Option<ClientInfo> {
        self.cache
            .get_snapshot()
            .clients
            .into_iter()
            .find(|c| c.client_id == client_id)
    }

    fn get_clients_summary(&self) -> ClientsSummary {
        self.cache
            .get_snapshot()
            .clients_summary
            .unwrap_or(ClientsSummary {
                total_clients: 0,
                total_channels: 0,
                extended_channels: 0,
                standard_channels: 0,
                total_hashrate: 0.0,
            })
    }
}

impl Sv1ClientsMonitoring for CachedMonitoring {
    fn get_sv1_clients(&self) -> Vec<Sv1ClientInfo> {
        self.cache.get_snapshot().sv1_clients
    }

    fn get_sv1_client_by_id(&self, client_id: usize) -> Option<Sv1ClientInfo> {
        self.cache
            .get_snapshot()
            .sv1_clients
            .into_iter()
            .find(|c| c.client_id == client_id)
    }

    fn get_sv1_clients_summary(&self) -> Sv1ClientsSummary {
        self.cache
            .get_snapshot()
            .sv1_summary
            .unwrap_or(Sv1ClientsSummary {
                total_clients: 0,
                total_hashrate: 0.0,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockServerMonitoring;
    impl ServerMonitoring for MockServerMonitoring {
        fn get_server(&self) -> ServerInfo {
            ServerInfo {
                extended_channels: vec![],
                standard_channels: vec![],
            }
        }
    }

    struct MockClientsMonitoring;
    impl ClientsMonitoring for MockClientsMonitoring {
        fn get_clients(&self) -> Vec<ClientInfo> {
            vec![]
        }
    }

    #[test]
    fn test_snapshot_cache_creation() {
        let cache = SnapshotCache::new(
            Duration::from_secs(5),
            Some(Arc::new(MockServerMonitoring)),
            Some(Arc::new(MockClientsMonitoring)),
        );

        assert!(cache.needs_refresh());
        assert_eq!(cache.refresh_interval(), Duration::from_secs(5));
    }

    #[test]
    fn test_snapshot_refresh() {
        let cache = SnapshotCache::new(
            Duration::from_secs(5),
            Some(Arc::new(MockServerMonitoring)),
            Some(Arc::new(MockClientsMonitoring)),
        );

        // Initially needs refresh
        assert!(cache.needs_refresh());

        // After refresh, should not need refresh
        cache.refresh();
        assert!(!cache.needs_refresh());

        // Snapshot should have timestamp
        let snapshot = cache.get_snapshot();
        assert!(snapshot.timestamp.is_some());
        assert!(snapshot.age().unwrap() < Duration::from_millis(100));
    }

    #[test]
    fn test_cached_monitoring_reads_from_cache() {
        let cache = Arc::new(SnapshotCache::new(
            Duration::from_secs(5),
            Some(Arc::new(MockServerMonitoring)),
            Some(Arc::new(MockClientsMonitoring)),
        ));

        cache.refresh();

        let cached = CachedMonitoring::new(cache);

        // These calls should NOT acquire any locks on the original sources
        let _ = cached.get_server();
        let _ = cached.get_clients();
        let _ = cached.get_server_summary();
        let _ = cached.get_clients_summary();
    }
}
