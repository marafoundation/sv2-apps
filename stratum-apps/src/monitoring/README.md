# Monitoring Module

HTTP JSON API and Prometheus metrics for SV2 applications.

## Architecture Overview

The monitoring system has two collection mechanisms:

1. **Snapshot-based (Gauges)**: Periodic sampling from business logic state via monitoring traits
2. **Event-based (Counters/Histograms)**: Real-time increments at the point where events occur

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Business Logic State                           │
│  (ChannelManagerData, ShareAccounting, etc.)                        │
└────────────┬────────────────────────────────────────────────────────┘
             │
             ├─── Snapshot Cache ──► Gauges (hashrate, channel counts)
             │    (periodic refresh)
             │
             └─── EventMetrics ────► Counters (shares accepted)
                  (real-time)        Histograms (latency - future)
```

## API Endpoints

Endpoints returning lists support pagination via `?offset=N&limit=M` query params.

| Endpoint | Description |
|----------|-------------|
| `/swagger-ui` | Swagger UI (interactive API docs) |
| `/api-docs/openapi.json` | OpenAPI specification |
| `/api/v1/health` | Health check |
| `/api/v1/global` | Global statistics |
| `/api/v1/server` | Server metadata |
| `/api/v1/server/channels` | Server channels (paginated) |
| `/api/v1/clients` | All Sv2 clients metadata (paginated) |
| `/api/v1/clients/{id}` | Single Sv2 client metadata |
| `/api/v1/clients/{id}/channels` | Sv2 client channels (paginated) |
| `/api/v1/sv1/clients` | Sv1 clients (Translator Proxy only, paginated) |
| `/api/v1/sv1/clients/{id}` | Single Sv1 client (Translator Proxy only) |
| `/metrics` | Prometheus metrics |

Server and client endpoints return metadata only (counts, hashrate). Use `/channels` sub-resource for channel details.

## Traits

Applications implement these traits on their data structures:

- `ServerMonitoring` - For upstream connection info
- `ClientsMonitoring` - For downstream client info  
- `Sv1ClientsMonitoring` - For Sv1 clients (Translator Proxy only)

## Usage

```rust
use stratum_apps::monitoring::MonitoringServer;
use std::sync::Arc;

let server = MonitoringServer::new(
    "127.0.0.1:9090".parse()?,
    Some(Arc::new(channel_manager.clone())), // server monitoring
    Some(Arc::new(channel_manager.clone())), // clients monitoring
)?;

// For Translator, add SV1 monitoring
let server = server.with_sv1_monitoring(Arc::new(sv1_server.clone()))?;

// Create a shutdown signal (any Future that completes when shutdown is needed)
let (shutdown_tx, mut shutdown_rx) = tokio::sync::broadcast::channel::<()>(1);
let shutdown_signal = async move {
    shutdown_rx.recv().await.ok();
};

// Spawn monitoring server
tokio::spawn(async move {
    if let Err(e) = server.run(shutdown_signal).await {
        eprintln!("Monitoring server error: {}", e);
    }
});

// Later, trigger shutdown:
// shutdown_tx.send(()).ok();
```

## Adding a New Metric

### Step 1: Choose the Metric Type

| Question | Gauge | Counter | Histogram |
|----------|-------|---------|-----------|
| Does the value go up AND down? | ✅ | ❌ | ❌ |
| Is it a cumulative count of events? | ❌ | ✅ | ❌ |
| Do you need `rate()` calculations? | ❌ | ✅ | ✅ |
| Is it measuring duration/latency? | ❌ | ❌ | ✅ |
| Is it current state (queue depth, memory)? | ✅ | ❌ | ❌ |

**Examples:**
- **Gauge**: `hashrate_total`, `channels_active`, `memory_used_bytes`, `queue_depth`
- **Counter**: `shares_accepted_total`, `errors_total`, `blocks_found_total`
- **Histogram**: `share_validation_latency_seconds`, `job_distribution_latency_seconds`

### Step 2a: Implementing a Gauge (Snapshot-Based)

Gauges are populated from the snapshot cache during Prometheus scrapes.

**1. Add field to `SnapshotMetrics` struct:**

```rust
// snapshot_metrics.rs
pub struct SnapshotMetrics {
    // ... existing fields ...
    pub my_new_gauge: Option<Gauge>,
    // Or for labeled metrics:
    pub my_new_gauge_vec: Option<GaugeVec>,
}
```

**2. Register in `SnapshotMetrics::new()`:**

```rust
let my_new_gauge = Gauge::new("sv2_my_metric", "Description of metric")?;
registry.register(Box::new(my_new_gauge.clone()))?;

// For labeled metrics:
let my_new_gauge_vec = GaugeVec::new(
    Opts::new("sv2_my_metric", "Description"),
    &["label1", "label2"],
)?;
registry.register(Box::new(my_new_gauge_vec.clone()))?;
```

**3. Add data to monitoring trait (if needed):**

```rust
// server.rs or client.rs
pub struct ChannelInfo {
    // ... existing fields ...
    pub my_new_field: f64,
}
```

**4. Populate in `handle_prometheus_metrics()`:**

```rust
// http_server.rs
if let Some(ref metric) = state.metrics.my_new_gauge {
    metric.set(server.my_new_field);
}
```

### Step 2b: Implementing a Counter (Event-Based)

Counters are incremented in real-time at the point where events occur.

**1. Add field to `EventMetrics` struct:**

```rust
// event_metrics.rs
pub struct EventMetrics {
    // ... existing fields ...
    pub my_counter: Option<CounterVec>,
}
```

**2. Register in `EventMetrics::new()`:**

```rust
let my_counter = if enable_clients_metrics {
    let counter = CounterVec::new(
        Opts::new("sv2_my_counter_total", "Description"),
        &["client_id", "channel_id"],
    )?;
    registry.register(Box::new(counter.clone()))?;
    Some(counter)
} else {
    None
};
```

**3. Add helper method:**

```rust
impl EventMetrics {
    pub fn inc_my_counter(&self, client_id: usize, channel_id: u32) {
        if let Some(ref counter) = self.my_counter {
            counter
                .with_label_values(&[&client_id.to_string(), &channel_id.to_string()])
                .inc();
        }
    }
}
```

**4. Pass EventMetrics to the component that needs it:**

```rust
// In the app's lib/mod.rs (e.g., pool-apps/pool/src/lib/mod.rs)
let event_metrics = monitoring_server.event_metrics();
channel_manager = channel_manager.with_event_metrics(event_metrics);
```

**5. Add field and setter to the component:**

```rust
// In the component (e.g., channel_manager/mod.rs)
pub struct ChannelManager {
    // ... existing fields ...
    event_metrics: Option<Arc<EventMetrics>>,
}

impl ChannelManager {
    pub fn with_event_metrics(mut self, metrics: Arc<EventMetrics>) -> Self {
        self.event_metrics = Some(metrics);
        self
    }
}
```

**6. Increment at the event call site:**

```rust
// In the message handler where the event occurs
if let Some(ref metrics) = self.event_metrics {
    metrics.inc_my_counter(client_id, channel_id);
}
```

### Step 2c: Implementing a Histogram (Event-Based) - Future

Histograms track distributions (e.g., latency percentiles). Implementation is similar to counters but uses `HistogramVec` and `observe()` instead of `inc()`.

```rust
// Registration
let histogram = HistogramVec::new(
    HistogramOpts::new("sv2_latency_seconds", "Operation latency")
        .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
    &["operation"],
)?;

// Usage at call site
let start = std::time::Instant::now();
// ... do work ...
metrics.observe_latency("share_validation", start.elapsed().as_secs_f64());
```

## Prometheus Metrics

**System:**
- `sv2_uptime_seconds` - Server uptime

**Server:**
- `sv2_server_channels_total` - Total server channels
- `sv2_server_channels_extended` - Extended server channels
- `sv2_server_channels_standard` - Standard server channels
- `sv2_server_hashrate_total` - Total server hashrate
- `sv2_server_channel_hashrate{channel_id, user_identity}` - Per-channel hashrate
- `sv2_server_shares_accepted_total{channel_id, user_identity}` - Per-channel shares

**Clients:**
- `sv2_clients_total` - Connected client count
- `sv2_client_channels_total` - Total client channels
- `sv2_client_channels_extended` - Extended client channels
- `sv2_client_channels_standard` - Standard client channels
- `sv2_client_hashrate_total` - Total client hashrate
- `sv2_client_channel_hashrate{client_id, channel_id, user_identity}` - Per-channel hashrate
- `sv2_client_shares_accepted_total{client_id, channel_id, user_identity}` - Per-channel shares
- `sv2_client_channel_shares_per_minute{client_id, channel_id, user_identity}` - Per-channel share rate

**Sv1 (Translator Proxy only):**
- `sv1_clients_total` - Sv1 client count
- `sv1_hashrate_total` - Sv1 total hashrate

## File Organization

| File | Purpose |
|------|---------|
| `snapshot_metrics.rs` | Gauge definitions (snapshot-based metrics) |
| `event_metrics.rs` | Counter and Histogram definitions (event-based metrics) |
| `server.rs` | Server monitoring trait implementation |
| `client.rs` | Client monitoring trait implementation |
| `sv1_server.rs` | Sv1 server monitoring trait implementation (Translator Proxy only) |