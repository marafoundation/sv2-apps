# Monitoring Module

HTTP JSON API and Prometheus metrics for SV2 applications.

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

## OpenAPI Schema

The committed `openapi.json` is generated with `monitoring,asic-rs-telemetry` and represents the superset schema used by downstream consumers such as `sv2-ui`. Runtime schemas exposed at `/api-docs/openapi.json` depend on the features used to build each app; apps built without `asic-rs-telemetry` omit miner telemetry fields.

## Traits

Applications implement these traits on their data structures:

- `ServerMonitoring` - For upstream connection info
- `Sv2ClientsMonitoring` - For Sv2 downstream client info (Pool, JDC)
- `Sv1ClientsMonitoring` - For Sv1 downstream client info (Translator Proxy only)
- `UpstreamMonitoring` - For the connection state of the configured upstream endpoints (optional)

## Usage

```rust
use stratum_apps::monitoring::MonitoringServer;
use std::sync::Arc;

let server = MonitoringServer::new(
    "127.0.0.1:9090".parse()?,
    Some(Arc::new(channel_manager.clone())), // server monitoring
    Some(Arc::new(channel_manager.clone())), // Sv2 clients monitoring
    std::time::Duration::from_secs(15),      // cache refresh interval
)?;

// For Translator, add SV1 monitoring
let server = server.with_sv1_monitoring(Arc::new(sv1_server.clone()))?;

// Optionally expose upstream connection state. This also makes `/api/v1/health` answer
// 503 `upstream_disconnected` while `is_connected()` is false. Without it, health is always 200.
let server = server.with_upstream_monitoring(Arc::new(upstream_state.clone()))?;

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

## Prometheus Metrics

**System:**
- `sv2_uptime_seconds` - Server uptime
- `sv2_heap_in_use_bytes`, `sv2_heap_free_bytes`, `sv2_heap_mapped_bytes` - Heap memory still in use, freed but held by the allocator, and obtained from the OS (glibc builds only, which then need glibc 2.33 or later to start). Resident memory that grows while `in_use` stays flat is allocator retention, not a leak.
- The heap gauges are absent when an application registers a non-glibc allocator without statistics (`heap::use_allocator` with a `stats` that returns `None`), as the translator does when built with `--features jemalloc`.

**Server:**
- `sv2_server_channels{channel_type}` - Server channels by type (extended/standard)
- `sv2_server_hashrate_total` - Total server hashrate
- `sv2_server_channel_hashrate{channel_id, user_identity}` - Per-channel hashrate
- `sv2_server_shares_accepted_total{channel_id, user_identity}` - Per-channel shares
- `sv2_server_shares_rejected_total{channel_id, user_identity, error_code}` - Per-channel rejected shares by error code
- `sv2_server_blocks_found_total` - Total blocks found across all current server channels

**Clients:**
- `sv2_clients_total` - Connected client count
- `sv2_client_channels{channel_type}` - Client channels by type (extended/standard)
- `sv2_client_hashrate_total` - Total client hashrate
- `sv2_client_channel_hashrate{client_id, channel_id, user_identity}` - Per-channel hashrate
- `sv2_client_shares_accepted_total{client_id, channel_id, user_identity}` - Per-channel shares
- `sv2_client_shares_rejected_total{client_id, channel_id, user_identity, error_code}` - Per-channel rejected shares by error code
- `sv2_client_blocks_found_total` - Total blocks found across all current client channels

**Upstream (only with `with_upstream_monitoring`):**
- `sv2_upstream_connected{pool_address}` - 1 for the connected upstream, 0 for every other configured one
- `sv2_upstream_fallbacks_total` - Fallbacks to another upstream since the process started

**Sv1 (Translator Proxy only):**
- `sv1_clients_total` - Sv1 client count
- `sv1_hashrate_total` - Sv1 total hashrate
- `sv1_shares_accepted_total` - Sv1 shares accepted by the translator since it started. The upstream can still reject them, which is counted in `sv2_server_shares_rejected_total`.
- `sv1_shares_rejected_total{reason}` - Sv1 shares rejected since start, by the `mining.submit` error sent to the miner: `job-not-found` (21, which includes stale shares), `duplicate` (22), `low-difficulty` (23), `unauthorized-worker` (24), `not-subscribed` (25), `other`. Per-miner counts are in `/api/v1/sv1/clients` under `shares`.
- `sv1_shares_below_upstream_target_total` - Accepted Sv1 shares not sent upstream because they miss the upstream channel's target. Expected wherever a miner's own difficulty is easier than the channel's, as in aggregated mode.
- `sv1_shares_dropped_total{reason}` - Accepted Sv1 shares not sent upstream for any other reason, since start: `stale` (their job was replaced by a new chain tip), `invalid-job-id`, `invalid`, `duplicate`, `bad-extranonce-size`, `version-rolling-not-allowed`, `no-chain-tip`, `unknown-channel`, `upstream-gone` (the upstream went away in a fallback before the share could be sent). Each of these was answered `true` to the miner and is already part of `sv1_shares_accepted_total`, so add them to the numerator of a stale or reject rate, not to its denominator. For example, stale % per pod, with every term aggregated to the same labels so `+` and `/` match: `(sum by (pod) (rate(sv1_shares_rejected_total{reason="job-not-found"}[5m])) + sum by (pod) (rate(sv1_shares_dropped_total{reason=~"stale|invalid-job-id"}[5m]))) / (sum by (pod) (rate(sv1_shares_accepted_total[5m])) + sum by (pod) (rate(sv1_shares_rejected_total[5m])))`; drop `by (pod)` for the fleet.
- `sv1_block_found_info{sv1_username, worker, user_identity, share_hash, height}` - One series for each recent block candidate: a share that met the network target as the translator saw it, before the pool confirms the block (it can still lose a race or be rejected upstream). Names the miner by its full SV1 username (`account.worker`) and is valued at the Unix time the share was submitted. The source keeps a bounded number of them (the translator keeps 32), and older ones drop out. `height` is empty when the coinbase could not be read.
- `sv1_blocks_found_total` - Block candidates since start, for alerting and rates; it does not drop entries like `sv1_block_found_info`.
