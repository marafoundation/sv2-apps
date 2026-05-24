# Shape Proxy — Implementation Plan

Reference: `docs/shape-proxy-design-v2.md`

## Dependencies

| Dependency | From | Purpose |
|-----------|------|---------|
| `stratum-apps` | workspace path dep | `network_helpers::{connect_with_noise, accept_noise_connection}`, `key_utils`, `config_helpers`, `stratum_core` re-export |
| `stratum-core` | via stratum-apps | `mining_sv2::*` message types, `codec_sv2` framing, `noise_sv2` handshake roles, `parsers_sv2::AnyMessage` |
| `tokio` | crates.io | async runtime, TCP, timers, mpsc/watch channels |
| `axum` | crates.io | HTTP API |
| `prometheus` | crates.io | metrics export |
| `serde` / `serde_json` | crates.io | config + API serialization |
| `toml` | crates.io | config file parsing |
| `tracing` | crates.io | structured logging |
| `clap` | crates.io | CLI args |

No new protocol crates. The proxy speaks raw SV2 frames using `NoiseTcpStream<AnyMessage>`.

## Runtime architecture

```
┌───────────────────────────────────────────────────────────────────┐
│ shape-proxy process                                               │
│                                                                   │
│  ┌──────────────┐                                                 │
│  │ Downstream   │  spawn per connection:                          │
│  │ Listener     │──► read task → downstream_tx ──┐                │
│  └──────────────┘                                │                │
│                                                  ▼                │
│  ┌──────────────┐         ┌─────────────────────────────────┐    │
│  │ Upstream     │         │ ProxyCore (single select loop)   │    │
│  │ read task    │────────►│                                  │    │
│  └──────────────┘         │  pairs: HashMap<u32, Pair>       │    │
│                           │  upstream_writer                  │    │
│  ┌──────────────┐         │  downstream_writers              │    │
│  │ HTTP API     │────────►│                                  │    │
│  │ (axum task)  │         └─────────────────────────────────┘    │
│  └──────────────┘                                                 │
└───────────────────────────────────────────────────────────────────┘
```

### Multiplexing strategy

The core problem: `tokio::select!` requires a fixed number of branches. We have a dynamic number of downstream connections.

**Solution**: each downstream connection spawns a dedicated read task that deserializes frames and sends them into a shared `mpsc::Sender<DownstreamEvent>`. The `ProxyCore` select loop consumes from one `mpsc::Receiver<DownstreamEvent>` — always exactly one branch regardless of downstream count.

```rust
enum DownstreamEvent {
    Connected { id: u32, writer: NoiseWriter<AnyMessage> },
    Frame { id: u32, frame: Sv2Frame },
    Disconnected { id: u32 },
}
```

Similarly, the upstream read runs in its own task, sending `UpstreamEvent` into a separate channel. The ProxyCore select loop has exactly four branches:

```rust
loop {
    tokio::select! {
        Some(evt) = upstream_rx.recv() => self.handle_upstream(evt),
        Some(evt) = downstream_rx.recv() => self.handle_downstream(evt),
        Some(cmd) = api_rx.recv() => self.handle_api(cmd),
        Ok(stream) = listener.accept() => self.spawn_downstream(stream),
    }
}
```

All state mutation happens in one task. Write halves (`NoiseWriter`) are stored in `ProxyCore` and used directly from the select loop — no contention.

## ID spaces and lookups

Three independent channel ID namespaces:

| Namespace | Assigned by | Used in |
|-----------|-------------|---------|
| `upstream_channel_id` | Pool | All upstream frames |
| `downstream_channel_id` | Proxy | All downstream frames |
| `internal_pair_id` | Proxy | Internal bookkeeping |

The proxy maintains two lookup maps:
- `by_downstream: HashMap<u32, PairId>` — miner frame arrives with downstream_channel_id → find pair
- `by_upstream: HashMap<u32, PairId>` — pool frame arrives with upstream_channel_id → find pair

Both point to the same `ChannelPair` (stored in a `Vec` or `HashMap<PairId, ChannelPair>`).

## Data model

```rust
struct ProxyCore {
    config: Config,

    // Upstream (one TCP connection, multiple logical channels)
    upstream_writer: NoiseWriter<AnyMessage>,

    // Downstreams (one TCP connection per miner)
    downstream_writers: HashMap<u32, NoiseWriter<AnyMessage>>,  // keyed by downstream_conn_id

    // Channel pairs
    pairs: Vec<ChannelPair>,
    by_upstream: HashMap<u32, usize>,     // upstream_channel_id → pairs index
    by_downstream: HashMap<u32, usize>,   // downstream_channel_id → pairs index

    // Phantoms (upstream channels with no pair)
    phantoms: Vec<u32>,

    // Communication
    downstream_rx: mpsc::Receiver<DownstreamEvent>,
    downstream_tx: mpsc::Sender<DownstreamEvent>,  // cloned into each read task
    upstream_rx: mpsc::Receiver<UpstreamEvent>,
    api_rx: mpsc::Receiver<ApiCommand>,
    status_tx: watch::Sender<ProxyStatus>,

    // Counters
    next_downstream_channel_id: u32,
}

struct Config {
    pub upstream_address: String,
    pub upstream_authority_pubkey: Option<Secp256k1PublicKey>,
    pub downstream_listen: SocketAddr,
    pub authority_pubkey: Secp256k1PublicKey,
    pub authority_secret: Secp256k1SecretKey,
    pub cert_validity_secs: u64,
    pub min_downstream_difficulty: f64,     // floor (scalar)
    pub phantom_channels: u32,
    pub default_profile: RateProfile,
    pub api_listen: SocketAddr,
}

struct ChannelPair {
    downstream_conn_id: u32,               // which downstream TCP connection
    downstream_channel_id: u32,            // ID assigned to miner
    upstream_channel_id: u32,              // ID assigned by pool
    extranonce_prefix: Vec<u8>,            // verbatim from pool
    extranonce_size: u16,

    // Difficulty
    upstream_target: Target,               // pool's current target
    downstream_target: Target,             // harder_of(upstream, floor)

    // Gate
    gate: ShareGate,

    // Upstream share submission
    upstream_seq: u32,                     // next sequence number for upstream

    // Downstream ack tracking
    downstream_ack_seq: u32,              // last sequence number acked to miner
    downstream_ack_count: u32,            // shares acked in current batch

    // Metrics
    metrics: ChannelMetrics,
}

struct ShareGate {
    profile: RateProfile,
    started_at: Instant,
    bucket: f64,
    capacity: f64,
    last_refill: Instant,
}

struct ChannelMetrics {
    shares_received: u64,
    shares_forwarded: u64,
    shares_gated: u64,
    shares_stale: u64,
    shares_rejected_upstream: u64,
    supply_timestamps: VecDeque<Instant>,    // ring buffer, last 15s of arrival times
    forward_timestamps: VecDeque<Instant>,   // ring buffer, last 15s of forward times
}
```

### Rolling rate calculation

`supply_timestamps` and `forward_timestamps` are `VecDeque<Instant>` bounded to a 15-second window. On each share arrival:
1. Push `now` to the back
2. Pop entries older than `now - 15s` from the front
3. Rate = `len() / 15.0 * 60.0` (convert to spm)

Simple, O(1) amortized per share, no floating-point accumulation drift.

### Downstream ack construction

When the proxy acks a miner's share, it must produce a valid `SubmitSharesSuccess`:

```rust
fn ack_share(&mut self, share: &SubmitSharesExtended) -> SubmitSharesSuccess {
    self.downstream_ack_count += 1;
    self.downstream_ack_seq = share.sequence_number;
    SubmitSharesSuccess {
        channel_id: share.channel_id,
        last_sequence_number: share.sequence_number,
        new_submits_accepted_count: self.downstream_ack_count,
        new_shares_sum: 0,  // proxy doesn't track share work
    }
}
```

The proxy acks every share individually (batch size = 1). This is the simplest correct behavior — the miner gets immediate feedback. `new_shares_sum` is 0 because the proxy doesn't compute share work (it's not accounting for payouts).

## File breakdown

| File | Responsibility |
|------|---------------|
| `main.rs` | CLI args, config load, init logging, spawn tasks, run ProxyCore |
| `config.rs` | TOML deserialization, validation |
| `proxy.rs` | `ProxyCore` struct, select loop, event dispatch |
| `channel_pair.rs` | `ChannelPair`, difficulty floor logic, ack construction |
| `share_gate.rs` | `ShareGate`, token bucket, profile evaluation |
| `profile.rs` | `RateProfile` enum, `rate_at()` |
| `upstream.rs` | Connect, noise handshake (initiator), SetupConnection exchange |
| `downstream.rs` | Accept, noise handshake (responder), read task spawn |
| `api.rs` | axum router, ApiCommand enum, ProxyStatus serialization |
| `metrics.rs` | ChannelMetrics, rolling rate, prometheus export |

## Message flow

### Startup
```
1. Parse config, init logging
2. Connect upstream TCP + noise handshake (Initiator)
3. Exchange SetupConnection with pool
4. Open phantom channels (if configured)
5. Spawn upstream read task (→ upstream_rx)
6. Spawn axum HTTP server (→ api_rx)
7. Bind downstream listener
8. Enter ProxyCore select loop
```

### Downstream miner connects
```
1. Listener accepts TCP connection
2. Spawn read task: noise handshake (Responder) → read loop → downstream_tx
3. Read task sends DownstreamEvent::Connected { id, writer }
4. ProxyCore stores writer in downstream_writers
5. Miner sends SetupConnection → proxy responds SetupConnectionSuccess
6. Miner sends OpenExtendedMiningChannel
7. ProxyCore mirrors to pool: OpenExtendedMiningChannel (same nominal_hr, max_target)
8. Pool responds: OpenExtendedMiningChannelSuccess (upstream_channel_id, extranonce_prefix, target)
9. ProxyCore creates ChannelPair, inserts into both lookup maps
10. ProxyCore responds to miner: OpenExtendedMiningChannelSuccess
    (downstream_channel_id, same extranonce_prefix, harder_of(target, floor))
```

### Share arrives from miner
```
1. DownstreamEvent::Frame { id, frame } received
2. Parse as SubmitSharesExtended
3. Look up pair by downstream_channel_id
4. Construct and send SubmitSharesSuccess to miner (always, immediately)
5. Check: is share.job_id current? No → increment stale, stop
6. Check: does share meet upstream_target? No → increment gated, stop
7. Call gate.should_forward(now)? No → increment gated, stop
8. Rewrite: channel_id = upstream_channel_id, seq = next upstream_seq
9. Send upstream
10. Record in forward_timestamps
```

### Pool sends SetTarget
```
1. UpstreamEvent::Frame received, parse as SetTarget
2. Look up pair by upstream_channel_id
3. Store pair.upstream_target = new target
4. Compute downstream_target = harder_of(new_target, floor)
5. If downstream_target differs from current → send SetTarget to miner
```

### Pool sends NewExtendedMiningJob / SetNewPrevHash
```
1. Look up pair by upstream_channel_id
2. Rewrite channel_id: upstream → downstream
3. Forward to miner
4. For SetNewPrevHash: update current valid job_id
```

## Build steps (Phase 1)

Each step is testable in isolation.

### Step 1: Scaffold + upstream connection
- Create `test-tools/shape-proxy/Cargo.toml` (deps: stratum-apps, tokio, tracing, clap, toml, serde)
- `config.rs`: Config struct + TOML parsing
- `upstream.rs`: TCP connect + noise initiator + SetupConnection exchange
- `main.rs`: load config, connect upstream, log success
- **Test**: `cargo run -- -c config.toml` connects to pool, prints SetupConnectionSuccess

### Step 2: Downstream accept + channel open
- `downstream.rs`: TCP accept + noise responder + read task spawn
- `proxy.rs`: ProxyCore skeleton with select loop (upstream_rx + downstream_rx + listener)
- Channel-open mirroring: downstream OpenExtendedMiningChannel → upstream mirror → pair creation → downstream response
- **Test**: mining_device connects through proxy, channel opens, proxy logs extranonce_prefix

### Step 3: Full passthrough (no gating)
- Forward all pool messages (job, target, prevhash) downstream with channel_id rewrite
- Forward all miner shares upstream with channel_id + seq rewrite
- Ack all shares downstream immediately
- **Test**: mining_device mines through proxy at full rate, pool accepts shares, no errors

### Step 4: Token bucket + hold profile
- `share_gate.rs`: bucket with refill logic
- `profile.rs`: `Hold` variant + `rate_at()`
- Gate integration in share path: forward only when bucket allows
- Difficulty floor in SetTarget path
- **Test**: configure hold(10), observe ~10 spm forwarded while miner produces more. Pool's vardiff responds (SetTarget messages appear in log). Floor activates.

### Step 5: HTTP API
- `api.rs`: axum with `GET /status`, `POST /channels/{id}/profile`
- ApiCommand channel from axum task to ProxyCore
- ProxyStatus via watch channel for GET /status
- **Test**: curl to set hold(5) mid-run, observe rate change

### Step 6: Step profile + headroom monitoring
- `profile.rs`: `Step` variant
- `metrics.rs`: rolling rate calculation, headroom status derivation
- Include headroom in /status response
- **Test**: step(15, 8, at=0), verify headroom stays comfortable, forwarded rate tracks

### Step 7: Prometheus metrics
- Register counters/gauges, expose at `GET /metrics`
- **Test**: prometheus scrapes, values match manual observation

### Step 8: Phantom channels
- At startup after SetupConnection: open N upstream channels with no downstream pairing
- Receive and discard their jobs/targets
- Report in /status and metrics
- **Test**: configure phantom_channels=10, pool reports 10 channels open, proxy stable

### Step 9: Integration test
- Automated: start pool, start proxy, start mining_device
- Run hold(12) for 60s → step to 6 → hold 60s
- Assert: forwarded count within ±20% of expected (statistical tolerance)
- Assert: pool sent at least one SetTarget after the step (vardiff responded)
- Assert: headroom never clamped
- Assert: upstream rejections < 2%

## Phase 2 (after Phase 1 ships)

- Multiple simultaneous downstream connections (proven by architecture, just needs testing)
- Remaining profiles: ramp, stall, burst, oscillate
- Broadcast endpoint (`POST /profile`)
- Sequence execution with settle logic
- `POST /channels/{id}/sequence` + `GET /channels/{id}/sequence`

## Phase 3

- mara-deploy integration (polling, slider, scenario buttons)
- Grafana dashboard for shape-proxy metrics
- Automated sim-to-live comparison tooling
