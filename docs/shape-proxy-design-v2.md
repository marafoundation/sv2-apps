# Shape Proxy — Design (v2)

## Problem

We have a vardiff simulation framework that predicts algorithm behavior under synthetic conditions. We need to validate those predictions against a live pool. The simulation says FullRemedy converges in X minutes with Y overshoot — does the real implementation match?

Real miners hash at a fixed rate determined by hardware. The only variable is what qualifies as a *share* — determined by the difficulty target. The shape proxy sits between miner and pool, accepting all shares from the miner but forwarding only a controlled subset upstream. The pool sees a channel whose share arrival rate follows whatever pattern we prescribe.

## Mechanism

```
Miner ──[all shares]──► Shape Proxy ──[shaped share stream]──► Pool
                              │
                         token bucket
                         driven by
                         rate profile
```

Three invariants:

1. **Miner always acked.** Every share gets `SubmitSharesSuccess` downstream. The miner's firmware behavior is unaffected.
2. **Only real shares forwarded.** The proxy selects which valid shares to pass through — it never fabricates work.
3. **Pool sees a normal channel.** The shaped output is indistinguishable from a miner with variable hashrate.

Vardiff's only input is share arrival timestamps. By controlling which shares reach the pool, we control vardiff's stimulus with simulation-level precision while running against real pool code and real protocol handling.

## Supply, demand, and the difficulty floor

### Definitions

- **Hashrate**: hashes per second. Fixed by hardware. The miner cannot change this.
- **Target**: 256-bit threshold set via `SetTarget`. A hash below this target qualifies as a share.
- **Share rate** (supply): how many shares the miner finds per minute. Determined by `hashrate × (target / 2^256) × 60`. Not a property of the miner alone — it emerges from hashrate AND target together.
- **Profile target**: the share rate (shares/min) the gate aims to forward upstream.
- **Headroom**: ratio of supply to profile target. Must be ≥ 2× for clean shaping.

### The feedback loop

The pool's vardiff observes the shaped share rate (gate output), computes an estimated hashrate from it, and sends `SetTarget` to adjust difficulty. The proxy passes `SetTarget` through to the miner (subject to a floor). The miner's share rate changes accordingly.

This loop converges because the pool adjusts based on the *apparent* hashrate (what the gate forwards), while the miner produces shares based on its *real* hashrate. Since real >> apparent (the gate is attenuating), the pool's difficulty adjustments produce proportionally larger supply changes than the pool expects:

```
Pool targets 15 spm from apparent hashrate X → sets target T
Real miner at hashrate Y (where Y >> X) produces: Y × T / 2^256 × 60 spm
  = 15 × (Y/X) spm of supply
```

The attenuation ratio (Y/X) is the structural headroom. The pool cannot collapse it — it's adjusting T based on X, not Y. The system self-stabilizes with supply permanently above what the gate needs.

### The difficulty floor

Without a floor, the pool keeps lowering difficulty (raising target) as it chases the apparent hashrate downward. The miner's share rate grows unboundedly — eventually producing thousands of shares/second, all of which get acked and dropped. This wastes miner interrupt cycles, network bandwidth, and proxy CPU.

The proxy enforces a minimum difficulty (maximum target) on `SetTarget` forwarded downstream:

```rust
fn on_upstream_set_target(&mut self, pool_target: Target) {
    self.upstream_target = pool_target;
    let downstream_target = pool_target.harder_of(self.config.min_downstream_target);
    self.send_downstream(SetTarget { target: downstream_target });
}
```

Shares meeting the harder floor always meet the pool's easier target — no shares are wasted. The floor bounds the miner's share rate while preserving full upstream validity.

**Floor sizing**: produce ~4× the maximum profile target in supply. For 30 spm peak profiles on a 200 TH/s miner, set the floor to produce ~120 spm. This gives ~2 shares/sec from the miner — negligible overhead — with the gate dropping ~75%.

The floor doesn't affect the test. The pool's vardiff is tested against the gate's output, not the miner's raw share rate.

### Cold start

If the pool's initial difficulty is very hard (inherited from a prior session), supply may briefly be below the profile target. The pool's vardiff will lower difficulty within a few retarget cycles. The proxy reports `clamped` status during this transient. Tests should not begin until `comfortable` status is reached.

## Recommended supply architecture

```
Native SV2 ASIC ──► Shape Proxy ──► Pool
```

Why: zero intermediate vardiff layers, minimal latency (~1ms per share), pure Poisson share-finding statistics, and sufficient supply from a single miner for all scenarios in the 2–30 spm range.

**SV1 fallback**: SV1 miners connect via the translator with `enable_vardiff = false`. The translator becomes a pure protocol converter — it passes `SetTarget` through to the miner as `mining.set_difficulty` without running its own retarget cycle. Residual differences vs native SV2: SV1 firmware batch-submission patterns (~2-5 share micro-bursts) and brief supply dips during `mining.set_difficulty` propagation. Both are negligible at the spm measurement timescale.

**Do not use**: translator with vardiff enabled (introduces a second adaptive controller), JDC (always runs its own vardiff, no passthrough mode), or any aggregation topology for vardiff testing (unnecessary — single-miner supply suffices, and aggregation hides per-miner noise).

## Rate profiles

A profile is a function `f(t) → target shares/min`. The gate evaluates it on every share arrival.

| Profile | Behavior | Sim scenario |
|---------|----------|-------------|
| `hold(rate)` | Constant | `Scenario::Stable` |
| `step(before, after, at)` | Instant change | `Scenario::Step` |
| `ramp(from, to, duration)` | Linear transition | `Scenario::ColdStart` |
| `stall(rate, at, duration)` | Zero then resume | Miner disconnect |
| `burst(base, peak, at, duration)` | Transient spike | Hashboard recovery |
| `oscillate(base, amp, period)` | Sinusoidal | Thermal throttle |
| `sequence([...])` | Chained profiles | Multi-phase plans |

## Token bucket

The gate's forwarding decision uses a token bucket refilled at the profile's current target rate:

```rust
fn on_share(&mut self, share: Share) {
    self.ack_downstream(&share);

    let target_spm = self.profile.rate_at(elapsed());
    let dt = now() - self.last_refill;
    self.last_refill = now();
    self.bucket = (self.bucket + (target_spm / 60.0) * dt.as_secs_f64()).min(self.capacity);

    if self.bucket >= 1.0 {
        self.bucket -= 1.0;
        self.forward_upstream(&share);
    }
}
```

**Capacity sizing**: `capacity = max(2.0, target_spm × bucket_window_secs / 60.0)` where `bucket_window_secs` is configurable (default: 12 seconds, i.e., the bucket can absorb a burst of up to 12 seconds of share-arrival silence followed by a cluster). At 2× headroom with Poisson arrivals, a 12-second window provides >99.5% probability of the bucket never being the bottleneck during a hold.

The capacity automatically scales with the profile target — at high target rates the bucket absorbs larger absolute clusters, at low rates it stays small enough to respond quickly to profile changes.

## Settling between scenarios

Each simulation scenario starts from a pre-converged steady state. On a live system, that precondition must be created explicitly. After each profile action, the proxy holds at the terminal rate and waits for convergence before advancing to the next action in a sequence.

```rust
struct SequenceEntry {
    profile: RateProfile,
    duration_secs: f64,
    settle: SettlePolicy,
}

enum SettlePolicy {
    None,                           // advance immediately
    WaitForConvergence,             // wait for criteria below
    FixedCooldown { secs: f64 },    // simpler: just wait N seconds
}
```

### Convergence criteria (`WaitForConvergence`)

All three must hold simultaneously for `settle_hold_secs` (configurable, default 30s):

1. **Headroom ≥ 2.0×** — supply has stabilized
2. **No `SetTarget` received in `settle_hold_secs`** — vardiff stopped adjusting
3. **Forwarded rate within 10% of profile target** — gate is shaping cleanly

Timeout (default 5 min): if criteria aren't met, advance with a warning and mark subsequent measurements as "unsettled start."

Note: criterion 2 depends on the pool's retarget interval. If the pool retargets every 60s, `settle_hold_secs` must be > 60s to avoid false convergence detection. The operator should set this based on known pool configuration.

## Guardrails

### Headroom monitoring

Per-channel status derived from `supply_rate_spm / profile_target_spm`:
- **comfortable** (≥ 2.0×): output tracks profile cleanly
- **marginal** (1.0–2.0×): elevated short-term variance
- **clamped** (< 1.0×): supply-limited — **test invalid**

### Profile divergence

If forwarded rate diverges >20% from profile target for >30s during a hold, emit a warning. Causes: supply dropped (miner disconnected?), floor too high, bucket misconfigured.

### Upstream share rejection

If the pool sends `SubmitSharesError`, log the reason. Track per channel; >2% rejection rate means the proxy's job invalidation handling has a bug.

### Miner disconnect

Downstream drops while profile active: hold upstream channel open, stop profile clock, mark `supply_interrupted`. Resume on reconnect or abort after timeout.

## Phantom channels

Upstream channels with no miner attached. For scale testing (channel load, broadcast latency, per-channel resource cost), not vardiff testing. Typical: 2–4 real channels alongside 50–100 phantoms.

## Metrics

Three categories serving different consumers.

### Safety (proxy internal)

Drives the proxy's own decisions. Answers: "is the proxy operating correctly?"

| Metric | Type | Purpose |
|--------|------|---------|
| `supply_rate_spm` | gauge | Headroom input. Share arrivals from miner (15s rolling). |
| `headroom_status` | enum | comfortable / marginal / clamped |
| `floor_active` | bool | Difficulty floor overriding pool target. Expected steady state. |
| `downstream_difficulty` | gauge | What miner works against (harder of pool target and floor). |

### Test observation (understanding pool response)

The measurements that make the tool useful. Answers: "what did the pool do?"

| Metric | Type | Purpose |
|--------|------|---------|
| `profile_target_spm` | gauge | Gate's intended output rate — the test stimulus. |
| `forwarded_rate_spm` | gauge | What the pool received (15s rolling). |
| `pool_difficulty` | gauge | Pool's current target for this channel (scalar). |
| `pool_set_target_count` | counter | Times pool adjusted difficulty. |
| `shares_forwarded_total` | counter | Cumulative shares pool saw. |
| `shares_gated_total` | counter | Cumulative shares dropped by gate. |
| `shares_rejected_total` | counter | Pool rejected after forwarding (stale race). |
| `settle_phase` | enum | active / settling / idle |

### Operations (Grafana / alerting)

Answers: "is the test infrastructure healthy?"

| Metric | Type | Purpose |
|--------|------|---------|
| `upstream_connected` | bool | Pool connection alive. |
| `downstream_miners` | gauge | Connected miner count. |
| `channels_active` | gauge | Paired channels (miner + upstream). |
| `channels_phantom` | gauge | Upstream-only channels. |

All per-channel metrics carry a `channel` label. Exposed at `GET /metrics` in prometheus format.

## API

Two consumers: mara-deploy (interactive UI) and automated test scripts.

### `GET /status` — full state snapshot

mara-deploy polls this at 1–2s to render its panel:

```json
{
  "upstream_connected": true,
  "channels": [
    {
      "id": 1,
      "miner_connected": true,
      "profile": { "type": "hold", "rate": 15.0 },
      "phase": "active",
      "target_spm": 15.0,
      "forwarded_spm": 14.8,
      "supply_spm": 62.3,
      "headroom": "comfortable",
      "floor_active": true,
      "pool_difficulty": 284000,
      "downstream_difficulty": 23000
    }
  ],
  "phantoms": 4
}
```

### `POST /channels/{id}/profile` — set rate profile

For mara-deploy's slider (maps 0–100% to a share rate):
```json
{ "type": "hold", "rate": 12.5 }
```

For scripted tests:
```json
{ "type": "step", "before": 15, "after": 8, "at_secs": 0 }
```

### `POST /profile` — broadcast to all active channels

Same body as above. Applies atomically with shared timestamp.

### `POST /channels/{id}/sequence` — automated test plan

```json
{
  "entries": [
    { "profile": { "type": "hold", "rate": 15 }, "duration_secs": 60, "settle": "wait_for_convergence" },
    { "profile": { "type": "step", "before": 15, "after": 8, "at_secs": 0 }, "duration_secs": 180, "settle": "fixed_cooldown", "cooldown_secs": 60 }
  ]
}
```

### `GET /channels/{id}/sequence` — sequence progress

```json
{
  "running": true,
  "current_entry": 1,
  "entries_total": 2,
  "phase": "settling",
  "settle_elapsed_secs": 42
}
```

### `POST /reset` — tear down everything

### mara-deploy integration

The shape proxy replaces the current handicap-file approach with HTTP, giving bidirectional feedback:

1. **Deploy**: mara-deploy starts shape-proxy container on the same Docker network as the pool.
2. **Poll**: `GET /status` at 1–2s updates a `ShapeProxyState` in the UI (headroom, phase, pool difficulty — visible context the file approach couldn't provide).
3. **Slider control**: Intensity slider maps to `POST /channels/{id}/profile` with `hold` at the computed rate. Same ergonomics as current throttle slider.
4. **Scenario execution**: "Run test" button posts a sequence, UI shows progress via polling `GET /channels/{id}/sequence`.
5. **Multi-channel**: Each miner gets its own channel panel. Broadcast button hits `POST /profile` for synchronized scenarios.

## Implementation

### Crate location

```
sv2-apps/test-tools/shape-proxy/
├── Cargo.toml
└── src/
    ├── main.rs
    ├── config.rs
    ├── proxy.rs           # ShapeProxy, channel registry
    ├── channel_pair.rs    # downstream ↔ upstream + gate
    ├── share_gate.rs      # token bucket + profile eval
    ├── profile.rs         # RateProfile, SequenceEntry, SettlePolicy
    ├── upstream.rs        # SV2 client connection to pool
    ├── downstream.rs      # SV2 server accepting miners
    └── api.rs             # axum HTTP + prometheus
```

### Data model

```rust
struct ShapeProxy {
    config: Config,
    upstream: PoolConnection,       // one TCP connection, multiple channels
    pairs: HashMap<u32, ChannelPair>,
    phantoms: Vec<u32>,
}

struct Config {
    upstream_address: SocketAddr,
    downstream_listen: SocketAddr,
    min_downstream_target: Target,  // difficulty floor
    phantom_channels: u32,
    default_profile: RateProfile,
    settle_hold_secs: f64,          // how long criteria must hold (default 30)
    settle_timeout_secs: f64,       // max wait before advancing (default 300)
}

struct ChannelPair {
    downstream_id: u32,
    upstream_id: u32,
    extranonce_prefix: Vec<u8>,     // verbatim from pool
    upstream_target: Target,        // pool's current (for forwarding validity check)
    downstream_target: Target,      // max(pool_target, floor) — what miner works against
    prev_upstream_target: Option<(Target, Instant)>,  // grace window for in-flight shares
    gate: ShareGate,
    metrics: ChannelMetrics,
}

struct ShareGate {
    profile: RateProfile,
    started_at: Instant,
    bucket: f64,
    capacity: f64,
    last_refill: Instant,
    settle_state: Option<SettleState>,
}
```

### Phased delivery

**Phase 1**: One channel. `hold` + `step` profiles. Difficulty floor. `FixedCooldown` settle. HTTP control. Headroom/floor/divergence monitoring.
*Validates*: single-channel vardiff step response.

**Phase 2**: Multiple channels. All profiles. Broadcast. `WaitForConvergence` settle. Full prometheus metrics. Sequence profiles.
*Validates*: multi-channel isolation, automated test plans.

**Phase 3**: Phantom channels. Scale testing.
*Validates*: pool under channel load.

**Phase 4**: Sim-to-live automation — replay sim scenarios, collect pool metrics, compare against predicted values.
*Validates*: simulation prediction accuracy.

## Edge cases

### `SetTarget` while floor is active

Pool sends many `SetTarget` messages as vardiff hunts. If all are below the floor, the miner never sees them — it stays at the floor difficulty producing shares at a bounded rate. The gate continues shaping from this stable supply. This is the normal steady-state operating mode.

When the pool's target crosses *above* the floor (gets harder than the floor), the proxy forwards it to the miner directly. Supply drops. This can happen if vardiff overshoots upward — the headroom monitor will flag if supply drops below 2×.

### In-flight shares after `SetTarget`

The miner has shares in its pipeline computed against the previous target. After a target *increase* (harder), these in-flight shares may not meet the new target. The proxy accepts shares meeting either current or previous upstream target for a 5-second grace window after each change.

This only matters when the pool's target gets harder — rare in floor-active steady state (pool is converging downward toward apparent hashrate, not upward).

### `SetNewPrevHash` (new block)

Invalidates all work on the previous block. The proxy forwards immediately to the miner. Shares arriving with stale `job_id` after this point are dropped (not forwarded, still acked downstream). Creates a brief supply gap (~1s) while the miner switches — invisible at typical share rates.

### Upstream connection drop

All channel pairs share one TCP connection. If it drops, all upstream channels are lost. The proxy: holds downstream connections open, reconnects upstream with backoff, re-opens all channels, resumes gate operation. Downstream miners see a brief gap in jobs (no `NewExtendedMiningJob` during reconnect) but no disconnection.

During reconnect the pool assigns new `extranonce_prefix` values. The proxy must update downstream miners via a new `OpenExtendedMiningChannelSuccess`. This forces the miner to discard current work and restart — effectively a cold-start transient. The settle logic handles this naturally.

### Pool rejects a forwarded share

`SubmitSharesError` with reason (stale-share, difficulty-too-low, invalid-job-id). Logged as a test artifact. The gate doesn't retry or adjust — it already acked the miner. If rejection rate exceeds a few percent, investigate: likely a race between `SetNewPrevHash` and the forwarding decision, or the grace window is too short.
