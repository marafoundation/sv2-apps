# Shape-Proxy Architecture

The shape-proxy is a Stratum V2 middlebox that sits between mining devices and a pool, enabling controlled modification of the share submission rate to test pool vardiff behavior.

## Overview

```
[Miner(s)] <--SV2--> [Shape-Proxy] <--SV2--> [Pool]
                          |
                       HTTP API (control)
```

**Purpose**: Smooth share streams and inject controlled rate patterns to observe pool vardiff responses.

**Key features**:
- Share gating via token bucket algorithm
- Multiple rate profiles (Hold, Track, Step, Ramp, Stall, Burst, Oscillate)
- Supply-relative and absolute-rate modes
- HTTP API for dynamic profile control
- Difficulty floor enforcement

---

## Architecture

### Core Components

1. **ProxyCore** (`proxy.rs`)
   - Main event loop
   - Routes messages between upstream (pool) and downstream (miners)
   - Manages channel mappings
   - Applies share gating via ShareGate

2. **ShareGate** (`share_gate.rs`)
   - Token bucket implementation
   - Driven by RateProfile
   - Measures supply (shares/min from miner)
   - Decides whether to forward or drop each share

3. **RateProfile** (`profile.rs`)
   - Defines target rate as a function of time
   - Supports absolute rates (spm) and supply-relative factors (0.0–1.0+)

4. **Upstream** (`upstream.rs`)
   - Pool connection management
   - Resilient reconnection with exponential backoff
   - Message codec (SV2 framing)

5. **Downstream** (`downstream.rs`)
   - Miner connection handler (one task per miner)
   - Message routing to ProxyCore

6. **API** (`api.rs`)
   - HTTP JSON API for status and profile control
   - Endpoints: `/status`, `/profile`, `/channels/{id}/profile`

---

## Message Flow

### Channel Open

```
Miner → Proxy: OpenExtendedMiningChannel (request_id: 1, ds_channel_id: 1)
  ↓
Proxy: Store pending request, forward to pool
  ↓
Proxy → Pool: OpenExtendedMiningChannel (same request_id: 1)
  ↓
Pool → Proxy: OpenExtendedMiningChannelSuccess (request_id: 1, upstream_channel_id: 42, initial_target)
  ↓
Proxy: Map ds_channel_id=1 ↔ upstream_channel_id=42
  ↓
Proxy → Miner: OpenExtendedMiningChannelSuccess (request_id: 1, channel_id: 1, initial_target)
```

**Note**: The proxy forwards the pool's initial difficulty target to the miner. This target is **dynamically updated** as the pool's vardiff adjusts (see SetTarget handling below).

---

### Share Submission (Normal Path)

```
Miner → Proxy: SubmitSharesExtended (channel_id: 1, seq: 1, nonce: ...)
  ↓
Proxy: Immediately ACK the miner (invariant: always ACK instantly)
  ↓
Proxy → Miner: SubmitSharesSuccess (channel_id: 1, seq: 1)
  ↓
Proxy: Record share in ShareGate, measure supply
  ↓
ShareGate: Check token bucket (driven by profile)
  ↓
  if bucket >= 1.0:
    ↓
    Proxy → Pool: SubmitSharesExtended (channel_id: 42, seq: 1, nonce: ...)
    (Miner was already ACKed; no response needed from pool to miner)
  else:
    ↓
    Drop share (gate closed, increment shares_gated counter)
```

**Invariant**: The miner always receives an instant ACK, regardless of whether the share is forwarded or gated. This prevents the miner from stalling or adjusting its own difficulty.

---

### SetTarget (Difficulty Adjustment) – Critical Design

When the pool adjusts difficulty based on observed hashrate:

```
Pool → Proxy: SetTarget (channel_id: 42, maximum_target: 0x00000000FFFF...)
  ↓
Proxy: Store pool_difficulty for metrics
  ↓
Proxy: Apply difficulty floor (if configured)
  if pool_difficulty < config.min_downstream_difficulty:
    ↓
    Proxy: Absorb message, don't forward (floor active)
  else:
    ↓
    Proxy → Miner: SetTarget (channel_id: 1, maximum_target: ...)
```

**Why forward SetTarget to the miner?**

The miner must work at the pool's current difficulty target. If the proxy absorbs SetTarget and doesn't forward it, the miner continues producing shares at the old (stale) difficulty. When the pool raises difficulty, those shares will be rejected as `diff-too-low`, causing a death spiral:

1. Pool sees hashrate → raises difficulty (sends SetTarget)
2. Proxy absorbs SetTarget → miner doesn't update difficulty
3. Miner produces shares at old (easier) difficulty
4. Pool rejects all shares (diff-too-low)
5. Accepted share rate → 0, pool sees zero hashrate
6. Pool lowers difficulty → repeat

**Solution**: Forward SetTarget to the miner so it produces shares that meet the pool's current difficulty target.

**Difficulty-Weighted Supply Tracking**

The proxy tracks **difficulty-weighted supply** (true hashrate) instead of raw share count:

- Each share is weighted by the miner's current difficulty when recording supply
- Supply measurement becomes stable regardless of pool vardiff changes
- For example: If pool doubles difficulty, miner produces half as many shares, but each counts 2× → supply stays constant
- Supply-relative profiles (Track, Step-relative) now work correctly with pool vardiff

This makes the gate's behavior independent of pool difficulty adjustments while still preventing share rejections.

---

### NewMiningJob / SetNewPrevHash

These messages are forwarded verbatim from pool to miner:

```
Pool → Proxy: NewMiningJob / SetNewPrevHash
  ↓
Proxy → Miner: Forward without modification
```

The proxy does not interpret job templates; it only gates share submission.

---

## Share Gating (Token Bucket)

The ShareGate uses a token bucket to smooth share flow:

```rust
Bucket capacity: 12 seconds worth of tokens (12 × target_spm / 60)
Refill rate: target_spm / 60 tokens per second

On each share arrival:
  1. Record share in supply measurement (rolling 60s window)
  2. Refill bucket based on elapsed time
  3. if bucket >= 1.0:
       Forward share, decrement bucket by 1.0
     else:
       Drop share (gate closed)
```

**Supply measurement** (`current_supply_spm`):
- Rolling 60-second window of share arrivals
- Smoothed instantaneous rate in shares/minute
- Used by supply-relative profiles (Track, Step-relative, etc.)

**Target rate** (`target_spm`):
- Computed by the active RateProfile
- For absolute profiles: fixed value (e.g., Hold {rate: 60.0} → 60 spm)
- For relative profiles: factor × current_supply_spm (e.g., Track {factor: 0.5} → 0.5 × supply)

**Bootstrap case** (Track profile, supply = 0):
- Use temporary target of 1000 spm to forward everything until supply is measured
- Prevents death spiral where target = 0 × factor = 0 → bucket never refills

---

## Difficulty Floor

The proxy can enforce a minimum downstream difficulty to prevent pool vardiff from lowering difficulty too much:

**Config**: `min_downstream_difficulty` (in difficulty units, e.g., 1024.0)

**Behavior**:
- When pool sends SetTarget with `pool_difficulty < min_downstream_difficulty`:
  - Proxy absorbs the message (sets `floor_active = true`)
  - Miner keeps its current (harder) difficulty
- When pool sends SetTarget with `pool_difficulty >= min_downstream_difficulty`:
  - Proxy forwards the message (sets `floor_active = false`)
  - Miner updates to pool's target

**Use case**: Prevent pool from lowering difficulty to a point where the miner produces too many shares (overwhelming the proxy or pool).

---

## Metrics & Monitoring

### Status Endpoint (`GET /status`)

Returns JSON with per-channel metrics:

```json
{
  "upstream_connected": true,
  "channels": [
    {
      "id": 1,
      "miner_connected": true,
      "profile": {
        "type": "track",
        "description": "1.0× supply"
      },
      "profile_elapsed_secs": 123.4,
      "profile_duration_secs": null,
      "target_spm": 60.0,
      "forwarded_spm": 59.8,
      "supply_spm": 60.2,
      "headroom": "comfortable",
      "floor_active": false,
      "pool_difficulty": 2048.0,
      "shares_forwarded": 120,
      "shares_gated": 5,
      "shares_rejected_difficulty": 0
    }
  ]
}
```

**Key metrics**:
- `supply_spm`: Shares/min arriving from miner (smoothed, 60s window)
- `target_spm`: Profile's target rate
- `forwarded_spm`: Actual shares/min sent to pool (60s window)
- `headroom`: Supply vs target ratio
  - "comfortable": supply > 1.5× target
  - "adequate": supply > 1.1× target
  - "tight": supply > target
  - "critical": supply ≤ target (risk of underdelivery)
- `pool_difficulty`: Pool's current difficulty target (from most recent SetTarget)
- `shares_forwarded`: Total shares sent to pool
- `shares_gated`: Total shares dropped by gate
- `shares_rejected_difficulty`: Reserved for future use (not yet incremented)

---

## Profile Control

### Set Profile for All Channels

```bash
curl -X POST http://localhost:8080/profile \
  -H "Content-Type: application/json" \
  -d '{"type": "track", "factor": 0.5}'
```

### Set Profile for Specific Channel

```bash
curl -X POST http://localhost:8080/channels/1/profile \
  -H "Content-Type: application/json" \
  -d '{"type": "step", "before": 1.0, "after": 0.5, "at_secs": 300, "relative": true}'
```

Profiles are applied immediately; the gate's token bucket is reset and the profile timer starts.

---

## Connection Resilience

### Upstream (Pool) Reconnection

When the pool connection drops:
1. Proxy enters reconnect loop with exponential backoff (1s → 2s → 4s → ... → 60s max)
2. Miners stay connected (downstream connections are not dropped)
3. On reconnect, proxy re-opens all channels (using stored `open_request` for each)
4. Share gating continues (shares may be dropped if gate is closed, but miner sees no disruption)

### Downstream (Miner) Disconnection

When a miner disconnects:
1. Proxy removes the downstream connection
2. Channel mapping remains in place (for reconnection within same session)
3. If miner reconnects and opens a new channel, a new mapping is created

---

## Design Rationale

### Why immediate ACK to miner?

SV2 miners expect fast ACKs. Delaying the ACK until the pool responds would:
- Introduce latency (miner stalls waiting for ACK)
- Expose pool behavior to miner (rejected shares → miner adjusts difficulty)
- Break the gating abstraction (miner would see dropped shares as rejections)

By ACKing instantly, the proxy decouples the miner from the pool's share validation.

### Why forward SetTarget to miner?

If the proxy absorbs SetTarget, the miner's difficulty becomes stale relative to the pool's expectation, causing share rejections. Forwarding SetTarget ensures shares from the miner meet the pool's current difficulty target.

The proxy tracks difficulty-weighted supply (hashrate) so supply measurement stays stable even when pool vardiff changes the miner's difficulty.

### Why measure supply over 15 seconds?

The rolling window uses a 15-second window. Shorter windows (e.g., 1 second) are too noisy; longer windows (e.g., 5 minutes) are too slow to respond to real changes. 15 seconds balances smoothness and responsiveness for the gate's refill calculations.

### Why 12-second bucket capacity?

This allows brief bursts (up to 12 seconds of "credit") without runaway accumulation. It smooths short-term variance but prevents the proxy from forwarding weeks' worth of gated shares in a single burst.

---

## Known Limitations

1. **No share validation**: The proxy does not re-hash shares to check if they meet the pool's target. It relies on the miner to produce valid shares at the forwarded difficulty. If the miner is buggy or malicious, invalid shares may be forwarded.

2. **Single upstream**: The proxy connects to one pool. It does not support failover or load-balancing across multiple pools.

3. **Channel ID collision risk**: The proxy reuses the miner's `request_id` when forwarding to the pool (assumes no collision). In a multi-miner setup with many simultaneous channel opens, there's a small risk of collision if miners use the same request_id concurrently.

4. **No persistence**: State is in-memory only. On restart, all channels are closed and miners must reconnect.

---

## Testing Patterns

See [PROFILES.md](PROFILES.md) for detailed examples of using profiles to test pool vardiff.

**Common scenarios**:
- **Noise reduction**: Track {factor: 0.7} — forward 70% of supply, hiding short-term variance
- **Sudden drop**: Step (1.0 → 0.5, relative) — instant 50% hashrate drop
- **Gradual ramp**: Ramp (0.5 → 1.0, relative) — 2× hashrate increase over time
- **Miner disappearance**: Stall (rate: 1.0, at_secs: 120, duration_secs: 60) — zero shares for 1 minute
- **Hashrate spike**: Burst (base: 0.8, peak: 1.5, relative) — temporary 1.5× burst
- **Periodic variance**: Oscillate (base: 0.9, amp: 0.1) — sinusoidal fluctuation

---

## Future Enhancements

1. **Share validation**: Check if shares meet the pool's current target before forwarding, increment `shares_rejected_difficulty` when they don't.

2. **Multi-pool failover**: Connect to multiple pools, automatically switch on disconnect.

3. **Persistence**: Save channel state to disk, restore on restart.

4. **Profile scheduling**: Time-based profile switching (e.g., "run profile A for 10 minutes, then switch to profile B").

5. **Per-channel floor**: Allow different difficulty floors for different channels.

6. **Metrics export**: Prometheus endpoint for monitoring integration.
