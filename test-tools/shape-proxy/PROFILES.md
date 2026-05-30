# Shape-Proxy Rate Profiles

The shape-proxy supports **supply-relative** and **absolute-rate** profiles for flexible share gating.

## Quick Start

```bash
# Get current status (shows supply_spm, target_spm, forwarded_spm)
curl http://localhost:8080/status | jq

# Set profile for all channels
curl -X POST http://localhost:8080/profile \
  -H "Content-Type: application/json" \
  -d '{"type": "track", "factor": 0.5}'

# Set profile for specific channel
curl -X POST http://localhost:8080/channels/1/profile \
  -H "Content-Type: application/json" \
  -d '{"type": "step", "before": 1.0, "after": 0.5, "at_secs": 300, "relative": true}'
```

---

## Profile Types

### 1. Track (Supply-Relative, Default)

**Purpose**: Forward a constant percentage of measured supply. Use for noise reduction while exposing real hashrate changes.

```json
{"type": "track", "factor": 1.0}
```

| Field | Type | Description |
|-------|------|-------------|
| `factor` | f64 | Multiplier for supply (1.0 = 100%, 0.5 = 50%) |

**Example**: `factor: 0.5` with 80 spm supply → forwards 40 spm

**When to use**: Production smoothing. Hides short-term variance but tracks sustained changes.

---

### 2. Hold (Absolute Rate)

**Purpose**: Forward a fixed rate regardless of supply.

```json
{"type": "hold", "rate": 60.0}
```

| Field | Type | Description |
|-------|------|-------------|
| `rate` | f64 | Fixed rate in shares per minute |

**Example**: Always forwards exactly 60 spm, even if supply is 200 spm.

**When to use**: Fixed-rate testing or when supply is irrelevant.

---

### 3. Step (Absolute or Relative)

**Purpose**: Instant transition between two rates.

```json
{"type": "step", "before": 1.0, "after": 0.5, "at_secs": 300, "relative": true}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `before` | f64 | required | Rate/factor before transition |
| `after` | f64 | required | Rate/factor after transition |
| `at_secs` | f64 | required | When to transition (seconds from profile start) |
| `relative` | bool | false | If true, values are factors; if false, absolute spm |

**Example (relative)**: 
- Supply is 100 spm
- `before: 1.0, after: 0.5, at_secs: 300, relative: true`
- First 5 minutes: forwards 100 spm (100%)
- After 5 minutes: forwards 50 spm (50%)
- **Pool sees 50% hashrate drop**

**Example (absolute)**:
- `before: 80.0, after: 40.0, at_secs: 300, relative: false`
- First 5 minutes: forwards 80 spm
- After 5 minutes: forwards 40 spm
- **Pool sees 50% hashrate drop regardless of actual supply**

**When to use**: Test pool's vardiff response to sudden changes.

---

### 4. Ramp (Absolute or Relative)

**Purpose**: Linear interpolation between two rates over time.

```json
{"type": "ramp", "from": 0.5, "to": 1.0, "duration_secs": 600, "relative": true}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `from` | f64 | required | Starting rate/factor |
| `to` | f64 | required | Ending rate/factor |
| `duration_secs` | f64 | required | Ramp duration |
| `relative` | bool | false | If true, values are factors |

**Example (relative)**:
- Supply is 80 spm
- `from: 0.5, to: 1.0, duration_secs: 600, relative: true`
- Start: forwards 40 spm (50%)
- After 5 min: forwards 60 spm (75%)
- After 10 min: forwards 80 spm (100%)
- **Pool sees gradual 2× hashrate increase**

**When to use**: Test pool's vardiff tracking of gradual changes.

---

### 5. Stall (Absolute or Relative)

**Purpose**: Drop to zero for a period, then resume.

```json
{"type": "stall", "rate": 1.0, "at_secs": 120, "duration_secs": 60, "relative": true}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rate` | f64 | required | Normal rate/factor (when not stalled) |
| `at_secs` | f64 | required | When to start stall |
| `duration_secs` | f64 | required | How long to stall |
| `relative` | bool | false | If true, `rate` is a factor |

**Example (relative)**:
- Supply is 100 spm
- `rate: 1.0, at_secs: 120, duration_secs: 60, relative: true`
- 0-2 min: forwards 100 spm
- 2-3 min: forwards 0 spm (complete stall)
- 3+ min: forwards 100 spm

**When to use**: Test pool's response to miner disappearance/reconnection.

---

### 6. Burst (Absolute or Relative)

**Purpose**: Spike to a higher rate for a period.

```json
{"type": "burst", "base": 0.8, "peak": 1.5, "at_secs": 180, "duration_secs": 60, "relative": true}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base` | f64 | required | Normal rate/factor |
| `peak` | f64 | required | Burst rate/factor |
| `at_secs` | f64 | required | When to start burst |
| `duration_secs` | f64 | required | How long to burst |
| `relative` | bool | false | If true, values are factors |

**Example (relative)**:
- Supply is 100 spm
- `base: 0.8, peak: 1.5, at_secs: 180, duration_secs: 60, relative: true`
- 0-3 min: forwards 80 spm (80%)
- 3-4 min: forwards 150 spm (150% — drains buffered shares faster)
- 4+ min: forwards 80 spm (80%)

**When to use**: Test pool's response to temporary hashrate spikes.

**Note**: `peak > 1.0` (>100%) forwards more than current supply by draining the token bucket faster. Only works if you've been gating (factor < 1.0) earlier to build up a reserve.

---

### 7. Oscillate (Absolute or Relative)

**Purpose**: Sinusoidal wave oscillation.

```json
{"type": "oscillate", "base": 0.9, "amp": 0.1, "period_secs": 300, "relative": true}
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `base` | f64 | required | Center rate/factor |
| `amp` | f64 | required | Amplitude (±range) |
| `period_secs` | f64 | required | Full cycle duration |
| `relative` | bool | false | If true, values are factors |

**Formula**: `rate = base + amp × sin(2π × elapsed / period)`

**Example (relative)**:
- Supply is 100 spm
- `base: 0.9, amp: 0.1, period_secs: 300, relative: true`
- Oscillates between 80 spm (90% - 10%) and 100 spm (90% + 10%)
- Full cycle every 5 minutes

**When to use**: Test pool's vardiff tracking of periodic fluctuations.

---

## Common Patterns

### Noise Reduction Only
Forward 70% of supply to reduce short-term variance:
```bash
curl -X POST http://localhost:8080/profile \
  -d '{"type": "track", "factor": 0.7}'
```

### Test 50% Hashrate Drop
Run at 100% for 5 minutes, then drop to 50%:
```bash
curl -X POST http://localhost:8080/profile \
  -d '{"type": "step", "before": 1.0, "after": 0.5, "at_secs": 300, "relative": true}'
```

### Test Gradual Ramp-Up
Ramp from 50% to 100% over 10 minutes:
```bash
curl -X POST http://localhost:8080/profile \
  -d '{"type": "ramp", "from": 0.5, "to": 1.0, "duration_secs": 600, "relative": true}'
```

### Test Pool's Vardiff Window
Oscillate ±10% around 90% with 5-minute period:
```bash
curl -X POST http://localhost:8080/profile \
  -d '{"type": "oscillate", "base": 0.9, "amp": 0.1, "period_secs": 300, "relative": true}'
```

### Fixed Absolute Rate (Ignore Supply)
Always forward exactly 60 spm:
```bash
curl -X POST http://localhost:8080/profile \
  -d '{"type": "hold", "rate": 60.0}'
```

---

## Monitoring

Check the `/status` endpoint to see how profiles are performing:

```bash
curl http://localhost:8080/status | jq '.channels[] | {
  id,
  profile: .profile.description,
  supply_spm,
  target_spm,
  forwarded_spm,
  headroom,
  gated: .shares_gated,
  forwarded: .shares_forwarded
}'
```

**Key metrics**:
- `supply_spm`: Shares arriving from miner (smoothed, 60s window)
- `target_spm`: Current profile target (formula result)
- `forwarded_spm`: Actual shares sent to pool (may lag target slightly)
- `headroom`: Supply vs target ratio ("comfortable", "tight", "critical")

---

## Notes

- **Supply tracking**: Uses a 60-second rolling window. Short bursts (<60s) are smoothed out.
- **Token bucket**: Capacity is 12 seconds worth of tokens. Allows brief bursts but prevents runaway.
- **Relative profiles scale with supply**: If your hashrate drops 50%, a relative profile's output also drops 50% (exposing the change to the pool).
- **Absolute profiles ignore supply**: Fixed rate regardless of what the miner produces.
- **Default profile**: `Track {factor: 1.0}` (forward 100% of smoothed supply).
- **Difficulty tracking**: The proxy forwards `SetTarget` messages from the pool to the miner, so the miner's difficulty tracks the pool's vardiff adjustments. This ensures shares from the miner meet the pool's current difficulty target (preventing `diff-too-low` rejections). As a side effect, supply (shares/min) varies with difficulty changes even if hashrate is constant. See [ARCHITECTURE.md](ARCHITECTURE.md) for details.
