use std::time::Instant;

use crate::metrics::RollingWindow;
use crate::profile::RateProfile;

pub struct ShareGate {
    profile: RateProfile,
    started_at: Instant,
    bucket: f64,
    capacity: f64,
    last_refill: Instant,
    supply_window: RollingWindow,
    current_supply_spm: f64,
    /// Current miner difficulty, used to convert between difficulty-weighted and effective shares
    miner_difficulty: f64,
}

impl ShareGate {
    pub fn new(profile: RateProfile) -> Self {
        let (initial_value, _) = profile.rate_at(0.0);
        let capacity = Self::compute_capacity(initial_value);
        Self {
            profile,
            started_at: Instant::now(),
            bucket: capacity,
            capacity,
            last_refill: Instant::now(),
            supply_window: RollingWindow::new(),
            current_supply_spm: 0.0,
            miner_difficulty: 1.0,
        }
    }

    /// Record a share arriving from the downstream miner, weighted by difficulty.
    /// This tracks true hashrate instead of raw share count.
    pub fn record_share_arrived(&mut self, now: Instant, difficulty: f64) {
        self.supply_window.record_weighted(now, difficulty);
        self.current_supply_spm = self.supply_window.rate_spm(now);
        self.miner_difficulty = difficulty;
    }

    pub fn should_forward(&mut self) -> bool {
        let now = Instant::now();
        self.refill(now);

        if self.bucket >= 1.0 {
            self.bucket -= 1.0;
            true
        } else {
            false
        }
    }

    pub fn current_target_spm(&self) -> f64 {
        let elapsed = self.started_at.elapsed().as_secs_f64();
        let (value, is_relative) = self.profile.rate_at(elapsed);

        if is_relative {
            if self.current_supply_spm == 0.0 {
                // Bootstrap: show 0 in API until we have measurements
                0.0
            } else {
                value * self.current_supply_spm
            }
        } else {
            value
        }
    }

    pub fn current_supply_spm(&self) -> f64 {
        self.current_supply_spm
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn current_profile(&self) -> &RateProfile {
        &self.profile
    }

    pub fn set_profile(&mut self, profile: RateProfile) {
        let (initial_value, is_relative) = profile.rate_at(0.0);
        let initial_rate_effective = if is_relative {
            if self.current_supply_spm == 0.0 || self.miner_difficulty == 0.0 {
                1000.0 // Bootstrap
            } else {
                let effective_supply = self.current_supply_spm / self.miner_difficulty;
                initial_value * effective_supply
            }
        } else {
            initial_value
        };

        self.profile = profile;
        self.started_at = Instant::now();
        self.last_refill = Instant::now();
        self.capacity = Self::compute_capacity(initial_rate_effective);
        self.bucket = self.bucket.min(self.capacity);
    }

    fn refill(&mut self, now: Instant) {
        let dt = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;

        let elapsed = now.duration_since(self.started_at).as_secs_f64();
        let (value, is_relative) = self.profile.rate_at(elapsed);

        // Compute target in effective shares/min (not difficulty-weighted).
        // The token bucket operates on raw share counts: each should_forward() consumes 1.0 token.
        let target_spm_effective = if is_relative {
            // For relative profiles:
            // - current_supply_spm is difficulty-weighted (e.g., 6.47e16)
            // - Convert to effective supply: supply_weighted / difficulty
            // - Apply factor: target = factor × effective_supply
            if self.current_supply_spm == 0.0 || self.miner_difficulty == 0.0 {
                // Bootstrap: forward everything until we have measurements
                1000.0
            } else {
                let effective_supply_spm = self.current_supply_spm / self.miner_difficulty;
                value * effective_supply_spm
            }
        } else {
            // For absolute profiles, value is already in effective shares/min
            value
        };

        self.capacity = Self::compute_capacity(target_spm_effective);
        let tokens_earned = (target_spm_effective / 60.0) * dt;
        self.bucket = (self.bucket + tokens_earned).min(self.capacity);
    }

    fn compute_capacity(target_spm: f64) -> f64 {
        // 12-second window worth of tokens, minimum 2
        (target_spm * 12.0 / 60.0).max(2.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn hold_profile_forwards_at_rate() {
        let mut gate = ShareGate::new(RateProfile::Hold { rate: 60.0 });
        // At 60 spm = 1/sec, bucket starts with capacity ~12 tokens
        // Should forward several immediately (draining pre-filled bucket)
        let mut forwarded = 0;
        for _ in 0..15 {
            if gate.should_forward() {
                forwarded += 1;
            }
        }
        // Bucket capacity is max(2, 60*12/60) = 12, starts full
        assert!(forwarded >= 10 && forwarded <= 12);
    }

    #[test]
    fn zero_rate_blocks_all() {
        let mut gate = ShareGate::new(RateProfile::Hold { rate: 0.0 });
        // Capacity is 2 (minimum), but refill rate is 0
        // Bucket starts at min(initial_bucket, capacity) — bucket is initialized to capacity
        // Actually: capacity = max(2, 0*12/60) = 2, bucket starts at 2
        // First 2 forwards drain it, then blocks
        let mut forwarded = 0;
        for _ in 0..10 {
            if gate.should_forward() {
                forwarded += 1;
            }
        }
        assert_eq!(forwarded, 2); // initial capacity drained
    }

    #[test]
    fn step_profile_changes_rate() {
        let mut gate = ShareGate::new(RateProfile::Step {
            before: 60.0,
            after: 0.0,
            at_secs: 0.0, // immediate step to 0
            relative: false,
        });
        // After the step, rate is 0. Capacity drops to 2 (minimum).
        // Bucket was at old capacity (12), clamps to new capacity (2).
        // After draining those 2, no more refill.
        let mut forwarded = 0;
        for _ in 0..10 {
            if gate.should_forward() {
                forwarded += 1;
            }
        }
        assert_eq!(forwarded, 2);
    }

    #[test]
    fn track_profile_adapts_to_supply() {
        let mut gate = ShareGate::new(RateProfile::Track { factor: 0.5 });
        let now = Instant::now();

        // Simulate 60 spm supply (1 share/sec at difficulty 1.0)
        for i in 0..60 {
            gate.record_share_arrived(now + Duration::from_secs(i), 1.0);
        }

        // After recording supply, target should be ~30 spm (50% of 60)
        let target = gate.current_target_spm();
        assert!(target > 25.0 && target < 35.0, "Expected ~30 spm, got {}", target);
    }
}
