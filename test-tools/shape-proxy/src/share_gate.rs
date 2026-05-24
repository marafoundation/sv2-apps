use std::time::Instant;

use crate::profile::RateProfile;

pub struct ShareGate {
    profile: RateProfile,
    started_at: Instant,
    bucket: f64,
    capacity: f64,
    last_refill: Instant,
}

impl ShareGate {
    pub fn new(profile: RateProfile) -> Self {
        let capacity = Self::compute_capacity(profile.rate_at(0.0));
        Self {
            profile,
            started_at: Instant::now(),
            bucket: capacity,
            capacity,
            last_refill: Instant::now(),
        }
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
        self.profile.rate_at(elapsed)
    }

    pub fn set_profile(&mut self, profile: RateProfile) {
        let initial_rate = profile.rate_at(0.0);
        self.profile = profile;
        self.started_at = Instant::now();
        self.last_refill = Instant::now();
        self.capacity = Self::compute_capacity(initial_rate);
        self.bucket = self.bucket.min(self.capacity);
    }

    fn refill(&mut self, now: Instant) {
        let dt = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;

        let elapsed = now.duration_since(self.started_at).as_secs_f64();
        let target_spm = self.profile.rate_at(elapsed);

        self.capacity = Self::compute_capacity(target_spm);
        let tokens_earned = (target_spm / 60.0) * dt;
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
    use std::thread::sleep;
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
}
