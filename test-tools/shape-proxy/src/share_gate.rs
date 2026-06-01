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
    miner_difficulty: f64,
}

impl ShareGate {
    pub fn new(profile: RateProfile) -> Self {
        Self {
            profile,
            started_at: Instant::now(),
            bucket: 3.0,
            capacity: 3.0,
            last_refill: Instant::now(),
            supply_window: RollingWindow::new(),
            current_supply_spm: 0.0,
            miner_difficulty: 1.0,
        }
    }

    pub fn record_share_arrived(&mut self, now: Instant, difficulty: f64) {
        self.supply_window.record_weighted(now, difficulty);
        self.current_supply_spm = self.supply_window.rate_spm(now);
        self.miner_difficulty = difficulty;
    }

    pub fn should_forward(&mut self) -> bool {
        let now = Instant::now();
        self.current_supply_spm = self.supply_window.rate_spm(now);

        // Bootstrap: forward everything until we have supply measurements.
        if self.effective_supply_spm() == 0.0 {
            return true;
        }

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
        let factor = self.profile.factor_at(elapsed);
        let effective_supply = self.effective_supply_spm();
        if effective_supply == 0.0 {
            0.0
        } else {
            factor * effective_supply
        }
    }

    pub fn current_supply_spm(&self) -> f64 {
        self.effective_supply_spm()
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn current_profile(&self) -> &RateProfile {
        &self.profile
    }

    pub fn set_profile(&mut self, profile: RateProfile) {
        let factor = profile.factor_at(0.0);
        let effective_supply = self.effective_supply_spm();
        let initial_target = factor * effective_supply;

        self.profile = profile;
        self.started_at = Instant::now();
        self.last_refill = Instant::now();
        self.capacity = Self::compute_capacity(initial_target);
        self.bucket = self.bucket.min(self.capacity);
    }

    fn refill(&mut self, now: Instant) {
        let dt = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;

        let elapsed = now.duration_since(self.started_at).as_secs_f64();
        let factor = self.profile.factor_at(elapsed);
        let target_spm = factor * self.effective_supply_spm();

        self.capacity = Self::compute_capacity(target_spm);
        let tokens_earned = (target_spm / 60.0) * dt;
        self.bucket = (self.bucket + tokens_earned).min(self.capacity);
    }

    fn effective_supply_spm(&self) -> f64 {
        if self.miner_difficulty == 0.0 {
            return 0.0;
        }
        self.current_supply_spm / self.miner_difficulty
    }

    fn compute_capacity(target_spm: f64) -> f64 {
        (target_spm * 10.0 / 60.0).max(3.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn track_forwards_all_at_factor_1() {
        let mut gate = ShareGate::new(RateProfile::Track { factor: 1.0 });
        let now = Instant::now();

        // Simulate 10 spm supply, then check forwarding
        for i in 0..15 {
            gate.record_share_arrived(now + Duration::from_secs(i * 6), 1.0);
        }

        // At factor=1.0 with supply established, should forward
        assert!(gate.should_forward());
    }

    #[test]
    fn stall_profile_reports_zero_target() {
        let mut gate = ShareGate::new(RateProfile::Track { factor: 1.0 });
        let now = Instant::now();

        // Establish supply (10 spm at difficulty 1.0)
        for i in 0..15 {
            gate.record_share_arrived(now + Duration::from_secs(i * 6), 1.0);
        }

        // Verify supply is measured
        assert!(gate.current_supply_spm() > 5.0);

        // Switch to stall (immediately active)
        gate.set_profile(RateProfile::Stall {
            at_secs: 0.0,
            duration_secs: 999.0,
        });

        // Target should be 0 during stall (factor=0 * supply)
        assert_eq!(gate.current_target_spm(), 0.0);
    }

    #[test]
    fn track_profile_adapts_to_supply() {
        let mut gate = ShareGate::new(RateProfile::Track { factor: 0.5 });
        let now = Instant::now();

        // Simulate 10 spm supply (1 share/6s at difficulty 1.0)
        for i in 0..15 {
            gate.record_share_arrived(now + Duration::from_secs(i * 6), 1.0);
        }

        let target = gate.current_target_spm();
        // Supply ~10.7 spm, target = 0.5 * supply ~5.4
        assert!(
            target > 4.0 && target < 7.0,
            "Expected ~5 spm, got {}",
            target
        );
    }
}
