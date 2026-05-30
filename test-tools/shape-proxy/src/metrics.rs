use std::collections::VecDeque;
use std::time::Instant;

const WINDOW_SECS: f64 = 15.0;

/// Rolling window that tracks weighted events (timestamp + weight) and computes rate.
/// Used for difficulty-weighted share tracking (hashrate measurement).
pub struct RollingWindow {
    events: VecDeque<(Instant, f64)>,
}

impl RollingWindow {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
        }
    }

    /// Record an event with a weight (e.g., difficulty for hashrate tracking).
    /// For raw share counting, use weight = 1.0.
    pub fn record(&mut self, now: Instant) {
        self.record_weighted(now, 1.0);
    }

    pub fn record_weighted(&mut self, now: Instant, weight: f64) {
        self.events.push_back((now, weight));
        self.expire(now);
    }

    /// Compute rate as weighted-events-per-minute.
    /// For difficulty-weighted shares, this returns hashrate in (difficulty-units/min).
    /// For raw shares (weight=1.0), this returns shares/min.
    pub fn rate_spm(&self, now: Instant) -> f64 {
        let cutoff = now - std::time::Duration::from_secs_f64(WINDOW_SECS);
        let sum: f64 = self.events.iter()
            .filter(|(t, _)| *t >= cutoff)
            .map(|(_, w)| w)
            .sum();
        (sum / WINDOW_SECS) * 60.0
    }

    fn expire(&mut self, now: Instant) {
        let cutoff = now - std::time::Duration::from_secs_f64(WINDOW_SECS);
        while let Some(&(front_time, _)) = self.events.front() {
            if front_time < cutoff {
                self.events.pop_front();
            } else {
                break;
            }
        }
    }
}

/// Headroom status derived from supply/target ratio.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HeadroomStatus {
    /// supply >= 2× target: output tracks profile cleanly
    Comfortable,
    /// 1× <= supply < 2× target: elevated variance
    Marginal,
    /// supply < target: gate is supply-limited, test invalid
    Clamped,
    /// No data yet (no shares received)
    Unknown,
}

impl HeadroomStatus {
    pub fn from_ratio(supply_spm: f64, target_spm: f64) -> Self {
        if target_spm <= 0.0 || supply_spm <= 0.0 {
            return Self::Unknown;
        }
        let ratio = supply_spm / target_spm;
        if ratio >= 2.0 {
            Self::Comfortable
        } else if ratio >= 1.0 {
            Self::Marginal
        } else {
            Self::Clamped
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Comfortable => "comfortable",
            Self::Marginal => "marginal",
            Self::Clamped => "clamped",
            Self::Unknown => "unknown",
        }
    }
}
