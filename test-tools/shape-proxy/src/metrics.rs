use std::collections::VecDeque;
use std::time::Instant;

const WINDOW_SECS: f64 = 15.0;

/// Rolling window that tracks event timestamps and computes rate (events/min).
pub struct RollingWindow {
    timestamps: VecDeque<Instant>,
}

impl RollingWindow {
    pub fn new() -> Self {
        Self {
            timestamps: VecDeque::new(),
        }
    }

    pub fn record(&mut self, now: Instant) {
        self.timestamps.push_back(now);
        self.expire(now);
    }

    pub fn rate_spm(&self, now: Instant) -> f64 {
        let cutoff = now - std::time::Duration::from_secs_f64(WINDOW_SECS);
        let count = self.timestamps.iter().filter(|&&t| t >= cutoff).count();
        (count as f64 / WINDOW_SECS) * 60.0
    }

    fn expire(&mut self, now: Instant) {
        let cutoff = now - std::time::Duration::from_secs_f64(WINDOW_SECS);
        while let Some(&front) = self.timestamps.front() {
            if front < cutoff {
                self.timestamps.pop_front();
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
