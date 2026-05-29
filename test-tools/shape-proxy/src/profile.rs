use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RateProfile {
    /// Fixed absolute rate (backward compatibility)
    Hold {
        rate: f64,
    },
    /// Track supply with optional smoothing factor
    Track {
        #[serde(default = "default_factor")]
        factor: f64,
    },
    /// Step from one factor to another
    Step {
        before: f64,
        after: f64,
        at_secs: f64,
        #[serde(default)]
        relative: bool,
    },
    /// Ramp between factors
    Ramp {
        from: f64,
        to: f64,
        duration_secs: f64,
        #[serde(default)]
        relative: bool,
    },
    /// Stall: go to zero, then resume
    Stall {
        rate: f64,
        at_secs: f64,
        duration_secs: f64,
        #[serde(default)]
        relative: bool,
    },
    /// Burst: spike to higher rate/factor
    Burst {
        base: f64,
        peak: f64,
        at_secs: f64,
        duration_secs: f64,
        #[serde(default)]
        relative: bool,
    },
    /// Oscillate around a baseline
    Oscillate {
        base: f64,
        amp: f64,
        period_secs: f64,
        #[serde(default)]
        relative: bool,
    },
}

fn default_factor() -> f64 {
    1.0
}

impl RateProfile {
    /// Returns (rate_or_factor, is_relative) at the given elapsed time.
    /// If is_relative=true, caller should multiply by current supply.
    pub fn rate_at(&self, elapsed_secs: f64) -> (f64, bool) {
        match self {
            Self::Hold { rate } => (*rate, false),
            Self::Track { factor } => (*factor, true),
            Self::Step {
                before,
                after,
                at_secs,
                relative,
            } => {
                let value = if elapsed_secs < *at_secs {
                    *before
                } else {
                    *after
                };
                (value, *relative)
            }
            Self::Ramp {
                from,
                to,
                duration_secs,
                relative,
            } => {
                let value = if elapsed_secs <= 0.0 {
                    *from
                } else if elapsed_secs >= *duration_secs {
                    *to
                } else {
                    from + (elapsed_secs / duration_secs) * (to - from)
                };
                (value, *relative)
            }
            Self::Stall {
                rate,
                at_secs,
                duration_secs,
                relative,
            } => {
                let value = if elapsed_secs < *at_secs {
                    *rate
                } else if elapsed_secs < at_secs + duration_secs {
                    0.0
                } else {
                    *rate
                };
                (value, *relative)
            }
            Self::Burst {
                base,
                peak,
                at_secs,
                duration_secs,
                relative,
            } => {
                let value = if elapsed_secs < *at_secs {
                    *base
                } else if elapsed_secs < at_secs + duration_secs {
                    *peak
                } else {
                    *base
                };
                (value, *relative)
            }
            Self::Oscillate {
                base,
                amp,
                period_secs,
                relative,
            } => {
                let val = base + amp * (2.0 * std::f64::consts::PI * elapsed_secs / period_secs).sin();
                (val.max(0.0), *relative)
            }
        }
    }

    /// Check if this profile is supply-relative
    pub fn is_relative(&self) -> bool {
        match self {
            Self::Hold { .. } => false,
            Self::Track { .. } => true,
            Self::Step { relative, .. }
            | Self::Ramp { relative, .. }
            | Self::Stall { relative, .. }
            | Self::Burst { relative, .. }
            | Self::Oscillate { relative, .. } => *relative,
        }
    }
}

impl RateProfile {
    /// How long the profile's active/transitioning phase lasts (seconds).
    /// After this, the profile holds at its terminal rate.
    /// Returns None for profiles that never settle (oscillate, hold, track).
    pub fn active_duration_secs(&self) -> Option<f64> {
        match self {
            Self::Hold { .. } => None,
            Self::Track { .. } => None,
            Self::Step { at_secs, .. } => Some(*at_secs),
            Self::Ramp { duration_secs, .. } => Some(*duration_secs),
            Self::Stall { at_secs, duration_secs, .. } => Some(at_secs + duration_secs),
            Self::Burst { at_secs, duration_secs, .. } => Some(at_secs + duration_secs),
            Self::Oscillate { .. } => None,
        }
    }
}

impl Default for RateProfile {
    fn default() -> Self {
        Self::Track { factor: 1.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_constant() {
        let p = RateProfile::Hold { rate: 10.0 };
        assert_eq!(p.rate_at(0.0), (10.0, false));
        assert_eq!(p.rate_at(100.0), (10.0, false));
    }

    #[test]
    fn track_relative() {
        let p = RateProfile::Track { factor: 0.5 };
        assert_eq!(p.rate_at(0.0), (0.5, true));
        assert_eq!(p.rate_at(100.0), (0.5, true));
        assert!(p.is_relative());
    }

    #[test]
    fn step_transitions_absolute() {
        let p = RateProfile::Step { before: 15.0, after: 5.0, at_secs: 10.0, relative: false };
        assert_eq!(p.rate_at(0.0), (15.0, false));
        assert_eq!(p.rate_at(9.9), (15.0, false));
        assert_eq!(p.rate_at(10.0), (5.0, false));
        assert_eq!(p.rate_at(100.0), (5.0, false));
    }

    #[test]
    fn step_transitions_relative() {
        let p = RateProfile::Step { before: 1.0, after: 0.5, at_secs: 10.0, relative: true };
        assert_eq!(p.rate_at(0.0), (1.0, true));
        assert_eq!(p.rate_at(10.0), (0.5, true));
        assert!(p.is_relative());
    }

    #[test]
    fn ramp_linear() {
        let p = RateProfile::Ramp { from: 0.0, to: 30.0, duration_secs: 60.0, relative: false };
        assert_eq!(p.rate_at(0.0), (0.0, false));
        assert_eq!(p.rate_at(30.0), (15.0, false));
        assert_eq!(p.rate_at(60.0), (30.0, false));
        assert_eq!(p.rate_at(90.0), (30.0, false)); // clamps at end
    }

    #[test]
    fn stall_drops_to_zero() {
        let p = RateProfile::Stall { rate: 12.0, at_secs: 5.0, duration_secs: 10.0, relative: false };
        assert_eq!(p.rate_at(0.0), (12.0, false));
        assert_eq!(p.rate_at(4.9), (12.0, false));
        assert_eq!(p.rate_at(5.0), (0.0, false));
        assert_eq!(p.rate_at(14.9), (0.0, false));
        assert_eq!(p.rate_at(15.0), (12.0, false));
    }

    #[test]
    fn burst_spikes() {
        let p = RateProfile::Burst { base: 10.0, peak: 25.0, at_secs: 5.0, duration_secs: 3.0, relative: false };
        assert_eq!(p.rate_at(0.0), (10.0, false));
        assert_eq!(p.rate_at(5.0), (25.0, false));
        assert_eq!(p.rate_at(7.9), (25.0, false));
        assert_eq!(p.rate_at(8.0), (10.0, false));
    }

    #[test]
    fn oscillate_sinusoidal() {
        let p = RateProfile::Oscillate { base: 15.0, amp: 5.0, period_secs: 10.0, relative: false };
        assert_eq!(p.rate_at(0.0), (15.0, false)); // sin(0) = 0
        let (quarter, _) = p.rate_at(2.5); // sin(π/2) = 1 → 15 + 5 = 20
        assert!((quarter - 20.0).abs() < 0.01);
        let (three_quarter, _) = p.rate_at(7.5); // sin(3π/2) = -1 → 15 - 5 = 10
        assert!((three_quarter - 10.0).abs() < 0.01);
    }

    #[test]
    fn oscillate_never_negative() {
        let p = RateProfile::Oscillate { base: 3.0, amp: 10.0, period_secs: 10.0, relative: false };
        // base - amp would be -7, but clamped to 0
        let (bottom, _) = p.rate_at(7.5); // sin(3π/2) = -1 → 3 - 10 = -7 → clamped to 0
        assert_eq!(bottom, 0.0);
    }
}
