use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RateProfile {
    Hold {
        rate: f64,
    },
    Step {
        before: f64,
        after: f64,
        at_secs: f64,
    },
    Ramp {
        from: f64,
        to: f64,
        duration_secs: f64,
    },
    Stall {
        rate: f64,
        at_secs: f64,
        duration_secs: f64,
    },
    Burst {
        base: f64,
        peak: f64,
        at_secs: f64,
        duration_secs: f64,
    },
    Oscillate {
        base: f64,
        amp: f64,
        period_secs: f64,
    },
}

impl RateProfile {
    pub fn rate_at(&self, elapsed_secs: f64) -> f64 {
        match self {
            Self::Hold { rate } => *rate,
            Self::Step {
                before,
                after,
                at_secs,
            } => {
                if elapsed_secs < *at_secs {
                    *before
                } else {
                    *after
                }
            }
            Self::Ramp {
                from,
                to,
                duration_secs,
            } => {
                if elapsed_secs <= 0.0 {
                    *from
                } else if elapsed_secs >= *duration_secs {
                    *to
                } else {
                    from + (elapsed_secs / duration_secs) * (to - from)
                }
            }
            Self::Stall {
                rate,
                at_secs,
                duration_secs,
            } => {
                if elapsed_secs < *at_secs {
                    *rate
                } else if elapsed_secs < at_secs + duration_secs {
                    0.0
                } else {
                    *rate
                }
            }
            Self::Burst {
                base,
                peak,
                at_secs,
                duration_secs,
            } => {
                if elapsed_secs < *at_secs {
                    *base
                } else if elapsed_secs < at_secs + duration_secs {
                    *peak
                } else {
                    *base
                }
            }
            Self::Oscillate {
                base,
                amp,
                period_secs,
            } => {
                let val = base + amp * (2.0 * std::f64::consts::PI * elapsed_secs / period_secs).sin();
                val.max(0.0)
            }
        }
    }
}

impl Default for RateProfile {
    fn default() -> Self {
        Self::Hold { rate: 15.0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hold_constant() {
        let p = RateProfile::Hold { rate: 10.0 };
        assert_eq!(p.rate_at(0.0), 10.0);
        assert_eq!(p.rate_at(100.0), 10.0);
    }

    #[test]
    fn step_transitions() {
        let p = RateProfile::Step { before: 15.0, after: 5.0, at_secs: 10.0 };
        assert_eq!(p.rate_at(0.0), 15.0);
        assert_eq!(p.rate_at(9.9), 15.0);
        assert_eq!(p.rate_at(10.0), 5.0);
        assert_eq!(p.rate_at(100.0), 5.0);
    }

    #[test]
    fn ramp_linear() {
        let p = RateProfile::Ramp { from: 0.0, to: 30.0, duration_secs: 60.0 };
        assert_eq!(p.rate_at(0.0), 0.0);
        assert_eq!(p.rate_at(30.0), 15.0);
        assert_eq!(p.rate_at(60.0), 30.0);
        assert_eq!(p.rate_at(90.0), 30.0); // clamps at end
    }

    #[test]
    fn stall_drops_to_zero() {
        let p = RateProfile::Stall { rate: 12.0, at_secs: 5.0, duration_secs: 10.0 };
        assert_eq!(p.rate_at(0.0), 12.0);
        assert_eq!(p.rate_at(4.9), 12.0);
        assert_eq!(p.rate_at(5.0), 0.0);
        assert_eq!(p.rate_at(14.9), 0.0);
        assert_eq!(p.rate_at(15.0), 12.0);
    }

    #[test]
    fn burst_spikes() {
        let p = RateProfile::Burst { base: 10.0, peak: 25.0, at_secs: 5.0, duration_secs: 3.0 };
        assert_eq!(p.rate_at(0.0), 10.0);
        assert_eq!(p.rate_at(5.0), 25.0);
        assert_eq!(p.rate_at(7.9), 25.0);
        assert_eq!(p.rate_at(8.0), 10.0);
    }

    #[test]
    fn oscillate_sinusoidal() {
        let p = RateProfile::Oscillate { base: 15.0, amp: 5.0, period_secs: 10.0 };
        assert_eq!(p.rate_at(0.0), 15.0); // sin(0) = 0
        let quarter = p.rate_at(2.5); // sin(π/2) = 1 → 15 + 5 = 20
        assert!((quarter - 20.0).abs() < 0.01);
        let three_quarter = p.rate_at(7.5); // sin(3π/2) = -1 → 15 - 5 = 10
        assert!((three_quarter - 10.0).abs() < 0.01);
    }

    #[test]
    fn oscillate_never_negative() {
        let p = RateProfile::Oscillate { base: 3.0, amp: 10.0, period_secs: 10.0 };
        // base - amp would be -7, but clamped to 0
        let bottom = p.rate_at(7.5); // sin(3π/2) = -1 → 3 - 10 = -7 → clamped to 0
        assert_eq!(bottom, 0.0);
    }
}
