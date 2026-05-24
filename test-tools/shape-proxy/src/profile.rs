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
        }
    }
}

impl Default for RateProfile {
    fn default() -> Self {
        Self::Hold { rate: 15.0 }
    }
}
