use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use crate::profile::RateProfile;

/// Commands sent from the HTTP API to the ProxyCore select loop.
#[derive(Debug)]
pub enum ApiCommand {
    SetProfile {
        channel_id: u32,
        profile: RateProfile,
    },
    SetAllProfiles {
        profile: RateProfile,
    },
}

/// Snapshot of proxy state, published by ProxyCore for the API to read.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ProxyStatus {
    pub upstream_connected: bool,
    pub channels: Vec<ChannelStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelStatus {
    pub id: u32,
    pub miner_connected: bool,
    pub profile: ProfileInfo,
    pub profile_elapsed_secs: f64,
    pub profile_duration_secs: Option<f64>,
    pub target_spm: f64,
    pub forwarded_spm: f64,
    pub supply_spm: f64,
    pub headroom: String,
    pub floor_active: bool,
    pub pool_difficulty: Option<f64>,
    pub shares_forwarded: u64,
    pub shares_gated: u64,
    pub shares_rejected_difficulty: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileInfo {
    #[serde(rename = "type")]
    pub profile_type: String,
    pub description: String,
}

impl ProfileInfo {
    pub fn from_profile(p: &RateProfile) -> Self {
        let relative_suffix = if p.is_relative() { " (relative)" } else { "" };

        match p {
            RateProfile::Hold { rate } => ProfileInfo {
                profile_type: "hold".into(),
                description: format!("{:.1} spm{}", rate, relative_suffix),
            },
            RateProfile::Track { factor } => ProfileInfo {
                profile_type: "track".into(),
                description: format!("{:.1}× supply", factor),
            },
            RateProfile::Step { before, after, at_secs, relative } => ProfileInfo {
                profile_type: "step".into(),
                description: if *relative {
                    format!("{:.1}× → {:.1}× @ {:.0}s", before, after, at_secs)
                } else {
                    format!("{:.1} → {:.1} spm @ {:.0}s", before, after, at_secs)
                },
            },
            RateProfile::Ramp { from, to, duration_secs, relative } => ProfileInfo {
                profile_type: "ramp".into(),
                description: if *relative {
                    format!("{:.1}× → {:.1}× over {:.0}s", from, to, duration_secs)
                } else {
                    format!("{:.1} → {:.1} spm over {:.0}s", from, to, duration_secs)
                },
            },
            RateProfile::Stall { rate, at_secs, duration_secs, relative } => ProfileInfo {
                profile_type: "stall".into(),
                description: if *relative {
                    format!("{:.1}×, zero @ {:.0}s for {:.0}s", rate, at_secs, duration_secs)
                } else {
                    format!("{:.1} spm, zero @ {:.0}s for {:.0}s", rate, at_secs, duration_secs)
                },
            },
            RateProfile::Burst { base, peak, at_secs, duration_secs, relative } => ProfileInfo {
                profile_type: "burst".into(),
                description: if *relative {
                    format!("{:.1}× → {:.1}× @ {:.0}s for {:.0}s", base, peak, at_secs, duration_secs)
                } else {
                    format!("{:.1} → {:.1} spm @ {:.0}s for {:.0}s", base, peak, at_secs, duration_secs)
                },
            },
            RateProfile::Oscillate { base, amp, period_secs, relative } => ProfileInfo {
                profile_type: "oscillate".into(),
                description: if *relative {
                    format!("{:.1}±{:.1}×, period {:.0}s", base, amp, period_secs)
                } else {
                    format!("{:.1}±{:.1} spm, period {:.0}s", base, amp, period_secs)
                },
            },
        }
    }
}

struct AppState {
    status_rx: watch::Receiver<ProxyStatus>,
    cmd_tx: mpsc::UnboundedSender<ApiCommand>,
}

pub fn create_router(
    status_rx: watch::Receiver<ProxyStatus>,
    cmd_tx: mpsc::UnboundedSender<ApiCommand>,
) -> Router {
    let state = Arc::new(AppState { status_rx, cmd_tx });

    Router::new()
        .route("/status", get(get_status))
        .route("/channels/{id}/profile", post(set_channel_profile))
        .route("/profile", post(set_all_profiles))
        .with_state(state)
}

async fn get_status(State(state): State<Arc<AppState>>) -> Json<ProxyStatus> {
    Json(state.status_rx.borrow().clone())
}

#[derive(Deserialize)]
struct ProfileRequest {
    #[serde(flatten)]
    profile: RateProfile,
}

async fn set_channel_profile(
    State(state): State<Arc<AppState>>,
    Path(id): Path<u32>,
    Json(req): Json<ProfileRequest>,
) -> StatusCode {
    let cmd = ApiCommand::SetProfile {
        channel_id: id,
        profile: req.profile,
    };
    match state.cmd_tx.send(cmd) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn set_all_profiles(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ProfileRequest>,
) -> StatusCode {
    let cmd = ApiCommand::SetAllProfiles {
        profile: req.profile,
    };
    match state.cmd_tx.send(cmd) {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}
