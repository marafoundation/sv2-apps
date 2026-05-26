use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use stratum_apps::{
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
    stratum_core::{
        codec_sv2::StandardSv2Frame,
        mining_sv2::{OpenExtendedMiningChannel, SubmitSharesSuccess},
        parsers_sv2::Mining,
    },
};
use tokio::{net::TcpListener, select, sync::mpsc};
use tracing::{debug, error, info, warn};

use crate::{
    api::{self, ApiCommand, ChannelStatus, ProfileInfo, ProxyStatus},
    config::Config,
    downstream::{accept_downstream, DownstreamEvent, DownstreamId},
    metrics::{HeadroomStatus, RollingWindow},
    profile::RateProfile,
    share_gate::ShareGate,
    upstream::{self, Message, UpstreamEvent, Writer},
};

/// Maps a downstream channel to its upstream counterpart.
struct ChannelMapping {
    /// The channel ID we assigned to the downstream miner.
    downstream_channel_id: u32,
    /// The channel ID the pool assigned upstream (populated after success response).
    upstream_channel_id: Option<u32>,
    /// Which downstream connection owns this channel.
    downstream_id: DownstreamId,
    /// Share gate (token bucket driven by rate profile).
    gate: ShareGate,
    /// Whether the difficulty floor is currently overriding the pool's target.
    floor_active: bool,
    /// Shares forwarded upstream.
    shares_forwarded: u64,
    /// Shares dropped by the gate.
    shares_gated: u64,
    /// Rolling window: all shares arriving from downstream (supply).
    supply_window: RollingWindow,
    /// Rolling window: shares forwarded upstream.
    forward_window: RollingWindow,
    /// The original OpenExtendedMiningChannel request for re-opening on reconnect.
    open_request: Option<OpenExtendedMiningChannel<'static>>,
    /// Whether this is a standard channel (vs extended).
    #[allow(dead_code)]
    is_standard: bool,
    /// The original standard channel open message bytes for re-opening on reconnect.
    open_standard_msg: Option<Mining<'static>>,
}

/// Tracks a pending channel open request we forwarded upstream.
#[derive(Debug)]
#[allow(dead_code)]
struct PendingChannelOpen {
    /// The request_id the downstream miner used.
    original_request_id: u32,
    /// The downstream_channel_id we pre-assigned.
    downstream_channel_id: u32,
    /// Which downstream connection this belongs to.
    downstream_id: DownstreamId,
}

/// State for a connected downstream.
struct DownstreamState {
    writer: Writer,
}

/// The core proxy event loop.
pub struct ProxyCore {
    config: Config,
    upstream_writer: Option<Writer>,
    upstream_connected: bool,
    /// Authority keys for accepting downstream connections.
    pub_key: Secp256k1PublicKey,
    secret_key: Secp256k1SecretKey,
    /// Next downstream ID to assign.
    next_downstream_id: AtomicU64,
    /// Next downstream channel ID to assign (starts at 1).
    next_channel_id: u32,
    /// Connected downstreams keyed by their ID.
    downstreams: HashMap<DownstreamId, DownstreamState>,
    /// Channel mappings keyed by downstream_channel_id.
    channels: HashMap<u32, ChannelMapping>,
    /// Pending channel open requests keyed by the request_id we sent upstream.
    /// (We reuse the downstream's request_id when forwarding since we only have one upstream.)
    pending_opens: HashMap<u32, PendingChannelOpen>,
}

impl ProxyCore {
    pub fn new(config: Config) -> Result<Self, String> {
        let pub_key: Secp256k1PublicKey = config
            .authority_pubkey
            .parse()
            .map_err(|e| format!("Invalid authority_pubkey: {e}"))?;
        let secret_key: Secp256k1SecretKey = config
            .authority_secret
            .parse()
            .map_err(|e| format!("Invalid authority_secret: {e}"))?;

        Ok(Self {
            config,
            upstream_writer: None,
            upstream_connected: false,
            pub_key,
            secret_key,
            next_downstream_id: AtomicU64::new(1),
            next_channel_id: 1,
            downstreams: HashMap::new(),
            channels: HashMap::new(),
            pending_opens: HashMap::new(),
        })
    }

    /// Run the main proxy event loop.
    pub async fn run(mut self) -> Result<(), String> {
        let listener = TcpListener::bind(self.config.downstream_listen)
            .await
            .map_err(|e| format!("Failed to bind downstream listener: {e}"))?;

        info!("Downstream listener bound to {}", self.config.downstream_listen);

        let (ds_event_tx, mut ds_event_rx) = mpsc::unbounded_channel::<DownstreamEvent>();

        // Upstream manager channel
        let (upstream_tx, mut upstream_rx) = mpsc::unbounded_channel::<UpstreamEvent>();

        // Spawn the upstream manager task
        let upstream_config = self.config.clone();
        tokio::spawn(async move {
            upstream::upstream_manager_task(upstream_config, upstream_tx).await;
        });

        // API channels
        let (api_cmd_tx, mut api_cmd_rx) = mpsc::unbounded_channel::<ApiCommand>();
        let (status_tx, status_rx) = tokio::sync::watch::channel(ProxyStatus::default());

        // Spawn HTTP API server
        let api_listen = self.config.api_listen;
        let router = api::create_router(status_rx, api_cmd_tx);
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(api_listen)
                .await
                .expect("Failed to bind API listener");
            info!("HTTP API listening on {}", api_listen);
            axum::serve(listener, router).await.ok();
        });

        loop {
            // Publish status snapshot for the API
            let _ = status_tx.send(self.build_status());

            select! {
                // Accept new downstream connections.
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            let id = self.next_downstream_id.fetch_add(1, Ordering::Relaxed);
                            let pub_key = self.pub_key;
                            let secret_key = self.secret_key;
                            let cert_validity = self.config.cert_validity_secs;
                            let tx = ds_event_tx.clone();
                            tokio::spawn(async move {
                                accept_downstream(
                                    stream, peer_addr, id,
                                    pub_key, secret_key, cert_validity, tx,
                                ).await;
                            });
                        }
                        Err(e) => {
                            error!("Listener accept error: {e}");
                        }
                    }
                }

                // Handle downstream events.
                Some(event) = ds_event_rx.recv() => {
                    self.handle_downstream_event(event).await;
                }

                // Handle upstream events from the manager task.
                Some(event) = upstream_rx.recv() => {
                    match event {
                        UpstreamEvent::Connected { writer } => {
                            info!("Upstream connected");
                            self.upstream_writer = Some(writer);
                            self.upstream_connected = true;
                            // Re-open channels for miners still connected downstream.
                            self.reopen_channels().await;
                        }
                        UpstreamEvent::Frame { frame } => {
                            self.handle_upstream_frame(frame).await;
                        }
                        UpstreamEvent::Disconnected => {
                            warn!("Upstream disconnected, will retry in background");
                            self.upstream_writer = None;
                            self.upstream_connected = false;
                            // Clear pending opens since those will never get responses now.
                            self.pending_opens.clear();
                            // Clear upstream channel IDs since they're no longer valid.
                            for mapping in self.channels.values_mut() {
                                mapping.upstream_channel_id = None;
                            }
                        }
                    }
                }

                // Handle API commands.
                Some(cmd) = api_cmd_rx.recv() => {
                    self.handle_api_command(cmd);
                }
            }
        }
    }

    /// Re-open channels for all downstream miners that are still connected.
    /// Called after upstream reconnects.
    async fn reopen_channels(&mut self) {
        // Collect the channel open requests we need to re-send.
        let mut reopens: Vec<(u32, u32, DownstreamId, Option<OpenExtendedMiningChannel<'static>>, Option<Mining<'static>>)> = Vec::new();

        for mapping in self.channels.values() {
            // Only re-open if the downstream miner is still connected.
            if !self.downstreams.contains_key(&mapping.downstream_id) {
                continue;
            }
            reopens.push((
                mapping.downstream_channel_id,
                mapping.downstream_channel_id, // use as request_id for re-open
                mapping.downstream_id,
                mapping.open_request.clone(),
                mapping.open_standard_msg.clone(),
            ));
        }

        for (ds_channel_id, _request_id, downstream_id, open_req, open_std_msg) in reopens {
            if let Some(open_req) = open_req {
                info!(
                    downstream_id,
                    ds_channel_id,
                    "Re-opening extended channel after upstream reconnect"
                );
                // Store as pending open
                self.pending_opens.insert(
                    open_req.request_id,
                    PendingChannelOpen {
                        original_request_id: open_req.request_id,
                        downstream_channel_id: ds_channel_id,
                        downstream_id,
                    },
                );
                // Forward upstream
                let frame: StandardSv2Frame<Message> =
                    match Message::Mining(Mining::OpenExtendedMiningChannel(open_req)).try_into() {
                        Ok(f) => f,
                        Err(e) => {
                            error!("Failed to encode re-open channel: {e:?}");
                            continue;
                        }
                    };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to send re-open channel upstream: {e:?}");
                    }
                }
            } else if let Some(std_msg) = open_std_msg {
                info!(
                    downstream_id,
                    ds_channel_id,
                    "Re-opening standard channel after upstream reconnect"
                );
                // Extract request_id from the standard channel open message
                if let Mining::OpenStandardMiningChannel(ref m) = std_msg {
                    let req_id = m.get_request_id_as_u32();
                    self.pending_opens.insert(
                        req_id,
                        PendingChannelOpen {
                            original_request_id: req_id,
                            downstream_channel_id: ds_channel_id,
                            downstream_id,
                        },
                    );
                }
                let frame: StandardSv2Frame<Message> = match Message::Mining(std_msg).try_into() {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Failed to encode re-open standard channel: {e:?}");
                        continue;
                    }
                };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to send re-open standard channel upstream: {e:?}");
                    }
                }
            }
        }
    }

    fn handle_api_command(&mut self, cmd: ApiCommand) {
        match cmd {
            ApiCommand::SetProfile { channel_id, profile } => {
                if let Some(mapping) = self.channels.get_mut(&channel_id) {
                    info!(channel_id, "Setting profile: {:?}", profile);
                    mapping.gate.set_profile(profile);
                } else {
                    warn!(channel_id, "SetProfile for unknown channel");
                }
            }
            ApiCommand::SetAllProfiles { profile } => {
                info!("Broadcasting profile to all channels: {:?}", profile);
                for mapping in self.channels.values_mut() {
                    mapping.gate.set_profile(profile.clone());
                }
            }
        }
    }

    fn build_status(&self) -> ProxyStatus {
        let now = std::time::Instant::now();
        let channels = self
            .channels
            .values()
            .map(|m| {
                let supply_spm = m.supply_window.rate_spm(now);
                let forwarded_spm = m.forward_window.rate_spm(now);
                let target_spm = m.gate.current_target_spm();
                let headroom = HeadroomStatus::from_ratio(supply_spm, target_spm);
                let profile = m.gate.current_profile();
                let profile_duration = profile.active_duration_secs();
                let profile_elapsed = m.gate.elapsed_secs();
                ChannelStatus {
                    id: m.downstream_channel_id,
                    miner_connected: self.downstreams.contains_key(&m.downstream_id),
                    profile: ProfileInfo::from_profile(profile),
                    profile_elapsed_secs: profile_elapsed,
                    profile_duration_secs: profile_duration,
                    target_spm,
                    forwarded_spm,
                    supply_spm,
                    headroom: headroom.as_str().to_string(),
                    floor_active: m.floor_active,
                    shares_forwarded: m.shares_forwarded,
                    shares_gated: m.shares_gated,
                }
            })
            .collect();

        ProxyStatus {
            upstream_connected: self.upstream_connected,
            channels,
        }
    }

    /// Process an event from a downstream connection.
    async fn handle_downstream_event(&mut self, event: DownstreamEvent) {
        match event {
            DownstreamEvent::Connected { id, writer } => {
                info!(downstream_id = id, "Downstream connected and ready");
                self.downstreams.insert(id, DownstreamState { writer });
            }
            DownstreamEvent::Message { id, msg } => {
                self.handle_downstream_message(id, msg).await;
            }
            DownstreamEvent::Disconnected { id } => {
                info!(downstream_id = id, "Downstream disconnected");
                self.downstreams.remove(&id);
                // Remove any channel mappings for this downstream.
                self.channels.retain(|_, mapping| mapping.downstream_id != id);
                self.pending_opens.retain(|_, pending| pending.downstream_id != id);
            }
        }
    }

    /// Handle a mining message from a downstream miner.
    async fn handle_downstream_message(&mut self, id: DownstreamId, msg: Mining<'static>) {
        match msg {
            Mining::OpenExtendedMiningChannel(open_req) => {
                self.handle_open_channel(id, open_req).await;
            }
            Mining::OpenStandardMiningChannel(ref m) => {
                // Mirror standard channel opens upstream verbatim.
                let request_id = m.get_request_id_as_u32();
                let ds_channel_id = self.next_channel_id;
                self.next_channel_id += 1;
                info!(
                    downstream_id = id,
                    ds_channel_id,
                    request_id,
                    "Mirroring OpenStandardMiningChannel to upstream"
                );
                self.pending_opens.insert(
                    request_id,
                    PendingChannelOpen {
                        original_request_id: request_id,
                        downstream_channel_id: ds_channel_id,
                        downstream_id: id,
                    },
                );
                // Store the open message for reconnect re-opening.
                // We'll insert the channel mapping on success, but store the msg now
                // in a temporary spot. Actually, store it after we get the mapping.
                let open_msg_clone = msg.clone();
                let frame: StandardSv2Frame<Message> = match Message::Mining(msg).try_into() {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Failed to encode OpenStandardMiningChannel: {e:?}");
                        return;
                    }
                };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to send OpenStandardMiningChannel upstream: {e:?}");
                    }
                } else {
                    warn!(downstream_id = id, "Upstream not connected, cannot open standard channel");
                }
                // Store the open message temporarily so we can attach it to the mapping on success.
                // Use a workaround: store in the pending_opens won't work, so we store in channels
                // preemptively with upstream_channel_id = None.
                self.channels.insert(
                    ds_channel_id,
                    ChannelMapping {
                        downstream_channel_id: ds_channel_id,
                        upstream_channel_id: None,
                        downstream_id: id,
                        gate: ShareGate::new(RateProfile::default()),
                        floor_active: false,
                        shares_forwarded: 0,
                        shares_gated: 0,
                        supply_window: RollingWindow::new(),
                        forward_window: RollingWindow::new(),
                        open_request: None,
                        is_standard: true,
                        open_standard_msg: Some(open_msg_clone),
                    },
                );
            }
            Mining::SubmitSharesExtended(ref share) => {
                let channel_id = share.channel_id;
                let seq = share.sequence_number;

                // Always ack the miner immediately (invariant 1).
                let ack = SubmitSharesSuccess {
                    channel_id,
                    last_sequence_number: seq,
                    new_submits_accepted_count: 1,
                    new_shares_sum: 0,
                };
                self.send_to_downstream(
                    id,
                    Message::Mining(Mining::SubmitSharesSuccess(ack)),
                )
                .await;

                // Find the channel mapping and check the gate.
                let mapping = self
                    .channels
                    .values_mut()
                    .find(|m| {
                        m.downstream_id == id
                            && (m.downstream_channel_id == channel_id
                                || m.upstream_channel_id == Some(channel_id))
                    });

                let Some(mapping) = mapping else {
                    debug!(downstream_id = id, channel_id, "Share for unknown channel");
                    return;
                };

                let now = std::time::Instant::now();
                mapping.supply_window.record(now);

                if !mapping.gate.should_forward() {
                    mapping.shares_gated += 1;
                    return;
                }

                mapping.shares_forwarded += 1;
                mapping.forward_window.record(now);

                // Only forward if upstream is connected.
                if let Some(ref mut writer) = self.upstream_writer {
                    let frame: StandardSv2Frame<Message> =
                        match Message::Mining(msg).try_into() {
                            Ok(f) => f,
                            Err(e) => {
                                warn!(downstream_id = id, "Failed to encode share: {e:?}");
                                return;
                            }
                        };
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to forward share upstream: {e:?}");
                    }
                }
            }
            Mining::SubmitSharesStandard(ref share) => {
                let channel_id = share.channel_id;
                let seq = share.sequence_number;

                // Always ack immediately.
                let ack = SubmitSharesSuccess {
                    channel_id,
                    last_sequence_number: seq,
                    new_submits_accepted_count: 1,
                    new_shares_sum: 0,
                };
                self.send_to_downstream(
                    id,
                    Message::Mining(Mining::SubmitSharesSuccess(ack)),
                )
                .await;

                // Gate check.
                let mapping = self
                    .channels
                    .values_mut()
                    .find(|m| {
                        m.downstream_id == id
                            && (m.downstream_channel_id == channel_id
                                || m.upstream_channel_id == Some(channel_id))
                    });

                let Some(mapping) = mapping else {
                    debug!(downstream_id = id, channel_id, "Standard share for unknown channel");
                    return;
                };

                let now = std::time::Instant::now();
                mapping.supply_window.record(now);

                if !mapping.gate.should_forward() {
                    mapping.shares_gated += 1;
                    return;
                }

                mapping.shares_forwarded += 1;
                mapping.forward_window.record(now);

                // Only forward if upstream is connected.
                if let Some(ref mut writer) = self.upstream_writer {
                    let frame: StandardSv2Frame<Message> =
                        match Message::Mining(msg).try_into() {
                            Ok(f) => f,
                            Err(e) => {
                                warn!(downstream_id = id, "Failed to encode standard share: {e:?}");
                                return;
                            }
                        };
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to forward standard share upstream: {e:?}");
                    }
                }
            }
            other => {
                debug!(downstream_id = id, "Forwarding message upstream: {other}");
                let frame: StandardSv2Frame<Message> = match Message::Mining(other).try_into() {
                    Ok(f) => f,
                    Err(e) => {
                        warn!(downstream_id = id, "Failed to encode message: {e:?}");
                        return;
                    }
                };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to forward message upstream: {e:?}");
                    }
                } else {
                    warn!(downstream_id = id, "Upstream not connected, dropping message");
                }
            }
        }
    }

    /// Handle an OpenExtendedMiningChannel request from downstream.
    /// Assigns a local channel ID and mirrors the request to the pool.
    async fn handle_open_channel(
        &mut self,
        downstream_id: DownstreamId,
        open_req: OpenExtendedMiningChannel<'static>,
    ) {
        let ds_channel_id = self.next_channel_id;
        self.next_channel_id += 1;

        let request_id = open_req.request_id;
        info!(
            downstream_id,
            ds_channel_id,
            request_id,
            user_identity = %open_req.user_identity.as_utf8_or_hex(),
            "Mirroring OpenExtendedMiningChannel to upstream"
        );

        // Store the pending open so we can correlate the pool's response.
        self.pending_opens.insert(
            request_id,
            PendingChannelOpen {
                original_request_id: request_id,
                downstream_channel_id: ds_channel_id,
                downstream_id,
            },
        );

        // Pre-create the channel mapping so we have the open_request stored for reconnect.
        self.channels.insert(
            ds_channel_id,
            ChannelMapping {
                downstream_channel_id: ds_channel_id,
                upstream_channel_id: None,
                downstream_id,
                gate: ShareGate::new(RateProfile::default()),
                floor_active: false,
                shares_forwarded: 0,
                shares_gated: 0,
                supply_window: RollingWindow::new(),
                forward_window: RollingWindow::new(),
                open_request: Some(open_req.clone()),
                is_standard: false,
                open_standard_msg: None,
            },
        );

        // Forward the open request upstream verbatim.
        let frame: StandardSv2Frame<Message> =
            match Message::Mining(Mining::OpenExtendedMiningChannel(open_req)).try_into() {
                Ok(f) => f,
                Err(e) => {
                    error!("Failed to encode OpenExtendedMiningChannel: {e:?}");
                    return;
                }
            };
        if let Some(ref mut writer) = self.upstream_writer {
            if let Err(e) = writer.write_frame(frame.into()).await {
                error!("Failed to send OpenExtendedMiningChannel upstream: {e:?}");
            }
        } else {
            warn!(downstream_id, "Upstream not connected, channel open queued for reconnect");
        }
    }

    /// Handle a frame received from the upstream pool.
    async fn handle_upstream_frame(
        &mut self,
        frame: stratum_apps::stratum_core::codec_sv2::StandardEitherFrame<Message>,
    ) {
        let mut sv2_frame: StandardSv2Frame<Message> = match frame.try_into() {
            Ok(f) => f,
            Err(_) => {
                warn!("Invalid frame from upstream");
                return;
            }
        };

        let msg_type = sv2_frame.get_header().unwrap().msg_type();
        let payload = sv2_frame.payload();

        let mining_msg: Result<Mining<'_>, _> = (msg_type, payload).try_into();
        match mining_msg {
            Ok(m) => {
                let m_static = m.into_static();
                self.handle_upstream_mining_message(m_static).await;
            }
            Err(_) => {
                debug!("Upstream non-mining message type 0x{msg_type:02x} (ignored)");
            }
        }
    }

    /// Handle a parsed mining message from upstream.
    async fn handle_upstream_mining_message(&mut self, msg: Mining<'static>) {
        match msg {
            Mining::OpenExtendedMiningChannelSuccess(ref success) => {
                let request_id = success.request_id;
                let upstream_channel_id = success.channel_id;

                if let Some(pending) = self.pending_opens.remove(&request_id) {
                    info!(
                        downstream_id = pending.downstream_id,
                        ds_channel_id = pending.downstream_channel_id,
                        upstream_channel_id,
                        "Channel opened successfully"
                    );

                    // Update the existing channel mapping with the upstream channel ID.
                    if let Some(mapping) = self.channels.get_mut(&pending.downstream_channel_id) {
                        mapping.upstream_channel_id = Some(upstream_channel_id);
                    } else {
                        // Shouldn't happen, but create mapping if missing.
                        let gate = ShareGate::new(RateProfile::default());
                        self.channels.insert(
                            pending.downstream_channel_id,
                            ChannelMapping {
                                downstream_channel_id: pending.downstream_channel_id,
                                upstream_channel_id: Some(upstream_channel_id),
                                downstream_id: pending.downstream_id,
                                gate,
                                floor_active: false,
                                shares_forwarded: 0,
                                shares_gated: 0,
                                supply_window: RollingWindow::new(),
                                forward_window: RollingWindow::new(),
                                open_request: None,
                                is_standard: false,
                                open_standard_msg: None,
                            },
                        );
                    }

                    // Forward the success response to the downstream miner.
                    self.send_to_downstream(pending.downstream_id, Message::Mining(msg))
                        .await;
                } else {
                    warn!(
                        request_id,
                        "Received OpenExtendedMiningChannelSuccess for unknown request"
                    );
                }
            }
            Mining::OpenStandardMiningChannelSuccess(ref success) => {
                let request_id = success.get_request_id_as_u32();
                let upstream_channel_id = success.channel_id;

                if let Some(pending) = self.pending_opens.remove(&request_id) {
                    // For standard channels, the miner receives the pool's channel_id
                    // directly (we forward verbatim), so use upstream_channel_id as the
                    // downstream_channel_id for share routing.
                    let effective_ds_channel_id = upstream_channel_id;
                    info!(
                        downstream_id = pending.downstream_id,
                        effective_ds_channel_id,
                        upstream_channel_id,
                        "Standard channel opened successfully"
                    );

                    // Remove the pre-created mapping (keyed by our internal ds_channel_id)
                    // and re-insert keyed by the effective (upstream) channel_id.
                    let mut mapping = self.channels.remove(&pending.downstream_channel_id)
                        .unwrap_or_else(|| ChannelMapping {
                            downstream_channel_id: effective_ds_channel_id,
                            upstream_channel_id: Some(upstream_channel_id),
                            downstream_id: pending.downstream_id,
                            gate: ShareGate::new(RateProfile::default()),
                            floor_active: false,
                            shares_forwarded: 0,
                            shares_gated: 0,
                            supply_window: RollingWindow::new(),
                            forward_window: RollingWindow::new(),
                            open_request: None,
                            is_standard: true,
                            open_standard_msg: None,
                        });
                    mapping.downstream_channel_id = effective_ds_channel_id;
                    mapping.upstream_channel_id = Some(upstream_channel_id);
                    self.channels.insert(effective_ds_channel_id, mapping);

                    self.send_to_downstream(pending.downstream_id, Message::Mining(msg))
                        .await;
                } else {
                    warn!(
                        request_id,
                        "Received OpenStandardMiningChannelSuccess for unknown request"
                    );
                }
            }
            Mining::OpenMiningChannelError(ref err) => {
                let request_id = err.request_id;
                if let Some(pending) = self.pending_opens.remove(&request_id) {
                    warn!(
                        downstream_id = pending.downstream_id,
                        request_id,
                        "Channel open rejected by pool: {}",
                        err.error_code.as_utf8_or_hex()
                    );
                    // Remove the pre-created channel mapping.
                    self.channels.remove(&pending.downstream_channel_id);
                    self.send_to_downstream(pending.downstream_id, Message::Mining(msg))
                        .await;
                } else {
                    warn!(
                        request_id,
                        "Received OpenMiningChannelError for unknown request"
                    );
                }
            }
            Mining::NewExtendedMiningJob(ref job) => {
                let upstream_ch = job.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::NewMiningJob(ref job) => {
                let upstream_ch = job.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::SetNewPrevHash(ref prev) => {
                let upstream_ch = prev.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::SetTarget(ref target) => {
                let upstream_ch = target.channel_id;
                // Store the pool's target for share validity checks, but do NOT
                // forward to the miner. The miner works at a fixed difficulty
                // (set at channel-open time from the floor config) and never changes.
                // This guarantees constant supply regardless of pool vardiff behavior.
                let pair = self
                    .channels
                    .values_mut()
                    .find(|m| m.upstream_channel_id == Some(upstream_ch));
                if let Some(pair) = pair {
                    pair.floor_active = true;
                    debug!(upstream_ch, "SetTarget absorbed (miner keeps fixed difficulty)");
                }
            }
            Mining::SetExtranoncePrefix(ref prefix) => {
                let upstream_ch = prefix.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::SubmitSharesSuccess(ref s) => {
                // Pool acked our forwarded shares. Log only -- miner was already acked.
                debug!(
                    channel_id = s.channel_id,
                    last_seq = s.last_sequence_number,
                    accepted = s.new_submits_accepted_count,
                    "Pool acknowledged shares"
                );
            }
            Mining::SubmitSharesError(ref e) => {
                // Pool rejected a forwarded share (stale, difficulty race).
                warn!(
                    channel_id = e.channel_id,
                    seq = e.sequence_number,
                    error = %e.error_code.as_utf8_or_hex(),
                    "Pool rejected share"
                );
            }
            other => {
                debug!("Upstream mining message (unhandled): {other}");
            }
        }
    }

    /// Send a message to a specific downstream connection.
    /// Route an upstream message to the downstream that owns the given upstream channel.
    /// For extended channels, the message is forwarded WITHOUT rewriting channel_id --
    /// Apply the difficulty floor to a SetTarget message.
    /// If the pool's target is easier than the floor (higher U256 value),
    /// substitute the floor target. Only forward if the effective target changed.
    async fn handle_set_target_with_floor(
        &mut self,
        upstream_channel_id: u32,
        set_target: stratum_apps::stratum_core::mining_sv2::SetTarget<'static>,
    ) {
        let pair = self
            .channels
            .values_mut()
            .find(|m| m.upstream_channel_id == Some(upstream_channel_id));

        let Some(pair) = pair else {
            debug!(upstream_channel_id, "No mapping for SetTarget (floor check)");
            return;
        };

        // Convert floor difficulty to a max target: target = 2^256 / difficulty
        // For U256 target representation, higher value = easier.
        // Pool's target is in set_target.maximum_target (le bytes).
        let floor_difficulty = self.config.min_downstream_difficulty;

        // Compare: is pool's target easier (larger) than the floor target?
        let pool_target_bytes: &[u8] = set_target.maximum_target.inner_as_ref();
        let pool_target_difficulty = target_to_difficulty(pool_target_bytes);

        if pool_target_difficulty < floor_difficulty {
            // Pool's difficulty is below floor -- override with floor.
            pair.floor_active = true;
            debug!(
                upstream_channel_id,
                pool_diff = pool_target_difficulty,
                floor_diff = floor_difficulty,
                "Floor active: pool difficulty below floor, not forwarding SetTarget"
            );
            // Don't forward -- miner keeps its current (harder) target.
        } else {
            // Pool's difficulty is at or above floor -- forward normally.
            pair.floor_active = false;
            let ds_id = pair.downstream_id;
            self.send_to_downstream(ds_id, Message::Mining(Mining::SetTarget(set_target)))
                .await;
        }
    }

    /// the downstream received the pool's channel_id in OpenExtendedMiningChannelSuccess
    /// and expects subsequent messages on that same ID.
    async fn forward_to_downstream_by_upstream_channel(
        &mut self,
        upstream_channel_id: u32,
        msg: Mining<'static>,
    ) {
        // Find the channel pair that has this upstream_channel_id.
        let pair = self
            .channels
            .values()
            .find(|m| m.upstream_channel_id == Some(upstream_channel_id));

        let Some(pair) = pair else {
            // No pair found -- might be for a phantom or not-yet-mapped channel.
            debug!(upstream_channel_id, "No downstream mapping for upstream channel");
            return;
        };

        let ds_id = pair.downstream_id;

        // Forward verbatim -- don't rewrite channel_id. The downstream received the
        // pool's channel_id in the OpenExtendedMiningChannelSuccess and expects
        // all subsequent messages to use it.
        self.send_to_downstream(ds_id, Message::Mining(msg)).await;
    }

    /// Rewrite the channel_id field in a Mining message.
    #[allow(dead_code)]
    fn rewrite_channel_id(msg: Mining<'static>, new_channel_id: u32) -> Mining<'static> {
        match msg {
            Mining::NewExtendedMiningJob(mut job) => {
                job.channel_id = new_channel_id;
                Mining::NewExtendedMiningJob(job)
            }
            Mining::NewMiningJob(mut job) => {
                job.channel_id = new_channel_id;
                Mining::NewMiningJob(job)
            }
            Mining::SetNewPrevHash(mut prev) => {
                prev.channel_id = new_channel_id;
                Mining::SetNewPrevHash(prev)
            }
            Mining::SetTarget(mut target) => {
                target.channel_id = new_channel_id;
                Mining::SetTarget(target)
            }
            Mining::SetExtranoncePrefix(mut prefix) => {
                prefix.channel_id = new_channel_id;
                Mining::SetExtranoncePrefix(prefix)
            }
            other => other,
        }
    }

    async fn send_to_downstream(&mut self, id: DownstreamId, msg: Message) {
        let frame: StandardSv2Frame<Message> = match msg.try_into() {
            Ok(f) => f,
            Err(e) => {
                warn!(downstream_id = id, "Failed to encode message for downstream: {e:?}");
                return;
            }
        };

        if let Some(ds) = self.downstreams.get_mut(&id) {
            if let Err(e) = ds.writer.write_frame(frame.into()).await {
                warn!(downstream_id = id, "Failed to write to downstream: {e:?}");
                self.downstreams.remove(&id);
            }
        }
    }
}

/// Convert a 32-byte LE target to an approximate scalar difficulty.
/// difficulty ~ 2^256 / target
fn target_to_difficulty(target_le: &[u8]) -> f64 {
    // Find the most significant non-zero byte to get a rough magnitude.
    // Then compute difficulty from the top 8 bytes for precision.
    let mut msb_index = 31;
    while msb_index > 0 && target_le[msb_index] == 0 {
        msb_index -= 1;
    }
    if target_le[msb_index] == 0 {
        return f64::MAX; // target is zero -> infinite difficulty
    }

    // Build a f64 from the top bytes of the target
    let mut target_val: f64 = 0.0;
    for i in (0..=msb_index).rev() {
        target_val = target_val * 256.0 + target_le[i] as f64;
    }
    // Shift to account for the full 256-bit position
    // target_val represents the value of bytes [0..msb_index], which is
    // the full LE number already since we iterated in reverse.

    if target_val == 0.0 {
        return f64::MAX;
    }

    // 2^256 / target
    // Use log to avoid overflow: exp(256*ln(2) - ln(target))
    let log_diff = 256.0 * 2.0_f64.ln() - target_val.ln();
    log_diff.exp()
}
