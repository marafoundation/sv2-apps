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
    config::Config,
    downstream::{accept_downstream, DownstreamEvent, DownstreamId},
    profile::RateProfile,
    share_gate::ShareGate,
    upstream::{Message, Reader, Writer},
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
    /// Shares forwarded upstream.
    shares_forwarded: u64,
    /// Shares dropped by the gate.
    shares_gated: u64,
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
    upstream_reader: Reader,
    upstream_writer: Writer,
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
    pub fn new(
        config: Config,
        upstream_reader: Reader,
        upstream_writer: Writer,
    ) -> Result<Self, String> {
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
            upstream_reader,
            upstream_writer,
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

        loop {
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

                // Handle upstream frames.
                upstream_frame = self.upstream_reader.read_frame() => {
                    match upstream_frame {
                        Ok(frame) => {
                            self.handle_upstream_frame(frame).await;
                        }
                        Err(e) => {
                            error!("Upstream connection lost: {e:?}");
                            return Err("Upstream disconnected".to_string());
                        }
                    }
                }
            }
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
                    .find(|m| m.downstream_id == id && m.downstream_channel_id == channel_id);

                let Some(mapping) = mapping else {
                    debug!(downstream_id = id, channel_id, "Share for unknown channel");
                    return;
                };

                if !mapping.gate.should_forward() {
                    mapping.shares_gated += 1;
                    return;
                }

                mapping.shares_forwarded += 1;

                // Forward upstream.
                let frame: StandardSv2Frame<Message> =
                    match Message::Mining(msg).try_into() {
                        Ok(f) => f,
                        Err(e) => {
                            warn!(downstream_id = id, "Failed to encode share: {e:?}");
                            return;
                        }
                    };
                if let Err(e) = self.upstream_writer.write_frame(frame.into()).await {
                    error!("Failed to forward share upstream: {e:?}");
                }
            }
            Mining::SubmitSharesStandard(ref share) => {
                // Ack and ignore standard shares (we only support extended).
                let ack = SubmitSharesSuccess {
                    channel_id: share.channel_id,
                    last_sequence_number: share.sequence_number,
                    new_submits_accepted_count: 1,
                    new_shares_sum: 0,
                };
                self.send_to_downstream(
                    id,
                    Message::Mining(Mining::SubmitSharesSuccess(ack)),
                )
                .await;
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
                if let Err(e) = self.upstream_writer.write_frame(frame.into()).await {
                    error!("Failed to forward message upstream: {e:?}");
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

        // Forward the open request upstream verbatim.
        let frame: StandardSv2Frame<Message> =
            match Message::Mining(Mining::OpenExtendedMiningChannel(open_req)).try_into() {
                Ok(f) => f,
                Err(e) => {
                    error!("Failed to encode OpenExtendedMiningChannel: {e:?}");
                    return;
                }
            };
        if let Err(e) = self.upstream_writer.write_frame(frame.into()).await {
            error!("Failed to send OpenExtendedMiningChannel upstream: {e:?}");
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

                    // Store the channel mapping with a share gate.
                    let gate = ShareGate::new(RateProfile::default());
                    self.channels.insert(
                        pending.downstream_channel_id,
                        ChannelMapping {
                            downstream_channel_id: pending.downstream_channel_id,
                            upstream_channel_id: Some(upstream_channel_id),
                            downstream_id: pending.downstream_id,
                            gate,
                            shares_forwarded: 0,
                            shares_gated: 0,
                        },
                    );

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
            Mining::OpenMiningChannelError(ref err) => {
                let request_id = err.request_id;
                if let Some(pending) = self.pending_opens.remove(&request_id) {
                    warn!(
                        downstream_id = pending.downstream_id,
                        request_id,
                        "Channel open rejected by pool: {}",
                        err.error_code.as_utf8_or_hex()
                    );
                    self.send_to_downstream(pending.downstream_id, Message::Mining(msg))
                        .await;
                } else {
                    warn!(
                        request_id,
                        "Received OpenMiningChannelError for unknown request"
                    );
                }
            }
            Mining::NewExtendedMiningJob(_)
            | Mining::SetNewPrevHash(_)
            | Mining::SetTarget(_)
            | Mining::SetExtranoncePrefix(_) => {
                // Broadcast to all connected downstreams.
                debug!("Broadcasting upstream message to all downstreams: {msg}");
                let downstream_ids: Vec<DownstreamId> =
                    self.downstreams.keys().copied().collect();
                for ds_id in downstream_ids {
                    self.send_to_downstream(ds_id, Message::Mining(msg.clone()))
                        .await;
                }
            }
            Mining::SubmitSharesSuccess(ref s) => {
                // Pool acked our forwarded shares. Log only — miner was already acked.
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
