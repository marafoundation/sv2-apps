use crate::{
    error::{self, Action, LoopControl, TproxyError, TproxyErrorKind, TproxyResult},
    sv1::job_store::Sv1JobStore,
    utils::{
        KEEPALIVE_JOB_ID_DELIMITER, MAX_FUTURE_BLOCK_TIME,
        SV1_MIN_DIFFICULTY_FOR_INTEGER_POWER_OF_TWO_ROUNDING, SubmitShareWithChannelId,
    },
};
use async_channel::{Receiver, Sender};
#[cfg(feature = "monitoring")]
use std::net::IpAddr;
use std::{
    collections::VecDeque,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use stratum_apps::{
    channel_utils::ReceiverCleanup,
    stratum_core::{
        bitcoin::{BlockHash, Target},
        channels_sv2::client::MAX_SEEN_SHARES,
        stratum_translation::sv2_to_sv1::build_sv1_set_difficulty_from_sv2_target_with_integer_power_of_two_rounding,
        sv1_api::{
            json_rpc,
            methods::Client2Server,
            server_to_client,
            utils::{Extranonce, HexU32Be},
        },
    },
    sync::SharedLock,
    task_manager::TaskManager,
    utils::types::{ChannelId, DownstreamId, Hashrate},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// tProxy-local work queued by the SV1 server for a single downstream task.
///
/// Setup completion is queued alongside notifications so that this task releases cached mining
/// notifications in FIFO order after both setup responses have been queued to the miner.
/// `SetupComplete` and `notify_miner` are local delivery-order state rather than SV1 protocol
/// messages; protocol-level server-to-client message types remain in `sv1_api`.
#[derive(Clone, Debug)]
pub(super) enum Sv1ServerEvent {
    Notify(Arc<server_to_client::Notify>),
    SetDifficulty(json_rpc::Message),
    SetExtranonce {
        message: server_to_client::SetExtranonce,
        /// Decided by the server in the same task that builds and records subscribe responses.
        /// Otherwise the prefix is already included in the upcoming subscribe response.
        notify_miner: bool,
    },
    SetupComplete,
    /// The upstream generation ended and this miner is held: everything queued before this
    /// event belonged to it.
    Hold,
    /// Keeps a held miner busy: its last job, nTime rolled by `interval_secs`, as job `#counter`.
    /// Never valid for share submission.
    HoldKeepalive {
        counter: u32,
        interval_secs: u32,
    },
    /// A held miner has a channel on the new upstream with this extranonce.
    Reattach {
        extranonce1: Extranonce,
        extranonce2_len: usize,
    },
}

/// How a held miner was moved onto a new upstream.
#[derive(Debug, Default)]
pub(super) struct ReattachCounters {
    pub(super) unchanged: AtomicU64,
    pub(super) set_extranonce: AtomicU64,
    pub(super) reconnect: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct DownstreamIo {
    pub downstream_sv1_sender: Sender<json_rpc::Message>,
    downstream_sv1_receiver: Receiver<json_rpc::Message>,
    sv1_server_sender: Sender<(DownstreamId, json_rpc::Message)>,
    sv1_server_receiver: Receiver<Sv1ServerEvent>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl DownstreamIo {
    fn new(
        downstream_sv1_sender: Sender<json_rpc::Message>,
        downstream_sv1_receiver: Receiver<json_rpc::Message>,
        sv1_server_sender: Sender<(DownstreamId, json_rpc::Message)>,
        sv1_server_receiver: Receiver<Sv1ServerEvent>,
    ) -> Self {
        Self {
            downstream_sv1_receiver,
            downstream_sv1_sender,
            sv1_server_receiver,
            sv1_server_sender,
        }
    }

    fn close(&self) {
        debug!("Dropping downstream channel state");
        self.downstream_sv1_sender.close();
        self.downstream_sv1_receiver.close_and_drain();
        self.sv1_server_receiver.close_and_drain();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Sv1SessionState {
    /// Setup responses are still pending, or cached notifications are still being delivered.
    Starting { subscribed: bool, authorized: bool },
    /// Setup is complete and job notifications can be forwarded normally.
    Ready,
}

impl Default for Sv1SessionState {
    fn default() -> Self {
        Self::Starting {
            subscribed: false,
            authorized: false,
        }
    }
}

impl Sv1SessionState {
    /// Records a queued setup response and returns `true` only when both required responses have
    /// been queued for the first time.
    pub(super) fn record_response(&mut self, request: Sv1SetupRequest) -> bool {
        let Self::Starting {
            subscribed,
            authorized,
        } = self
        else {
            return false;
        };

        let was_complete = *subscribed && *authorized;
        match request {
            Sv1SetupRequest::Subscribe => *subscribed = true,
            Sv1SetupRequest::Authorize => *authorized = true,
        }
        !was_complete && *subscribed && *authorized
    }

    pub(super) fn is_ready(self) -> bool {
        self == Self::Ready
    }

    pub(super) fn is_subscribed(self) -> bool {
        match self {
            Self::Starting { subscribed, .. } => subscribed,
            Self::Ready => true,
        }
    }

    pub(super) fn setup_complete(self) -> bool {
        match self {
            Self::Starting {
                subscribed,
                authorized,
            } => subscribed && authorized,
            Self::Ready => true,
        }
    }
}

/// Downstream-specific values needed to validate and translate a share for one advertised job.
///
/// Difficulty and extranonce assignments take effect on job boundaries, so late shares must use
/// the values that accompanied their own job rather than the downstream's newest values.
#[derive(Clone, Debug)]
pub(super) struct Sv1JobValidationContext {
    pub(super) extranonce: Extranonce,
    pub(super) extranonce2_len: usize,
    pub(super) target: Target,
}

/// Accepted SV1 share hashes retained for per-downstream duplicate detection.
///
/// This mirrors the client-channel policy in `channels_sv2`: the oldest hash is evicted once the
/// shared `MAX_SEEN_SHARES` bound is reached, and the cache is cleared on a chain-tip transition.
/// It remains separate from the SV2 channel cache because tProxy accepts shares at each miner's
/// advertised target even when they do not meet the upstream channel target.
#[derive(Debug, Default)]
pub(super) struct Sv1AcceptedShareCache {
    hashes: VecDeque<BlockHash>,
}

impl Sv1AcceptedShareCache {
    /// Records a share hash and returns `false` when it was already present.
    pub(super) fn insert_if_new(&mut self, hash: BlockHash) -> bool {
        if self.hashes.contains(&hash) {
            return false;
        }
        if self.hashes.len() == MAX_SEEN_SHARES {
            self.hashes.pop_front();
        }
        self.hashes.push_back(hash);
        true
    }

    fn clear(&mut self) {
        self.hashes.clear();
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.hashes.len()
    }
}

#[derive(Debug)]
pub struct DownstreamData {
    pub channel_id: Option<ChannelId>,
    /// Upstream generation epoch `channel_id` was assigned under; only that generation's
    /// channel manager knows the channel.
    pub channel_epoch: u64,
    pub extranonce1: Extranonce,
    pub extranonce2_len: usize,
    // Current SV1 share-validation target. This follows the advertised
    // difficulty sent to the miner, including any SV1 pow2 rounding.
    pub target: Target,
    pub hashrate: Option<Hashrate>,
    #[cfg(feature = "monitoring")]
    pub connection_ip: IpAddr,
    pub version_rolling_mask: Option<HexU32Be>,
    pub version_rolling_min_bit: Option<HexU32Be>,
    pub sv1_username: String,
    pub sv1_worker_name: String,
    pub cached_set_difficulty: Option<json_rpc::Message>,
    pub cached_notify: Option<Arc<server_to_client::Notify>>,
    /// Prefix notification paired with the next deliverable job. Capability is checked only
    /// when that job is sent, allowing the miner to announce support during setup.
    pub(super) cached_set_extranonce: Option<server_to_client::SetExtranonce>,
    pub(super) session_state: Sv1SessionState,
    /// Per-job downstream state retained for late-share validation under the current chain tip.
    pub(super) job_validation_contexts: Sv1JobStore<Sv1JobValidationContext>,
    /// Bounded hashes of shares accepted from this miner under the current chain tip.
    pub(super) accepted_share_hashes: Sv1AcceptedShareCache,
    /// Number of queued `mining.set_extranonce` notifications not yet applied by this downstream.
    pub(super) pending_set_extranonce_notifications: usize,
    // Next advertised SV1 target, applied when the corresponding
    // mining.set_difficulty is sent with a new mining.notify.
    pub pending_target: Option<Target>,
    pub pending_hashrate: Option<Hashrate>,
    pub stable_hashrate: bool,
    // Queue of Sv1 handshake messages received while waiting for SV2 channel to open
    pub queued_sv1_handshake_messages: Vec<Client2Server>,
    // Stores pending shares to be sent to the sv1_server
    pub pending_share: Option<SubmitShareWithChannelId>,
    // Exact target currently accepted upstream, used to decide whether a
    // stricter downstream difficulty must wait for a SetTarget response.
    pub upstream_target: Option<Target>,
    /// Timestamp anchoring the next keepalive interval.
    ///
    /// `None` before the first job and while an extranonce change is waiting for the job that
    /// activates it.
    pub keepalive_timer_anchor: Option<Instant>,
    pub(super) supports_set_extranonce: bool,
    /// Kept connected while no upstream generation serves it; cleared once it has a channel
    /// on the next one.
    pub(super) held: bool,
    /// The next job sent to the miner tells it to drop all previous work.
    pub(super) force_clean_jobs: bool,
    /// Suffix of the `.minerN` user identity, kept so a re-opened channel uses the same one.
    pub(super) miner_id: Option<u32>,
    /// The last job sent to the miner and the nTime of the upstream job it derives from, the
    /// base for keep-alive jobs while it is held. Kept across upstream generations.
    pub(super) last_job: Option<(server_to_client::Notify, u32)>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Sv1SetupRequest {
    Subscribe,
    Authorize,
}

impl DownstreamData {
    pub fn new(
        hashrate: Option<Hashrate>,
        target: Target,
        max_past_jobs: Option<usize>,
        #[cfg(feature = "monitoring")] connection_ip: IpAddr,
    ) -> Self {
        DownstreamData {
            channel_id: None,
            channel_epoch: 0,
            extranonce1: vec![0; 8]
                .try_into()
                .expect("8-byte extranonce is always valid"),
            extranonce2_len: 4,
            target,
            hashrate,
            #[cfg(feature = "monitoring")]
            connection_ip,
            version_rolling_mask: None,
            version_rolling_min_bit: None,
            sv1_username: String::new(),
            sv1_worker_name: String::new(),
            cached_set_difficulty: None,
            cached_notify: None,
            cached_set_extranonce: None,
            session_state: Sv1SessionState::default(),
            job_validation_contexts: Sv1JobStore::new(max_past_jobs),
            accepted_share_hashes: Sv1AcceptedShareCache::default(),
            pending_set_extranonce_notifications: 0,
            pending_target: None,
            pending_hashrate: None,
            stable_hashrate: false,
            queued_sv1_handshake_messages: Vec::new(),
            pending_share: None,
            upstream_target: None,
            keepalive_timer_anchor: None,
            supports_set_extranonce: false,
            held: false,
            force_clean_jobs: false,
            miner_id: None,
            last_job: None,
        }
    }

    fn record_last_job(&mut self, notify: &server_to_client::Notify) {
        let base_time = match &self.last_job {
            Some((_, base_time)) if notify.job_id.contains(KEEPALIVE_JOB_ID_DELIMITER) => {
                *base_time
            }
            _ => notify.time.0,
        };
        self.last_job = Some((notify.clone(), base_time));
    }

    /// The next keep-alive job for a held, ready miner: its last job as job `{original}#counter`
    /// with nTime rolled forward by `interval_secs`, at most to Bitcoin Core's future-time limit
    /// from the upstream job it derives from. Past that limit only the job id changes, which is
    /// still new work to firmware.
    fn next_hold_keepalive(
        &mut self,
        counter: u32,
        interval_secs: u32,
    ) -> Option<server_to_client::Notify> {
        if !self.held || !self.session_state.is_ready() {
            return None;
        }
        let (last, base_time) = self.last_job.as_mut()?;
        let original_job_id = last
            .job_id
            .split_once(KEEPALIVE_JOB_ID_DELIMITER)
            .map_or(last.job_id.as_str(), |(original, _)| original)
            .to_string();
        last.job_id = format!("{original_job_id}{KEEPALIVE_JOB_ID_DELIMITER}{counter}");
        last.time = HexU32Be(
            last.time
                .0
                .saturating_add(interval_secs)
                .min(base_time.saturating_add(MAX_FUTURE_BLOCK_TIME)),
        );
        last.clean_jobs = false;
        Some(last.clone())
    }

    fn record_job_validation_context(&mut self, notify: &server_to_client::Notify) {
        if notify.clean_jobs {
            self.accepted_share_hashes.clear();
        }
        let context = Sv1JobValidationContext {
            extranonce: self.extranonce1.clone(),
            extranonce2_len: self.extranonce2_len,
            target: self.target,
        };
        self.job_validation_contexts
            .activate(notify.job_id.clone(), context, notify.clean_jobs);
        if self.pending_set_extranonce_notifications == 0 {
            self.keepalive_timer_anchor = Some(Instant::now());
        }
    }

    pub(super) fn job_validation_context(&self, job_id: &str) -> Option<Sv1JobValidationContext> {
        self.job_validation_contexts.get(job_id).cloned()
    }

    pub fn set_pending_target(&mut self, new_target: Target, downstream_id: DownstreamId) {
        self.pending_target = Some(new_target);
        debug!("Downstream {downstream_id}: Set pending target");
    }

    pub fn set_pending_hashrate(
        &mut self,
        new_hashrate: Option<Hashrate>,
        downstream_id: DownstreamId,
    ) {
        self.pending_hashrate = new_hashrate;
        debug!("Downstream {downstream_id}: Set pending hashrate");
    }

    pub fn set_upstream_target(&mut self, upstream_target: Target, downstream_id: DownstreamId) {
        self.upstream_target = Some(upstream_target);
        debug!(
            "Downstream {downstream_id}: Set upstream target to {}",
            upstream_target
        );
    }
}

/// Represents a downstream SV1 miner connection.
///
/// This struct manages the state and communication for a single SV1 miner connected
/// to the translator. It handles:
/// - SV1 protocol message processing (subscribe, authorize, submit)
/// - Bidirectional message routing between miner and SV1 server
/// - Mining job tracking and share validation
/// - Difficulty adjustment coordination
/// - Connection lifecycle management
///
/// Each downstream connection runs in its own async task that processes messages
/// from both the miner and the server, ensuring proper message ordering and
/// handling connection-specific state.
#[derive(Clone, Debug)]
pub struct Downstream {
    pub downstream_id: DownstreamId,
    pub downstream_data: SharedLock<DownstreamData>,
    pub downstream_io: DownstreamIo,
    /// Per-connection cancellation token (child of the global token).
    /// Cancelled when this downstream's task loop exits, causing
    /// the associated SV1 I/O task to shut down.
    downstream_cancellation_token: CancellationToken,
    reattach_counters: Arc<ReattachCounters>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl Downstream {
    /// Counts this miner's re-attachments in `counters`, shared with the SV1 server.
    pub(super) fn with_reattach_counters(mut self, counters: Arc<ReattachCounters>) -> Self {
        self.reattach_counters = counters;
        self
    }

    /// Stops this miner's connection. Its task cleanup callback removes the associated server and
    /// channel-manager state.
    pub(super) fn disconnect(&self) {
        self.downstream_cancellation_token.cancel();
    }

    /// Sends `client.reconnect` without parameters, asking the miner to reconnect to the host and
    /// port it already uses, then closes the outbound channel. The connection writer flushes what
    /// is queued (this message included) before it closes the socket, which then runs the same
    /// cleanup as [`Downstream::disconnect`].
    pub(super) fn reconnect(&self) {
        let sender = &self.downstream_io.downstream_sv1_sender;
        let _ = sender.try_send(json_rpc::Message::Notification(json_rpc::Notification {
            method: "client.reconnect".to_string(),
            params: serde_json::Value::Array(vec![]),
        }));
        sender.close();
    }

    #[cfg(test)]
    pub(super) fn is_disconnected(&self) -> bool {
        self.downstream_cancellation_token.is_cancelled()
    }

    /// Whether the miner was sent `client.reconnect`, which closes its outbound channel.
    #[cfg(test)]
    pub(super) fn was_asked_to_reconnect(&self) -> bool {
        self.downstream_io.downstream_sv1_sender.is_closed()
    }

    fn handle_error_action(
        &self,
        context: &str,
        e: &TproxyError<error::Downstream>,
        cancellation_token: &CancellationToken,
    ) -> LoopControl {
        if cancellation_token.is_cancelled() {
            debug!(
                downstream_id = self.downstream_id,
                error_kind = ?e.kind,
                "{context} returned an error after shutdown was requested"
            );
            return LoopControl::Continue;
        }

        match e.action {
            Action::Log => {
                warn!(
                    downstream_id = self.downstream_id,
                    error_kind = ?e.kind,
                    "{context} returned a log-only error"
                );
                LoopControl::Continue
            }
            Action::Disconnect(_) => {
                warn!(
                    downstream_id = self.downstream_id,
                    error_kind = ?e.kind,
                    "{context} requested disconnect; cancelling downstream token"
                );
                self.downstream_cancellation_token.cancel();
                LoopControl::Break
            }
            Action::Shutdown => {
                warn!(
                    downstream_id = self.downstream_id,
                    error_kind = ?e.kind,
                    "{context} requested shutdown; cancelling global token"
                );
                cancellation_token.cancel();
                LoopControl::Break
            }
            other => {
                warn!(
                    downstream_id = self.downstream_id,
                    action = ?other,
                    error_kind = ?e.kind,
                    "{context} returned an unhandled action"
                );
                LoopControl::Continue
            }
        }
    }

    /// Creates a new downstream connection instance.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        downstream_id: DownstreamId,
        downstream_sv1_sender: Sender<json_rpc::Message>,
        downstream_sv1_receiver: Receiver<json_rpc::Message>,
        sv1_server_sender: Sender<(DownstreamId, json_rpc::Message)>,
        sv1_server_receiver: Receiver<Sv1ServerEvent>,
        target: Target,
        hashrate: Option<Hashrate>,
        max_past_jobs: Option<usize>,
        #[cfg(feature = "monitoring")] connection_ip: IpAddr,
        downstream_cancellation_token: CancellationToken,
    ) -> Self {
        let downstream_data = SharedLock::new(DownstreamData::new(
            hashrate,
            target,
            max_past_jobs,
            #[cfg(feature = "monitoring")]
            connection_ip,
        ));
        let downstream_channel_io = DownstreamIo::new(
            downstream_sv1_sender,
            downstream_sv1_receiver,
            sv1_server_sender,
            sv1_server_receiver,
        );
        Self {
            downstream_id,
            downstream_data,
            downstream_io: downstream_channel_io,
            downstream_cancellation_token,
            reattach_counters: Arc::default(),
        }
    }

    /// Spawns and runs the main task loop for this downstream connection.
    ///
    /// This method creates an async task that handles all communication for this
    /// downstream connection. The task runs a select loop that processes:
    /// - The global cancellation signal. A downstream outlives upstream fallbacks.
    /// - Messages from the miner (subscribe, authorize, submit)
    /// - Messages from the SV1 server (notify, set_difficulty, etc.)
    ///
    /// The task will continue running until a cancellation signal is received or
    /// an unrecoverable error occurs. It ensures graceful cleanup of resources
    /// and proper error reporting.
    pub(super) fn start<F, Fut>(
        self,
        cancellation_token: CancellationToken,
        task_manager: Arc<TaskManager>,
        on_disconnect: F,
    ) where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let downstream_id = self.downstream_id;
        task_manager.spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = cancellation_token.cancelled() => {
                        info!("Downstream {downstream_id}: received app shutdown signal");
                        break;
                    }
                    // Handle downstream -> server message
                    res = self.handle_downstream_message() => {
                        if let Err(e) = res {
                            error!("Downstream {downstream_id}: error in downstream message handler: {e:?}");
                            if let LoopControl::Break = self.handle_error_action(
                                "Downstream::handle_downstream_message",
                                &e,
                                &cancellation_token,
                            ) {
                                break;
                            }
                        }
                    }

                    // Handle server -> downstream message
                    res = self.handle_sv1_server_message() => {
                        if let Err(e) = res {
                            error!("Downstream {downstream_id}: error in server message handler: {e:?}");
                            if let LoopControl::Break = self.handle_error_action(
                                "Downstream::handle_sv1_server_message",
                                &e,
                                &cancellation_token,
                            ) {
                                break;
                            }
                        }
                    }

                    else => {
                        warn!("Downstream {downstream_id}: all channels closed; exiting task");
                        break;
                    }
                }
            }

            warn!("Downstream {downstream_id}: unified task shutting down");
            self.downstream_cancellation_token.cancel();
            self.downstream_io.close();
            on_disconnect().await;
        });
    }

    /// Processes typed notifications in per-downstream FIFO order.
    ///
    /// Jobs wait for subscribe and authorize responses. Difficulty and extranonce changes are
    /// sent with the next deliverable job; prefix support is required only at that boundary.
    pub(super) async fn handle_sv1_server_message(&self) -> TproxyResult<(), error::Downstream> {
        let event = self
            .downstream_io
            .sv1_server_receiver
            .recv()
            .await
            .map_err(|error| TproxyError::disconnect(error, self.downstream_id))?;
        match event {
            Sv1ServerEvent::SetupComplete => self.enable_notification_forwarding().await?,
            Sv1ServerEvent::SetDifficulty(message) => {
                self.downstream_data
                    .with(|data| data.cached_set_difficulty = Some(message))
                    .map_err(TproxyError::shutdown)?;
            }
            Sv1ServerEvent::Notify(notify) => {
                let ready = self
                    .downstream_data
                    .with(|data| {
                        if data.session_state.is_ready() {
                            true
                        } else {
                            data.cached_notify = Some(notify.clone());
                            false
                        }
                    })
                    .map_err(TproxyError::shutdown)?;
                if ready {
                    self.send_job(notify).await?;
                }
            }
            Sv1ServerEvent::SetExtranonce {
                message,
                notify_miner,
            } => {
                self.downstream_data
                    .with(|data| {
                        data.pending_set_extranonce_notifications =
                            data.pending_set_extranonce_notifications.saturating_sub(1);
                        // Pre-subscribe updates were applied by the server before response
                        // construction. Reapplying this event could overwrite a newer prefix
                        // already included in that response.
                        if notify_miner {
                            data.extranonce1 = message.extra_nonce1.clone();
                            data.extranonce2_len = message.extra_nonce2_size;
                            data.cached_set_extranonce = Some(message);
                        }
                        // A cached job predates this transition and has not reached the miner.
                        data.cached_notify = None;
                    })
                    .map_err(TproxyError::shutdown)?;
            }
            Sv1ServerEvent::Hold => {
                self.downstream_data
                    .with(|data| {
                        data.job_validation_contexts.clear();
                        data.accepted_share_hashes.clear();
                        data.pending_share = None;
                        data.cached_notify = None;
                        data.cached_set_extranonce = None;
                        data.pending_set_extranonce_notifications = 0;
                        data.keepalive_timer_anchor = None;
                        // A difficulty change waiting for the next job belongs to the ended
                        // generation; the miner keeps the difficulty it is working at.
                        data.cached_set_difficulty = None;
                        data.pending_target = None;
                        data.pending_hashrate = None;
                    })
                    .map_err(TproxyError::shutdown)?;
            }
            Sv1ServerEvent::HoldKeepalive {
                counter,
                interval_secs,
            } => {
                let keepalive = self
                    .downstream_data
                    .with(|data| data.next_hold_keepalive(counter, interval_secs))
                    .map_err(TproxyError::shutdown)?;
                if let Some(keepalive) = keepalive {
                    self.send_to_miner(json_rpc::Message::from(keepalive))
                        .await?;
                }
            }
            Sv1ServerEvent::Reattach {
                extranonce1,
                extranonce2_len,
            } => self.reattach(extranonce1, extranonce2_len)?,
        }
        Ok(())
    }

    /// Moves a held miner onto its channel with the new upstream.
    ///
    /// An unchanged extranonce needs nothing but fresh work. A changed one is sent with the next
    /// job as `mining.set_extranonce` to a miner that announced support for it; any other miner
    /// is asked to reconnect, since work it hashed with its old extranonce would be invalid.
    #[allow(clippy::result_large_err)]
    fn reattach(
        &self,
        extranonce1: Extranonce,
        extranonce2_len: usize,
    ) -> TproxyResult<(), error::Downstream> {
        let reconnect = self
            .downstream_data
            .with(|data| {
                data.cached_notify = None;
                data.force_clean_jobs = true;
                // Firmware may reset its difficulty with its session; repeat the one the miner
                // is held at, which is also the one its shares are validated against.
                data.cached_set_difficulty =
                    build_sv1_set_difficulty_from_sv2_target_with_integer_power_of_two_rounding(
                        data.target,
                        SV1_MIN_DIFFICULTY_FOR_INTEGER_POWER_OF_TWO_ROUNDING,
                    )
                    .ok();
                if data.extranonce1 == extranonce1 && data.extranonce2_len == extranonce2_len {
                    self.reattach_counters
                        .unchanged
                        .fetch_add(1, Ordering::Relaxed);
                    return false;
                }
                if !data.supports_set_extranonce {
                    return true;
                }
                data.extranonce1 = extranonce1.clone();
                data.extranonce2_len = extranonce2_len;
                data.cached_set_extranonce = Some(server_to_client::SetExtranonce {
                    extra_nonce1: extranonce1,
                    extra_nonce2_size: extranonce2_len,
                });
                self.reattach_counters
                    .set_extranonce
                    .fetch_add(1, Ordering::Relaxed);
                false
            })
            .map_err(TproxyError::shutdown)?;
        if reconnect {
            info!(
                downstream_id = self.downstream_id,
                "Extranonce changed on the new upstream and the miner cannot be told; asking it to reconnect"
            );
            self.reattach_counters
                .reconnect
                .fetch_add(1, Ordering::Relaxed);
            self.reconnect();
        }
        Ok(())
    }

    async fn send_to_miner(
        &self,
        message: json_rpc::Message,
    ) -> TproxyResult<(), error::Downstream> {
        self.downstream_io
            .downstream_sv1_sender
            .send(message)
            .await
            .map_err(|error| {
                error!("Down: Failed to send message to miner: {error:?}");
                TproxyError::disconnect(TproxyErrorKind::ChannelErrorSender, self.downstream_id)
            })
    }

    /// Handles messages received from the downstream SV1 miner.
    ///
    /// This method processes SV1 protocol messages sent by the miner, including:
    /// - `mining.subscribe` - Subscription requests
    /// - `mining.authorize` - Authorization requests
    /// - `mining.submit` - Share submissions
    /// - Other SV1 protocol messages
    ///
    /// The method delegates message processing to the downstream data handler,
    /// which implements the SV1 protocol logic and generates appropriate responses.
    /// Responses are sent back to the miner, while share submissions are forwarded
    /// to the SV1 server for upstream processing.
    async fn handle_downstream_message(&self) -> TproxyResult<(), error::Downstream> {
        let downstream_id = self.downstream_id;
        let message = match self.downstream_io.downstream_sv1_receiver.recv().await {
            Ok(msg) => msg,
            Err(e) => {
                error!("Error receiving downstream message: {:?}", e);
                return Err(TproxyError::disconnect(e, downstream_id));
            }
        };

        self.downstream_io
            .sv1_server_sender
            .send((downstream_id, message))
            .await
            .map_err(|_| TproxyError::shutdown(TproxyErrorKind::ChannelErrorSender))?;

        Ok(())
    }

    /// Releases the latest cached job once both setup responses have been queued.
    /// Capability announcements may also arrive after setup, provided they are processed before
    /// the first job delivery requiring a changed extranonce.
    async fn enable_notification_forwarding(&self) -> TproxyResult<(), error::Downstream> {
        let (enable, notify) = self
            .downstream_data
            .with(|data| {
                if data.session_state.is_ready() || !data.session_state.setup_complete() {
                    return (false, None);
                }
                (true, data.cached_notify.take())
            })
            .map_err(TproxyError::shutdown)?;
        if !enable {
            return Ok(());
        }
        if let Some(notify) = notify {
            self.send_job(notify).await?;
        }
        self.downstream_data
            .with(|data| data.session_state = Sv1SessionState::Ready)
            .map_err(TproxyError::shutdown)?;
        Ok(())
    }

    /// Sends pending difficulty, then the required extranonce immediately before its job.
    /// A miner that has not announced prefix-update support is sent `client.reconnect` and
    /// disconnected without advertising work it would hash with the wrong extranonce. No
    /// capability timeout is introduced.
    async fn send_job(
        &self,
        notify: Arc<server_to_client::Notify>,
    ) -> TproxyResult<(), error::Downstream> {
        let messages = self
            .downstream_data
            .with(|data| {
                if data.cached_set_extranonce.is_some() && !data.supports_set_extranonce {
                    return None;
                }
                let difficulty = data.cached_set_difficulty.take();
                if difficulty.is_some() {
                    if let Some(target) = data.pending_target.take() {
                        data.target = target;
                    }
                    if let Some(hashrate) = data.pending_hashrate.take() {
                        data.hashrate = Some(hashrate);
                    }
                }
                let extranonce = data.cached_set_extranonce.take();
                let notify = if std::mem::take(&mut data.force_clean_jobs) && !notify.clean_jobs {
                    let mut clean = (*notify).clone();
                    clean.clean_jobs = true;
                    Arc::new(clean)
                } else {
                    notify
                };
                data.record_job_validation_context(&notify);
                data.record_last_job(&notify);
                Some((difficulty, extranonce, notify))
            })
            .map_err(TproxyError::shutdown)?;
        let Some((difficulty, extranonce, notify)) = messages else {
            warn!(
                downstream_id = self.downstream_id,
                "Asking SV1 miner to reconnect before a job requiring unannounced mining.set_extranonce support"
            );
            self.reconnect();
            return Ok(());
        };
        for message in [
            difficulty,
            extranonce.map(json_rpc::Message::from),
            Some(json_rpc::Message::from((*notify).clone())),
        ]
        .into_iter()
        .flatten()
        {
            self.downstream_io
                .downstream_sv1_sender
                .send(message)
                .await
                .map_err(|error| {
                    error!("Down: Failed to send mining job messages: {error:?}");
                    TproxyError::disconnect(TproxyErrorKind::ChannelErrorSender, self.downstream_id)
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_channel::unbounded;
    use stratum_apps::stratum_core::{bitcoin::hashes::Hash, sv1_api::json_rpc::Message};

    // JSON fixtures exercise the same typed events as production, without parsing internal messages
    // on the per-miner job delivery path.
    impl From<json_rpc::Message> for Sv1ServerEvent {
        fn from(message: json_rpc::Message) -> Self {
            let json_rpc::Message::Notification(notification) = &message else {
                panic!("expected a notification fixture");
            };
            match notification.method.as_str() {
                "mining.notify" => Self::Notify(Arc::new(
                    server_to_client::Notify::try_from(notification.clone()).unwrap(),
                )),
                "mining.set_difficulty" => Self::SetDifficulty(message),
                "mining.set_extranonce" => Self::SetExtranonce {
                    message: server_to_client::SetExtranonce::try_from(notification.clone())
                        .unwrap(),
                    notify_miner: true,
                },
                _ => panic!("unexpected notification fixture"),
            }
        }
    }

    #[test]
    fn accepted_share_cache_uses_channels_sv2_bound() {
        let mut cache = Sv1AcceptedShareCache::default();
        let mut first_hash = None;
        let mut last_hash = None;

        for value in 0..=MAX_SEEN_SHARES {
            let mut bytes = [0; 32];
            bytes[..8].copy_from_slice(&(value as u64).to_le_bytes());
            let hash = BlockHash::from_byte_array(bytes);
            first_hash.get_or_insert(hash);
            last_hash = Some(hash);
            assert!(cache.insert_if_new(hash));
        }

        assert_eq!(cache.len(), MAX_SEEN_SHARES);
        assert!(cache.insert_if_new(first_hash.unwrap()));
        assert!(!cache.insert_if_new(last_hash.unwrap()));
    }

    fn notify_with_clean_jobs(job_id: &str, clean_jobs: bool) -> Message {
        serde_json::from_value(serde_json::json!({
            "id": null,
            "method": "mining.notify",
            "params": [
                job_id,
                "00".repeat(32),
                "00",
                "00",
                [],
                "20000000",
                "1d00ffff",
                "00000001",
                clean_jobs
            ]
        }))
        .unwrap()
    }

    fn notify(job_id: &str) -> Message {
        notify_with_clean_jobs(job_id, true)
    }

    fn set_difficulty() -> Message {
        serde_json::from_value(serde_json::json!({
            "id": null,
            "method": "mining.set_difficulty",
            "params": [1.0]
        }))
        .unwrap()
    }

    fn assert_message_eq(actual: &Message, expected: &Message) {
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }

    #[tokio::test]
    async fn repeated_handshake_completion_preserves_cached_difficulty() {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (_sv1_server_sender, sv1_server_receiver) = unbounded();
        let old_target = Target::from_le_bytes([0x11; 32]);
        let new_target = Target::from_le_bytes([0x22; 32]);
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender,
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            old_target,
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );

        downstream
            .downstream_data
            .with(|data| {
                data.session_state = Sv1SessionState::Ready;
                data.cached_set_difficulty = Some(set_difficulty());
                data.pending_target = Some(new_target);
            })
            .unwrap();

        downstream.enable_notification_forwarding().await.unwrap();

        assert!(downstream_sv1_receiver.try_recv().is_err());
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(data.target, old_target);
                assert_eq!(data.pending_target, Some(new_target));
                assert!(data.cached_set_difficulty.is_some());
                assert_eq!(data.session_state, Sv1SessionState::Ready);
            })
            .unwrap();
    }

    #[tokio::test]
    async fn notify_queued_before_setup_completion_is_flushed() {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (sv1_server_message_sender, sv1_server_receiver) = unbounded();
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender.clone(),
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            Target::from_le_bytes([0x11; 32]),
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        let queued_notify = notify("queued");
        downstream
            .downstream_data
            .with(|data| {
                data.session_state = Sv1SessionState::Starting {
                    subscribed: true,
                    authorized: true,
                };
            })
            .unwrap();

        sv1_server_message_sender
            .send(Sv1ServerEvent::from(queued_notify.clone()))
            .await
            .unwrap();
        sv1_server_message_sender
            .send(Sv1ServerEvent::SetupComplete)
            .await
            .unwrap();

        downstream.handle_sv1_server_message().await.unwrap();
        assert!(downstream_sv1_receiver.try_recv().is_err());
        downstream.handle_sv1_server_message().await.unwrap();

        assert_message_eq(
            &downstream_sv1_receiver.recv().await.unwrap(),
            &queued_notify,
        );
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(data.session_state, Sv1SessionState::Ready);
                assert!(data.cached_notify.is_none());
            })
            .unwrap();
    }

    #[tokio::test]
    async fn difficulty_queued_before_setup_completion_waits_for_next_notify() {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (sv1_server_message_sender, sv1_server_receiver) = unbounded();
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender.clone(),
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            Target::from_le_bytes([0x11; 32]),
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        let queued_difficulty = set_difficulty();
        downstream
            .downstream_data
            .with(|data| {
                data.session_state = Sv1SessionState::Starting {
                    subscribed: true,
                    authorized: true,
                };
            })
            .unwrap();

        sv1_server_message_sender
            .send(Sv1ServerEvent::from(queued_difficulty.clone()))
            .await
            .unwrap();
        sv1_server_message_sender
            .send(Sv1ServerEvent::SetupComplete)
            .await
            .unwrap();

        downstream.handle_sv1_server_message().await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        assert!(downstream_sv1_receiver.try_recv().is_err());
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(data.session_state, Sv1SessionState::Ready);
                assert_message_eq(
                    data.cached_set_difficulty.as_ref().unwrap(),
                    &queued_difficulty,
                );
            })
            .unwrap();

        let next_notify = notify_with_clean_jobs("next", false);
        sv1_server_message_sender
            .send(Sv1ServerEvent::from(next_notify.clone()))
            .await
            .unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        assert_message_eq(
            &downstream_sv1_receiver.recv().await.unwrap(),
            &queued_difficulty,
        );
        let forwarded_notify = downstream_sv1_receiver.recv().await.unwrap();
        let Message::Notification(notification) = &forwarded_notify else {
            panic!("expected mining.notify");
        };
        let forwarded_notify = server_to_client::Notify::try_from(notification.clone()).unwrap();
        assert_eq!(forwarded_notify.job_id, "next");
        assert!(!forwarded_notify.clean_jobs);
    }

    #[tokio::test]
    async fn difficulty_change_preserves_previous_job_validation_context() {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (sv1_server_message_sender, sv1_server_receiver) = unbounded();
        let old_target = Target::from_le_bytes([0x22; 32]);
        let new_target = Target::from_le_bytes([0x11; 32]);
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender,
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            old_target,
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        downstream
            .downstream_data
            .with(|data| data.session_state = Sv1SessionState::Ready)
            .unwrap();

        sv1_server_message_sender
            .send(Sv1ServerEvent::from(notify("old")))
            .await
            .unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        downstream_sv1_receiver.recv().await.unwrap();

        downstream
            .downstream_data
            .with(|data| data.pending_target = Some(new_target))
            .unwrap();
        for message in [set_difficulty(), notify_with_clean_jobs("new", false)] {
            sv1_server_message_sender
                .send(Sv1ServerEvent::from(message))
                .await
                .unwrap();
            downstream.handle_sv1_server_message().await.unwrap();
        }
        downstream_sv1_receiver.recv().await.unwrap();
        let forwarded_notify = downstream_sv1_receiver.recv().await.unwrap();
        let Message::Notification(notification) = forwarded_notify else {
            panic!("expected mining.notify");
        };
        let forwarded_notify = server_to_client::Notify::try_from(notification).unwrap();
        assert!(!forwarded_notify.clean_jobs);

        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(data.target, new_target);
                assert_eq!(
                    data.job_validation_context("old")
                        .map(|context| context.target),
                    Some(old_target)
                );
                assert_eq!(
                    data.job_validation_context("new")
                        .map(|context| context.target),
                    Some(new_target)
                );
                assert_eq!(data.job_validation_contexts.len(), 2);
                assert!(
                    data.accepted_share_hashes
                        .insert_if_new(BlockHash::all_zeros())
                );
            })
            .unwrap();

        sv1_server_message_sender
            .send(Sv1ServerEvent::from(notify("new-tip")))
            .await
            .unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        downstream_sv1_receiver.recv().await.unwrap();

        downstream
            .downstream_data
            .with(|data| {
                assert!(data.job_validation_context("old").is_none());
                assert!(data.job_validation_context("new").is_none());
                assert!(data.job_validation_context("new-tip").is_some());
                assert_eq!(data.job_validation_contexts.len(), 1);
                assert_eq!(data.accepted_share_hashes.len(), 0);
            })
            .unwrap();
    }

    #[tokio::test]
    async fn extranonce_change_cannot_overtake_queued_setup_job() {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (sv1_server_message_sender, sv1_server_receiver) = unbounded();
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender.clone(),
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            Target::from_le_bytes([0x11; 32]),
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        let old_extranonce = downstream
            .downstream_data
            .with(|data| data.extranonce1.clone())
            .unwrap();
        let new_extranonce: Extranonce = vec![1, 2, 3, 4].try_into().unwrap();
        let old_notify = notify("old");
        downstream
            .downstream_data
            .with(|data| {
                data.supports_set_extranonce = true;
                data.session_state = Sv1SessionState::Starting {
                    subscribed: true,
                    authorized: true,
                };
                data.pending_set_extranonce_notifications = 1;
            })
            .unwrap();

        sv1_server_message_sender
            .send(Sv1ServerEvent::from(old_notify.clone()))
            .await
            .unwrap();
        sv1_server_message_sender
            .send(Sv1ServerEvent::SetupComplete)
            .await
            .unwrap();
        sv1_server_message_sender
            .send(Sv1ServerEvent::from(Message::from(
                server_to_client::SetExtranonce {
                    extra_nonce1: new_extranonce.clone(),
                    extra_nonce2_size: 4,
                },
            )))
            .await
            .unwrap();

        downstream.handle_sv1_server_message().await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();

        assert_message_eq(&downstream_sv1_receiver.recv().await.unwrap(), &old_notify);
        assert!(downstream_sv1_receiver.try_recv().is_err());
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(
                    data.job_validation_context("old")
                        .map(|context| context.extranonce),
                    Some(old_extranonce)
                );
                assert_eq!(data.extranonce1, new_extranonce);
                assert!(data.cached_set_extranonce.is_some());
                assert_eq!(data.session_state, Sv1SessionState::Ready);
                assert!(data.keepalive_timer_anchor.is_none());
            })
            .unwrap();
    }

    #[tokio::test]
    async fn extranonce_change_is_applied_to_the_next_job() {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (sv1_server_message_sender, sv1_server_receiver) = unbounded();
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender,
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            Target::from_le_bytes([0x11; 32]),
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        downstream
            .downstream_data
            .with(|data| {
                data.session_state = Sv1SessionState::Ready;
                data.supports_set_extranonce = true;
            })
            .unwrap();

        let old_extranonce = downstream
            .downstream_data
            .with(|data| data.extranonce1.clone())
            .unwrap();
        let new_extranonce: Extranonce = vec![1, 2, 3, 4].try_into().unwrap();
        let old_notify = notify("old");
        let new_notify = notify_with_clean_jobs("new", false);

        // The old job was queued before SetExtranoncePrefix, but the server installs the pending
        // marker before this downstream task processes either message.
        sv1_server_message_sender
            .send(Sv1ServerEvent::from(old_notify.clone()))
            .await
            .unwrap();
        downstream
            .downstream_data
            .with(|data| {
                data.pending_set_extranonce_notifications = 1;
            })
            .unwrap();
        sv1_server_message_sender
            .send(Sv1ServerEvent::from(Message::from(
                server_to_client::SetExtranonce {
                    extra_nonce1: new_extranonce.clone(),
                    extra_nonce2_size: 6,
                },
            )))
            .await
            .unwrap();

        downstream.handle_sv1_server_message().await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        sv1_server_message_sender
            .send(Sv1ServerEvent::from(new_notify.clone()))
            .await
            .unwrap();
        downstream.handle_sv1_server_message().await.unwrap();

        assert_message_eq(&downstream_sv1_receiver.recv().await.unwrap(), &old_notify);
        assert!(matches!(
            downstream_sv1_receiver.recv().await.unwrap(),
            Message::Notification(notification)
                if notification.method == "mining.set_extranonce"
        ));
        assert_message_eq(&downstream_sv1_receiver.recv().await.unwrap(), &new_notify);
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(
                    data.job_validation_context("old")
                        .map(|context| context.extranonce),
                    Some(old_extranonce)
                );
                assert_eq!(
                    data.job_validation_context("new")
                        .map(|context| context.extranonce),
                    Some(new_extranonce)
                );
                assert_eq!(
                    data.job_validation_context("old")
                        .map(|context| context.extranonce2_len),
                    Some(4)
                );
                assert_eq!(
                    data.job_validation_context("new")
                        .map(|context| context.extranonce2_len),
                    Some(6)
                );
                assert!(data.keepalive_timer_anchor.is_some());
            })
            .unwrap();
    }

    /// A ready, held downstream plus the miner's wire and the server's event sender.
    fn held_downstream(
        supports_set_extranonce: bool,
    ) -> (Downstream, Receiver<Message>, Sender<Sv1ServerEvent>) {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (events, sv1_server_receiver) = unbounded();
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender,
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            Target::from_le_bytes([0x11; 32]),
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        downstream
            .downstream_data
            .with(|data| {
                data.session_state = Sv1SessionState::Ready;
                data.supports_set_extranonce = supports_set_extranonce;
                data.held = true;
            })
            .unwrap();
        (downstream, downstream_sv1_receiver, events)
    }

    #[tokio::test]
    async fn hold_drops_the_ended_generations_jobs_and_keepalives_are_not_valid_work() {
        let (downstream, wire, events) = held_downstream(true);
        events
            .send(Sv1ServerEvent::from(notify("old")))
            .await
            .unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        let Message::Notification(sent) = wire.try_recv().unwrap() else {
            unreachable!();
        };
        let sent = server_to_client::Notify::try_from(sent).unwrap();
        events.send(Sv1ServerEvent::Hold).await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        for counter in [1, 2] {
            events
                .send(Sv1ServerEvent::HoldKeepalive {
                    counter,
                    interval_secs: 30,
                })
                .await
                .unwrap();
            downstream.handle_sv1_server_message().await.unwrap();
            let Message::Notification(keepalive) = wire.try_recv().unwrap() else {
                panic!("expected a keep-alive job");
            };
            let keepalive = server_to_client::Notify::try_from(keepalive).unwrap();
            assert_eq!(keepalive.job_id, format!("old#{counter}"));
            assert_eq!(keepalive.time.0, sent.time.0 + 30 * counter);
            assert!(!keepalive.clean_jobs);
        }

        downstream
            .downstream_data
            .with(|data| {
                assert!(data.job_validation_context("old").is_none());
                assert!(data.job_validation_context("old#1").is_none());
            })
            .unwrap();
    }

    #[test]
    fn hold_keepalive_stops_rolling_ntime_at_the_future_limit_but_keeps_changing_the_job() {
        let (downstream, _wire, _events) = held_downstream(true);
        let Message::Notification(job) = notify("job") else {
            unreachable!();
        };
        let job = server_to_client::Notify::try_from(job).unwrap();
        let base = job.time.0;
        downstream
            .downstream_data
            .with(|data| {
                data.record_last_job(&job);
                let first = data
                    .next_hold_keepalive(1, MAX_FUTURE_BLOCK_TIME + 60)
                    .unwrap();
                assert_eq!(first.time.0, base + MAX_FUTURE_BLOCK_TIME);
                let second = data.next_hold_keepalive(2, 30).unwrap();
                assert_eq!(second.time.0, base + MAX_FUTURE_BLOCK_TIME);
                assert_eq!(second.job_id, "job#2");
                data.held = false;
                assert!(data.next_hold_keepalive(3, 30).is_none());
            })
            .unwrap();
    }

    #[tokio::test]
    async fn reattach_with_a_new_extranonce_sends_it_before_a_clean_job() {
        let (downstream, wire, events) = held_downstream(true);
        let new_extranonce: Extranonce = vec![1, 2, 3, 4].try_into().unwrap();
        events
            .send(Sv1ServerEvent::Reattach {
                extranonce1: new_extranonce.clone(),
                extranonce2_len: 4,
            })
            .await
            .unwrap();
        events
            .send(Sv1ServerEvent::from(notify_with_clean_jobs("new", false)))
            .await
            .unwrap();
        for _ in 0..2 {
            downstream.handle_sv1_server_message().await.unwrap();
        }

        let expected_difficulty =
            build_sv1_set_difficulty_from_sv2_target_with_integer_power_of_two_rounding(
                Target::from_le_bytes([0x11; 32]),
                SV1_MIN_DIFFICULTY_FOR_INTEGER_POWER_OF_TWO_ROUNDING,
            )
            .unwrap();
        assert_message_eq(&wire.try_recv().unwrap(), &expected_difficulty);
        let Message::Notification(update) = wire.try_recv().unwrap() else {
            panic!("expected mining.set_extranonce");
        };
        assert_eq!(update.method, "mining.set_extranonce");
        assert_message_eq(
            &wire.try_recv().unwrap(),
            &notify_with_clean_jobs("new", true),
        );
        assert!(!downstream.is_disconnected());
        assert_eq!(
            downstream
                .reattach_counters
                .set_extranonce
                .load(Ordering::Relaxed),
            1
        );
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(
                    data.job_validation_context("new").map(|c| c.extranonce),
                    Some(new_extranonce)
                );
            })
            .unwrap();
    }

    #[tokio::test]
    async fn reattach_with_the_same_extranonce_needs_only_a_clean_job() {
        let (downstream, wire, events) = held_downstream(false);
        let extranonce1 = downstream
            .downstream_data
            .with(|data| data.extranonce1.clone())
            .unwrap();
        events
            .send(Sv1ServerEvent::Reattach {
                extranonce1,
                extranonce2_len: 4,
            })
            .await
            .unwrap();
        events
            .send(Sv1ServerEvent::from(notify_with_clean_jobs("new", false)))
            .await
            .unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();

        let Message::Notification(difficulty) = wire.try_recv().unwrap() else {
            panic!("expected mining.set_difficulty");
        };
        assert_eq!(difficulty.method, "mining.set_difficulty");
        assert_message_eq(
            &wire.try_recv().unwrap(),
            &notify_with_clean_jobs("new", true),
        );
        assert!(wire.try_recv().is_err());
        assert!(!downstream.was_asked_to_reconnect());
    }

    #[tokio::test]
    async fn reattach_asks_a_miner_that_cannot_take_a_new_extranonce_to_reconnect() {
        let (downstream, wire, events) = held_downstream(false);
        events
            .send(Sv1ServerEvent::Reattach {
                extranonce1: vec![1, 2, 3, 4].try_into().unwrap(),
                extranonce2_len: 4,
            })
            .await
            .unwrap();
        downstream.handle_sv1_server_message().await.unwrap();

        let Message::Notification(reconnect) = wire.try_recv().unwrap() else {
            panic!("expected client.reconnect");
        };
        assert_eq!(reconnect.method, "client.reconnect");
        assert!(downstream.was_asked_to_reconnect());
        assert_eq!(
            downstream
                .reattach_counters
                .reconnect
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn consecutive_extranonce_changes_wait_for_the_latest_job() {
        let (downstream_sv1_sender, _downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (sv1_server_message_sender, sv1_server_receiver) = unbounded();
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender,
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            Target::from_le_bytes([0x11; 32]),
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        downstream
            .downstream_data
            .with(|data| {
                data.supports_set_extranonce = true;
                data.session_state = Sv1SessionState::Ready;
                data.pending_set_extranonce_notifications = 2;
            })
            .unwrap();
        let first_extranonce: Extranonce = vec![1, 2, 3, 4].try_into().unwrap();
        let second_extranonce: Extranonce = vec![5, 6, 7, 8].try_into().unwrap();
        for message in [
            Message::from(server_to_client::SetExtranonce {
                extra_nonce1: first_extranonce.clone(),
                extra_nonce2_size: 4,
            }),
            notify_with_clean_jobs("first", false),
            Message::from(server_to_client::SetExtranonce {
                extra_nonce1: second_extranonce.clone(),
                extra_nonce2_size: 4,
            }),
            notify_with_clean_jobs("second", false),
        ] {
            sv1_server_message_sender
                .send(Sv1ServerEvent::from(message))
                .await
                .unwrap();
        }

        downstream.handle_sv1_server_message().await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(data.pending_set_extranonce_notifications, 1);
                assert!(data.keepalive_timer_anchor.is_none());
                assert_eq!(
                    data.job_validation_context("first")
                        .map(|context| context.extranonce),
                    Some(first_extranonce)
                );
            })
            .unwrap();

        downstream.handle_sv1_server_message().await.unwrap();
        downstream.handle_sv1_server_message().await.unwrap();
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(data.pending_set_extranonce_notifications, 0);
                assert!(data.keepalive_timer_anchor.is_some());
                assert_eq!(
                    data.job_validation_context("second")
                        .map(|context| context.extranonce),
                    Some(second_extranonce)
                );
            })
            .unwrap();
    }

    #[tokio::test]
    async fn presubscribe_event_does_not_overwrite_a_newer_response_prefix() {
        let (downstream_sv1_sender, downstream_sv1_receiver) = unbounded();
        let (_downstream_sender, downstream_receiver) = unbounded();
        let (sv1_server_sender, _sv1_server_receiver) = unbounded();
        let (sv1_server_message_sender, sv1_server_receiver) = unbounded();
        let downstream = Downstream::new(
            1,
            downstream_sv1_sender,
            downstream_receiver,
            sv1_server_sender,
            sv1_server_receiver,
            Target::from_le_bytes([0x11; 32]),
            None,
            None,
            #[cfg(feature = "monitoring")]
            "127.0.0.1".parse().unwrap(),
            CancellationToken::new(),
        );
        let new_extranonce: Extranonce = vec![1, 2, 3, 4].try_into().unwrap();
        downstream
            .downstream_data
            .with(|data| {
                let Sv1ServerEvent::Notify(notify) = Sv1ServerEvent::from(notify("old")) else {
                    panic!("expected notify fixture");
                };
                data.cached_notify = Some(notify);
                data.pending_set_extranonce_notifications = 1;
                // The server has already applied the latest prefix for response construction.
                data.extranonce1 = new_extranonce.clone();
            })
            .unwrap();
        sv1_server_message_sender
            .send(Sv1ServerEvent::SetExtranonce {
                message: server_to_client::SetExtranonce {
                    extra_nonce1: vec![9; 4].try_into().unwrap(),
                    extra_nonce2_size: 4,
                },
                notify_miner: false,
            })
            .await
            .unwrap();

        downstream.handle_sv1_server_message().await.unwrap();

        assert!(downstream_sv1_receiver.try_recv().is_err());
        assert!(!downstream.is_disconnected());
        downstream
            .downstream_data
            .with(|data| {
                assert_eq!(data.extranonce1, new_extranonce);
                assert!(data.cached_notify.is_none());
                assert!(data.cached_set_extranonce.is_none());
                assert_eq!(data.pending_set_extranonce_notifications, 0);
                assert!(data.keepalive_timer_anchor.is_none());
            })
            .unwrap();
    }

    #[test]
    fn session_setup_completes_once_in_either_request_order() {
        for requests in [
            [Sv1SetupRequest::Subscribe, Sv1SetupRequest::Authorize],
            [Sv1SetupRequest::Authorize, Sv1SetupRequest::Subscribe],
        ] {
            let mut state = Sv1SessionState::default();

            assert!(!state.record_response(requests[0]));
            assert!(state.record_response(requests[1]));
            assert!(!state.is_ready());
            assert!(!state.record_response(requests[0]));
            assert!(!state.record_response(requests[1]));
        }
    }
}
