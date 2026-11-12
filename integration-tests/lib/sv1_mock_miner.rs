//! A raw-TCP SV1 miner for tests that need to observe a single SV1 connection over time.
//!
//! Unlike [`crate::sv1_sniffer::SnifferSV1`], which sits between two roles, this miner is the
//! client itself. It records every message the server sends, knows its own local address (so a
//! test can tell whether the *same* connection survived an event) and notices when the server
//! closes the connection.

use serde_json::{Value, json};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpStream, tcp::OwnedWriteHalf},
    sync::Notify,
    time::Instant,
};

const CONFIGURE_ID: u64 = 1;
const SUBSCRIBE_ID: u64 = 2;
const EXTRANONCE_SUBSCRIBE_ID: u64 = 3;
const AUTHORIZE_ID: u64 = 4;
const FIRST_SUBMIT_ID: u64 = 5;

const SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(30);

/// Index of `ntime` in the params of a `mining.notify`.
const NOTIFY_NTIME_INDEX: usize = 7;

/// Which optional handshake steps the miner performs, and the worker it authorizes as.
#[derive(Debug, Clone)]
pub struct Sv1MockMinerOptions {
    pub extranonce_subscribe: bool,
    pub configure_version_rolling: bool,
    pub worker: String,
}

impl Default for Sv1MockMinerOptions {
    fn default() -> Self {
        Self {
            extranonce_subscribe: false,
            configure_version_rolling: false,
            worker: "mock_miner".to_string(),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    messages: Vec<Value>,
    /// Index of the first message `wait_for_method` has not yet looked at.
    cursor: usize,
    closed: bool,
    extranonce1: Option<String>,
    extranonce2_size: usize,
    /// `ntime` of the latest `mining.notify` seen for each job id.
    job_ntime: HashMap<String, String>,
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    changed: Notify,
}

impl Shared {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("mock miner state poisoned")
    }

    /// Re-evaluates `check` every time the state changes until it yields a value or `timeout`
    /// elapses.
    async fn wait_until<T>(
        &self,
        timeout: Duration,
        mut check: impl FnMut(&mut State) -> Option<T>,
    ) -> Option<T> {
        let deadline = Instant::now() + timeout;
        loop {
            // Register interest before checking so a change in between is not missed.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            if let Some(value) = check(&mut self.state()) {
                return Some(value);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return check(&mut self.state());
            }
        }
    }

    fn record(&self, message: Value) {
        let mut state = self.state();
        if message.get("id").and_then(Value::as_u64) == Some(SUBSCRIBE_ID) {
            if let Some([_, extranonce1, extranonce2_size]) =
                message["result"].as_array().map(Vec::as_slice)
            {
                state.extranonce1 = extranonce1.as_str().map(str::to_string);
                state.extranonce2_size = extranonce2_size.as_u64().unwrap_or_default() as usize;
            }
        }
        match message["method"].as_str() {
            Some("mining.set_extranonce") => {
                if let (Some(extranonce1), Some(size)) =
                    (message["params"][0].as_str(), message["params"][1].as_u64())
                {
                    state.extranonce1 = Some(extranonce1.to_string());
                    state.extranonce2_size = size as usize;
                }
            }
            Some("mining.notify") => {
                if let (Some(job_id), Some(ntime)) = (
                    message["params"][0].as_str(),
                    message["params"][NOTIFY_NTIME_INDEX].as_str(),
                ) {
                    state
                        .job_ntime
                        .insert(job_id.to_string(), ntime.to_string());
                }
            }
            _ => {}
        }
        state.messages.push(message);
        drop(state);
        self.changed.notify_waiters();
    }

    fn close(&self) {
        self.state().closed = true;
        self.changed.notify_waiters();
    }
}

/// A raw-TCP SV1 miner that records everything the server sends it.
#[derive(Debug)]
pub struct Sv1MockMiner {
    local_addr: SocketAddr,
    worker: String,
    shared: Arc<Shared>,
    writer: tokio::sync::Mutex<OwnedWriteHalf>,
    next_submit_id: AtomicU64,
}

impl Sv1MockMiner {
    /// Connects and performs the SV1 handshake (`mining.configure` and
    /// `mining.extranonce.subscribe` if enabled, `mining.subscribe`, `mining.authorize`), then
    /// keeps reading in the background.
    ///
    /// Returns once the `mining.subscribe` response has arrived, so [`Self::extranonce`] is
    /// meaningful. Retries the connection until the listener is up, for at most
    /// [`SUBSCRIBE_TIMEOUT`]. Panics if it never is or no such response arrives.
    pub async fn connect(addr: SocketAddr, opts: Sv1MockMinerOptions) -> Self {
        let deadline = Instant::now() + SUBSCRIBE_TIMEOUT;
        let stream = loop {
            match TcpStream::connect(addr).await {
                Ok(stream) => break stream,
                Err(e) if Instant::now() < deadline => {
                    tracing::debug!("mock miner: {addr} not accepting yet ({e}), retrying");
                    tokio::time::sleep(crate::utils::CONNECT_RETRY_INTERVAL).await;
                }
                Err(e) => panic!("mock miner could not connect to {addr}: {e}"),
            }
        };
        let local_addr = stream.local_addr().expect("mock miner has a local address");
        let (reader, writer) = stream.into_split();

        let shared = Arc::new(Shared::default());
        tokio::spawn({
            let shared = shared.clone();
            async move {
                let mut lines = BufReader::new(reader).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    match serde_json::from_str(&line) {
                        Ok(message) => shared.record(message),
                        Err(_) => shared.record(Value::String(line)),
                    }
                }
                shared.close();
            }
        });

        let miner = Self {
            local_addr,
            worker: opts.worker.clone(),
            shared,
            writer: tokio::sync::Mutex::new(writer),
            next_submit_id: AtomicU64::new(FIRST_SUBMIT_ID),
        };

        if opts.configure_version_rolling {
            miner
                .send(json!({
                    "id": CONFIGURE_ID,
                    "method": "mining.configure",
                    "params": [
                        ["version-rolling"],
                        {
                            "version-rolling.mask": "1fffe000",
                            "version-rolling.min-bit-count": 2,
                        },
                    ],
                }))
                .await;
        }
        miner
            .send(json!({
                "id": SUBSCRIBE_ID,
                "method": "mining.subscribe",
                "params": ["sv1-mock-miner"],
            }))
            .await;
        if opts.extranonce_subscribe {
            miner
                .send(json!({
                    "id": EXTRANONCE_SUBSCRIBE_ID,
                    "method": "mining.extranonce.subscribe",
                    "params": [],
                }))
                .await;
        }
        miner
            .send(json!({
                "id": AUTHORIZE_ID,
                "method": "mining.authorize",
                "params": [opts.worker, "x"],
            }))
            .await;

        let subscribed = miner
            .shared
            .wait_until(SUBSCRIBE_TIMEOUT, |state| {
                state.extranonce1.as_ref().map(|_| ())
            })
            .await;
        assert!(
            subscribed.is_some(),
            "no mining.subscribe response; received: {}",
            miner.log()
        );
        miner
    }

    /// The local address of this miner's TCP connection, recorded at connect time.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Whether the server closed the connection (EOF or read error).
    pub fn is_closed(&self) -> bool {
        self.shared.state().closed
    }

    /// Waits up to `timeout` for the server to close the connection.
    pub async fn wait_closed(&self, timeout: Duration) -> bool {
        self.shared
            .wait_until(timeout, |state| state.closed.then_some(()))
            .await
            .is_some()
    }

    /// Waits for the next server message with the given `method` that this function has not
    /// already looked at, and returns its params. Messages that arrived before the call count,
    /// so a test cannot race the server. Skipped messages stay available through
    /// [`Self::received`]. Panics, printing everything received, on timeout.
    pub async fn wait_for_method(&self, method: &str, timeout: Duration) -> Value {
        let found = self
            .shared
            .wait_until(timeout, |state| {
                while state.cursor < state.messages.len() {
                    let message = &state.messages[state.cursor];
                    state.cursor += 1;
                    if message["method"].as_str() == Some(method) {
                        return Some(message["params"].clone());
                    }
                }
                None
            })
            .await;
        found.unwrap_or_else(|| panic!("no {method} within {timeout:?}; received: {}", self.log()))
    }

    /// Every message received so far, responses and notifications, in arrival order.
    pub fn received(&self) -> Vec<Value> {
        self.shared.state().messages.clone()
    }

    /// Extranonce1 (hex) and extranonce2 size, from the `mining.subscribe` response and any later
    /// `mining.set_extranonce`.
    pub fn extranonce(&self) -> (String, usize) {
        let state = self.shared.state();
        (
            state.extranonce1.clone().unwrap_or_default(),
            state.extranonce2_size,
        )
    }

    /// Submits a dummy share for `job_id` and returns the server's JSON-RPC response. The share
    /// uses the job's `ntime` and an all-zero extranonce2 of the current size. Panics if the job
    /// was never notified or no response arrives within `timeout`.
    pub async fn submit(&self, job_id: &str, timeout: Duration) -> Value {
        let (ntime, extranonce2) = {
            let state = self.shared.state();
            let ntime = state
                .job_ntime
                .get(job_id)
                .unwrap_or_else(|| panic!("no mining.notify seen for job {job_id}"))
                .clone();
            (ntime, "00".repeat(state.extranonce2_size))
        };
        let id = self.next_submit_id.fetch_add(1, Ordering::SeqCst);
        self.send(json!({
            "id": id,
            "method": "mining.submit",
            "params": [self.worker, job_id, extranonce2, ntime, "00000000"],
        }))
        .await;

        let response = self
            .shared
            .wait_until(timeout, |state| {
                state
                    .messages
                    .iter()
                    .find(|m| m["id"].as_u64() == Some(id) && m.get("method").is_none())
                    .cloned()
            })
            .await;
        response.unwrap_or_else(|| {
            panic!(
                "no response to mining.submit within {timeout:?}; received: {}",
                self.log()
            )
        })
    }

    async fn send(&self, message: Value) {
        let mut line = message.to_string();
        line.push('\n');
        // The server may already have closed the connection; that is observable via `is_closed`.
        let _ = self.writer.lock().await.write_all(line.as_bytes()).await;
    }

    fn log(&self) -> String {
        Value::Array(self.received()).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{io::AsyncWriteExt, net::TcpListener, sync::oneshot};

    const TIMEOUT: Duration = Duration::from_secs(5);

    /// A minimal SV1 server: answers the handshake, sends a difficulty and a job after
    /// `mining.authorize`, and after the first `mining.submit` accepts it, changes the
    /// extranonce, asks the miner to reconnect and closes the connection.
    async fn fake_server(listener: TcpListener, peer: oneshot::Sender<SocketAddr>) {
        let (stream, peer_addr) = listener.accept().await.unwrap();
        peer.send(peer_addr).unwrap();
        let (reader, mut writer) = stream.into_split();
        let mut lines = BufReader::new(reader).lines();
        while let Some(line) = lines.next_line().await.unwrap() {
            let request: Value = serde_json::from_str(&line).unwrap();
            let id = request["id"].clone();
            let mut out = Vec::new();
            match request["method"].as_str().unwrap() {
                "mining.subscribe" => out.push(json!({
                    "id": id,
                    "result": [[["mining.notify", "1"]], "aabbccdd", 4],
                    "error": null,
                })),
                "mining.authorize" => {
                    out.push(json!({"id": id, "result": true, "error": null}));
                    out.push(json!({
                        "id": null,
                        "method": "mining.set_difficulty",
                        "params": [1.0],
                    }));
                    out.push(json!({
                        "id": null,
                        "method": "mining.notify",
                        "params": [
                            "job1", "00", "01", "02", [], "20000000", "1d00ffff", "6553f100",
                            true,
                        ],
                    }));
                }
                "mining.submit" => {
                    assert_eq!(request["params"][1], "job1");
                    assert_eq!(request["params"][2], "00000000");
                    assert_eq!(request["params"][3], "6553f100");
                    out.push(json!({"id": id, "result": true, "error": null}));
                    out.push(json!({
                        "id": null,
                        "method": "mining.set_extranonce",
                        "params": ["11223344", 8],
                    }));
                    out.push(json!({
                        "id": null,
                        "method": "client.reconnect",
                        "params": ["pool.example", 3333, 0],
                    }));
                }
                _ => out.push(json!({"id": id, "result": true, "error": null})),
            }
            let closing = request["method"] == "mining.submit";
            for message in out {
                writer
                    .write_all(format!("{message}\n").as_bytes())
                    .await
                    .unwrap();
            }
            if closing {
                return;
            }
        }
    }

    #[tokio::test]
    async fn mock_miner_tracks_its_connection_and_server_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (peer_tx, peer_rx) = oneshot::channel();
        tokio::spawn(fake_server(listener, peer_tx));

        let miner = Sv1MockMiner::connect(
            addr,
            Sv1MockMinerOptions {
                extranonce_subscribe: true,
                configure_version_rolling: true,
                worker: "worker.1".to_string(),
            },
        )
        .await;

        assert_eq!(miner.local_addr(), peer_rx.await.unwrap());
        assert_eq!(miner.extranonce(), ("aabbccdd".to_string(), 4));
        assert!(!miner.is_closed());
        assert!(!miner.wait_closed(Duration::from_millis(100)).await);

        // The difficulty arrived first; waiting for the job skips it but keeps it recorded.
        let notify = miner.wait_for_method("mining.notify", TIMEOUT).await;
        assert_eq!(notify[0], "job1");
        let methods: Vec<_> = miner
            .received()
            .iter()
            .filter_map(|m| m["method"].as_str().map(str::to_string))
            .collect();
        assert_eq!(methods, ["mining.set_difficulty", "mining.notify"]);

        let response = miner.submit("job1", TIMEOUT).await;
        assert_eq!(response["result"], true);

        // Messages already consumed are not returned again.
        let reconnect = miner.wait_for_method("client.reconnect", TIMEOUT).await;
        assert_eq!(reconnect[1], 3333);
        assert_eq!(miner.extranonce(), ("11223344".to_string(), 8));

        assert!(miner.wait_closed(TIMEOUT).await);
        assert!(miner.is_closed());
    }

    #[tokio::test]
    #[should_panic(expected = "no mining.set_difficulty")]
    async fn wait_for_method_does_not_return_consumed_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (peer_tx, _peer_rx) = oneshot::channel();
        tokio::spawn(fake_server(listener, peer_tx));

        let miner = Sv1MockMiner::connect(addr, Sv1MockMinerOptions::default()).await;
        miner.wait_for_method("mining.notify", TIMEOUT).await;
        miner
            .wait_for_method("mining.set_difficulty", Duration::from_millis(200))
            .await;
    }
}
