use std::net::SocketAddr;

use stratum_apps::{
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
    network_helpers::accept_noise_connection,
    stratum_core::{
        codec_sv2::StandardSv2Frame,
        common_messages_sv2::{Protocol, SetupConnectionSuccess},
        parsers_sv2::{CommonMessages, Mining},
    },
};
use tokio::{net::TcpStream, sync::mpsc};
use tracing::{debug, error, info, warn};

use crate::upstream::{Message, Reader, Writer};

/// Unique identifier for a downstream connection.
pub type DownstreamId = u64;

/// Events sent from downstream read tasks into the proxy core.
pub enum DownstreamEvent {
    /// A new downstream completed noise handshake and SetupConnection.
    Connected { id: DownstreamId, writer: Writer },
    /// A parsed mining message from the downstream.
    Message {
        id: DownstreamId,
        msg: Mining<'static>,
    },
    /// The downstream disconnected (read returned error or EOF).
    Disconnected { id: DownstreamId },
}

impl std::fmt::Debug for DownstreamEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected { id, .. } => f.debug_struct("Connected").field("id", id).finish(),
            Self::Message { id, msg } => {
                f.debug_struct("Message").field("id", id).field("msg", msg).finish()
            }
            Self::Disconnected { id } => f.debug_struct("Disconnected").field("id", id).finish(),
        }
    }
}

/// Accept a noise connection on the given TcpStream, perform the SetupConnection handshake,
/// then spawn a read loop that forwards mining messages into `event_tx`.
pub async fn accept_downstream(
    stream: TcpStream,
    peer_addr: SocketAddr,
    id: DownstreamId,
    pub_key: Secp256k1PublicKey,
    secret_key: Secp256k1SecretKey,
    cert_validity: u64,
    event_tx: mpsc::UnboundedSender<DownstreamEvent>,
) {
    info!(downstream_id = id, %peer_addr, "Accepting downstream connection");

    let noise_stream =
        match accept_noise_connection::<Message>(stream, pub_key, secret_key, cert_validity).await {
            Ok(s) => s,
            Err(e) => {
                error!(downstream_id = id, %peer_addr, "Noise handshake failed: {e}");
                return;
            }
        };

    let (mut reader, mut writer) = noise_stream.into_split();

    // Wait for SetupConnection from the downstream miner and respond.
    if let Err(e) = handle_setup_connection(&mut reader, &mut writer, id).await {
        error!(downstream_id = id, "SetupConnection handshake failed: {e}");
        return;
    }

    info!(downstream_id = id, %peer_addr, "Downstream setup complete");

    // Notify ProxyCore that this downstream is ready.
    if event_tx
        .send(DownstreamEvent::Connected { id, writer })
        .is_err()
    {
        return; // ProxyCore dropped
    }

    // Run read loop inline (this function was spawned by the caller).
    downstream_read_loop(reader, id, event_tx).await;
}

/// Handle the SetupConnection message from the miner and respond with SetupConnectionSuccess.
async fn handle_setup_connection(
    reader: &mut Reader,
    writer: &mut Writer,
    id: DownstreamId,
) -> Result<(), String> {
    let response = reader
        .read_frame()
        .await
        .map_err(|e| format!("Read error waiting for SetupConnection: {e:?}"))?;

    let mut frame: StandardSv2Frame<Message> = response
        .try_into()
        .map_err(|_| "Invalid frame from downstream".to_string())?;

    let msg_type = frame.get_header().unwrap().msg_type();
    let payload = frame.payload();

    let msg: CommonMessages = (msg_type, payload)
        .try_into()
        .map_err(|_| format!("Failed to parse message type 0x{msg_type:02x}"))?;

    match msg {
        CommonMessages::SetupConnection(setup) => {
            if setup.protocol != Protocol::MiningProtocol {
                return Err("Downstream requested non-mining protocol".to_string());
            }
            info!(
                downstream_id = id,
                "Downstream SetupConnection: vendor={}, version={}-{}",
                setup.vendor.as_utf8_or_hex(),
                setup.min_version,
                setup.max_version,
            );
        }
        _ => {
            return Err(format!(
                "Expected SetupConnection, got msg_type=0x{msg_type:02x}"
            ));
        }
    }

    // Respond with SetupConnectionSuccess.
    let success = SetupConnectionSuccess {
        used_version: 2,
        flags: 0,
    };
    let frame: StandardSv2Frame<Message> = Message::Common(success.into())
        .try_into()
        .map_err(|e| format!("Frame encode error: {e:?}"))?;
    writer
        .write_frame(frame.into())
        .await
        .map_err(|e| format!("Write error: {e:?}"))?;

    info!(downstream_id = id, "Sent SetupConnectionSuccess to downstream");
    Ok(())
}

/// Read loop that deserializes frames and forwards Mining messages to ProxyCore.
async fn downstream_read_loop(
    mut reader: Reader,
    id: DownstreamId,
    event_tx: mpsc::UnboundedSender<DownstreamEvent>,
) {
    loop {
        let frame = match reader.read_frame().await {
            Ok(f) => f,
            Err(e) => {
                debug!(downstream_id = id, "Downstream read error: {e:?}");
                let _ = event_tx.send(DownstreamEvent::Disconnected { id });
                return;
            }
        };

        let mut sv2_frame: StandardSv2Frame<Message> = match frame.try_into() {
            Ok(f) => f,
            Err(_) => {
                warn!(downstream_id = id, "Invalid frame from downstream");
                let _ = event_tx.send(DownstreamEvent::Disconnected { id });
                return;
            }
        };

        let msg_type = sv2_frame.get_header().unwrap().msg_type();
        let payload = sv2_frame.payload();

        // Try parsing as Mining message.
        let mining_msg: Result<Mining<'_>, _> = (msg_type, payload).try_into();
        match mining_msg {
            Ok(m) => {
                let m_static = m.into_static();
                debug!(downstream_id = id, "Downstream mining msg: {m_static}");
                if event_tx
                    .send(DownstreamEvent::Message { id, msg: m_static })
                    .is_err()
                {
                    return;
                }
            }
            Err(_) => {
                warn!(
                    downstream_id = id,
                    "Unhandled message type 0x{msg_type:02x} from downstream"
                );
            }
        }
    }
}
