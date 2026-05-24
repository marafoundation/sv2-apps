use stratum_apps::{
    key_utils::Secp256k1PublicKey,
    network_helpers::{
        connect_with_noise,
        noise_stream::{NoiseTcpReadHalf, NoiseTcpWriteHalf},
        TCP_CONNECT_TIMEOUT,
    },
    stratum_core::{
        codec_sv2::{StandardEitherFrame, StandardSv2Frame},
        common_messages_sv2::{Protocol, SetupConnection},
        parsers_sv2::{AnyMessage, CommonMessages},
    },
};
use tokio::net::TcpStream;
use tracing::{error, info};

use crate::config::Config;

pub type Message = AnyMessage<'static>;
pub type Reader = NoiseTcpReadHalf<Message>;
pub type Writer = NoiseTcpWriteHalf<Message>;

pub async fn connect_upstream(
    config: &Config,
) -> Result<(Reader, Writer), String> {
    info!("Connecting to upstream pool at {}", config.upstream_address);

    let stream = tokio::time::timeout(
        TCP_CONNECT_TIMEOUT,
        TcpStream::connect(&config.upstream_address),
    )
    .await
    .map_err(|_| "TCP connect timed out".to_string())?
    .map_err(|e| format!("TCP connect failed: {e}"))?;

    info!("TCP connected to {}", config.upstream_address);

    let authority_pub_key: Option<Secp256k1PublicKey> = config
        .upstream_authority_pubkey
        .as_ref()
        .map(|s| s.parse::<Secp256k1PublicKey>())
        .transpose()
        .map_err(|e| format!("Invalid upstream authority pubkey: {e}"))?;

    let noise_stream = connect_with_noise(stream, authority_pub_key)
        .await
        .map_err(|e| format!("Noise handshake failed: {e:?}"))?;

    info!("Noise handshake complete with upstream");
    Ok(noise_stream.into_split())
}

pub async fn setup_connection(
    reader: &mut Reader,
    writer: &mut Writer,
) -> Result<(), String> {
    let setup = SetupConnection {
        protocol: Protocol::MiningProtocol,
        min_version: 2,
        max_version: 2,
        flags: 0,
        endpoint_host: "0.0.0.0".to_string().try_into().unwrap(),
        endpoint_port: 0,
        vendor: "shape-proxy".to_string().try_into().unwrap(),
        hardware_version: "".to_string().try_into().unwrap(),
        firmware: "".to_string().try_into().unwrap(),
        device_id: "shape-proxy-0".to_string().try_into().unwrap(),
    };

    let frame: StandardSv2Frame<Message> = Message::Common(setup.into())
        .try_into()
        .map_err(|e| format!("Frame encode error: {e:?}"))?;
    writer
        .write_frame(frame.into())
        .await
        .map_err(|e| format!("Write error: {e:?}"))?;
    info!("Sent SetupConnection to upstream");

    let response = reader
        .read_frame()
        .await
        .map_err(|e| format!("Read error: {e:?}"))?;
    let mut frame: StandardSv2Frame<Message> = response
        .try_into()
        .map_err(|_| "Invalid frame from upstream".to_string())?;
    let msg_type = frame.get_header().unwrap().msg_type();
    let payload = frame.payload();

    let msg: CommonMessages = (msg_type, payload)
        .try_into()
        .map_err(|_| format!("Failed to parse message type {msg_type}"))?;

    match msg {
        CommonMessages::SetupConnectionSuccess(m) => {
            info!(
                "Upstream SetupConnectionSuccess: version={}, flags={}",
                m.used_version, m.flags
            );
            Ok(())
        }
        CommonMessages::SetupConnectionError(m) => {
            let code = std::str::from_utf8(m.error_code.as_ref()).unwrap_or("unknown");
            error!("Upstream SetupConnectionError: {}", code);
            Err(format!("SetupConnectionError: {code}"))
        }
        _ => Err("Unexpected message type during setup".to_string()),
    }
}
