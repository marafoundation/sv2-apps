use std::sync::Arc;

use async_channel::{Receiver, Sender};
use bitcoin_core_sv2::CancellationToken;
use stratum_apps::{
    fallback_coordinator::FallbackCoordinator,
    network_helpers::noise_stream::{NoiseTcpReadHalf, NoiseTcpWriteHalf},
    stratum_core::framing_sv2::framing::Frame,
    task_manager::TaskManager,
    utils::types::{Message, Sv2Frame},
};
use tracing::{error, trace, warn, Instrument as _};

/// Spawns async reader and writer tasks for handling framed I/O with shutdown support.
#[track_caller]
#[allow(clippy::too_many_arguments)]
#[cfg_attr(not(test), hotpath::measure)]
pub fn spawn_io_tasks(
    task_manager: Arc<TaskManager>,
    mut reader: NoiseTcpReadHalf<Message>,
    mut writer: NoiseTcpWriteHalf<Message>,
    outbound_rx: Receiver<Sv2Frame>,
    inbound_tx: Sender<Sv2Frame>,
    cancellation_token: CancellationToken,
    fallback_coordinator: FallbackCoordinator,
) {
    let caller = std::panic::Location::caller();
    let outbound_rx_clone = outbound_rx.clone();
    // Dedicated token for reader→writer notification on read errors.
    // When the reader fails, it cancels this token to unblock the writer.
    let io_cancellation = CancellationToken::new();

    {
        let cancellation_token_clone = cancellation_token.clone();
        let fallback_coordinator_clone = fallback_coordinator.clone();
        let io_cancellation_clone = io_cancellation.clone();

        task_manager.spawn(
            async move {
                // we just spawned a new task that's relevant to fallback coordination
                // so register it with the fallback coordinator
                let fallback_handler = fallback_coordinator_clone.register();

                // get the cancellation token that signals fallback
                let fallback_token = fallback_coordinator_clone.token();

                trace!("Reader task started");
                loop {
                    tokio::select! {
                        _ = cancellation_token_clone.cancelled() => {
                            trace!("Received shutdown signal");
                            inbound_tx.close();
                            break;
                        }
                        _ = fallback_token.cancelled() => {
                            trace!("Received fallback signal");
                            inbound_tx.close();
                            break;
                        }
                        res = reader.read_frame() => {
                            match res {
                                Ok(frame) => {
                                    match frame {
                                        Frame::HandShake(frame) => {
                                            error!(?frame, "Received handshake frame");
                                            drop(frame);
                                            break;
                                        },
                                        Frame::Sv2(sv2_frame) => {
                                            trace!("Received inbound frame");
                                            if let Err(e) = inbound_tx.send(sv2_frame).await {
                                                inbound_tx.close();
                                                error!(error=?e, "Failed to forward inbound frame");
                                                break;
                                            }
                                        },
                                    }
                                }
                                Err(e) => {
                                    error!(error=?e, "Reader error");
                                    inbound_tx.close();
                                    break;
                                }
                            }
                        }
                    }
                }
                inbound_tx.close();
                outbound_rx_clone.close();
                // Signal the writer task to exit so it drops its inbound_tx clone,
                // allowing tp_receiver.recv() to return Err and propagate fallback.
                io_cancellation_clone.cancel();
                drop(inbound_tx);
                drop(outbound_rx_clone);

                // signal fallback coordinator that this task has completed its cleanup
                fallback_handler.done();
                warn!("Reader task exited.");
            }
            .instrument(tracing::trace_span!(
                "reader_task",
                spawned_at = %format!("{}:{}", caller.file(), caller.line())
            )),
        );
    }

    {
        let fallback_coordinator_clone = fallback_coordinator.clone();
        task_manager.spawn(
            async move {
                // we just spawned a new task that's relevant to fallback coordination
                // so register it with the fallback coordinator
                let fallback_handler = fallback_coordinator_clone.register();

                // get the cancellation token that signals fallback
                let fallback_token = fallback_coordinator_clone.token();

                trace!("Writer task started");
                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            trace!("Received shutdown signal");
                            outbound_rx.close();
                            break;
                        }
                        _ = fallback_token.cancelled() => {
                            trace!("Received fallback signal");
                            outbound_rx.close();
                            break;
                        }
                        _ = io_cancellation.cancelled() => {
                            trace!("Reader signaled exit");
                            outbound_rx.close();
                            break;
                        }
                        res = outbound_rx.recv() => {
                            match res {
                                Ok(frame) => {
                                    trace!("Sending outbound frame");
                                    if let Err(e) = writer.write_frame(frame.into()).await {
                                        error!(error=?e, "Writer error");
                                        outbound_rx.close();
                                        break;
                                    }
                                }
                                Err(_) => {
                                    outbound_rx.close();
                                    warn!("Outbound channel closed");
                                    break;
                                }
                            }
                        }
                    }
                }
                outbound_rx.close();
                drop(outbound_rx);

                // signal fallback coordinator that this task has completed its cleanup
                fallback_handler.done();
                warn!("Writer task exited.");
            }
            .instrument(tracing::trace_span!(
                "writer_task",
                spawned_at = %format!("{}:{}", caller.file(), caller.line())
            )),
        );
    }
}
