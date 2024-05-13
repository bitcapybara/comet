use std::time::Duration;

use s2n_quic::connection as s2n_conn;

use comet_common::{codec::Codec, io::writer, protocol::Packet, utils::defer::defer};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::error;

pub async fn writer_loop<C>(
    keepalive: u16,
    stream_opener: s2n_conn::Handle,
    request_rx: mpsc::UnboundedReceiver<writer::Request>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());

    let (inner_tx, inner_rx) = mpsc::unbounded_channel();
    let _ = tokio::join!(
        writer::start_writer::<C>(stream_opener, inner_rx, token.clone()),
        start(keepalive, request_rx, inner_tx, token.clone())
    );
}

async fn start(
    keepalive: u16,
    mut request_rx: mpsc::UnboundedReceiver<writer::Request>,
    inner_tx: mpsc::UnboundedSender<writer::Request>,
    token: CancellationToken,
) {
    let _guard = defer(|| token.cancel());
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(keepalive.into())) => {
                let (res_tx, res_rx) = oneshot::channel();
                if inner_tx.send(writer::Request {
                    packet: Packet::Ping,
                    res_tx: writer::ReplyHandle::WaitReply(res_tx),
                }).is_err() {
                    break;
                }
                tokio::select! {
                    res = res_rx => match res {
                        Ok(Ok(Packet::Pong)) => {
                            continue;
                        }
                        Ok(Ok(p)) => {
                            error!("unexpected packet {} received, want PONG", p.packet_type());
                            break
                        }
                        Ok(Err(e)) => {
                            error!("send ping error: {e:?}");
                            break;
                        }
                        Err(_) => {
                            break;
                        }
                    },
                    _ = token.cancelled() => break
                }
            },
            Some(request) = request_rx.recv() =>  {
                if inner_tx.send(request).is_err() {
                    break
                }
            },
            _ = token.cancelled() => break,
            else => break,
        }
    }
}
