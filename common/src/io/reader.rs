use futures::{stream::FuturesOrdered, SinkExt, StreamExt};
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::protocol::{
    response::{Response, ReturnCode},
    Packet,
};

use super::split_quic_stream;

pub struct Request {
    pub packet: Packet,
    pub res_tx: oneshot::Sender<Packet>,
}

/// receive packets from quic stream, send to super level read loop
pub async fn start_read(
    mut stream_acceptor: StreamAcceptor,
    request_tx: mpsc::Sender<Request>,
    token: CancellationToken,
) {
    let mut futs = JoinSet::new();
    loop {
        select! {
            stream = stream_acceptor.accept_bidirectional_stream() => {
                let stream = match stream {
                    Ok(Some(stream)) => stream,
                    Ok(None) => {
                        break
                    }
                    Err(e) => {
                        error!("accept new quic stream error: {e}");
                        break
                    }
                };
                futs.spawn(read_stream(stream, request_tx.clone(), token.child_token()));
            }
            _ = token.cancelled() => {
                while futs.join_next().await.is_some() {}
                return
            }
        }
    }
    token.cancel();
    while futs.join_next().await.is_some() {}
}

async fn read_stream(
    stream: BidirectionalStream,
    request_tx: mpsc::Sender<Request>,
    token: CancellationToken,
) {
    let (mut recv_stream, mut send_stream) = split_quic_stream(stream);

    // waiting for multi response from broker at once
    let mut futs: FuturesOrdered<oneshot::Receiver<Packet>> = FuturesOrdered::new();

    loop {
        select! {

            biased;

            Some(packet) = futs.next(), if !futs.is_empty() => {
                // error indicate that res_tx has dropped without sent packet, so we needn't to send to quic stream
                if let Ok(packet) = packet {
                    let packet_type = packet.packet_type().to_string();
                    if let Err(e) = send_stream.send(packet).await {
                        error!(packet_type, "send response packet to quic stream error: {e}");
                        break
                    }
                }
            }

            Some(packet) = recv_stream.next() => {
                let packet = match packet {
                    Ok(packet) => packet,
                    Err(e) => {
                        error!("read from quic stream error: {e}");
                        break;
                    }
                };
                let (res_tx, res_rx) = oneshot::channel::<Packet>();
                if request_tx.send(Request {packet, res_tx}).await.is_err() {
                    error!("endpoint closed, cannot process packet any more");
                    send_stream.send(Packet::Response(Response(ReturnCode::EndpointClosed))).await.ok();
                    break;
                }
                // managed by FuturesOrdered
                futs.push_back(res_rx);
            }

            _ = token.cancelled() => {
                break
            }

            else => break
        }
    }
}
