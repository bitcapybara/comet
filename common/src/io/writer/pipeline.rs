use futures::{SinkExt, StreamExt};
use s2n_quic::stream::{BidirectionalStream, ReceiveStream, SendStream};
use snafu::{IntoError, ResultExt};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};
use tracing::error;

use crate::{
    io::{
        self, split_quic_stream, writer::ReplyHandle, DefaultPacketCodec, StreamReadSnafu,
        StreamWriteSnafu,
    },
    protocol::Packet,
};

use super::Request;

/// start by write loop when recv a packet request and opened a new stream
/// managed by FuturesUnordered
/// FuturesUnordered is held by write loop
pub async fn start_pipeline_write(
    quic_stream: BidirectionalStream,
    input_stream: mpsc::Receiver<Request>,
    token: CancellationToken,
) {
    let (recv_stream, send_stream) = split_quic_stream(quic_stream);
    let (resp_tx, resp_rx) = mpsc::channel(100);
    let child_token = token.child_token();
    let _ = futures::future::join(
        tokio::spawn(start_send(
            input_stream,
            send_stream,
            resp_tx,
            child_token.clone(),
        )),
        tokio::spawn(start_recv(recv_stream, resp_rx, child_token.clone())),
    )
    .await;
}

async fn start_send(
    mut input_stream: mpsc::Receiver<Request>,
    mut send_stream: FramedWrite<SendStream, DefaultPacketCodec>,
    resp_tx: mpsc::Sender<oneshot::Sender<Result<Packet, io::Error>>>,
    token: CancellationToken,
) {
    loop {
        select! {
            Some(Request { packet, res_tx })= input_stream.recv() => {
                let packet_type = packet.packet_type().to_string();
                if let Err(e) = send_stream.send(packet).await {
                    error!(packet_type, "write packet to stream error: {e}");
                    res_tx.send_err(StreamWriteSnafu.into_error(e));
                    break
                }
                match res_tx {
                    ReplyHandle::WaitResp(res_tx) => {
                        resp_tx.send(res_tx).await.ok();
                    }
                    ReplyHandle::NoWait(res_tx) => {
                        res_tx.send(Ok(())).ok();
                    }
                }
            }

            _ = token.cancelled() => {
                return
            }

            else => break
        }
    }
}

async fn start_recv(
    mut recv_stream: FramedRead<ReceiveStream, DefaultPacketCodec>,
    mut resp_rx: mpsc::Receiver<oneshot::Sender<Result<Packet, io::Error>>>,
    token: CancellationToken,
) {
    loop {
        select! {
            Some(res_tx) = resp_rx.recv() => {
                match recv_stream.next().await {
                    Some(Ok(resp)) => {
                        res_tx.send(Ok(resp)).ok();
                    }
                    Some(Err(e)) => {
                        res_tx.send(Err(e).context(StreamReadSnafu)).ok();
                    }
                    None => {
                        break
                    }
                }
            }
            _ = token.cancelled() => {
                return
            }
            else => break
        }
    }
}
