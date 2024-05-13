use futures::{SinkExt, StreamExt};
use s2n_quic::stream::{BidirectionalStream, ReceiveStream, SendStream};
use snafu::IntoError;
use tokio::sync::{mpsc, oneshot};
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};
use tracing::error;

use crate::{
    codec::Codec,
    io::{self, split_quic_stream, writer::ReplyHandle, StreamReadSnafu, StreamWriteSnafu},
    protocol::{Packet, PacketCodec},
    utils::defer::defer,
};

use super::Request;

/// start by write loop when recv a packet request and opened a new stream
/// managed by FuturesUnordered
/// FuturesUnordered is held by write loop
pub async fn start_pipeline_write<C>(
    quic_stream: BidirectionalStream,
    input_stream: mpsc::Receiver<Request>,
    token: CancellationToken,
) where
    C: Codec,
{
    let (recv_stream, send_stream) = split_quic_stream::<C>(quic_stream);
    let (resp_tx, resp_rx) = mpsc::channel(100);
    tokio::join!(
        start_send(input_stream, send_stream, resp_tx, token.clone()),
        start_recv(recv_stream, resp_rx, token.clone())
    );
}

async fn start_send<C>(
    mut input_stream: mpsc::Receiver<Request>,
    mut send_stream: FramedWrite<SendStream, PacketCodec<C>>,
    resp_tx: mpsc::Sender<oneshot::Sender<Result<Packet, io::Error>>>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    loop {
        tokio::select! {
            Some(Request { packet, res_tx })= input_stream.recv() => {
                let packet_type = packet.packet_type();
                match send_stream.send(packet).await {
                    Ok(_) => match res_tx {
                        ReplyHandle::WaitReply(res_tx) => {
                            resp_tx.send(res_tx).await.ok();
                        }
                        ReplyHandle::NoReply(res_tx) => {
                            res_tx.send(Ok(())).ok();
                        }
                    }
                    Err(e) => {
                        error!(%packet_type, "write packet to stream error: {e:?}");
                        match res_tx {
                            ReplyHandle::WaitReply(sender) => {
                                sender.send(Err(StreamWriteSnafu.into_error(e))).ok();
                            }
                            ReplyHandle::NoReply(sender) => {
                                sender.send(Err(StreamWriteSnafu.into_error(e))).ok();
                            }
                        }
                        break
                    }
                }
            }
            _ = token.cancelled() => break,
            else => break
        }
    }

    // NOTE: 服务端关闭后，关闭客户端这里 send_stream.close() 会阻塞
    // NOTE: send_stream.into_inner().finish() 会使客户端 disconnect 报错
    // NOTE: "The Stream had been reset with the error application::Error(0) by the remote endpoint"
}

async fn start_recv<C>(
    mut recv_stream: FramedRead<ReceiveStream, PacketCodec<C>>,
    mut resp_rx: mpsc::Receiver<oneshot::Sender<Result<Packet, io::Error>>>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    loop {
        tokio::select! {
            Some(res_tx) = resp_rx.recv() => {
                match recv_stream.next().await {
                    Some(Ok(resp)) => {
                        res_tx.send(Ok(resp)).ok();
                    }
                    Some(Err(e)) => {
                        res_tx.send(Err(StreamReadSnafu.into_error(e))).ok();
                        break;
                    }
                    None => {
                        break
                    }
                }
            }
            _ = token.cancelled() => break,
            else => break
        }
    }

    // NOTE: while recv_stream.next().await.is_some() {} 客户端连接后，先关闭 server 会阻塞
}
