use std::time::Instant;

use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    SinkExt, StreamExt,
};
use metrics::{describe_histogram, histogram};
use s2n_quic::{connection::StreamAcceptor, stream::BidirectionalStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    codec::Codec,
    protocol::{Packet, PacketType},
    utils::defer::defer,
};

use super::split_quic_stream;

#[derive(Debug)]
pub struct Request {
    pub packet: Packet,
    pub res_tx: ReplyHandle,
}

#[derive(Debug)]
pub enum ReplyHandle {
    NoReply,
    WaitReply(oneshot::Sender<Packet>),
}

/// receive packets from quic stream, send to super level read loop
pub async fn start_reader<C>(
    mut stream_acceptor: StreamAcceptor,
    request_tx: mpsc::Sender<Request>,
    token: CancellationToken,
) -> Result<(), super::Error>
where
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    let mut futs = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            Some(_) = futs.next(), if !futs.is_empty() => {}
            Ok(Some(stream)) = stream_acceptor.accept_bidirectional_stream() => {
                futs.push(read_stream::<C>(stream, request_tx.clone(), token.child_token()));
            }
            else => {
                token.cancel();
                break;
            }
        }
    }
    while futs.next().await.is_some() {}
    Ok(())
}

async fn read_stream<C>(
    stream: BidirectionalStream,
    request_tx: mpsc::Sender<Request>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    let (mut recv_stream, mut send_stream) = split_quic_stream::<C>(stream);

    // waiting for multi response from broker on current thread
    let mut futs = FuturesOrdered::new();

    describe_histogram!(
        "common.io.reader.wait.duration",
        "The processing time of the processor for message packets."
    );

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            Some(fut) = futs.next(), if !futs.is_empty() => {
                let fut: Option<Result<Packet, oneshot::error::RecvError>> = fut;
                match fut {
                    Some(Ok(packet)) => {
                        let packet_type = packet.packet_type().to_string();
                        if let Err(e) = send_stream.send(packet).await {
                            error!(packet_type, "send response packet to quic stream error: {e:?}");
                        }
                    },
                    Some(Err(_)) => {
                        // broker disconnected
                        error!("broker disconnected");
                        break
                    },
                    None => break,
                }
            }
            Some(packet) = recv_stream.next() => {
                let packet = match packet {
                    Ok(packet) => packet,
                    Err(e) => {
                        error!("read from quic stream error: {e:?}");
                        break;
                    }
                };
                let packet_type = packet.packet_type();
                match packet_type {
                    PacketType::Connect |
                    PacketType::CreateProducer |
                    PacketType::Publish |
                    PacketType::Subscribe |
                    PacketType::ControlFlow |
                    PacketType::Send |
                    PacketType::Acknowledge|
                    PacketType::Unsubscribe |
                    PacketType::Ping => {
                        let (res_tx, res_rx) = oneshot::channel::<Packet>();
                        if request_tx.send(Request {packet, res_tx: ReplyHandle::WaitReply(res_tx)}).await.is_err() {
                            error!("shutting down, cannot process packet any more");
                            send_stream.send(Packet::Disconnect).await.ok();
                            break;
                        }
                        // managed by FuturesOrdered
                        let start = Instant::now();
                        let token = token.child_token();
                        futs.push_back(async move {tokio::select! {
                            res = res_rx => {
                                histogram!(
                                    "common.io.reader.wait.duration",
                                    &[("packet_type", packet_type.to_string())],
                                )
                                .record(start.elapsed());
                                Some(res)
                            },
                            _ = token.cancelled() => None,
                        }});
                    },
                    PacketType::Disconnect | PacketType::CloseProducer | PacketType::CloseConsumer => {
                        if request_tx.send(Request {packet, res_tx: ReplyHandle::NoReply}).await.is_err() {
                            error!("shutting down, cannot process packet any more");
                            break;
                        }
                    },
                    _ => unreachable!()
                }

            }
            else => break
        }
    }

    // NOTE: while recv_stream.next().await.is_some() {} 客户端连接后，先关闭 server 会阻塞
    // NOTE: 使用 send_stream.close() 在 client 已断开连接的情况下会阻塞
    // NOTE: send_stream.into_inner().finish().ok() 会使客户端 disconnect 报错 The Stream had been reset with the error application::Error(0) by the remote endpoint
    while futs.next().await.is_some() {}
}
