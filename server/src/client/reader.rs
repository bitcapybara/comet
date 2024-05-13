use std::time::Duration;

use comet_common::{
    codec::Codec,
    io::{self, reader, writer},
    protocol::{connect::Connect, response::ReturnCode, Packet},
    utils::defer::defer,
};
use s2n_quic::connection::StreamAcceptor;
use snafu::{Location, Snafu};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::broker::ClientPacket;

#[derive(Debug, Snafu)]
pub enum Error {
    HandshakeWaitTimeout {
        #[snafu(implicit)]
        location: Location,
    },
    HandshakeExpected {
        #[snafu(implicit)]
        location: Location,
    },
    Disconnect {
        #[snafu(implicit)]
        location: Location,
    },
    ServerShutdown {
        #[snafu(implicit)]
        location: Location,
    },
}

pub async fn reader_loop<C>(
    stream_acceptor: StreamAcceptor,
    broker_tx: mpsc::UnboundedSender<ClientPacket>,
    client_tx: mpsc::UnboundedSender<writer::Request>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    let (request_tx, request_rx) = mpsc::channel(100);

    let _ = tokio::join!(
        io::reader::start_reader::<C>(stream_acceptor, request_tx, token.clone(),),
        start_reader(request_rx, broker_tx, client_tx, token.clone(),)
    );
}

pub async fn start_reader(
    mut request_rx: mpsc::Receiver<reader::Request>,
    broker_tx: mpsc::UnboundedSender<ClientPacket>,
    client_tx: mpsc::UnboundedSender<writer::Request>,
    token: CancellationToken,
) {
    // 这个 token 也可以控制底层的 reader loop 退出
    let _guard = defer(|| token.cancel());

    let keepalive = match handshake(
        &mut request_rx,
        &broker_tx,
        client_tx.clone(),
        token.clone(),
    )
    .await
    {
        Ok(keepalive) => keepalive as u64,
        Err(e) => {
            error!("connection handshake error: {e:?}");
            return;
        }
    };
    let keepalive = Duration::from_millis(keepalive + keepalive / 2);
    loop {
        tokio::select! {
            Some(request) = request_rx.recv() => {
                match process(request, &broker_tx).await {
                    Ok(_) => {}
                    Err(Error::Disconnect {..}) => break,
                    Err(e) => {
                        error!("reader loop process packet error: {e:?}")
                    }
                }
            }
            _ = tokio::time::sleep(keepalive) => {
                // 向客户端发送一次 ping 确认一下
                let (res_tx, res_rx) = oneshot::channel();
                match client_tx.send(writer::Request { packet: Packet::Ping, res_tx: writer::ReplyHandle::WaitReply(res_tx) }) {
                    Ok(_) => {
                        tokio::select! {
                            res = res_rx => {
                                match res {
                                    Ok(Ok(Packet::Pong)) => {},
                                    Ok(Ok(_)) => unreachable!(),
                                    Ok(Err(e)) => {
                                        // 向stream发送失败，连接已断开
                                        error!("send ping to client error: {e:?}");
                                        break
                                    }
                                    Err(_) => break,
                                }
                            }
                            _ = token.cancelled() => break
                        }
                    }
                    Err(_e) => {
                        // 无法再向 writer 发送消息了，直接退出
                        break
                    }
                }
                // notify broker to disconnect
                if broker_tx.send(ClientPacket::NoReply(Packet::Disconnect)).is_err() {
                    error!("broker loop shutdown")
                }
                // notify client to disconnect
                let (res_tx, res_rx) = oneshot::channel();
                if client_tx.send(writer::Request { packet: Packet::Disconnect, res_tx: writer::ReplyHandle::NoReply(res_tx) }).is_ok() {
                    tokio::select! {
                        _ = res_rx => {}
                        _ = token.cancelled() => {}
                    }
                }
                break;
            }
            _ = token.cancelled() => break,
            else => {
                break
            }
        }
    }
}

async fn handshake(
    receiver: &mut mpsc::Receiver<reader::Request>,
    broker_tx: &mpsc::UnboundedSender<ClientPacket>,
    clint_tx: mpsc::UnboundedSender<writer::Request>,
    token: CancellationToken,
) -> Result<u16, Error> {
    const HANDSHAKE_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
    tokio::select! {
        _ = tokio::time::sleep(HANDSHAKE_WAIT_TIMEOUT) => {
            HandshakeWaitTimeoutSnafu.fail()
        }
        res = receiver.recv() => match res {
            Some(reader::Request { packet, res_tx }) => match packet {
                Packet::Connect(Connect { keepalive }) => {
                    let reader::ReplyHandle::WaitReply(res_tx) = res_tx else {
                        unreachable!()
                    };
                    match broker_tx.send(ClientPacket::Connect(clint_tx)) {
                        Ok(_) => {
                            res_tx.send(Packet::ok()).ok();
                            Ok(keepalive)
                        }
                        Err(_) => {
                            res_tx.send(Packet::err(ReturnCode::BrokerUnavailable)).ok();
                            ServerShutdownSnafu.fail()
                        }
                    }
                },
                _ => {
                    if let reader::ReplyHandle::WaitReply(res_tx) = res_tx {
                        res_tx
                            .send(Packet::err(ReturnCode::HandshakeExpected))
                            .map_err(|_| DisconnectSnafu.build())?;
                    }
                    HandshakeExpectedSnafu.fail()
                },
            },
            None => DisconnectSnafu.fail(),
        },
        _ = token.cancelled() => ServerShutdownSnafu.fail()
    }
}

async fn process(
    reader::Request { packet, res_tx }: reader::Request,
    broker_tx: &mpsc::UnboundedSender<ClientPacket>,
) -> Result<(), Error> {
    match res_tx {
        reader::ReplyHandle::NoReply => match packet {
            packet @ (Packet::Disconnect | Packet::CloseProducer(_) | Packet::CloseConsumer(_)) => {
                broker_tx
                    .send(ClientPacket::NoReply(packet))
                    .map_err(|_| ServerShutdownSnafu.build())?;
            }
            _ => unreachable!(),
        },
        reader::ReplyHandle::WaitReply(res_tx) => match packet {
            Packet::Connect(_) => {
                res_tx
                    .send(Packet::err(ReturnCode::DuplicatedConnect))
                    .map_err(|_| DisconnectSnafu.build())?;
            }
            Packet::Ping => {
                res_tx
                    .send(Packet::Pong)
                    .map_err(|_| DisconnectSnafu.build())?;
            }
            Packet::ProducerReceipt(_) | Packet::Send(_) | Packet::Pong => {
                res_tx
                    .send(Packet::err(ReturnCode::UnexpectedPacket))
                    .map_err(|_| DisconnectSnafu.build())?;
            }
            packet => {
                broker_tx
                    .send(ClientPacket::WaitReply(packet, res_tx))
                    .map_err(|_| ServerShutdownSnafu.build())?;
            }
        },
    }

    Ok(())
}
