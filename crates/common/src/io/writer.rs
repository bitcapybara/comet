mod pipeline;
mod pool;

use futures::{stream::FuturesUnordered, StreamExt};
use s2n_quic::connection::Handle;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    codec::Codec, io::writer::pipeline::start_pipeline_write, protocol::Packet, utils::defer::defer,
};

use self::pool::Pool;

pub struct Request {
    pub packet: Packet,
    pub res_tx: ReplyHandle,
}

pub enum ReplyHandle {
    NoReply(oneshot::Sender<Result<(), super::Error>>),
    WaitReply(oneshot::Sender<Result<Packet, super::Error>>),
}

pub async fn start_writer<C>(
    mut stream_opener: Handle,
    mut request_rx: mpsc::UnboundedReceiver<Request>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    let pool = Pool::new();
    let mut futs = FuturesUnordered::new();
    'outer: loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            Some(_) = futs.next(), if !futs.is_empty() => {}
            Some(mut request) = request_rx.recv() => {
                loop {
                    // 拿到一个连接，尝试发送，如果满了，就再拿出一个，直到池子里空
                    match pool.get() {
                        Some(stream) => match stream.try_send(request) {
                            Ok(_) => {
                                break
                            },
                            Err(mpsc::error::TrySendError::Full(failed_req)) => {
                                request = failed_req;
                                continue;
                            },
                            Err(mpsc::error::TrySendError::Closed(failed_req)) => {
                                request = failed_req;
                                stream.close();
                                continue;
                            }
                        },
                        None => {
                            let quic_stream = match stream_opener.open_bidirectional_stream().await {
                                Ok(s) => s,
                                Err(e) => {
                                    error!("quic open bi stream error: {e:?}");
                                    break 'outer
                                }
                            };
                            let (input_tx, input_rx) = mpsc::channel(100);
                            // 启动后一直在后台运行着
                            futs.push(start_pipeline_write::<C>(quic_stream, input_rx, token.child_token()));
                            // 池化的是 channel 的 tx 端
                            pool.put(input_tx);
                        },
                    }
                }
            }
            else => {
                token.cancel();
                break;
            }
        }
    }
    while futs.next().await.is_some() {}
    // NOTE: stream_opener.close(0u8.into()) 会使 client 端报错 packet codec error
}
