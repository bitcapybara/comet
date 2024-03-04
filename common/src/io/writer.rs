mod pipeline;
mod pool;

use s2n_quic::connection::Handle;
use tokio::{
    select,
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{io::writer::pipeline::start_pipeline_write, protocol::Packet};

use self::pool::{Pool, PooledStream};

pub struct Request {
    packet: Packet,
    res_tx: ReplyHandle,
}

pub enum ReplyHandle {
    WaitResp(oneshot::Sender<Result<Packet, super::Error>>),
    NoWait(oneshot::Sender<Result<(), super::Error>>),
}

impl ReplyHandle {
    pub fn send_err(self, e: super::Error) {
        match self {
            ReplyHandle::WaitResp(sender) => {
                sender.send(Err(e)).ok();
            }
            ReplyHandle::NoWait(sender) => {
                sender.send(Err(e)).ok();
            }
        }
    }
}

pub async fn start_writer(
    mut stream_opener: Handle,
    mut request_rx: mpsc::Receiver<Request>,
    token: CancellationToken,
) {
    let pool = Pool::new();
    let mut futs = JoinSet::new();
    loop {
        select! {
            Some(request) = request_rx.recv() => {
                let stream = match pool.get() {
                    Some(s) => s,
                    None => {
                        let quic_stream = match stream_opener.open_bidirectional_stream().await {
                            Ok(s) => s,
                            Err(e) => {
                                error!("quic open bi stream error: {e}");
                                break
                            }
                        };
                        let (input_tx, input_rx) = mpsc::channel(100);
                        futs.spawn(start_pipeline_write(quic_stream, input_rx, token.child_token()));
                        PooledStream::new(pool.clone(), input_tx)
                    },
                };
                stream.send(request).await.ok();
            }
            _ = token.cancelled() => {
                while futs.join_next().await.is_some() {}
                return;
            }
            else => break
        }
    }
    token.cancel();
    while futs.join_next().await.is_some() {}
}
