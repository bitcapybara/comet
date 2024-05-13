use std::collections::{HashMap, VecDeque};

use comet_common::{
    codec::Codec,
    io::reader,
    protocol::{self, Packet},
    utils::defer::defer,
};
use s2n_quic::connection::StreamAcceptor;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_util::sync::CancellationToken;

use crate::client::consumer::TopicMessageInner;

use super::ConsumerEvent;

pub async fn reader_loop<C>(
    stream_acceptor: StreamAcceptor,
    register_rx: mpsc::UnboundedReceiver<ConsumerEvent>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    let (request_tx, request_rx) = mpsc::channel(1);
    let _ = tokio::join!(
        tokio::spawn(reader::start_reader::<C>(
            stream_acceptor,
            request_tx,
            token.clone()
        )),
        tokio::spawn(start_reader(request_rx, register_rx, token.clone()))
    );
}

pub async fn start_reader(
    mut request_rx: mpsc::Receiver<reader::Request>,
    mut register_rx: mpsc::UnboundedReceiver<ConsumerEvent>,
    token: CancellationToken,
) {
    let _guard = defer(|| token.cancel());
    let mut consumers = HashMap::<u64, UnboundedSender<TopicMessageInner>>::new();
    let mut cached_messages = HashMap::<u64, VecDeque<protocol::send::Send>>::new();

    loop {
        tokio::select! {
            Some(reader::Request { packet, res_tx }) = request_rx.recv() => {
                match res_tx {
                    reader::ReplyHandle::WaitReply(res_tx) => match packet {
                        Packet::Ping => {
                            res_tx.send(Packet::Pong).ok();
                        },
                        Packet::Send(send) => {
                            let consumer_id = send.header.consumer_id;
                            if let Some(sender) = consumers.get(&consumer_id) {
                                if sender.send(send.into()).is_err() {
                                    consumers.remove(&consumer_id);
                                }
                            } else {
                                cached_messages.entry(consumer_id).or_default().push_back(send);
                            }
                            res_tx.send(Packet::ok()).ok();
                        },
                        _ => unreachable!()
                    },
                    reader::ReplyHandle::NoReply => match packet {
                        Packet::Disconnect => break,
                        _ => unreachable!()
                    }
                }
            }
            Some(event) = register_rx.recv() => match event {
                ConsumerEvent::AddConsumer { consumer_id, sender } => {
                    if let Some(queue) = cached_messages.get_mut(&consumer_id) {
                        while let Some(send) = queue.pop_front() {
                            if sender.send(send.into()).is_err() {
                                continue;
                            }
                        }
                        if queue.is_empty() {
                            cached_messages.remove(&consumer_id);
                        }
                    }
                    consumers.insert(consumer_id, sender);
                },
                ConsumerEvent::DelConsumer { consumer_id } => {
                    consumers.remove(&consumer_id);
                    cached_messages.remove(&consumer_id);
                },
            },
            _ = token.cancelled() => break,
            else => break,
        }
    }
}
