pub mod manager;
mod reader;
mod writer;

use snafu::{location, Location, ResultExt};
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};

use comet_common::{
    addr::ConnectAddress,
    io::{self, writer as comet_writer},
    protocol::{
        acknowledge::Acknowledge,
        connect::Connect,
        consumer::{CloseConsumer, Subscribe, SubscribeReceipt, Unsubscribe},
        control_flow::ControlFlow,
        producer::{CloseProducer, CreateProducer, ProducerReceipt},
        publish::Publish,
        Packet, PacketType,
    },
    types::AccessMode,
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    client::consumer::TopicMessageInner, ConnectionReadWriteSnafu, DisconnectSnafu,
    ServerResponseSnafu, UnexpectedResponsePacketSnafu,
};

pub enum ConsumerEvent {
    /// register consumer to client reader
    AddConsumer {
        consumer_id: u64,
        /// sender held by client reader
        /// receiver held by client consumer
        sender: mpsc::UnboundedSender<TopicMessageInner>,
    },
    /// unregister consumer from client reader
    DelConsumer { consumer_id: u64 },
}

/// 一个地址对应一个，新建时启动 write_loop 和 read_loop
#[derive(Clone)]
pub struct TopicConnection {
    pub addr: ConnectAddress,
    keepalive: u16,
    /// 给 writer loop 发送消息
    request_tx: mpsc::UnboundedSender<comet_writer::Request>,
    /// register consumer
    register_tx: mpsc::UnboundedSender<ConsumerEvent>,
    // closed: Arc<AtomicBool>,
    token: CancellationToken,
}

impl TopicConnection {
    #[tracing::instrument(skip(self))]
    pub async fn handshake(&self) -> Result<(), crate::Error> {
        self.send_ok(Packet::Connect(Connect {
            keepalive: self.keepalive,
        }))
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn create_producer(
        &self,
        name: &str,
        topic_name: &str,
        access_mode: AccessMode,
    ) -> Result<ProducerReceipt, crate::Error> {
        match self
            .send_request(Packet::CreateProducer(CreateProducer {
                producer_name: name.to_string(),
                topic_name: topic_name.to_string(),
                access_mode,
            }))
            .await?
        {
            Packet::ProducerReceipt(packet) => Ok(packet),
            Packet::Response(response) => ServerResponseSnafu { response }.fail(),
            p => UnexpectedResponsePacketSnafu {
                request: PacketType::CreateProducer,
                response: p.packet_type(),
            }
            .fail(),
        }
    }

    #[tracing::instrument(skip(self, res_tx))]
    pub fn publish(
        &self,
        publish: Publish,
        res_tx: oneshot::Sender<Result<Packet, io::Error>>,
    ) -> Result<(), crate::Error> {
        self.request_tx
            .send(comet_writer::Request {
                packet: Packet::Publish(publish),
                res_tx: comet_writer::ReplyHandle::WaitReply(res_tx),
            })
            .map_err(|_| DisconnectSnafu.build())
    }

    #[tracing::instrument(skip(self))]
    pub async fn close_producer(&self, producer_id: u64) -> Result<(), crate::Error> {
        self.send_async(Packet::CloseProducer(CloseProducer { producer_id }))
            .await
    }

    #[tracing::instrument(skip(self, sender))]
    pub(crate) async fn subscribe(
        &self,
        packet: &Subscribe,
        sender: mpsc::UnboundedSender<TopicMessageInner>,
    ) -> Result<SubscribeReceipt, crate::Error> {
        match self.send_request(Packet::Subscribe(packet.clone())).await? {
            Packet::SubscribeReceipt(receipt) => {
                self.register_tx
                    .send(ConsumerEvent::AddConsumer {
                        consumer_id: receipt.consumer_id,
                        sender,
                    })
                    .map_err(|_| DisconnectSnafu.build())?;
                Ok(receipt)
            }
            Packet::Response(response) => ServerResponseSnafu { response }.fail(),
            p => UnexpectedResponsePacketSnafu {
                request: PacketType::Subscribe,
                response: p.packet_type(),
            }
            .fail(),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn control_flow(&self, consumer_id: u64, permits: u32) -> Result<(), crate::Error> {
        self.send_ok(Packet::ControlFlow(ControlFlow {
            consumer_id,
            permits,
        }))
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn acks(&self, consumer_id: u64, message_ids: Vec<u64>) -> Result<(), crate::Error> {
        self.send_ok(Packet::Acknowledge(Acknowledge {
            consumer_id,
            message_ids,
        }))
        .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn unsubscribe(&self, consumer_id: u64) -> Result<(), crate::Error> {
        self.send_ok(Packet::Unsubscribe(Unsubscribe { consumer_id }))
            .await?;
        self.register_tx
            .send(ConsumerEvent::DelConsumer { consumer_id })
            .map_err(|_| DisconnectSnafu.build())?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn close_consumer(&self, consumer_id: u64) -> Result<(), crate::Error> {
        self.send_async(Packet::CloseConsumer(CloseConsumer { consumer_id }))
            .await
    }

    #[tracing::instrument(skip(self))]
    pub async fn disconnect(self) -> Result<(), crate::Error> {
        self.send_async(Packet::Disconnect).await
    }

    pub async fn send_async(&self, packet: Packet) -> Result<(), crate::Error> {
        let (res_tx, res_rx) = oneshot::channel();
        self.request_tx
            .send(comet_writer::Request {
                packet,
                res_tx: comet_writer::ReplyHandle::NoReply(res_tx),
            })
            .map_err(|_| DisconnectSnafu.build())?;
        res_rx
            .await
            .map_err(|_| DisconnectSnafu.build())?
            .context(ConnectionReadWriteSnafu)?;
        Ok(())
    }

    pub async fn send_ok(&self, packet: Packet) -> Result<(), crate::Error> {
        let (res_tx, res_rx) = oneshot::channel();
        self.request_tx
            .send(comet_writer::Request {
                packet,
                res_tx: comet_writer::ReplyHandle::WaitReply(res_tx),
            })
            .map_err(|_| DisconnectSnafu.build())?;
        let packet = res_rx
            .await
            .map_err(|_| DisconnectSnafu.build())?
            .context(ConnectionReadWriteSnafu)?;
        match packet {
            Packet::Response(resp) if resp.is_success() => Ok(()),
            Packet::Response(response) => ServerResponseSnafu { response }.fail(),
            _ => unreachable!(),
        }
    }

    pub async fn send_request(&self, packet: Packet) -> Result<Packet, crate::Error> {
        let (res_tx, res_rx) = oneshot::channel();
        self.request_tx
            .send(comet_writer::Request {
                packet,
                res_tx: comet_writer::ReplyHandle::WaitReply(res_tx),
            })
            .map_err(|_| DisconnectSnafu.build())?;
        let Ok(res) = res_rx.await else {
            return DisconnectSnafu.fail();
        };
        match res.context(ConnectionReadWriteSnafu)? {
            Packet::Response(response) if !response.is_success() => {
                Err(crate::Error::ServerResponse {
                    response,
                    location: location!(),
                })
            }
            p => Ok(p),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.request_tx.is_closed() || self.register_tx.is_closed() || self.token.is_cancelled()
    }

    pub fn wait_close(&self) -> WaitForCancellationFutureOwned {
        self.token.clone().cancelled_owned()
    }
}
