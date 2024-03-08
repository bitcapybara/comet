mod state;
mod topic;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use etcd_client as etcd;
use snafu::{ResultExt, Snafu};
use sqlx::PgPool;
use tokio::{
    select,
    sync::{mpsc, oneshot, RwLock},
};
use tokio_util::sync::CancellationToken;

use comet_common::{
    defer::defer,
    io as comet_io,
    protocol::{
        self,
        acknowledge::Acknowledge,
        consumer::{self, CloseConsumer, Subscribe, Unsubscribe},
        control_flow::ControlFlow,
        producer::{CloseProducer, CreateProducer},
        publish::Publish,
        response::ReturnCode,
        Packet,
    },
    utils::{local::LocalId, snoyflake::Sonyflake, IdGenerator},
};
use tracing::error;

use crate::{server::meta::Meta, storage::Storage};

use self::{
    state::{BrokerState, ConsumerInfo, ProducerInfo, Session},
    topic::Topic,
};

#[derive(Debug, Snafu)]
pub enum Error {
    SendLeaseGrant { source: etcd::Error },
    SendLeaseKeepalive { source: etcd::Error },
    Topic { source: topic::Error },
}

pub enum ClientPacket {
    /// Connect
    Connect(mpsc::UnboundedSender<BrokerMessage>),
    /// Disconnect/CloseProducer/CloseConsumer
    NoReply(Packet),
    /// Others
    WaitReply(Packet, oneshot::Sender<Packet>),
}

/// message from client to broker
pub struct ClientMessage {
    pub client_id: u64,
    pub packet: ClientPacket,
}

/// messages from broker to client
pub struct BrokerMessage {
    pub packet: protocol::Packet,
    pub res_tx: oneshot::Sender<Result<Packet, comet_io::Error>>,
}

pub struct Broker<M, S> {
    connect_addr: String,
    state: Arc<RwLock<BrokerState<S>>>,
    id_generator: LocalId,
    meta: M,
}

impl<M, S> Clone for Broker<M, S>
where
    M: Clone,
{
    fn clone(&self) -> Self {
        Self {
            connect_addr: self.connect_addr.clone(),
            state: self.state.clone(),
            id_generator: self.id_generator.clone(),
            meta: self.meta.clone(),
        }
    }
}

impl<M, S> Broker<M, S>
where
    M: Meta<Storage = S>,
    S: Storage,
{
    pub fn new(meta: M) -> Result<Self, Error> {
        todo!()
    }

    pub async fn handle_connect(
        &self,
        client_id: u64,
        client_tx: mpsc::UnboundedSender<BrokerMessage>,
    ) {
        let mut state = self.state.write().await;
        if state.sessions.contains_key(&client_id) {
            unreachable!()
        }
        state.sessions.insert(client_id, Session::new(client_tx));
    }

    pub async fn handle_close_producer(&self, client_id: u64, packet: CloseProducer) {
        let mut state = self.state.write().await;
        let Some(session) = state.sessions.get_mut(&client_id) else {
            return;
        };
        let producer_id = packet.producer_id;
        let Some(ProducerInfo { topic_name, .. }) = session.del_producer(producer_id) else {
            return;
        };
        let Some(topic) = state.topics.get(&topic_name) else {
            return;
        };
        topic.del_producer(producer_id).await;
    }

    pub async fn handle_close_consumer(&self, client_id: u64, packet: CloseConsumer) {
        let mut state = self.state.write().await;
        let Some(session) = state.sessions.get_mut(&client_id) else {
            return;
        };
        let consumer_id = packet.consumer_id;
        let Some(ConsumerInfo {
            topic_name,
            subscription_name,
            ..
        }) = session.del_consumer(consumer_id)
        else {
            return;
        };
        let Some(topic) = state.topics.get(&topic_name) else {
            return;
        };
        topic.del_consumer(&subscription_name, consumer_id).await;
    }

    pub async fn handle_disconnect(&self, client_id: u64) {
        let mut state = self.state.write().await;
        let Some(mut session) = state.sessions.remove(&client_id) else {
            return;
        };
        for (
            consumer_id,
            ConsumerInfo {
                topic_name,
                subscription_name,
                ..
            },
        ) in session.drain_consumer()
        {
            let Some(topic) = state.topics.get(&topic_name) else {
                continue;
            };
            topic.del_consumer(&subscription_name, consumer_id).await;
        }
        for (producer_id, ProducerInfo { topic_name, .. }) in session.drain_producer() {
            let Some(topic) = state.topics.get(&topic_name) else {
                continue;
            };
            topic.del_producer(producer_id).await;
        }
    }

    pub async fn handle_create_producer(
        &self,
        client_id: u64,
        producer: CreateProducer,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let mut state = self.state.write().await;
        let producer_id = self.id_generator.next_id();
        let topic_name = producer.topic_name.clone();
        let producer_name = producer.producer_name.clone();
        let Some(session) = state.sessions.get(&client_id) else {
            unreachable!()
        };
        if session.has_producer(&producer_name) {
            res_tx
                .send(Packet::err(ReturnCode::ProducerNameAlreadyExists))
                .ok();
            return;
        }

        match state.topics.get(&topic_name) {
            Some(topic) => match topic.add_producer(producer_id, producer).await {
                Ok(_) => {
                    if !topic.has_producer(producer_id).await {
                        return;
                    }
                    if let Some(session) = state.sessions.get_mut(&client_id) {
                        session.add_producer(producer_id, &producer_name, &topic_name);
                    }
                    res_tx.send(Packet::ok()).ok();
                }
                Err(topic::Error::PacketResponse { code }) => {
                    res_tx.send(Packet::err(code)).ok();
                }
                Err(e) => {
                    error!(
                        producer_name,
                        topic_name, "handle create producer error: {e}"
                    );
                    res_tx
                        .send(Packet::err(ReturnCode::Internal(e.to_string())))
                        .ok();
                }
            },
            None => {
                let storage = match self.meta.get_storage().await {
                    Ok(s) => s,
                    Err(e) => {
                        res_tx
                            .send(Packet::err(ReturnCode::Internal(e.to_string())))
                            .ok();
                        return;
                    }
                };
                let topic = Topic::new(&topic_name, storage, session.client_tx.clone());
                match topic.add_producer(producer_id, producer).await {
                    Ok(_) => {
                        state.topics.insert(topic_name.to_string(), topic.clone());
                        if !topic.has_producer(producer_id).await {
                            return;
                        }
                        if let Some(session) = state.sessions.get_mut(&client_id) {
                            session.add_producer(producer_id, &producer_name, &topic_name);
                        }
                        res_tx.send(Packet::ok()).ok();
                    }
                    Err(topic::Error::PacketResponse { code }) => {
                        res_tx.send(Packet::err(code)).ok();
                    }
                    Err(e) => {
                        error!(
                            producer_name,
                            topic_name, "handle create producer error: {e}"
                        );
                        res_tx
                            .send(Packet::err(ReturnCode::Internal(e.to_string())))
                            .ok();
                    }
                }
            }
        };
    }

    pub async fn handle_subscribe(
        &self,
        client_id: u64,
        subscribe: Subscribe,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let mut state = self.state.write().await;
        let consumer_id = self.id_generator.next_id();
        let consumer_name = subscribe.consumer_name.clone();
        let topic_name = subscribe.topic_name.clone();
        let subscription_name = subscribe.subscription_name.clone();
        let Some(session) = state.sessions.get(&client_id) else {
            unreachable!()
        };
        if session.has_consumer(&consumer_name) {
            res_tx
                .send(Packet::err(ReturnCode::ConsumerNameAlreadyExists))
                .ok();
            return;
        }
        match state.topics.get(&topic_name) {
            Some(topic) => match topic.add_consumer(consumer_id, subscribe).await {
                Ok(_) => {
                    if !topic.has_consumer(consumer_id).await {
                        return;
                    }
                    if let Some(session) = state.sessions.get_mut(&client_id) {
                        session.add_consumer(
                            consumer_id,
                            &consumer_name,
                            &topic_name,
                            &subscription_name,
                        );
                    }
                    res_tx.send(Packet::ok()).ok();
                }
                Err(topic::Error::PacketResponse { code }) => {
                    res_tx.send(Packet::err(code)).ok();
                }
                Err(e) => {
                    error!(
                        consumer_name,
                        topic_name, subscription_name, "handle subscribe error: {e}"
                    );
                    res_tx
                        .send(Packet::err(ReturnCode::Internal(e.to_string())))
                        .ok();
                }
            },
            None => {
                let storage = match self.meta.get_storage().await {
                    Ok(s) => s,
                    Err(e) => {
                        let packet = Packet::err(ReturnCode::Internal(e.to_string()));
                        res_tx.send(packet).ok();
                        return;
                    }
                };
                let topic = Topic::new(&topic_name, storage, session.client_tx.clone());
                match topic.add_consumer(consumer_id, subscribe).await {
                    Ok(_) => {
                        state.topics.insert(topic_name.to_string(), topic.clone());
                        if !topic.has_consumer(consumer_id).await {
                            return;
                        }
                        if let Some(session) = state.sessions.get_mut(&client_id) {
                            session.add_consumer(
                                consumer_id,
                                &consumer_name,
                                &topic_name,
                                &subscription_name,
                            );
                        }
                        res_tx.send(Packet::ok()).ok();
                    }
                    Err(topic::Error::PacketResponse { code }) => {
                        res_tx.send(Packet::err(code)).ok();
                    }
                    Err(e) => {
                        error!(
                            consumer_name,
                            subscription_name, "handle subscribe error: {e}"
                        );
                        res_tx
                            .send(Packet::err(ReturnCode::Internal(e.to_string())))
                            .ok();
                    }
                }
            }
        };
    }

    pub async fn handle_publish(
        &self,
        client_id: u64,
        packet: Publish,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let state = self.state.read().await;
        let Some(session) = state.sessions.get(&client_id) else {
            unreachable!()
        };
        let Some(producer) = session.get_producer(packet.header.producer_id) else {
            res_tx.send(Packet::err(ReturnCode::ProducerNotFound)).ok();
            return;
        };
        let topic = packet.header.topic_name.clone();
        let Some(topic) = state.topics.get(&topic) else {
            res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
            return;
        };
        let producer_name = &producer.producer_name;
        match topic.publish(packet).await {
            Ok(_) => {
                res_tx.send(Packet::ok()).ok();
            }
            Err(topic::Error::PacketResponse { code }) => {
                res_tx.send(Packet::err(code)).ok();
            }
            Err(e) => {
                error!(producer_name, "handle publish message error: {e}");
                res_tx
                    .send(Packet::err(ReturnCode::Internal(e.to_string())))
                    .ok();
            }
        }
    }

    pub async fn handle_control_flow(
        &self,
        client_id: u64,
        packet: ControlFlow,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let state = self.state.read().await;
        let Some(session) = state.sessions.get(&client_id) else {
            unreachable!()
        };
        let Some(consumer) = session.get_consumer(packet.consumer_id) else {
            res_tx.send(Packet::err(ReturnCode::ConsumerNotFound)).ok();
            return;
        };
        let Some(topic) = state.topics.get(&consumer.topic_name) else {
            res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
            return;
        };
        let consumer_name = &consumer.consumer_name;
        match topic
            .control_flow(&consumer.subscription_name, packet)
            .await
        {
            Ok(_) => {
                res_tx.send(Packet::ok()).ok();
            }
            Err(topic::Error::PacketResponse { code }) => {
                res_tx.send(Packet::err(code)).ok();
            }
            Err(e) => {
                error!(consumer_name, "handle controlflow error: {e}");
                res_tx
                    .send(Packet::err(ReturnCode::Internal(e.to_string())))
                    .ok();
            }
        }
    }

    pub async fn handle_acknowledge(
        &self,
        client_id: u64,
        packet: Acknowledge,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let state = self.state.read().await;
        let Some(session) = state.sessions.get(&client_id) else {
            unreachable!()
        };
        let Some(consumer) = session.get_consumer(packet.consumer_id) else {
            res_tx.send(Packet::err(ReturnCode::ConsumerNotFound)).ok();
            return;
        };
        let Some(topic) = state.topics.get(&consumer.topic_name) else {
            res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
            return;
        };
        let consumer_name = &consumer.consumer_name;
        match topic.acknowledge(&consumer.subscription_name, packet).await {
            Ok(_) => {
                res_tx.send(Packet::ok()).ok();
            }
            Err(topic::Error::PacketResponse { code }) => {
                res_tx.send(Packet::err(code)).ok();
            }
            Err(e) => {
                error!(consumer_name, "handle ack error: {e}");
                res_tx
                    .send(Packet::err(ReturnCode::Internal(e.to_string())))
                    .ok();
            }
        }
    }

    pub async fn handle_unsubscribe(
        &self,
        client_id: u64,
        packet: Unsubscribe,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let mut state = self.state.write().await;
        let Some(session) = state.sessions.get(&client_id) else {
            unreachable!()
        };
        let consumer_id = packet.consumer_id;
        let Some(consumer) = session.get_consumer(packet.consumer_id) else {
            res_tx.send(Packet::err(ReturnCode::ConsumerNotFound)).ok();
            return;
        };
        let Some(topic) = state.topics.get(&consumer.topic_name) else {
            res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
            return;
        };
        let consumer_name = &consumer.consumer_name;
        match topic.unsubscribe(packet).await {
            Ok(_) => {
                if let Some(s) = state.sessions.get_mut(&client_id) {
                    s.del_consumer(consumer_id);
                }
                res_tx.send(Packet::ok()).ok();
            }
            Err(topic::Error::PacketResponse { code }) => {
                res_tx.send(Packet::err(code)).ok();
            }
            Err(e) => {
                error!(consumer_name, "handle unsubscribe error: {e}");
                res_tx
                    .send(Packet::err(ReturnCode::Internal(e.to_string())))
                    .ok();
            }
        }
    }
}

pub async fn start_broker<M, S>(
    mut broker: Broker<M, S>,
    mut inbound_rx: mpsc::UnboundedReceiver<ClientMessage>,
    token: CancellationToken,
) -> Result<(), Error>
where
    M: Meta<Storage = S>,
    S: Storage,
{
    let _gurad = defer(|| token.cancel());
    loop {
        select! {
            Some(message) = inbound_rx.recv() => {
                let ClientMessage { client_id, packet } = message;
                match packet {
                    ClientPacket::Connect(client_tx) => {
                        broker.handle_connect(client_id, client_tx).await;
                    }
                    ClientPacket::NoReply(packet) => match packet {
                        Packet::CloseProducer(packet) => {
                            broker.handle_close_producer(client_id, packet).await;
                        },
                        Packet::CloseConsumer(packet) => {
                            broker.handle_close_consumer(client_id, packet).await;
                        },
                        Packet::Disconnect => {
                            broker.handle_disconnect(client_id).await;
                        },
                        _ => unreachable!()
                    }
                    ClientPacket::WaitReply(packet, res_tx) => match packet {
                        Packet::CreateProducer(packet) => {
                            broker.handle_create_producer(client_id, packet, res_tx).await;
                        },
                        Packet::Publish(packet) => {
                            broker.handle_publish(client_id, packet, res_tx).await;
                        },
                        Packet::Subscribe(packet) => {
                            broker.handle_subscribe(client_id, packet, res_tx).await;
                        },
                        Packet::ControlFlow(packet) => {
                            broker.handle_control_flow(client_id, packet, res_tx).await;
                        },
                        Packet::Acknowledge(packet) => {
                            broker.handle_acknowledge(client_id, packet, res_tx).await;
                        },
                        Packet::Unsubscribe(packet) => todo!(),
                        _ => unreachable!()
                    }
                }
            }
            _ = token.cancelled() => {
                return Ok(())
            }
            else => return Ok(())
        }
    }
}
