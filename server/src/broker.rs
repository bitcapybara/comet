use std::{cmp, collections::HashMap, fmt::Debug, sync::Arc};

use chrono::Utc;
use futures::{stream::FuturesUnordered, StreamExt};
use snafu::{Location, ResultExt, Snafu};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio_util::sync::CancellationToken;

use comet_common::{
    error::ResponsiveError,
    id::{local::LocalId, IdGenerator},
    io::writer,
    protocol::{
        acknowledge::Acknowledge,
        consumer::{CloseConsumer, Subscribe, SubscribeReceipt, Unsubscribe},
        control_flow::ControlFlow,
        producer::{CloseProducer, CreateProducer, ProducerReceipt},
        publish::Publish,
        response::{Response, ReturnCode},
        Packet,
    },
    utils::defer::defer,
};
use tracing::error;

use crate::{
    meta::{ConsumerInfo, MetaConsumer, MetaProducer, MetaStorage},
    scheme::StorageScheme,
    storage::PublishedMessage,
};

use self::topic::Topic;

pub(crate) mod subscription;
pub(crate) mod topic;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("topic error"))]
    Topic {
        source: topic::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ResponsiveError for Error {
    fn as_response(&self) -> Response {
        match self {
            Error::Topic { source, .. } => source.as_response(),
        }
    }
}

pub enum ClientPacket {
    /// Connect
    Connect(mpsc::UnboundedSender<writer::Request>),
    /// Disconnect/CloseProducer/CloseConsumer
    NoReply(Packet),
    /// Others
    WaitReply(Packet, oneshot::Sender<Packet>),
}

pub struct Broker<S>
where
    S: StorageScheme,
{
    scheme: S,
    meta: S::MetaStorage,
    /// key = client_id
    pub(crate) client_txs: Arc<RwLock<HashMap<u64, mpsc::UnboundedSender<writer::Request>>>>,
    /// key = topic
    #[allow(clippy::type_complexity)]
    pub(crate) topics: Arc<RwLock<HashMap<String, Topic<S::MetaStorage, S::TopicStorage>>>>,
    id_generator: LocalId,
    token: CancellationToken,
}

impl<S> Clone for Broker<S>
where
    S: StorageScheme,
{
    fn clone(&self) -> Self {
        Self {
            scheme: self.scheme.clone(),
            id_generator: self.id_generator.clone(),
            client_txs: self.client_txs.clone(),
            topics: self.topics.clone(),
            meta: self.meta.clone(),
            token: self.token.clone(),
        }
    }
}

impl<S> Broker<S>
where
    S: StorageScheme,
{
    pub fn new(scheme: S, meta: S::MetaStorage, token: CancellationToken) -> Self {
        Self {
            scheme,
            meta,
            id_generator: LocalId::new(),
            client_txs: Arc::new(RwLock::new(HashMap::new())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            token,
        }
    }

    #[tracing::instrument(skip(self, topic))]
    async fn add_topic(
        &self,
        topic_name: impl Into<String> + Debug,
        topic: Topic<S::MetaStorage, S::TopicStorage>,
    ) {
        self.topics.write().await.insert(topic_name.into(), topic);
    }

    #[tracing::instrument(skip(self))]
    async fn get_topic(&self, topic_name: &str) -> Option<Topic<S::MetaStorage, S::TopicStorage>> {
        self.topics.read().await.get(topic_name).cloned()
    }

    async fn get_client_tx(
        &self,
        client_id: u64,
    ) -> Option<mpsc::UnboundedSender<writer::Request>> {
        self.client_txs.read().await.get(&client_id).cloned()
    }

    pub async fn delete_topic(&self, topic_name: &str) -> Result<(), Error> {
        let mut topics = self.topics.write().await;
        let Some(topic) = topics.remove(topic_name) else {
            return Ok(());
        };
        topic.drop_storage().await.context(TopicSnafu)?;
        Ok(())
    }

    #[tracing::instrument(skip(self, client_tx))]
    pub async fn handle_connect(
        &self,
        client_id: u64,
        client_tx: mpsc::UnboundedSender<writer::Request>,
    ) {
        let mut client_txs = self.client_txs.write().await;
        if client_txs.contains_key(&client_id) {
            unreachable!()
        }
        client_txs.insert(client_id, client_tx);
    }

    #[tracing::instrument(skip(self))]
    pub async fn handle_close_producer(&self, packet: CloseProducer) {
        self.meta.del_producer(packet.producer_id).await.ok();
    }

    #[tracing::instrument(skip(self))]
    pub async fn handle_close_consumer(&self, packet: CloseConsumer) {
        self.meta.del_consumer(packet.consumer_id).await.ok();
    }

    #[tracing::instrument(skip(self))]
    pub async fn handle_disconnect(&self, client_id: u64) {
        self.meta.clear_producer(client_id).await.ok();
        self.meta.clear_consumer(client_id).await.ok();
    }

    /// 不再需要在 topic 里维护 producer 的元数据，直接存放到 meta 的 session 里
    #[tracing::instrument(skip(self, res_tx))]
    pub async fn handle_create_producer(
        &self,
        client_id: u64,
        packet: CreateProducer,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let producer_id = self.id_generator.next_id();
        let topic_name = packet.topic_name.clone();
        let mut last_sequence_id = 0;

        match self.meta.get_topic(&topic_name).await {
            Ok(Some(info)) => {
                for partition_id in info.partitions {
                    let partitioned_topic_name = partitioned_topic_name(&topic_name, partition_id);

                    match self.get_topic(&partitioned_topic_name).await {
                        Some(topic) => {
                            last_sequence_id =
                                match topic.last_sequence_id(&packet.producer_name).await {
                                    Ok(sequence_id) => cmp::max(last_sequence_id, sequence_id),
                                    Err(e) => {
                                        res_tx.send(Packet::Response(e.as_response())).ok();
                                        return;
                                    }
                                };
                        }
                        None => {
                            let storage = match self
                                .scheme
                                .create_topic_storage(&partitioned_topic_name)
                                .await
                            {
                                Ok(storage) => storage,
                                Err(e) => {
                                    res_tx.send(Packet::Response(e.as_response())).ok();
                                    return;
                                }
                            };

                            let topic = match Topic::new(
                                &topic_name,
                                &partitioned_topic_name,
                                self.meta.clone(),
                                storage,
                                info.config.clone(),
                                self.token.child_token(),
                            )
                            .await
                            {
                                Ok(topic) => topic,
                                Err(e) => {
                                    res_tx.send(Packet::Response(e.as_response())).ok();
                                    return;
                                }
                            };

                            last_sequence_id =
                                match topic.last_sequence_id(&packet.producer_name).await {
                                    Ok(sequence_id) => cmp::max(last_sequence_id, sequence_id),
                                    Err(e) => {
                                        res_tx.send(Packet::Response(e.as_response())).ok();
                                        return;
                                    }
                                };

                            self.add_topic(partitioned_topic_name, topic).await;
                        }
                    }
                }
            }
            Ok(None) => {
                res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
                return;
            }
            Err(e) => {
                res_tx.send(Packet::Response(e.as_response())).ok();
                return;
            }
        }

        if let Err(e) = self.meta.add_producer(client_id, producer_id, packet).await {
            res_tx.send(Packet::Response(e.into())).ok();
            return;
        }
        res_tx
            .send(Packet::ProducerReceipt(ProducerReceipt {
                producer_id,
                last_sequence_id,
            }))
            .ok();
    }

    #[tracing::instrument(skip(self, res_tx))]
    pub async fn handle_subscribe(
        &self,
        client_id: u64,
        subscribe: Subscribe,
        res_tx: oneshot::Sender<Packet>,
    ) {
        let consumer_id = self.id_generator.next_id();
        let topic_name = subscribe.topic_name.clone();

        let Some(client_tx) = self.get_client_tx(client_id).await else {
            unreachable!("always connected before subscribe");
        };

        match self.meta.get_topic(&topic_name).await {
            Ok(Some(info)) => {
                for partition_id in info.partitions {
                    let partitioned_topic_name = partitioned_topic_name(&topic_name, partition_id);
                    let subscribe = Subscribe {
                        consumer_name: subscribe.consumer_name.clone(),
                        topic_name: partitioned_topic_name.clone(),
                        subscription_name: subscribe.subscription_name.clone(),
                        subscription_type: subscribe.subscription_type,
                        initial_position: subscribe.initial_position,
                        default_permits: subscribe.default_permits,
                        priority: subscribe.priority,
                    };
                    let consumer = match self
                        .meta
                        .add_consumer(client_id, consumer_id, subscribe, client_tx.clone())
                        .await
                    {
                        Ok(consumer) => consumer,
                        Err(e) => {
                            res_tx.send(Packet::Response(e.as_response())).ok();
                            return;
                        }
                    };
                    match self.get_topic(&partitioned_topic_name).await {
                        Some(topic) => {
                            if let Err(e) = topic.add_consumer(consumer.clone()).await {
                                res_tx.send(Packet::Response(e.as_response())).ok();
                                return;
                            }
                        }
                        None => {
                            let storage = match self
                                .scheme
                                .create_topic_storage(&partitioned_topic_name)
                                .await
                            {
                                Ok(storage) => storage,
                                Err(e) => {
                                    res_tx
                                        .send(Packet::err_with_message(
                                            ReturnCode::Internal,
                                            e.to_string(),
                                        ))
                                        .ok();
                                    return;
                                }
                            };

                            let topic = match Topic::new(
                                &topic_name,
                                &partitioned_topic_name,
                                self.meta.clone(),
                                storage,
                                info.config.clone(),
                                self.token.child_token(),
                            )
                            .await
                            {
                                Ok(topic) => topic,
                                Err(e) => {
                                    res_tx.send(Packet::Response(e.as_response())).ok();
                                    return;
                                }
                            };

                            if let Err(e) = topic.add_consumer(consumer.clone()).await {
                                res_tx.send(Packet::Response(e.as_response())).ok();
                                return;
                            }
                            self.add_topic(partitioned_topic_name, topic).await;
                        }
                    }
                }
            }
            Ok(None) => {
                res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
                return;
            }
            Err(e) => {
                res_tx.send(Packet::Response(e.as_response())).ok();
                return;
            }
        }

        res_tx
            .send(Packet::SubscribeReceipt(SubscribeReceipt { consumer_id }))
            .ok();
    }

    #[tracing::instrument(skip(self, res_tx))]
    pub async fn handle_publish(&self, packet: Publish, res_tx: oneshot::Sender<Packet>) {
        let handle_time = Utc::now();

        let topic_name = packet.header.topic_name.clone();
        match self.meta.get_topic(&topic_name).await {
            Ok(Some(info)) => {
                let producer = match self.meta.get_producer(packet.header.producer_id).await {
                    Ok(Some(producer)) => producer,
                    Ok(None) => {
                        res_tx.send(Packet::err(ReturnCode::ProducerNotFound)).ok();
                        return;
                    }
                    Err(e) => {
                        res_tx.send(Packet::Response(e.as_response())).ok();
                        return;
                    }
                };
                for partition_id in info.partitions {
                    let effective_topic_name = partitioned_topic_name(&topic_name, partition_id);
                    let Some(topic) = self.get_topic(&effective_topic_name).await else {
                        res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
                        return;
                    };

                    let producer_name = producer.info().producer_name;
                    let message = PublishedMessage {
                        topic_name: packet.header.topic_name.clone(),
                        producer_name: producer_name.clone(),
                        sequence_id: packet.header.sequence_id,
                        publish_time: packet.header.publish_time,
                        payload: packet.payload.clone(),
                        deliver_time: packet.header.deliver_time,
                        handle_time,
                    };
                    if let Err(e) = topic.publish(message).await {
                        error!(producer_name, "handle publish message error: {e:?}");
                        res_tx.send(Packet::Response(e.as_response())).ok();
                        return;
                    }
                }
            }
            Ok(None) => {
                res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
                return;
            }
            Err(e) => {
                res_tx.send(Packet::Response(e.as_response())).ok();
                return;
            }
        }

        res_tx.send(Packet::ok()).ok();
    }

    #[tracing::instrument(skip(self, res_tx))]
    pub async fn handle_control_flow(&self, packet: ControlFlow, res_tx: oneshot::Sender<Packet>) {
        let consumer = match self.meta.get_consumer(packet.consumer_id).await {
            Ok(Some(consumer)) => consumer,
            Ok(None) => {
                res_tx.send(Packet::err(ReturnCode::ConsumerNotFound)).ok();
                return;
            }
            Err(e) => {
                res_tx.send(Packet::Response(e.as_response())).ok();
                return;
            }
        };
        if let Err(e) = consumer.permits_add(packet.permits).await {
            res_tx.send(Packet::Response(e.as_response())).ok();
            return;
        }
        res_tx.send(Packet::ok()).ok();
    }

    #[tracing::instrument(skip(self, res_tx))]
    pub async fn handle_acknowledge(&self, packet: Acknowledge, res_tx: oneshot::Sender<Packet>) {
        let consumer = match self.meta.get_consumer(packet.consumer_id).await {
            Ok(Some(consumer)) => consumer,
            Ok(None) => {
                res_tx.send(Packet::err(ReturnCode::ConsumerNotFound)).ok();
                return;
            }
            Err(e) => {
                res_tx.send(Packet::Response(e.as_response())).ok();
                return;
            }
        };
        let ConsumerInfo {
            name: consumer_name,
            topic_name,
            subscription_name,
            ..
        } = &consumer.info();
        let Some(topic) = self.get_topic(topic_name).await else {
            res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
            return;
        };
        match topic.acknowledge(subscription_name, packet).await {
            Ok(_) => {
                res_tx.send(Packet::ok()).ok();
            }
            Err(e) => {
                error!(consumer_name, "handle ack error: {e:?}");
                res_tx.send(Packet::Response(e.as_response())).ok();
            }
        }
    }

    #[tracing::instrument(skip(self, res_tx))]
    pub async fn handle_unsubscribe(&self, packet: Unsubscribe, res_tx: oneshot::Sender<Packet>) {
        let consumer = match self.meta.del_consumer(packet.consumer_id).await {
            Ok(Some(consumer)) => consumer,
            Ok(None) => {
                res_tx.send(Packet::err(ReturnCode::ConsumerNotFound)).ok();
                return;
            }
            Err(e) => {
                res_tx.send(Packet::Response(e.as_response())).ok();
                return;
            }
        };

        let ConsumerInfo {
            name: consumer_name,
            topic_name,
            subscription_name,
            ..
        } = &consumer.info();
        let Some(topic) = self.get_topic(topic_name).await else {
            res_tx.send(Packet::err(ReturnCode::TopicNotFound)).ok();
            return;
        };
        match topic.unsubscribe(subscription_name, consumer_name).await {
            Ok(_) => {
                res_tx.send(Packet::ok()).ok();
            }
            Err(e) => {
                error!(consumer_name, "handle unsubscribe error: {e:?}");
                res_tx
                    .send(Packet::err_with_message(
                        ReturnCode::Internal,
                        e.to_string(),
                    ))
                    .ok();
            }
        }
    }

    pub async fn close(self) {
        self.token.cancel();
        let mut topics = self.topics.write().await;
        let mut futs =
            FuturesUnordered::from_iter(topics.drain().map(|(_, topic)| topic.shutdown()));
        while futs.next().await.is_some() {}
    }

    pub async fn start_handler(
        self,
        client_id: u64,
        mut inbound_rx: mpsc::UnboundedReceiver<ClientPacket>,
        token: CancellationToken,
    ) -> Result<(), Error>
    where
        S: StorageScheme,
    {
        let _guard = defer(|| token.cancel());
        loop {
            tokio::select! {
                Some(packet) = inbound_rx.recv() => match packet {
                    ClientPacket::Connect(client_tx) => {
                        self.handle_connect(client_id, client_tx).await;
                    }
                    ClientPacket::NoReply(packet) => match packet {
                        Packet::CloseProducer(packet) => {
                            self.handle_close_producer(packet).await;
                        },
                        Packet::CloseConsumer(packet) => {
                            self.handle_close_consumer(packet).await;
                        },
                        Packet::Disconnect => {
                            self.handle_disconnect(client_id).await;
                            return Ok(());
                        },
                        _ => unreachable!()
                    }
                    ClientPacket::WaitReply(packet, res_tx) => match packet {
                        Packet::CreateProducer(packet) => {
                            self.handle_create_producer(client_id, packet, res_tx).await;
                        },
                        Packet::Publish(packet) => {
                            self.handle_publish(packet, res_tx).await;
                        },
                        Packet::Subscribe(packet) => {
                            self.handle_subscribe(client_id, packet, res_tx).await;
                        },
                        Packet::ControlFlow(packet) => {
                            self.handle_control_flow(packet, res_tx).await;
                        },
                        Packet::Acknowledge(packet) => {
                            self.handle_acknowledge(packet, res_tx).await;
                        },
                        Packet::Unsubscribe(packet) => {
                            self.handle_unsubscribe(packet, res_tx).await;
                        },
                        _ => unreachable!()
                    }
                },
                _ = token.cancelled() => break,
                else => {
                    break
                }
            }
        }
        self.handle_disconnect(client_id).await;
        Ok(())
    }
}

fn partitioned_topic_name(topic_name: &str, partition_id: u16) -> String {
    format!("{topic_name}_{partition_id}")
}
