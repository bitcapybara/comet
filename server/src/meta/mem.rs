use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{self, AtomicBool, AtomicU32},
        Arc,
    },
};

use comet_common::{
    addr::ConnectAddress,
    error::ResponsiveError,
    io::{self, writer},
    protocol::{
        consumer::Subscribe,
        producer::CreateProducer,
        response::{Response, ReturnCode},
        send, Packet,
    },
    types::{AccessMode, InitialPosition, SubscriptionType},
};
use snafu::{Location, ResultExt, Snafu};
use tokio::sync::{mpsc, oneshot, RwLock};

use crate::broker::topic;

use super::{
    ConsumerInfo, MetaConsumer, MetaProducer, MetaStorage, NodeTopicInfo, ProducerInfo,
    SubscriptionInfo,
};

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("response error: {response}"))]
    Response {
        response: Response,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("client disconnect"))]
    ClientDisconnect {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("codec io error"))]
    CodecIo {
        #[snafu(source)]
        error: io::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ResponsiveError for Error {
    fn as_response(&self) -> Response {
        match self {
            Error::Response { response, .. } => response.clone(),
            e => Response::new(ReturnCode::Internal, e.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub(crate) broker_addr: ConnectAddress,
    pub(crate) http_addr: ConnectAddress,
}

#[derive(Clone)]
pub struct SessionState<P, C> {
    /// topic_name -> producers
    pub(crate) producers: Arc<RwLock<HashMap<String, SharedTopicProducers<P>>>>,
    /// topic_name -> subscription_name -> consumers
    #[allow(clippy::type_complexity)]
    pub(crate) consumers:
        Arc<RwLock<HashMap<String, HashMap<String, SharedSubscriptionConsumers<C>>>>>,
}

impl<P, C, E> SessionState<P, C>
where
    P: MetaProducer<Error = E>,
    C: MetaConsumer<Error = E>,
    E: From<Error>,
{
    pub fn new() -> Self {
        Self {
            producers: Arc::new(RwLock::new(HashMap::new())),
            consumers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn has_consumers(&self, topic_name: &str, subscription_name: &str) -> bool {
        self.consumers
            .read()
            .await
            .get(topic_name)
            .is_some_and(|t| t.contains_key(subscription_name))
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_consumer(&self, consumer_id: u64) -> Option<C> {
        let session = self.consumers.read().await;
        for consumers in session
            .values()
            .flat_map(|subscription| subscription.values())
        {
            let Some(consumer) = consumers.get(consumer_id).await else {
                continue;
            };
            return Some(consumer);
        }
        None
    }

    #[tracing::instrument(skip(self, consumer))]
    pub async fn add_consumer(&self, consumer: C) -> Result<C, E> {
        let mut session = self.consumers.write().await;
        let subscription = session
            .entry(consumer.info().topic_name.clone())
            .or_default()
            .entry(consumer.info().subscription_name.clone())
            .or_default();
        subscription.add(consumer.clone()).await?;
        Ok(consumer)
    }

    #[tracing::instrument(skip(self))]
    pub async fn del_consumer(&self, consumer_id: u64) -> Option<C> {
        let mut deleted = None;
        {
            let session = self.consumers.write().await;
            for consumers in session
                .values()
                .flat_map(|subscription| subscription.values())
            {
                if let Some(consumer) = consumers.del(consumer_id).await {
                    deleted = Some(consumer);
                    break;
                };
            }
        }
        self.shrink_consumers().await;
        deleted
    }

    #[tracing::instrument(skip(self))]
    pub async fn clear_consumer(&self, client_id: u64) {
        {
            let session = self.consumers.write().await;
            for consumers in session
                .values()
                .flat_map(|subscription| subscription.values())
            {
                consumers.del_by_client_id(client_id).await;
            }
        }

        self.shrink_consumers().await;
    }

    #[tracing::instrument(skip(self))]
    pub async fn shrink_consumers(&self) {
        let mut session = self.consumers.write().await;
        let mut topic_deletes = HashSet::new();
        for (topic, subscriptions) in session.iter_mut() {
            let mut sub_deletes = HashSet::new();
            for (sub, consumers) in subscriptions.iter() {
                if consumers.is_empty().await {
                    sub_deletes.insert(sub.to_owned());
                }
            }
            subscriptions.retain(|sub, _| !sub_deletes.contains(sub));
            if subscriptions.is_empty() {
                topic_deletes.insert(topic.to_owned());
            }
        }
        session.retain(|topic, _| !topic_deletes.contains(topic));
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_producer(&self, producer_id: u64) -> Option<P> {
        let session = self.producers.read().await;
        for producers in session.values() {
            let Some(producer) = producers.get(producer_id).await else {
                continue;
            };
            return Some(producer);
        }
        None
    }

    #[tracing::instrument(skip(self, producer))]
    pub async fn add_producer(&self, producer: P) -> Result<P, E> {
        let mut session = self.producers.write().await;
        let producers = session
            .entry(producer.info().topic_name.clone())
            .or_default();
        producers.add(producer.clone()).await?;
        Ok(producer)
    }

    #[tracing::instrument(skip(self))]
    pub async fn del_producer(&self, producer_id: u64) -> Option<P> {
        let mut deleted = None;
        {
            let session = self.producers.write().await;
            for producers in session.values() {
                if let Some(producer) = producers.del(producer_id).await {
                    deleted = Some(producer);
                    break;
                }
            }
        }
        self.shrink_producers().await;

        deleted
    }

    #[tracing::instrument(skip(self))]
    pub async fn clear_producer(&self, client_id: u64) {
        {
            let session = self.producers.write().await;
            for consumers in session.values() {
                consumers.del_by_client_id(client_id).await;
            }
        }
        self.shrink_producers().await;
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_available_consumers(
        &self,
        topic_name: &str,
        subscription_name: &str,
    ) -> Result<Vec<C>, E> {
        let session = self.consumers.write().await;
        let Some(topic) = session.get(topic_name) else {
            return Ok(vec![]);
        };
        let Some(subscription) = topic.get(subscription_name) else {
            return Ok(vec![]);
        };
        subscription.get_available_consumers().await
    }

    #[tracing::instrument(skip(self))]
    pub async fn shrink_producers(&self) {
        let mut session = self.producers.write().await;
        let mut topic_deletes = HashSet::new();
        for (topic, producers) in session.iter() {
            if producers.is_empty().await {
                topic_deletes.insert(topic.to_owned());
            }
        }
        session.retain(|topic, _| !topic_deletes.contains(topic));
    }
}

pub struct MemorySubscriptionInfo {
    pub subscription_name: String,
    pub subscription_type: SubscriptionType,
    pub initial_position: InitialPosition,
    pub consumers: HashSet<String>,
}

/// 单机版本不支持 topic 分区
#[derive(Clone)]
pub struct MemoryMeta {
    pub(crate) config: Config,
    session: SessionState<Producer, Consumer>,
    /// key: topic_name, value: topic_config
    topics: Arc<RwLock<HashMap<String, topic::Config>>>,
    /// topic_name: subscription_name: consumer_name
    #[allow(clippy::type_complexity)]
    subscribes: Arc<RwLock<HashMap<String, HashMap<String, MemorySubscriptionInfo>>>>,
}

impl MemoryMeta {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            session: SessionState::new(),
            topics: Arc::new(RwLock::new(HashMap::new())),
            subscribes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl MetaStorage for MemoryMeta {
    type Error = Error;

    type Consumer = Consumer;

    type Producer = Producer;

    #[tracing::instrument(skip(self))]
    async fn create_topic(
        &self,
        topic_name: &str,
        config: topic::Config,
    ) -> Result<(), Self::Error> {
        // 只校验一下参数即可，内存版本不需要记录 topic 信息
        if config.partitions > 1 {
            return ResponseSnafu {
                response: ReturnCode::PartitionedTopicNotSupported,
            }
            .fail();
        }
        self.topics
            .write()
            .await
            .insert(topic_name.to_owned(), config);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn delete_topic(&self, topic_name: &str) -> Result<(), Self::Error> {
        self.topics.write().await.remove(topic_name);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_topic(&self, topic_name: &str) -> Result<Option<NodeTopicInfo>, Self::Error> {
        let topics = self.topics.read().await;
        let Some(config) = topics.get(topic_name) else {
            return Ok(None);
        };
        Ok(Some(NodeTopicInfo {
            partitions: vec![0],
            config: config.clone(),
        }))
    }

    async fn get_topic_subscriptions(
        &self,
        topic_name: &str,
    ) -> Result<Vec<SubscriptionInfo>, Self::Error> {
        let subscribes = self.subscribes.read().await;
        let Some(subscriptions) = subscribes.get(topic_name) else {
            return Ok(vec![]);
        };

        Ok(subscriptions
            .values()
            .map(|sub| SubscriptionInfo {
                subscription_name: sub.subscription_name.clone(),
                subscription_type: sub.subscription_type,
                initial_position: sub.initial_position,
            })
            .collect())
    }

    #[tracing::instrument(skip(self))]
    async fn get_topic_addresses(
        &self,
        _topic_name: &str,
    ) -> Result<Vec<ConnectAddress>, Self::Error> {
        Ok(vec![self.config.broker_addr.clone()])
    }

    #[tracing::instrument(skip(self))]
    async fn get_server_http_addresses(&self) -> Result<Vec<ConnectAddress>, Self::Error> {
        Ok(vec![self.config.http_addr.clone()])
    }

    #[tracing::instrument(skip(self))]
    async fn get_consumer(&self, consumer_id: u64) -> Result<Option<Self::Consumer>, Self::Error> {
        Ok(self.session.get_consumer(consumer_id).await)
    }

    #[tracing::instrument(skip(self))]
    async fn add_consumer(
        &self,
        client_id: u64,
        consumer_id: u64,
        subscribe: Subscribe,
        client_tx: mpsc::UnboundedSender<writer::Request>,
    ) -> Result<Consumer, Self::Error> {
        let mut subscribes = self.subscribes.write().await;
        let subscription = subscribes
            .entry(subscribe.topic_name.clone())
            .or_default()
            .entry(subscribe.subscription_name.clone())
            .or_insert_with(|| MemorySubscriptionInfo {
                subscription_name: subscribe.subscription_name.clone(),
                subscription_type: subscribe.subscription_type,
                initial_position: subscribe.initial_position,
                consumers: HashSet::default(),
            });

        subscription
            .consumers
            .insert(subscribe.consumer_name.clone());
        self.session
            .add_consumer(Consumer::new(consumer_id, client_id, subscribe, client_tx))
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn del_consumer(&self, consumer_id: u64) -> Result<Option<Self::Consumer>, Self::Error> {
        Ok(self.session.del_consumer(consumer_id).await)
    }

    #[tracing::instrument(skip(self))]
    async fn clear_consumer(&self, client_id: u64) -> Result<(), Self::Error> {
        self.session.clear_consumer(client_id).await;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn unsubscribe(
        &self,
        topic_name: &str,
        subscription_name: &str,
        consumer_name: &str,
    ) -> Result<bool, Self::Error> {
        let mut subscribes = self.subscribes.write().await;
        let Some(subscriptions) = subscribes.get_mut(topic_name) else {
            return Ok(false);
        };
        let Some(subscription) = subscriptions.get_mut(subscription_name) else {
            return Ok(false);
        };

        subscription.consumers.remove(consumer_name);
        let remains = !subscription.consumers.is_empty();
        subscribes.retain(|_, c| !c.is_empty());
        Ok(remains)
    }

    #[tracing::instrument(skip(self))]
    async fn get_producer(&self, producer_id: u64) -> Result<Option<Producer>, Self::Error> {
        Ok(self.session.get_producer(producer_id).await)
    }

    #[tracing::instrument(skip(self))]
    async fn add_producer(
        &self,
        client_id: u64,
        producer_id: u64,
        packet: CreateProducer,
    ) -> Result<Producer, Self::Error> {
        self.session
            .add_producer(Producer::new(ProducerInfo::new(
                producer_id,
                client_id,
                packet,
            )))
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn del_producer(&self, producer_id: u64) -> Result<Option<Producer>, Self::Error> {
        Ok(self.session.del_producer(producer_id).await)
    }

    #[tracing::instrument(skip(self))]
    async fn clear_producer(&self, client_id: u64) -> Result<(), Self::Error> {
        self.session.clear_producer(client_id).await;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_available_consumers(
        &self,
        topic_name: &str,
        subscription_name: &str,
    ) -> Result<Vec<Self::Consumer>, Self::Error> {
        self.session
            .get_available_consumers(topic_name, subscription_name)
            .await
    }

    #[tracing::instrument(skip(self))]
    async fn close(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Consumer {
    pub info: ConsumerInfo,
    /// 消费者当前的 permits 值
    permits: Arc<AtomicU32>,
    /// 消费者当前是否已关闭
    closed: Arc<AtomicBool>,
    /// 消费者正在接收自己 已read 但是 unacked 的消息，还不可以接收新消息
    send_unacked_complete: Arc<AtomicBool>,
    pub client_tx: mpsc::UnboundedSender<writer::Request>,
}

impl Consumer {
    pub fn new(
        id: u64,
        client_id: u64,
        subscribe: Subscribe,
        client_tx: mpsc::UnboundedSender<writer::Request>,
    ) -> Self {
        Self {
            info: ConsumerInfo::new(id, client_id, subscribe),
            permits: Arc::new(AtomicU32::default()),
            closed: Arc::new(AtomicBool::default()),
            send_unacked_complete: Arc::new(AtomicBool::default()),
            client_tx,
        }
    }
}

pub enum SubscriptionConsumers<C> {
    Exclusive(C),
    /// sorted by priority, same priority in one group
    /// priority -> consumer_id -> consumer
    Shared(BTreeMap<u8, HashMap<u64, C>>),
}

/// 当前节点上指定 subscription 上连接的所有消费者
#[derive(Clone)]
pub struct SharedSubscriptionConsumers<C>(Arc<RwLock<Option<SubscriptionConsumers<C>>>>);

impl<C> Default for SharedSubscriptionConsumers<C> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<C, E> SharedSubscriptionConsumers<C>
where
    C: MetaConsumer<Error = E>,
    E: From<Error>,
{
    #[tracing::instrument(skip(self, consumer))]
    pub async fn add(&self, consumer: C) -> Result<(), E> {
        let mut consumers = self.0.write().await;
        match consumers.as_mut() {
            Some(sc) => match sc {
                SubscriptionConsumers::Exclusive(_) => {
                    return Err(ResponseSnafu {
                        response: ReturnCode::ConsumerAlreadyExclusive,
                    }
                    .build()
                    .into())
                }
                SubscriptionConsumers::Shared(shared) => {
                    if shared
                        .values()
                        .any(|cs| cs.values().any(|c| c.info().name == consumer.info().name))
                    {
                        return Err(ResponseSnafu {
                            response: ReturnCode::ConsumerNameAlreadyExists,
                        }
                        .build()
                        .into());
                    }
                    shared
                        .entry(consumer.info().priority)
                        .or_default()
                        .insert(consumer.info().id, consumer);
                }
            },
            None => match consumer.info().subscription_type {
                SubscriptionType::Exclusive => {
                    *consumers = Some(SubscriptionConsumers::Exclusive(consumer))
                }
                SubscriptionType::Shared => {
                    let priority = consumer.info().priority;
                    let mut shared: BTreeMap<u8, HashMap<u64, C>> = BTreeMap::new();
                    shared
                        .entry(priority)
                        .or_default()
                        .insert(consumer.info().id, consumer);
                    *consumers = Some(SubscriptionConsumers::Shared(shared));
                }
            },
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn del(&self, id: u64) -> Option<C> {
        let mut consumers = self.0.write().await;
        let mut consumer = None;
        match consumers.as_mut()? {
            SubscriptionConsumers::Exclusive(c) if c.info().id == id => {
                c.close().await.ok();
                consumer = Some(c.clone());
                *consumers = None;
            }
            SubscriptionConsumers::Shared(shared) if !shared.is_empty() => {
                for cs in shared.values_mut() {
                    if let Some(c) = cs.remove(&id) {
                        c.close().await.ok();
                        consumer = Some(c);
                        break;
                    }
                }
                shared.retain(|_, cs| !cs.is_empty());
                if shared.is_empty() {
                    *consumers = None;
                }
            }
            _ => return None,
        }
        consumer.take()
    }

    #[tracing::instrument(skip(self))]
    pub async fn del_by_client_id(&self, client_id: u64) {
        let mut consumers = self.0.write().await;
        let Some(cs) = consumers.as_mut() else {
            return;
        };
        match cs {
            SubscriptionConsumers::Exclusive(ex) => {
                if ex.info().client_id == client_id {
                    ex.close().await.ok();
                    *consumers = None;
                }
            }
            SubscriptionConsumers::Shared(sh) => {
                sh.retain(|_, b| {
                    b.retain(|_, d| d.info().client_id != client_id);
                    !b.is_empty()
                });
                if sh.is_empty() {
                    *consumers = None;
                }
            }
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn is_empty(&self) -> bool {
        self.0.read().await.is_none()
    }

    #[tracing::instrument(skip(self))]
    pub async fn get(&self, consumer_id: u64) -> Option<C> {
        let consumers = self.0.read().await;
        consumers.as_ref().and_then(|sc| match sc {
            SubscriptionConsumers::Exclusive(c) if c.info().id == consumer_id => Some(c).cloned(),
            SubscriptionConsumers::Shared(shared) => {
                shared.values().find_map(|sc| sc.get(&consumer_id)).cloned()
            }
            _ => None,
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_available_consumers(&self) -> Result<Vec<C>, E> {
        let consumers = self.0.read().await;
        let Some(consumers) = consumers.as_ref() else {
            return Ok(vec![]);
        };
        match consumers {
            SubscriptionConsumers::Exclusive(consumer) if consumer.is_available().await? => {
                Ok(vec![consumer.clone()])
            }
            SubscriptionConsumers::Shared(shared) => {
                for level in shared.values() {
                    let mut consumers = Vec::with_capacity(level.len());
                    for consumer in level.values() {
                        if consumer.is_available().await? {
                            consumers.push(consumer.clone());
                        }
                    }
                    if !consumers.is_empty() {
                        return Ok(consumers);
                    }
                }
                Ok(vec![])
            }
            _ => Ok(vec![]),
        }
    }
}

impl MetaConsumer for Consumer {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    fn info(&self) -> super::ConsumerInfo {
        self.info.clone()
    }

    #[tracing::instrument(skip(self))]
    async fn ready(&self) -> Result<(), Self::Error> {
        self.send_unacked_complete
            .store(true, atomic::Ordering::Release);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    #[tracing::instrument(skip(self))]
    async fn is_ready(&self) -> Result<bool, Self::Error> {
        Ok(self.send_unacked_complete.load(atomic::Ordering::Acquire))
    }

    #[tracing::instrument(skip(self))]
    async fn permits(&self) -> Result<u32, Self::Error> {
        Ok(self.permits.load(atomic::Ordering::Acquire))
    }

    #[tracing::instrument(skip(self))]
    async fn permits_add(&self, delta: u32) -> Result<(), Self::Error> {
        self.permits.fetch_add(delta, atomic::Ordering::Release);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn permits_sub(&self, delta: u32) -> Result<(), Self::Error> {
        self.permits.fetch_sub(delta, atomic::Ordering::Release);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<(), Self::Error> {
        self.closed.store(true, atomic::Ordering::Release);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn is_closed(&self) -> Result<bool, Self::Error> {
        Ok(self.closed.load(atomic::Ordering::Acquire))
    }

    #[tracing::instrument(skip(self))]
    async fn send(&self, message: send::Send) -> Result<(), Self::Error> {
        let (res_tx, res_rx) = oneshot::channel();
        if self
            .client_tx
            .send(writer::Request {
                packet: Packet::Send(message),
                res_tx: writer::ReplyHandle::WaitReply(res_tx),
            })
            .is_err()
        {
            self.close().await?;
            return ClientDisconnectSnafu.fail();
        }
        let Ok(result) = res_rx.await else {
            self.close().await?;
            return ClientDisconnectSnafu.fail();
        };
        match result.context(CodecIoSnafu)? {
            Packet::Response(response) if response.is_success() => Ok(()),
            _packet => ResponseSnafu {
                response: ReturnCode::UnexpectedPacket,
            }
            .fail(),
        }
    }
}

#[derive(Clone)]
pub struct Producer {
    pub info: ProducerInfo,
}

impl Producer {
    pub fn new(info: ProducerInfo) -> Self {
        Self { info }
    }
}

pub enum TopicProducers<P> {
    Exclusive(P),
    Shared(HashMap<u64, P>),
}

pub struct SharedTopicProducers<P>(Arc<RwLock<Option<TopicProducers<P>>>>);

impl<P> Default for SharedTopicProducers<P> {
    fn default() -> Self {
        Self(Default::default())
    }
}

impl<P, E> SharedTopicProducers<P>
where
    P: MetaProducer<Error = E>,
    E: From<Error>,
{
    #[tracing::instrument(skip(self))]
    pub async fn get(&self, id: u64) -> Option<P> {
        let producers = self.0.read().await;
        match producers.as_ref()? {
            TopicProducers::Exclusive(p) if p.info().id == id => Some(p.clone()),
            TopicProducers::Shared(shared) => shared.get(&id).cloned(),
            _ => None,
        }
    }

    #[tracing::instrument(skip(self, producer))]
    pub async fn add(&self, producer: P) -> Result<(), E> {
        let mut producers = self.0.write().await;
        match producers.as_mut() {
            Some(producers) => match producers {
                TopicProducers::Exclusive(_) => Err(ResponseSnafu {
                    response: ReturnCode::ProducerAlreadyExclusive,
                }
                .build()
                .into()),
                TopicProducers::Shared(shared) => match producer.info().access_mode {
                    AccessMode::Exclusive => Err(ResponseSnafu {
                        response: ReturnCode::ProducerAccessModeMismatched,
                    }
                    .build()
                    .into()),
                    AccessMode::Shared => {
                        shared.insert(producer.info().id, producer);
                        Ok(())
                    }
                },
            },
            None => match producer.info().access_mode {
                AccessMode::Exclusive => {
                    *producers = Some(TopicProducers::Exclusive(producer));
                    Ok(())
                }
                AccessMode::Shared => {
                    let mut shared = HashMap::new();
                    shared.insert(producer.info().id, producer);
                    *producers = Some(TopicProducers::Shared(shared));
                    Ok(())
                }
            },
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn del(&self, id: u64) -> Option<P> {
        let mut producers = self.0.write().await;
        let mut producer = None;
        match producers.as_mut()? {
            TopicProducers::Exclusive(p) if p.info().id == id => {
                producer = Some(p.clone());
                *producers = None;
            }
            TopicProducers::Shared(shared) if shared.contains_key(&id) => {
                producer = shared.remove(&id);
                if shared.is_empty() {
                    *producers = None;
                }
            }
            _ => {}
        }

        producer
    }

    #[tracing::instrument(skip(self))]
    pub async fn is_empty(&self) -> bool {
        self.0.read().await.is_none()
    }

    #[tracing::instrument(skip(self))]
    pub async fn del_by_client_id(&self, client_id: u64) {
        let mut producers = self.0.write().await;
        let Some(ps) = producers.as_mut() else {
            return;
        };
        match ps {
            TopicProducers::Exclusive(p) if p.info().client_id == client_id => {
                *producers = None;
            }
            TopicProducers::Shared(shared) => {
                shared.retain(|_, p| p.info().client_id != client_id);
                if shared.is_empty() {
                    *producers = None;
                }
            }
            _ => {}
        }
    }
}

impl MetaProducer for Producer {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    fn info(&self) -> ProducerInfo {
        self.info.clone()
    }
}
