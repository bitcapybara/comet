use comet_common::{
    addr::ConnectAddress,
    error::ResponsiveError,
    io::writer,
    protocol::{consumer::Subscribe, producer::CreateProducer, send},
    types::{AccessMode, InitialPosition, SubscriptionType},
};
use futures::Future;
use tokio::sync::mpsc;

use crate::broker::topic;

#[cfg(feature = "distributed")]
pub mod etcd;
#[cfg(feature = "local-memory")]
pub mod mem;
#[cfg(feature = "local-persist")]
pub mod redb;

#[derive(Debug, Clone)]
pub struct ConsumerInfo {
    pub id: u64,
    /// consumer name
    pub name: String,
    pub client_id: u64,
    /// subscribe topic
    pub topic_name: String,
    /// subscription id
    pub subscription_name: String,
    /// subscribe type
    pub subscription_type: SubscriptionType,
    /// consume init position
    pub initial_position: InitialPosition,
    /// default permits
    pub default_permits: u32,
    pub priority: u8,
}

impl ConsumerInfo {
    pub fn new(id: u64, client_id: u64, subscribe: Subscribe) -> Self {
        Self {
            id,
            name: subscribe.consumer_name,
            client_id,
            topic_name: subscribe.topic_name,
            subscription_name: subscribe.subscription_name,
            subscription_type: subscribe.subscription_type,
            initial_position: subscribe.initial_position,
            default_permits: subscribe.default_permits,
            priority: subscribe.priority,
        }
    }
}

pub(crate) trait MetaConsumer: Clone + Send + Sync + 'static {
    type Error: ResponsiveError;

    fn info(&self) -> ConsumerInfo;

    /// 设置 consumer 已准备好接收消息
    fn ready(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// consumer 是否准备好接收消息
    fn is_ready(&self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn permits(&self) -> impl Future<Output = Result<u32, Self::Error>> + Send;

    fn permits_add(&self, delta: u32) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn permits_sub(&self, delta: u32) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 关闭 consumer
    fn close(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// consumer 是否已关闭
    fn is_closed(&self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn send(&self, message: send::Send) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn is_available(&self) -> impl Future<Output = Result<bool, Self::Error>> + Send {
        async {
            Ok(self.is_ready().await? && !self.is_closed().await? && self.permits().await? > 0)
        }
    }
}

#[derive(Clone)]
pub struct ProducerInfo {
    pub id: u64,
    pub client_id: u64,
    pub producer_name: String,
    pub topic_name: String,
    pub access_mode: AccessMode,
}

impl ProducerInfo {
    pub fn new(id: u64, client_id: u64, producer: CreateProducer) -> Self {
        Self {
            id,
            client_id,
            producer_name: producer.producer_name,
            topic_name: producer.topic_name,
            access_mode: producer.access_mode,
        }
    }
}

/// 当前节点可以持有的 topic 信息
#[derive(Debug)]
pub(crate) struct NodeTopicInfo {
    /// 当前节点可以持有的所有分区
    pub partitions: Vec<u16>,
    pub config: topic::Config,
}

#[derive(Debug)]
pub struct SubscriptionInfo {
    pub subscription_name: String,
    pub subscription_type: SubscriptionType,
    pub initial_position: InitialPosition,
}

pub trait MetaProducer: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + 'static;

    fn info(&self) -> ProducerInfo;
}

pub trait MetaStorage: Clone + Send + Sync + 'static {
    type Error: ResponsiveError;

    type Consumer: MetaConsumer;

    type Producer: MetaProducer;

    /// 创建 topic 相关配置
    fn create_topic(
        &self,
        topic_name: &str,
        config: topic::Config,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 删除 topic 的配置信息
    fn delete_topic(
        &self,
        topic_name: &str,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 获取分配给本节点的 topic 分配结果
    fn get_topic(
        &self,
        topic_name: &str,
    ) -> impl Future<Output = Result<Option<NodeTopicInfo>, Self::Error>> + Send;

    /// 发送给领导者，领导者保存各个节点的 topic 的分配结果
    fn get_topic_addresses(
        &self,
        topic_name: &str,
    ) -> impl Future<Output = Result<Vec<ConnectAddress>, Self::Error>> + Send;

    fn get_topic_subscriptions(
        &self,
        topic_name: &str,
    ) -> impl Future<Output = Result<Vec<SubscriptionInfo>, Self::Error>> + Send;

    fn get_server_http_addresses(
        &self,
    ) -> impl Future<Output = Result<Vec<ConnectAddress>, Self::Error>> + Send;

    fn get_available_consumers(
        &self,
        topic_name: &str,
        subscription_name: &str,
    ) -> impl Future<Output = Result<Vec<Self::Consumer>, Self::Error>> + Send;

    fn get_consumer(
        &self,
        consumer_id: u64,
    ) -> impl Future<Output = Result<Option<Self::Consumer>, Self::Error>> + Send;

    /// consumers 不存在则报错
    /// 如果 subscription 不存在，则创建，并且将 consumer 注册到这个 subscription 上
    fn add_consumer(
        &self,
        client_id: u64,
        consumer_id: u64,
        subscribe: Subscribe,
        client_tx: mpsc::UnboundedSender<writer::Request>,
    ) -> impl Future<Output = Result<Self::Consumer, Self::Error>> + Send;

    /// 需要调用 consumer.close() 来关闭消费者
    /// consumers 为空直接删除
    fn del_consumer(
        &self,
        consumer_id: u64,
    ) -> impl Future<Output = Result<Option<Self::Consumer>, Self::Error>> + Send;

    /// 需要调用 consumer.close() 来关闭消费者
    fn clear_consumer(
        &self,
        client_id: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// 返回是否还有剩余的订阅者
    fn unsubscribe(
        &self,
        topic_name: &str,
        subscription_name: &str,
        consumer_name: &str,
    ) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    fn get_producer(
        &self,
        producer_id: u64,
    ) -> impl Future<Output = Result<Option<Self::Producer>, Self::Error>> + Send;

    fn add_producer(
        &self,
        client_id: u64,
        producer_id: u64,
        packet: CreateProducer,
    ) -> impl Future<Output = Result<Self::Producer, Self::Error>> + Send;

    fn del_producer(
        &self,
        producer_id: u64,
    ) -> impl Future<Output = Result<Option<Self::Producer>, Self::Error>> + Send;

    fn clear_producer(
        &self,
        client_id: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn close(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
