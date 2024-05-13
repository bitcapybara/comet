#[cfg(feature = "local-memory")]
pub mod mem;
#[cfg(feature = "distributed")]
pub mod postgres;
#[cfg(feature = "local-persist")]
pub mod sqlite;

use chrono::{DateTime, Utc};
use comet_common::{error::ResponsiveError, protocol, types::InitialPosition};
use std::future::Future;

#[derive(Debug, Clone)]
pub struct PublishedMessage {
    pub topic_name: String,
    pub producer_name: String,
    pub sequence_id: u64,
    pub publish_time: DateTime<Utc>,
    pub deliver_time: Option<DateTime<Utc>>,
    pub handle_time: DateTime<Utc>,
    pub payload: bytes::Bytes,
}

#[derive(Clone)]
pub struct TopicMessage {
    pub message_id: u64,
    pub topic_name: String,
    pub producer_name: String,
    pub sequence_id: u64,
    pub publish_time: DateTime<Utc>,
    pub deliver_time: Option<DateTime<Utc>>,
    pub handle_time: DateTime<Utc>,
    pub payload: bytes::Bytes,
}

impl TopicMessage {
    pub fn into_send_message(self, consumer_id: u64) -> protocol::send::Send {
        protocol::send::Send {
            header: protocol::send::SendHeader {
                message_id: self.message_id,
                consumer_id,
                payload_len: self.payload.len() as u64,
                publish_time: self.publish_time,
                send_time: Utc::now(),
                deliver_time: self.deliver_time,
                topic_name: self.topic_name,
                producer_name: self.producer_name,
                handle_time: self.handle_time,
            },
            payload: self.payload,
        }
    }

    #[cfg(any(feature = "local-persist", feature = "local-memory"))]
    pub fn from_publish_message(message: PublishedMessage, message_id: u64) -> Self {
        Self {
            message_id,
            topic_name: message.topic_name,
            producer_name: message.producer_name,
            sequence_id: message.sequence_id,
            publish_time: message.publish_time,
            payload: message.payload,
            deliver_time: message.deliver_time,
            handle_time: message.handle_time,
        }
    }
}

#[cfg(any(feature = "local-persist", feature = "distributed"))]
mod cache {
    #[derive(Clone)]
    pub(crate) struct SubscriptionStorageCache {
        /// 启动时 数据库第一个未读id，reads 的 index=0 代表的 id
        /// None: 数据库没有数据或全部是已读
        pub(crate) init_db_start_unread_id: Option<u64>,
        pub(crate) reads: bitvec::prelude::BitVec<u64, bitvec::prelude::Lsb0>,
        /// 内存中的 subscription 数据
        pub(crate) mem: super::mem::MemorySubscriptionStorage,
    }

    impl SubscriptionStorageCache {
        /// 是否有未读消息，有的话，是否在内存里
        pub(crate) async fn is_unread_all_in_memory(&self) -> Option<bool> {
            let init_db_start_id = self.init_db_start_unread_id?;
            let first_unread_id_in_reads = self.reads.first_zero().map(|n| n as u64)?;
            let Some(mem_first_unread_id) = self.mem.first_unread_id().await else {
                return Some(false);
            };

            Some(init_db_start_id + first_unread_id_in_reads >= mem_first_unread_id)
        }

        pub(crate) async fn has_unread(&self) -> bool {
            self.init_db_start_unread_id
                .is_some_and(|_| self.reads.first_zero().is_some())
        }
    }
}

pub trait TopicStorage: Clone + Send + Sync + 'static
where
    Self: Sized,
{
    type Error: ResponsiveError;

    type SubscriptionStorage: SubscriptionStorage<Error = Self::Error>;

    fn create_subscription(
        &self,
        subscription_name: &str,
        initial_position: InitialPosition,
    ) -> impl Future<Output = Result<Self::SubscriptionStorage, Self::Error>> + Send;

    /// save message to topic table and subscription tables
    fn publish_message(
        &self,
        message: PublishedMessage,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    fn retain_messages(
        &self,
        min_message_id: Option<u64>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn last_sequence_id(
        &self,
        producer_name: &str,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send;

    fn drop(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub trait SubscriptionStorage: Clone + Send + Sync + 'static {
    type Error: ResponsiveError;

    fn add_record(&self, message_id: u64) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn fetch_loaded_unread_messages(
        &self,
    ) -> impl Future<Output = Result<Vec<TopicMessage>, Self::Error>> + Send;

    /// 加载未读数据，loaded 置为 true
    fn fetch_unread_messages(
        &self,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<TopicMessage>, Self::Error>> + Send;

    /// 在 consumer 连接后，发送 read_by=consumer_name 且 acked=false 的数据
    fn fetch_unacked_messages(
        &self,
        consumer_name: &str,
        from: usize,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<TopicMessage>, Self::Error>> + Send;

    fn fetch_unloaded_delayed_messages(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<TopicMessage>, Self::Error>> + Send;

    /// 服务首次启动时加载 loaded=true 且 read_by=null 的数据
    fn fetch_unread_delayed_messages(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        from: usize,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<TopicMessage>, Self::Error>> + Send;

    fn set_read(
        &self,
        consumer_name: &str,
        message_id: u64,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn set_acks(&self, message_ids: &[u64])
        -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn has_unread(&self) -> impl Future<Output = Result<bool, Self::Error>> + Send;

    /// 此方法用于执行**已**确认消息的保留策略  
    /// `size_sum`: 数据库中需要保留的消息字节数  
    /// `after`: 数据库中需要保留的消息最早的创建时间
    fn retain_acked(
        &self,
        num_limit: Option<usize>,
        create_after: Option<DateTime<Utc>>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn drain_acked(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn first_unacked_id(&self) -> impl Future<Output = Result<Option<u64>, Self::Error>> + Send;

    fn drop(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
