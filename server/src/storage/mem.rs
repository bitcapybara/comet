use std::{cmp, collections::BTreeMap, sync::Arc};

use chrono::{DateTime, Utc};
use comet_common::{
    error::ResponsiveError,
    id::{local::LocalId, IdGenerator},
    protocol::response::Response,
    types::InitialPosition,
};
use snafu::Snafu;
use tokio::sync::RwLock;

use super::{PublishedMessage, SubscriptionStorage, TopicMessage, TopicStorage};

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("unreachable"))]
    _Infallible,
}

impl ResponsiveError for Error {
    fn as_response(&self) -> Response {
        unreachable!()
    }
}

/// 单机版本不支持分区
#[derive(Clone)]
pub struct MemoryTopicStorage {
    id_generator: LocalId,
    /// message id -> message
    topic: Arc<RwLock<BTreeMap<u64, TopicMessage>>>,
}

impl MemoryTopicStorage {
    pub fn new() -> Self {
        Self {
            id_generator: LocalId::default(),
            topic: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    #[cfg(any(feature = "local-persist", feature = "distributed"))]
    pub async fn shrink(&self, num_limit: usize) {
        let mut topic = self.topic.write().await;
        if topic.len() > num_limit {
            topic.pop_first();
        }
    }

    #[cfg(any(feature = "local-persist", feature = "distributed"))]
    pub async fn push_message(&self, message: TopicMessage) {
        self.topic.write().await.insert(message.message_id, message);
    }
}

impl TopicStorage for MemoryTopicStorage {
    type Error = Error;

    type SubscriptionStorage = MemorySubscriptionStorage;

    #[tracing::instrument(skip(self))]
    async fn create_subscription(
        &self,
        _subscription_name: &str,
        initial_position: InitialPosition,
    ) -> Result<Self::SubscriptionStorage, Self::Error> {
        let records = match initial_position {
            InitialPosition::Latest => BTreeMap::new(),
            InitialPosition::Earliest => {
                let mut records = BTreeMap::new();
                for &id in self.topic.read().await.keys() {
                    records.insert(id, Record::default());
                }
                records
            }
        };
        let subscription = MemorySubscriptionStorage {
            topic: self.topic.clone(),
            subscription: Arc::new(RwLock::new(records)),
        };

        Ok(subscription)
    }

    #[tracing::instrument(skip(self))]
    async fn publish_message(&self, message: PublishedMessage) -> Result<u64, Self::Error> {
        let message_id = self.id_generator.next_id();
        let mut message = TopicMessage::from_publish_message(message, message_id);
        message.message_id = message_id;
        self.topic.write().await.insert(message_id, message);
        Ok(message_id)
    }

    #[tracing::instrument(skip(self))]
    async fn retain_messages(&self, min_message_id: Option<u64>) -> Result<(), Self::Error> {
        let mut topic = self.topic.write().await;
        match min_message_id {
            Some(min_id) => {
                topic.retain(|id, _| id < &min_id);
            }
            None => {
                topic.clear();
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn last_sequence_id(&self, producer_name: &str) -> Result<u64, Self::Error> {
        Ok(self
            .topic
            .read()
            .await
            .values()
            .rev()
            .find(|v| v.producer_name == producer_name)
            .map(|v| v.sequence_id)
            .unwrap_or_default())
    }

    #[tracing::instrument(skip(self))]
    async fn drop(&self) -> Result<(), Self::Error> {
        self.topic.write().await.clear();
        Ok(())
    }
}

#[derive(Debug, Default)]
struct Record {
    loaded: bool,
    read_by: Option<String>,
    acked: bool,
    create_time: DateTime<Utc>,
    deliver_time: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct MemorySubscriptionStorage {
    /// message id -> message
    topic: Arc<RwLock<BTreeMap<u64, TopicMessage>>>,
    /// 当前的 subscription
    subscription: Arc<RwLock<BTreeMap<u64, Record>>>,
}

impl MemorySubscriptionStorage {
    #[cfg(any(feature = "local-persist", feature = "distributed"))]
    pub fn new(topic_storage: MemoryTopicStorage) -> Self {
        Self {
            topic: topic_storage.topic,
            subscription: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    #[cfg(any(feature = "local-persist", feature = "distributed"))]
    pub async fn first_unread_id(&self) -> Option<u64> {
        self.subscription
            .read()
            .await
            .iter()
            .find(|(_, record)| record.read_by.is_none())
            .map(|(id, _)| id)
            .copied()
    }
}

impl SubscriptionStorage for MemorySubscriptionStorage {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    async fn add_record(&self, message_id: u64) -> Result<(), Self::Error> {
        let topic = self.topic.read().await;

        let mut subscription = self.subscription.write().await;
        let record = Record {
            deliver_time: topic.get(&message_id).and_then(|m| m.deliver_time),
            ..Default::default()
        };
        subscription.insert(message_id, record);

        while let Some((id, record)) = subscription.pop_first() {
            if topic.contains_key(&id) {
                subscription.insert(id, record);
                return Ok(());
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_loaded_unread_messages(&self) -> Result<Vec<TopicMessage>, Self::Error> {
        let topic = self.topic.read().await;
        let mut messages = Vec::new();
        for (id, record) in self
            .subscription
            .write()
            .await
            .iter_mut()
            .filter(|(_, record)| {
                record.loaded
                    && record.read_by.is_none()
                    && !record.acked
                    && record.deliver_time.is_none()
            })
        {
            record.loaded = true;
            if let Some(message) = topic.get(id) {
                messages.push(message.clone())
            }
        }

        Ok(messages)
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unread_messages(&self, limit: usize) -> Result<Vec<TopicMessage>, Self::Error> {
        let topic = self.topic.read().await;
        let mut messages = Vec::with_capacity(limit);
        for (id, record) in self
            .subscription
            .write()
            .await
            .iter_mut()
            .filter(|(_, record)| {
                !record.loaded
                    && record.read_by.is_none()
                    && !record.acked
                    && record.deliver_time.is_none()
            })
            .take(limit)
        {
            record.loaded = true;
            if let Some(message) = topic.get(id) {
                messages.push(message.clone())
            }
        }

        Ok(messages)
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unacked_messages(
        &self,
        consumer_name: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, Self::Error> {
        let topic = self.topic.read().await;
        let messages = self
            .subscription
            .read()
            .await
            .iter()
            .filter_map(|(id, record)| {
                if record.read_by.is_none()
                    || record
                        .read_by
                        .as_ref()
                        .is_some_and(|by| by != consumer_name)
                    || record.acked
                    || record.deliver_time.is_some()
                {
                    return None;
                }

                topic.get(id)
            })
            .skip(from)
            .take(limit)
            .cloned()
            .collect();
        Ok(messages)
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unloaded_delayed_messages(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, Self::Error> {
        let topic = self.topic.read().await;
        let mut messages = Vec::with_capacity(limit);
        for (id, record) in self
            .subscription
            .write()
            .await
            .iter_mut()
            .filter(|(_, record)| {
                !record.loaded
                    && !record.acked
                    && record.read_by.is_none()
                    && record.deliver_time.is_some_and(|t| t > start && t <= end)
            })
            .take(limit)
        {
            record.loaded = true;
            if let Some(message) = topic.get(id) {
                messages.push(message.clone());
            }
        }
        Ok(messages)
    }

    #[tracing::instrument(skip(self))]
    async fn fetch_unread_delayed_messages(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        from: usize,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, Self::Error> {
        let topic = self.topic.read().await;
        let subscription = self.subscription.read().await;
        let messages = subscription
            .iter()
            .filter_map(|(id, record)| {
                if !record.loaded && !record.acked && record.read_by.is_none() {
                    topic
                        .get(id)
                        .filter(|m| m.deliver_time.is_some_and(|t| t > start && t <= end))
                } else {
                    None
                }
            })
            .skip(from)
            .take(limit)
            .cloned()
            .collect();
        Ok(messages)
    }

    #[tracing::instrument(skip(self))]
    async fn set_read(&self, consumer_name: &str, message_id: u64) -> Result<(), Self::Error> {
        let mut subscription = self.subscription.write().await;
        let Some(record) = subscription.get_mut(&message_id) else {
            return Ok(());
        };
        let _ = record.read_by.insert(consumer_name.to_string());
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn set_acks(&self, message_ids: &[u64]) -> Result<(), Self::Error> {
        let mut subscription = self.subscription.write().await;
        for message_id in message_ids {
            let Some(record) = subscription.get_mut(message_id) else {
                continue;
            };
            record.acked = true;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn has_unread(&self) -> Result<bool, Self::Error> {
        let subscription = self.subscription.read().await;
        let has_unread = subscription
            .iter()
            .any(|(_id, record)| !record.loaded && record.read_by.is_none());
        Ok(has_unread)
    }

    #[tracing::instrument(skip(self))]
    async fn drop(&self) -> Result<(), Self::Error> {
        self.subscription.write().await.clear();
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn retain_acked(
        &self,
        num_limit: Option<usize>,
        create_after: Option<DateTime<Utc>>,
    ) -> Result<(), Self::Error> {
        let mut size_min_id = None;
        if let Some(num_limit) = num_limit {
            let subscription = self.subscription.read().await;
            size_min_id = {
                subscription
                    .iter()
                    .rev()
                    .filter(|(_, record)| record.acked)
                    .nth(num_limit)
                    .map(|(id, _)| *id)
            }
        }

        let mut time_min_id = None;
        if let Some(create_after) = create_after {
            let subscription = self.subscription.read().await;
            time_min_id = subscription
                .iter()
                .rev()
                .filter(|(_, record)| record.acked)
                .find(|(_, record)| record.create_time < create_after)
                .map(|(id, _)| *id);
        }

        let Some(min_id) = cmp::max(size_min_id, time_min_id) else {
            return Ok(());
        };

        self.subscription.write().await.retain(|id, _| id > &min_id);

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn drain_acked(&self) -> Result<(), Self::Error> {
        self.subscription
            .write()
            .await
            .retain(|_, record| !record.acked);
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn first_unacked_id(&self) -> Result<Option<u64>, Self::Error> {
        Ok(self
            .subscription
            .read()
            .await
            .iter()
            .find(|(_, record)| record.acked)
            .map(|(id, _)| id)
            .copied())
    }
}
