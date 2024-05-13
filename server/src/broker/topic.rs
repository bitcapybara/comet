use std::{collections::HashMap, sync::Arc, time::Duration};

use comet_common::{
    error::{BoxedError, ResponsiveError},
    protocol::{
        acknowledge::Acknowledge,
        response::{Response, ReturnCode},
    },
};
use futures::{stream::FuturesUnordered, StreamExt};
use snafu::{IntoError, Location, ResultExt, Snafu};
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
    time::MissedTickBehavior,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    meta::{ConsumerInfo, MetaConsumer, MetaStorage},
    storage::{PublishedMessage, TopicStorage},
};

use super::subscription::{self, Subscription, SubscriptionInfo};

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("response error: {response}"))]
    PacketResponse {
        response: Response,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("subscription error"))]
    Subscription {
        source: subscription::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("meta error"))]
    Meta {
        source: comet_common::error::BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("storage error"))]
    Storage {
        source: comet_common::error::BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ResponsiveError for Error {
    fn as_response(&self) -> Response {
        match self {
            Error::PacketResponse { response, .. } => response.clone(),
            Error::Meta { source, .. } => source.as_response(),
            Error::Storage { source, .. } => source.as_response(),
            e => Response::new(ReturnCode::Internal, e.to_string()),
        }
    }
}

type SharedSubscriptions<M, T> = Arc<RwLock<HashMap<String, Subscription<M, T>>>>;

pub type Config = comet_common::http::topic::Config;
pub type AckedRetentionPolicyConfig = comet_common::http::topic::AckedRetentionPolicyConfig;

#[derive(Clone)]
pub struct Topic<M, T>
where
    M: MetaStorage,
    T: TopicStorage,
{
    config: Config,
    topic_name: String,
    partitioned_topic_name: String,
    meta: M,
    storage: T,
    handle: Arc<Mutex<JoinSet<()>>>,
    subscriptions: SharedSubscriptions<M, T::SubscriptionStorage>,
    token: CancellationToken,
}

impl<M, T> Topic<M, T>
where
    M: MetaStorage,
    T: TopicStorage,
{
    #[tracing::instrument(skip(meta, storage, token))]
    pub async fn new(
        topic_name: &str,
        partitioned_topic_name: &str,
        meta: M,
        storage: T,
        config: Config,
        token: CancellationToken,
    ) -> Result<Self, Error> {
        let subscriptions = {
            let subscription_infos = meta
                .get_topic_subscriptions(topic_name)
                .await
                .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?;
            let mut subscriptions = HashMap::with_capacity(subscription_infos.len());
            for info in subscription_infos {
                let storage = storage
                    .create_subscription(&info.subscription_name, info.initial_position)
                    .await
                    .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))?;
                let subscription = Subscription::new(
                    config.clone(),
                    meta.clone(),
                    storage,
                    SubscriptionInfo {
                        topic_name: partitioned_topic_name.to_owned(),
                        subscription_name: info.subscription_name.clone(),
                        subscription_type: info.subscription_type,
                    },
                    token.child_token(),
                )
                .await
                .context(SubscriptionSnafu)?;
                subscriptions.insert(info.subscription_name, subscription);
            }
            Arc::new(RwLock::new(subscriptions))
        };
        let mut handle = JoinSet::new();
        handle.spawn(start_acked_retention(
            subscriptions.clone(),
            storage.clone(),
            token.clone(),
        ));
        Ok(Self {
            config,
            topic_name: topic_name.to_owned(),
            meta,
            storage,
            handle: Arc::new(Mutex::new(handle)),
            subscriptions,
            token,
            partitioned_topic_name: partitioned_topic_name.to_owned(),
        })
    }

    #[tracing::instrument(skip(self, consumer))]
    pub async fn add_consumer(&self, consumer: M::Consumer) -> Result<(), Error> {
        let ConsumerInfo {
            subscription_name,
            subscription_type,
            initial_position,
            ..
        } = &consumer.info();
        let mut subscriptions = self.subscriptions.write().await;
        match subscriptions.get(subscription_name) {
            Some(subscription) => {
                subscription.add_consumer(consumer).await;
            }
            None => {
                let storage = self
                    .storage
                    .create_subscription(subscription_name, *initial_position)
                    .await
                    .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))?;
                let subscription = Subscription::new(
                    self.config.clone(),
                    self.meta.clone(),
                    storage,
                    SubscriptionInfo {
                        topic_name: self.partitioned_topic_name.clone(),
                        subscription_name: subscription_name.clone(),
                        subscription_type: *subscription_type,
                    },
                    self.token.child_token(),
                )
                .await
                .context(SubscriptionSnafu)?;
                subscription.add_consumer(consumer).await;
                subscriptions.insert(subscription_name.clone(), subscription);
            }
        }
        Ok(())
    }

    pub async fn last_sequence_id(&self, producer_name: &str) -> Result<u64, Error> {
        self.storage
            .last_sequence_id(producer_name)
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))
    }

    #[tracing::instrument(skip(self))]
    pub async fn publish(&self, message: PublishedMessage) -> Result<(), Error> {
        // save to database
        let message_id = self
            .storage
            .publish_message(message)
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))?;
        // notify all subscriptions
        let subscriptions = self.subscriptions.read().await;
        for subscription in subscriptions.values() {
            subscription
                .publish_notify(message_id)
                .await
                .context(SubscriptionSnafu)?;
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn acknowledge(
        &self,
        subscription_name: &str,
        ack: Acknowledge,
    ) -> Result<(), Error> {
        let subscriptions = self.subscriptions.read().await;
        let Some(subscription) = subscriptions.get(subscription_name) else {
            return PacketResponseSnafu {
                response: ReturnCode::SubscriptionNotFound,
            }
            .fail();
        };
        subscription
            .acks(&ack.message_ids)
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn unsubscribe(
        &self,
        subscription_name: &str,
        consumer_name: &str,
    ) -> Result<(), Error> {
        if self
            .meta
            .unsubscribe(&self.topic_name, subscription_name, consumer_name)
            .await
            .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?
        {
            return Ok(());
        }

        // 删除掉 subscription
        let mut subscriptions = self.subscriptions.write().await;
        let Some(subscription) = subscriptions.remove(subscription_name) else {
            unreachable!()
        };
        subscription
            .drop_storage()
            .await
            .context(SubscriptionSnafu)?;

        subscription.shutdown().await;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn drop_storage(&self) -> Result<(), Error> {
        self.storage
            .drop()
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))
    }

    #[tracing::instrument(skip(self))]
    pub async fn shutdown(self) {
        self.token.cancel();
        let mut subscriptions = self.subscriptions.write().await;
        let mut futs = FuturesUnordered::from_iter(
            subscriptions
                .drain()
                .map(|(_, subscription)| subscription.shutdown()),
        );
        while futs.next().await.is_some() {}
        let mut handle = self.handle.lock().await;
        while handle.join_next().await.is_some() {}
    }
}

async fn start_acked_retention<M, T>(
    subscriptions: SharedSubscriptions<M, T::SubscriptionStorage>,
    storage: T,
    token: CancellationToken,
) where
    M: MetaStorage,
    T: TopicStorage,
{
    let mut prev_acked_id = None;
    // TODO: 用户指定或使用随机值
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let min_unacked_id = {
                    let subscriptions = subscriptions.read().await;
                    if subscriptions.is_empty() {
                        continue;
                    }
                    let mut min_unacked_id = None;
                    for subscription in subscriptions.values() {
                        min_unacked_id = match (min_unacked_id, subscription.first_unacked_id().await) {
                            (None, Ok(Some(id)))  => Some(id),
                            (Some(min_id), Ok(Some(id))) => Some(std::cmp::min(min_id, id)),
                            (min_id, Ok(None)) => min_id,
                            (_, Err(e)) => {
                                error!("get subscription min acked id error: {e:?}");
                                continue;
                            }
                        }
                    }
                    min_unacked_id
                };
                let retain_min_id = match (prev_acked_id, min_unacked_id) {
                    (None, Some(min_id)) => Some(min_id),
                    (_, None) => None,
                    (Some(prev_id), Some(min_id)) if prev_id == min_id => continue,
                    (Some(_), Some(min_id)) => {
                        Some(min_id)
                    }
                };
                match storage.retain_messages(retain_min_id).await {
                    Ok(_) => {
                        prev_acked_id = min_unacked_id;
                    }
                    Err(e) => {
                        error!("topic retain messages error: {e:?}");
                        continue
                    }
                }
            }
            _ = token.cancelled() => break
        }
    }
}
