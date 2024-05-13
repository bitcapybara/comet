use std::collections::VecDeque;

use comet_common::{error::BoxedError, types::SubscriptionType, utils::defer::defer};
use futures::{stream::FuturesUnordered, StreamExt};
use snafu::{IntoError, Location, Snafu};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    meta::{MetaConsumer, MetaStorage},
    storage::{SubscriptionStorage, TopicMessage},
};

use super::SubscriptionInfo;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("consumer error"))]
    Consumer {
        source: comet_common::error::BoxedError,
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

pub async fn start_dispatch<M, S>(
    meta: M,
    storage: S,
    info: SubscriptionInfo,
    mut observer: watch::Receiver<()>,
    token: CancellationToken,
) where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    let _guard = defer(|| token.cancel());
    let SubscriptionInfo {
        topic_name,
        subscription_name,
        subscription_type,
    } = &info;

    // 把上次加载进内存但是没有发出去的先发一遍
    let mut messages = match storage.fetch_loaded_unread_messages().await {
        Ok(messages) => Some(messages),
        Err(e) => {
            error!("init fetch loaded unread messages error: {e:?}");
            None
        }
    };
    loop {
        tokio::select! {
            Ok(_) = observer.changed() => {
                 let consumers = meta
                    .get_available_consumers(topic_name, subscription_name)
                    .await
                    .map_err(|e| MetaSnafu.into_error(BoxedError::new(e))).unwrap_or_default();
                if consumers.is_empty() {
                    continue;
                }
                if let Some(messages) = messages.take() {
                    match subscription_type {
                        SubscriptionType::Exclusive => {
                            if let Err(e) = exclusive_consume(
                                meta.clone(),
                                storage.clone(),
                                &info,
                                messages,
                                token.clone(),
                            )
                            .await
                            {
                                error!(
                                    topic_name,
                                    subscription_name, "send messages to exclusive consumer error: {e:?}"
                                );
                            }
                        }
                        SubscriptionType::Shared => {
                            if let Err(e) = shared_consume(
                                meta.clone(),
                                storage.clone(),
                                &info,
                                messages,
                                token.clone(),
                            )
                            .await
                            {
                                error!("send messages to shared consumers error: {e:?}")
                            }
                        }
                    }
                }
                match storage.has_unread().await {
                    Ok(true) => {}
                    Ok(false) => continue,
                    Err(e) => {
                        error!(topic_name, subscription_name, "storage has_unread error: {e:?}");
                        continue;
                    }
                };
                match subscription_type {
                    SubscriptionType::Exclusive => {
                        exclusive_consume_loop(meta.clone(), storage.clone(), &info, token.clone()).await;
                    },
                    SubscriptionType::Shared => {
                        shared_consume_loop(meta.clone(), storage.clone(), &info, token.clone()).await;
                    },
                }
            }
            _ = token.cancelled() => {
                break
            }
            else => {
                token.cancel();
                break
            }
        }
    }
}

async fn shared_consume_loop<M, S>(
    meta: M,
    storage: S,
    subscription: &SubscriptionInfo,
    token: CancellationToken,
) where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    let SubscriptionInfo {
        topic_name,
        subscription_name,
        ..
    } = subscription;
    const FETCH_MESSAGES_LIMIT: usize = 100;
    loop {
        // load many messages from storage
        let messages = match storage.fetch_unread_messages(FETCH_MESSAGES_LIMIT).await {
            Ok(messages) => messages,
            Err(e) => {
                error!(
                    topic_name,
                    subscription_name, "storage fetch unread messages error: {e:?}"
                );
                break;
            }
        };

        if messages.is_empty() {
            break;
        }

        let messages_count = messages.len();

        if let Err(e) = shared_consume(
            meta.clone(),
            storage.clone(),
            subscription,
            messages,
            token.clone(),
        )
        .await
        {
            error!("send messages to shared consumers error: {e:?}")
        }

        if messages_count < FETCH_MESSAGES_LIMIT {
            break;
        }
    }
}

pub async fn shared_consume<M, S>(
    meta: M,
    storage: S,
    SubscriptionInfo {
        topic_name,
        subscription_name,
        ..
    }: &SubscriptionInfo,
    messages: Vec<TopicMessage>,
    token: CancellationToken,
) -> Result<(), Error>
where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    let mut consumers = {
        let consumers = meta
            .get_available_consumers(topic_name, subscription_name)
            .await
            .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?;
        if consumers.is_empty() {
            return Ok(());
        }
        VecDeque::from(consumers)
    };

    let mut futs = FuturesUnordered::new();
    let mut messages = messages.into_iter();
    while let Some(consumer) = consumers.pop_front() {
        let Some(message) = messages.next() else {
            return Ok(());
        };
        futs.push(consume::<M, S>(storage.clone(), consumer, message));
    }

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            Some(res) = futs.next(), if !futs.is_empty() => {
                match res {
                    Ok(Some(consumer)) => {
                        let Some(message) = messages.next() else {
                            break
                        };
                        futs.push(consume::<M, S>(storage.clone(), consumer, message));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        error!("consume message error: {e:?}");
                    }
                }
            }
            else => break,
        }
    }
    Ok(())
}

async fn exclusive_consume_loop<M, S>(
    meta: M,
    storage: S,
    subscription: &SubscriptionInfo,
    token: CancellationToken,
) where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    let SubscriptionInfo {
        topic_name,
        subscription_name,
        ..
    } = subscription;
    const FETCH_MESSAGES_LIMIT: usize = 100;
    loop {
        // load many messages from storage
        let messages = match storage.fetch_unread_messages(FETCH_MESSAGES_LIMIT).await {
            Ok(messages) => messages,
            Err(e) => {
                error!(
                    topic_name,
                    subscription_name, "storage fetch unread messages error: {e:?}"
                );
                break;
            }
        };
        if messages.is_empty() {
            break;
        }

        let messages_count = messages.len();
        if let Err(e) = exclusive_consume(
            meta.clone(),
            storage.clone(),
            subscription,
            messages,
            token.clone(),
        )
        .await
        {
            error!(
                topic_name,
                subscription_name, "send messages to exclusive consumer error: {e:?}"
            );
            break;
        }

        if messages_count < FETCH_MESSAGES_LIMIT {
            break;
        }
    }
}

pub async fn exclusive_consume<M, S>(
    meta: M,
    storage: S,
    SubscriptionInfo {
        topic_name,
        subscription_name,
        ..
    }: &SubscriptionInfo,
    messages: Vec<TopicMessage>,
    token: CancellationToken,
) -> Result<(), Error>
where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    let mut consumer = match meta
        .get_available_consumers(topic_name, subscription_name)
        .await
        .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?
        .pop()
    {
        Some(consumer) => consumer,
        None => return Ok(()),
    };

    for message in messages {
        if token.is_cancelled() {
            return Ok(());
        }
        match consume::<M, S>(storage.clone(), consumer, message).await? {
            Some(c) => {
                consumer = c;
            }
            None => return Ok(()),
        }
    }
    Ok(())
}

pub async fn consume<M, S>(
    storage: S,
    consumer: M::Consumer,
    message: TopicMessage,
) -> Result<Option<M::Consumer>, Error>
where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    if !consumer
        .is_available()
        .await
        .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?
    {
        return Ok(None);
    }
    let info = consumer.info();
    let message_id = message.message_id;

    consumer
        .send(message.into_send_message(info.id))
        .await
        .map_err(|e| ConsumerSnafu.into_error(BoxedError::new(e)))?;

    // 在subscription表中将这条消息标记为此消费者已读
    storage
        .set_read(&info.name, message_id)
        .await
        .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))?;

    // 更新 permits
    consumer
        .permits_sub(1)
        .await
        .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?;
    Ok(Some(consumer))
}
