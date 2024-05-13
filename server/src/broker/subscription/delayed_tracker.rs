use std::{
    cmp,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use chrono::{TimeDelta, Utc};
use comet_common::{types::SubscriptionType, utils::defer::defer};
use futures::{stream::SelectAll, StreamExt};
use tokio::sync::watch;
use tokio_util::{sync::CancellationToken, time::DelayQueue};
use tracing::error;

use crate::{
    broker::subscription::dispatch::{exclusive_consume, shared_consume},
    meta::MetaStorage,
    storage::SubscriptionStorage,
};

use super::SubscriptionInfo;

const MAX_SLEEP_DURATION: Duration = Duration::from_secs(30);
const MIN_SLEEP_DURATION: Duration = Duration::from_millis(100);
const DELAY_INTERVAL: TimeDelta = TimeDelta::minutes(30);
const MESSAGES_LOAD_LIMIT: usize = 500;

pub async fn start_delayed_tracker<M, S>(
    meta: M,
    info: SubscriptionInfo,
    storage: S,
    mut observer: watch::Receiver<()>,
    token: CancellationToken,
) where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    let _guard = defer(|| token.cancel());
    let mut trackers = SelectAll::new();

    // 加载上次加载到内存但是没来得及发送的数据
    let mut from = 0;
    loop {
        let now = Utc::now();
        let messages = match storage
            .fetch_unread_delayed_messages(now, now + DELAY_INTERVAL, from, MESSAGES_LOAD_LIMIT)
            .await
        {
            Ok(messages) => messages,
            Err(e) => {
                error!("fetch unread delayed messages error: {e:?}");
                continue;
            }
        };

        if messages.is_empty() {
            break;
        }

        let messages_len = messages.len();
        let mut tracker = DelayTracker(DelayQueue::new());
        for message in messages {
            let Some(deliver_time) = message.deliver_time else {
                unreachable!();
            };
            let Ok(duration) = deliver_time.signed_duration_since(now).to_std() else {
                error!("message deliver time already missed");
                continue;
            };
            tracker.0.insert(message, duration);
        }
        trackers.push(tracker.ready_chunks(100));

        if messages_len < MESSAGES_LOAD_LIMIT {
            break;
        }

        from += messages_len;
    }

    // 退避策略
    let mut duration = MIN_SLEEP_DURATION;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(duration) => {
                // 从 storage 加载 5 分钟内的消息
                let now = Utc::now();
                let messages = match storage.fetch_unloaded_delayed_messages(now, now + DELAY_INTERVAL, MESSAGES_LOAD_LIMIT).await {
                    Ok(messages) => messages,
                    Err(e) => {
                        error!("fetch unread delayed messages error: {e:?}");
                        continue;
                    }
                };

                if messages.is_empty() {
                    duration = cmp::min(duration * 2, MAX_SLEEP_DURATION);
                    continue;
                }

                let mut tracker = DelayTracker(DelayQueue::new());
                for message in messages {
                    let Some(deliver_time) = message.deliver_time else {
                        unreachable!();
                    };
                    let Ok(duration) = deliver_time.signed_duration_since(now).to_std() else {
                        error!("message deliver time already missed");
                        continue;
                    };
                    tracker.0.insert(message, duration);
                }
                trackers.push(tracker.ready_chunks(100));
            }
            _ = observer.changed() => {
                duration = MIN_SLEEP_DURATION;
            }
            Some(messages) = trackers.next() => {
                // send to consumers
                match info.subscription_type {
                    SubscriptionType::Exclusive => {
                        if let Err(e) = exclusive_consume(meta.clone(), storage.clone(), &info, messages, token.clone()).await {
                            error!("send message to exclusive consumer error: {e:?}")
                        }
                    }
                    SubscriptionType::Shared => {
                        if let Err(e) = shared_consume(meta.clone(), storage.clone(), &info, messages, token.clone()).await {
                            error!("send message to shared consumer error: {e:?}")
                        }
                    }
                }
            },
            _ = token.cancelled() => break,
            else => {
                token.cancel();
                break
            }
        }
    }
}

struct DelayTracker<T>(DelayQueue<T>);

impl<T> futures::Stream for DelayTracker<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(DelayQueue::poll_expired(&mut self.get_mut().0, cx)) {
            Some(item) => Poll::Ready(Some(item.into_inner())),
            None => Poll::Ready(None),
        }
    }
}
