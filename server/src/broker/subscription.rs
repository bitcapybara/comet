mod delayed_tracker;
pub mod dispatch;
mod retention;

use std::{pin::pin, time::Duration};

use comet_common::{
    error::{BoxedError, ResponsiveError},
    protocol::response::Response,
    types::SubscriptionType,
    utils::defer::defer,
};
use futures::{
    future::{self, Either},
    stream::FuturesUnordered,
    StreamExt,
};
use snafu::{IntoError, Location, Snafu};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    meta::{MetaConsumer, MetaStorage},
    storage::SubscriptionStorage,
};

use self::{
    delayed_tracker::start_delayed_tracker, dispatch::start_dispatch,
    retention::start_acked_retention,
};

use super::topic;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("response error: {response}"))]
    PacketResponse {
        response: Response,
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
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub topic_name: String,
    pub subscription_name: String,
    pub subscription_type: SubscriptionType,
}

pub struct Subscription<M, S>
where
    M: MetaStorage,
{
    pub config: topic::Config,
    pub info: SubscriptionInfo,
    storage: S,
    notifier: watch::Sender<()>,
    handle: JoinSet<()>,
    consumer_tx: mpsc::UnboundedSender<M::Consumer>,
    token: CancellationToken,
}

impl<M, S> Subscription<M, S>
where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    #[tracing::instrument(skip(meta, storage, token))]
    pub async fn new(
        config: topic::Config,
        meta: M,
        storage: S,
        info: SubscriptionInfo,
        token: CancellationToken,
    ) -> Result<Self, Error> {
        let (notifier, observer) = watch::channel(());

        let token = token.child_token();

        let mut handle = JoinSet::new();
        handle.spawn(start_delayed_tracker(
            meta.clone(),
            info.clone(),
            storage.clone(),
            observer.clone(),
            token.clone(),
        ));
        handle.spawn(start_dispatch(
            meta.clone(),
            storage.clone(),
            info.clone(),
            observer,
            token.clone(),
        ));
        handle.spawn(start_acked_retention(
            storage.clone(),
            config.acked_retention.clone(),
            token.clone(),
        ));

        let (consumer_tx, consumer_rx) = mpsc::unbounded_channel();
        handle.spawn(consume_unacked::<M, S>(
            consumer_rx,
            storage.clone(),
            notifier.clone(),
            token.clone(),
        ));

        Ok(Self {
            storage,
            notifier,
            handle,
            info,
            token,
            config,
            consumer_tx,
        })
    }

    #[tracing::instrument(skip(self, consumer))]
    pub async fn add_consumer(&self, consumer: M::Consumer) {
        self.consumer_tx.send(consumer).ok();
    }

    #[tracing::instrument(skip(self))]
    pub async fn acks(&self, message_ids: &[u64]) -> Result<(), Error> {
        self.storage
            .set_acks(message_ids)
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))
    }

    #[tracing::instrument(skip(self))]
    pub async fn publish_notify(&self, message_id: u64) -> Result<(), Error> {
        self.storage
            .add_record(message_id)
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))?;
        self.notifier.send(()).ok();
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn first_unacked_id(&self) -> Result<Option<u64>, Error> {
        self.storage
            .first_unacked_id()
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))
    }

    #[tracing::instrument(skip(self))]
    pub async fn drop_storage(&self) -> Result<(), Error> {
        self.storage
            .drop()
            .await
            .map_err(|e| StorageSnafu.into_error(BoxedError::new(e)))
    }

    #[tracing::instrument(skip(self))]
    pub async fn shutdown(mut self) {
        if !self.token.is_cancelled() {
            self.token.cancel();
        }
        while self.handle.join_next().await.is_some() {}
    }
}

async fn consume_unacked<M, S>(
    mut consumer_rx: mpsc::UnboundedReceiver<M::Consumer>,
    storage: S,
    notifier: watch::Sender<()>,
    token: CancellationToken,
) where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    let _guard = defer(|| token.cancel());
    let mut futs = FuturesUnordered::new();
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            Some(_) = futs.next(), if !futs.is_empty() => {
                if notifier.send(()).is_err() {
                    break
                }
            }
            Some(consumer) = consumer_rx.recv() => {
                futs.push(send_unacked::<M,S>(storage.clone(), consumer, token.clone()));
            }
            else => {
                token.cancel();
                break
            }
        }
    }
}

// loop won't end until all unacked messages sent
async fn send_unacked<M, S>(storage: S, consumer: M::Consumer, token: CancellationToken)
where
    M: MetaStorage,
    S: SubscriptionStorage,
{
    const MESSAGES_LOAD_LIMIT: usize = 500;
    let info = consumer.info();
    let consumer_name = &info.name;
    let mut from = 0;
    loop {
        if token.is_cancelled() {
            break;
        }
        let fetch_fut = storage.fetch_unacked_messages(consumer_name, from, MESSAGES_LOAD_LIMIT);
        // 从数据库获取该消费者已读但是未 ack 的消息，重新发送
        match future::select(pin!(fetch_fut), pin!(token.cancelled())).await {
            Either::Left((res, _)) => match res {
                Ok(messages) => {
                    let fetched_nums = messages.len();
                    for message in messages {
                        tokio::select! {
                            res = consumer.send(message.into_send_message(info.id)) => match res {
                                Ok(_) => continue,
                                Err(e) => {
                                    error!("send message to consumer error: {e:?}");
                                    return
                                }
                            },
                            _ = token.cancelled() => return,
                        }
                    }

                    // 状态设置为已经追赶完毕，可以接收新消息了
                    if fetched_nums < MESSAGES_LOAD_LIMIT {
                        if let Err(e) = consumer.ready().await {
                            error!("set consumer ready error: {e:?}")
                        }
                        break;
                    }
                    from += fetched_nums;
                }
                Err(e) => {
                    error!("fetch unacked messages error: {e:?}");
                    // TODO: backoff duration
                    match future::select(
                        pin!(sleep(Duration::from_millis(500))),
                        pin!(token.cancelled()),
                    )
                    .await
                    {
                        Either::Left(_) => {}
                        Either::Right(_) => break,
                    }
                }
            },
            Either::Right(_) => break,
        }
    }
}
