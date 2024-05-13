use std::{
    collections::{HashSet, VecDeque},
    fmt::Debug,
    marker::PhantomData,
    num::NonZeroU8,
    pin::Pin,
    sync::{
        atomic::{self, AtomicU32},
        Arc,
    },
    task::{ready, Context, Poll},
};

use chrono::{DateTime, Utc};
use comet_common::{
    addr::ConnectAddress,
    codec::Codec,
    http::api,
    protocol::{consumer::Subscribe, send},
    types::{InitialPosition, SubscriptionType},
    utils::{self, defer::defer},
};
use futures::{
    stream::{FuturesUnordered, SelectAll},
    Stream, StreamExt,
};
use pin_project::pin_project;
use snafu::{IntoError, ResultExt};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::{CancellationToken, WaitForCancellationFutureOwned};
use tracing::error;

use crate::{
    client::Client,
    connection::{manager::ConnectionManager, TopicConnection},
    dns::DnsResolver,
    CodecSnafu, DisconnectSnafu, HttpRequestSnafu, InvalidNameSnafu, ShutdownSnafu,
};

pub struct ConsumerBuilder<D, C, T>
where
    D: DnsResolver,
    C: Codec,
{
    name: String,
    topic_name: String,
    subscription_name: String,
    subscription_type: SubscriptionType,
    default_permits: u32,
    priority: u8,
    initial_position: InitialPosition,
    client: Client<D, C>,
    _p: PhantomData<T>,
}

impl<D, C, T> ConsumerBuilder<D, C, T>
where
    D: DnsResolver,
    C: Codec,
{
    pub fn new(
        name: String,
        topic_name: String,
        subscription_name: String,
        client: Client<D, C>,
    ) -> Result<Self, crate::Error> {
        if !utils::is_valid_name(&topic_name) {
            return InvalidNameSnafu { name: topic_name }.fail();
        }
        Ok(Self {
            name,
            topic_name,
            subscription_name,
            subscription_type: SubscriptionType::Exclusive,
            default_permits: 1000,
            client,
            priority: 0,
            initial_position: InitialPosition::Latest,
            _p: PhantomData,
        })
    }

    pub fn subscription_type(mut self, subscription_type: SubscriptionType) -> Self {
        self.subscription_type = subscription_type;
        self
    }

    pub fn priority(mut self, priority: NonZeroU8) -> Self {
        self.priority = priority.get();
        self
    }

    pub fn initial_position(mut self, initial_position: InitialPosition) -> Self {
        self.initial_position = initial_position;
        self
    }

    pub async fn build(self) -> Result<(ConsumerHandle, ConsumerStream<C, T>), crate::Error> {
        let server_http_addrs = self.client.refresh_http_addrs().await?;
        let server_broker_addrs = api::lookup_topic_addresses(&server_http_addrs, &self.topic_name)
            .await
            .context(HttpRequestSnafu)?;

        let subscribe = Subscribe {
            consumer_name: self.name,
            topic_name: self.topic_name,
            subscription_name: self.subscription_name,
            subscription_type: self.subscription_type,
            initial_position: self.initial_position,
            default_permits: self.default_permits,
            priority: self.priority,
        };

        let manager = self.client.conn_manager.clone();
        let mut topic_streams = SelectAll::new();
        let mut topic_conns = VecDeque::with_capacity(server_broker_addrs.len());
        let conn_cancel_futs = FuturesUnordered::new();

        let mut consumer_state = None;
        let permits = Arc::new(AtomicU32::default());
        for addr in server_broker_addrs {
            let conn = manager.get_connection::<C>(&addr).await?;
            let (message_tx, message_rx) = mpsc::unbounded_channel();
            let consumer_receipt = conn.subscribe(&subscribe, message_tx).await?;
            conn.control_flow(consumer_receipt.consumer_id, self.default_permits)
                .await?;
            permits.store(self.default_permits, atomic::Ordering::Release);
            consumer_state = consumer_state.or_else(|| {
                Some(ConsumerState {
                    id: consumer_receipt.consumer_id,
                    permits: permits.clone(),
                    subscribe: subscribe.clone(),
                    default_permits: self.default_permits,
                })
            });
            conn_cancel_futs.push(conn.wait_close());
            topic_conns.push_back(conn);
            topic_streams.push(StreamWithKey::new(
                addr,
                UnboundedReceiverStream::new(message_rx),
            ));
        }

        let conn_state = ConnectionState {
            topic_streams,
            manager,
            topic_conns,
            server_http_addrs,
        };

        let consumer_state = consumer_state.expect("initial consumer");

        let token = self.client.token.child_token();

        let (message_tx, message_rx) = mpsc::unbounded_channel();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (close_tx, close_rx) = watch::channel(());
        let mut tasks = self.client.tasks.lock().await;
        tasks.spawn(start_consumer::<_, C>(
            consumer_state,
            conn_state,
            message_tx,
            event_rx,
            conn_cancel_futs,
            close_rx,
            token,
        ));

        Ok((
            ConsumerHandle { event_tx },
            ConsumerStream {
                permits,
                close_tx,
                message_rx,
                _pc: PhantomData,
                _pt: PhantomData,
            },
        ))
    }
}

pub struct ConsumerState {
    id: u64,
    permits: Arc<AtomicU32>,
    default_permits: u32,
    subscribe: Subscribe,
}

pub struct ConnectionState<D>
where
    D: DnsResolver,
{
    server_http_addrs: Vec<ConnectAddress>,
    topic_conns: VecDeque<TopicConnection>,
    /// topic 所有分区所在的节点
    /// TODO: 考虑 topic partition 可能会在节点间迁移（这个功能暂时不做），这个字段需要根据 TopicNotExists 的回复来及时更新
    topic_streams:
        SelectAll<StreamWithKey<ConnectAddress, UnboundedReceiverStream<TopicMessageInner>>>,
    /// 根据 addr 获取对应的连接
    manager: ConnectionManager<D>,
}

impl<D> ConnectionState<D>
where
    D: DnsResolver,
{
    fn is_conns_valid(&self) -> bool {
        self.topic_conns.iter().all(|conn| !conn.is_closed())
    }
}

enum ConsumerEvent {
    Ack(Vec<u64>, oneshot::Sender<Result<(), crate::Error>>),
    Unsubscribe(oneshot::Sender<Result<(), crate::Error>>),
}

async fn start_consumer<D, C>(
    mut consumer: ConsumerState,
    mut conns: ConnectionState<D>,
    message_tx: mpsc::UnboundedSender<TopicMessageInner>,
    mut event_rx: mpsc::UnboundedReceiver<ConsumerEvent>,
    mut conn_cancel_futs: FuturesUnordered<WaitForCancellationFutureOwned>,
    mut close_rx: watch::Receiver<()>,
    token: CancellationToken,
) where
    D: DnsResolver,
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    #[derive(Debug, PartialEq)]
    enum State {
        Connected,
        Reconnect,
    }
    let mut state = State::Connected;
    loop {
        if state == State::Reconnect || !conns.is_conns_valid() {
            // reconnect
            let server_http_addrs =
                match api::get_server_http_addresses(&conns.server_http_addrs).await {
                    Ok(addrs) => addrs,
                    Err(e) => {
                        error!("reconnect but get server http addresses error: {e:?}");
                        return;
                    }
                };
            conns.server_http_addrs.clone_from(&server_http_addrs);
            let server_broker_addrs = match api::lookup_topic_addresses(
                &server_http_addrs,
                &consumer.subscribe.topic_name,
            )
            .await
            {
                Ok(addrs) => addrs,
                Err(e) => {
                    error!("reconnect but get server broker addrs error: {e:?}");
                    return;
                }
            };
            conns.topic_conns.retain(|conn| !conn.is_closed());
            let holds_addrs = conns
                .topic_conns
                .iter()
                .map(|conn| conn.addr.clone())
                .collect::<HashSet<ConnectAddress>>();
            for addr in server_broker_addrs {
                if holds_addrs.contains(&addr) {
                    continue;
                }
                match conns.manager.get_connection::<C>(&addr).await {
                    Ok(new_conn) => {
                        let (message_tx, message_rx) = mpsc::unbounded_channel();
                        match new_conn.subscribe(&consumer.subscribe, message_tx).await {
                            Ok(receipt) => {
                                consumer.id = receipt.consumer_id;
                            }
                            Err(e) => {
                                error!("consumer reconnect subscribe error: {e:?}");
                                return;
                            }
                        }
                        conn_cancel_futs.push(new_conn.wait_close());
                        conns.topic_conns.push_back(new_conn);
                        conns.topic_streams.push(StreamWithKey::new(
                            addr,
                            UnboundedReceiverStream::new(message_rx),
                        ));
                    }
                    Err(e) => {
                        error!("get new connection error: {e:?}");
                        return;
                    }
                }
            }
        }
        if consumer.permits.load(atomic::Ordering::Acquire) <= consumer.default_permits / 2 {
            // if error, state = State::Reconnect, continue
            let Some(conn) = conns.topic_conns.pop_front() else {
                state = State::Reconnect;
                continue;
            };
            let permits =
                consumer.default_permits - consumer.permits.load(atomic::Ordering::Acquire);
            match conn.control_flow(consumer.id, permits).await {
                Ok(_) => {
                    consumer
                        .permits
                        .store(consumer.default_permits, atomic::Ordering::Release);
                }
                Err(e) => {
                    error!("consumer control flow error: {e:?}");
                    state = State::Reconnect;
                    continue;
                }
            }
        }
        tokio::select! {
            biased;
            _ = token.cancelled() => return,
            _ = close_rx.changed() => {
                break
            }
            // 先检查下连接是否健康
            Some(_addr) = conn_cancel_futs.next(), if !conn_cancel_futs.is_empty() => {
                state = State::Reconnect;
                continue;
            }
            Some(event) = event_rx.recv() => {
                match event {
                    ConsumerEvent::Ack(message_ids, res_tx) => {
                        let Some(conn) = conns.topic_conns.pop_front() else {
                            state = State::Reconnect;
                            continue;
                        };
                        res_tx.send(conn.acks(consumer.id, message_ids).await).ok();
                        conns.topic_conns.push_back(conn);
                    },
                    ConsumerEvent::Unsubscribe(res_tx) => {
                        let Some(conn) = conns.topic_conns.pop_front() else {
                            state = State::Reconnect;
                            continue;
                        };
                        res_tx.send(conn.unsubscribe(consumer.id).await).ok();
                        conns.topic_conns.push_back(conn);
                    }
                }
            }
            // 接收消息
            Some((_addr, message)) = conns.topic_streams.next() => {
                if message_tx.send(message).is_err() {
                    break
                }
                consumer.permits.fetch_sub(1, atomic::Ordering::Release);
            }
            else => break,
        }
    }

    while let Some(conn) = conns.topic_conns.pop_front() {
        if let Err(e) = conn.close_consumer(consumer.id).await {
            error!("send close consumer request error: {e:?}")
        }
    }
}

pub(crate) struct TopicMessageInner {
    pub message_id: u64,
    pub topic_name: String,
    pub producer_name: String,
    pub consumer_id: u64,
    pub payload_len: u64,
    pub publish_time: DateTime<Utc>,
    pub deliver_time: Option<DateTime<Utc>>,
    pub handle_time: DateTime<Utc>,
    pub send_time: DateTime<Utc>,
    pub payload: bytes::Bytes,
    pub receive_time: DateTime<Utc>,
}

impl From<send::Send> for TopicMessageInner {
    fn from(send: send::Send) -> Self {
        TopicMessageInner {
            message_id: send.header.message_id,
            topic_name: send.header.topic_name,
            producer_name: send.header.producer_name,
            consumer_id: send.header.consumer_id,
            payload_len: send.header.payload_len,
            publish_time: send.header.publish_time,
            deliver_time: send.header.deliver_time,
            handle_time: send.header.handle_time,
            send_time: send.header.send_time,
            payload: send.payload,
            receive_time: Utc::now(),
        }
    }
}

pub struct TopicMessage<T> {
    pub id: u64,
    pub topic_name: String,
    pub producer_name: String,
    pub consumer_id: u64,
    pub payload_len: u64,
    pub publish_time: DateTime<Utc>,
    pub deliver_time: Option<DateTime<Utc>>,
    pub handle_time: DateTime<Utc>,
    pub send_time: DateTime<Utc>,
    pub payload: T,
    pub receive_time: DateTime<Utc>,
}

/// 不需要实现 Clone
#[pin_project]
pub struct ConsumerStream<C, T> {
    permits: Arc<AtomicU32>,
    close_tx: watch::Sender<()>,
    _pc: PhantomData<C>,
    _pt: PhantomData<T>,

    #[pin]
    message_rx: mpsc::UnboundedReceiver<TopicMessageInner>,
}

impl<C, T> Stream for ConsumerStream<C, T>
where
    C: Codec,
    T: for<'a> serde::Deserialize<'a>,
{
    type Item = Result<TopicMessage<T>, crate::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        match ready!(this.message_rx.poll_recv(cx)) {
            Some(item) => {
                this.permits.fetch_add(1, atomic::Ordering::Release);
                match C::decode(item.payload) {
                    Ok(payload) => Poll::Ready(Some(Ok(TopicMessage {
                        id: item.message_id,
                        topic_name: item.topic_name,
                        producer_name: item.producer_name,
                        consumer_id: item.consumer_id,
                        payload_len: item.payload_len,
                        publish_time: item.publish_time,
                        deliver_time: item.deliver_time,
                        handle_time: item.handle_time,
                        send_time: item.send_time,
                        payload,
                        receive_time: item.receive_time,
                    }))),
                    Err(e) => Poll::Ready(Some(Err(CodecSnafu.into_error(Box::new(e))))),
                }
            }
            None => Poll::Ready(None),
        }
    }
}

#[derive(Clone)]
pub struct ConsumerHandle {
    event_tx: mpsc::UnboundedSender<ConsumerEvent>,
}

impl ConsumerHandle {
    #[tracing::instrument(skip(self))]
    pub async fn acks<I>(&self, message_ids: I) -> Result<(), crate::Error>
    where
        I: Into<Vec<u64>> + Debug,
    {
        let (res_tx, res_rx) = oneshot::channel();
        self.event_tx
            .send(ConsumerEvent::Ack(message_ids.into(), res_tx))
            .map_err(|_| ShutdownSnafu.build())?;
        res_rx.await.map_err(|_| DisconnectSnafu.build())?
    }

    #[tracing::instrument(skip(self))]
    pub async fn unsubscribe(&self) -> Result<(), crate::Error> {
        let (res_tx, res_rx) = oneshot::channel();
        self.event_tx
            .send(ConsumerEvent::Unsubscribe(res_tx))
            .map_err(|_| ShutdownSnafu.build())?;
        res_rx.await.map_err(|_| DisconnectSnafu.build())?
    }
}

#[pin_project]
struct StreamWithKey<K, S> {
    key: K,
    #[pin]
    stream: S,
}

impl<K, S> StreamWithKey<K, S> {
    fn new(key: K, stream: S) -> Self {
        Self { key, stream }
    }
}

impl<K, S> Stream for StreamWithKey<K, S>
where
    K: Clone,
    S: Stream,
{
    type Item = (K, S::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let value = ready!(this.stream.poll_next(cx));
        Poll::Ready(value.map(|v| (this.key.clone(), v)))
    }
}
