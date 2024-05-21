use std::{
    collections::{HashSet, VecDeque},
    marker::PhantomData,
    num::NonZeroUsize,
    pin::{pin, Pin},
    task::{ready, Context, Poll},
};

use bytes::BytesMut;
use chrono::{DateTime, TimeDelta, Utc};
use comet_common::{
    addr::ConnectAddress,
    codec::Codec,
    http::api,
    io as comet_io,
    protocol::{
        publish::{Publish, PublishHeader},
        Packet,
    },
    types::AccessMode,
    utils::{self, defer::defer},
};
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    Sink, Stream, StreamExt,
};

use pin_project::pin_project;
use snafu::{IntoError, ResultExt};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::{CancellationToken, PollSender, WaitForCancellationFutureOwned};
use tracing::error;

use crate::{
    client::Client,
    connection::{manager::ConnectionManager, TopicConnection},
    dns::DnsResolver,
    CodecSnafu, ConnectionReadWriteSnafu, DisconnectSnafu, HttpRequestSnafu, InvalidNameSnafu,
    ServerResponseSnafu, ShutdownSnafu,
};

pub struct ProducerBuilder<D, C, T>
where
    D: DnsResolver,
    C: Codec,
{
    name: String,
    topic_name: String,
    access_mode: AccessMode,
    client: Client<D, C>,
    queue_size: usize,
    _p: PhantomData<T>,
}

impl<D, C, T> ProducerBuilder<D, C, T>
where
    D: DnsResolver,
    C: Codec,
{
    pub fn new(
        name: String,
        topic_name: String,
        client: Client<D, C>,
    ) -> Result<Self, crate::Error> {
        if !utils::is_valid_name(&topic_name) {
            return InvalidNameSnafu { name: topic_name }.fail();
        }
        Ok(Self {
            client,
            name,
            topic_name,
            queue_size: 1,
            access_mode: AccessMode::Exclusive,
            _p: PhantomData,
        })
    }

    pub fn queue_size(mut self, size: NonZeroUsize) -> Self {
        self.queue_size = size.get();
        self
    }

    pub fn access_mode(mut self, access_mode: AccessMode) -> Self {
        self.access_mode = access_mode;
        self
    }

    pub async fn build(self) -> Result<Producer<C, T>, crate::Error> {
        let server_http_addrs = self.client.refresh_http_addrs().await?;
        let server_broker_addrs = api::lookup_topic_addresses(&server_http_addrs, &self.topic_name)
            .await
            .context(HttpRequestSnafu)?;

        let manager = self.client.conn_manager.clone();
        let mut topic_conns = VecDeque::with_capacity(server_broker_addrs.len());
        let conn_cancel_futs = FuturesUnordered::new();
        let mut producer_state = None;
        for addr in server_broker_addrs {
            let conn = manager.get_connection::<C>(&addr).await?;
            let producer_receipt = conn
                .create_producer(&self.name, &self.topic_name, self.access_mode)
                .await?;
            producer_state = producer_state.or_else(|| {
                Some(ProducerState {
                    id: producer_receipt.producer_id,
                    name: self.name.clone(),
                    topic_name: self.topic_name.clone(),
                    sequence_id: producer_receipt.last_sequence_id,
                    access_mode: self.access_mode,
                })
            });
            conn_cancel_futs.push(conn.wait_close());
            topic_conns.push_back(conn);
        }

        let conn_state = ConnectionState {
            topic_conns,
            manager,
            server_http_addrs,
        };

        let token = self.client.token.child_token();

        let (request_tx, request_rx) = mpsc::channel(self.queue_size);

        let mut tasks = self.client.tasks.lock().await;
        tasks.spawn(start_producer::<_, C>(
            producer_state.expect("producer state initialized"),
            conn_state,
            request_rx,
            conn_cancel_futs,
            token.clone(),
        ));

        Ok(Producer {
            publish_tx: Some(PollSender::new(request_tx)),
            futs: FuturesOrdered::new(),
            _pc: PhantomData,
            _pt: PhantomData,
        })
    }
}

/// 不需要实现 Clone
#[pin_project]
pub struct Producer<C, T> {
    publish_tx: Option<PollSender<PublishRequest>>,
    _pc: PhantomData<C>,
    _pt: PhantomData<T>,

    #[pin]
    futs: FuturesOrdered<oneshot::Receiver<Result<Packet, comet_io::Error>>>,
}

impl<C, T> Sink<PublishMessage<T>> for Producer<C, T>
where
    C: Codec,
    T: serde::Serialize,
{
    type Error = crate::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let Some(publish_tx) = self.project().publish_tx else {
            return Poll::Ready(ShutdownSnafu.fail());
        };
        if publish_tx.is_closed() {
            return Poll::Ready(DisconnectSnafu.fail());
        }
        ready!(publish_tx
            .poll_reserve(cx)
            .map_err(|_| DisconnectSnafu.build())?);
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, mut message: PublishMessage<T>) -> Result<(), Self::Error> {
        let mut this = self.project();
        let Some(publish_tx) = this.publish_tx else {
            return ShutdownSnafu.fail();
        };
        let (res_tx, res_rx) = oneshot::channel();
        let mut buf = BytesMut::new();
        C::encode(&message.payload, &mut buf).map_err(|e| CodecSnafu.into_error(Box::new(e)))?;
        publish_tx
            .send_item(PublishRequest {
                message: PublishMessageInner {
                    deliver_time: message.deliver_time.take(),
                    payload: buf.freeze(),
                },
                res_tx,
            })
            .map_err(|_| DisconnectSnafu.build())?;
        this.futs.as_mut().push_back(res_rx);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        loop {
            match ready!(this.futs.as_mut().poll_next(cx)) {
                Some(res) => match res {
                    Ok(Ok(packet)) => match packet {
                        Packet::Response(response) if response.is_success() => continue,
                        Packet::Response(response) => {
                            return Poll::Ready(ServerResponseSnafu { response }.fail());
                        }
                        _ => unreachable!(),
                    },
                    Ok(Err(e)) => return Poll::Ready(Err(ConnectionReadWriteSnafu.into_error(e))),
                    Err(_) => return Poll::Ready(DisconnectSnafu.fail()),
                },
                None => return Poll::Ready(Ok(())),
            }
        }
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        match ready!(self.as_mut().poll_flush(cx)) {
            Ok(_) => {
                let this = self.project();
                if let Some(mut publish_tx) = this.publish_tx.take() {
                    publish_tx.close();
                }
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pub struct ProducerState {
    id: u64,
    name: String,
    topic_name: String,
    access_mode: AccessMode,
    sequence_id: u64,
}

pub struct PublishMessage<T> {
    deliver_time: Option<DateTime<Utc>>,
    payload: T,
}

impl<T> PublishMessage<T> {
    pub fn new(payload: impl Into<T>) -> Self {
        Self {
            deliver_time: None,
            payload: payload.into(),
        }
    }

    pub fn deliver_after(mut self, duration: std::time::Duration) -> Self {
        let Ok(duration) = TimeDelta::from_std(duration) else {
            unreachable!()
        };
        self.deliver_time = Utc::now().checked_add_signed(duration);
        self
    }

    pub fn deliver_at(mut self, at: DateTime<Utc>) -> Self {
        self.deliver_time = Some(at);
        self
    }
}

struct PublishMessageInner {
    deliver_time: Option<DateTime<Utc>>,
    payload: bytes::Bytes,
}

pub struct PublishRequest {
    message: PublishMessageInner,
    res_tx: oneshot::Sender<Result<Packet, comet_io::Error>>,
}

pub struct ConnectionState<D>
where
    D: DnsResolver,
{
    server_http_addrs: Vec<ConnectAddress>,
    /// topic 所有分区所在的节点 connection
    /// TODO: 考虑 topic partition 可能会在节点间迁移（这个功能暂时不做），这个字段需要根据 TopicNotExists 的回复来及时更新
    topic_conns: VecDeque<TopicConnection>,
    /// 根据 addr 获取对应的连接
    manager: ConnectionManager<D>,
}

impl<D> ConnectionState<D>
where
    D: DnsResolver,
{
    pub fn new(server_http_addrs: Vec<ConnectAddress>, manager: ConnectionManager<D>) -> Self {
        Self {
            server_http_addrs,
            topic_conns: VecDeque::new(),
            manager,
        }
    }

    fn is_conns_valid(&self) -> bool {
        self.topic_conns.iter().all(|conn| !conn.is_closed())
    }
}

pub async fn start_producer<D, C>(
    mut producer: ProducerState,
    mut conns: ConnectionState<D>,
    mut request_rx: mpsc::Receiver<PublishRequest>,
    mut conn_cancel_futs: FuturesUnordered<WaitForCancellationFutureOwned>,
    token: CancellationToken,
) where
    D: DnsResolver,
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    #[derive(PartialEq)]
    enum State {
        Connected,
        Reconnect,
    }
    let mut state = State::Connected;
    loop {
        if state == State::Reconnect || !conns.is_conns_valid() {
            // 获取新的连接地址
            let server_http_addrs =
                match api::get_server_http_addresses(&conns.server_http_addrs).await {
                    Ok(addrs) => addrs,
                    Err(e) => {
                        error!("reconnect but get server http addresses error: {e:?}");
                        return;
                    }
                };
            conns.server_http_addrs.clone_from(&server_http_addrs);
            let server_broker_addrs =
                match api::lookup_topic_addresses(&server_http_addrs, &producer.topic_name).await {
                    Ok(addrs) => addrs,
                    Err(e) => {
                        error!("reconnect but get server broker addresses error: {e:?}");
                        return;
                    }
                };
            // 删除掉持有的多余连接和当前失效的这个连接
            conns
                .topic_conns
                .retain(|conn| server_broker_addrs.contains(&conn.addr));
            // 重新添加当前断开的连接
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
                        // 新的连接，重新向 broker 注册当前生产者
                        match new_conn
                            .create_producer(
                                &producer.name,
                                &producer.topic_name,
                                producer.access_mode,
                            )
                            .await
                        {
                            Ok(receipt) => {
                                producer.sequence_id = receipt.last_sequence_id;
                                producer.id = receipt.producer_id;
                            }
                            Err(e) => {
                                error!("create producer error: {e:?}");
                            }
                        }
                        // 监听这个 conn 的退出事件
                        conn_cancel_futs.push(new_conn.wait_close());
                        conns.topic_conns.push_back(new_conn);
                    }
                    Err(crate::Error::Disconnect { .. }) => return,
                    Err(e) => {
                        error!(domain = %addr.domain, "get connection error: {e:?}");
                    }
                }
            }
        }

        tokio::select! {
            biased;
            _ = token.cancelled() => return,
            Some(_addr) = conn_cancel_futs.next(), if !conn_cancel_futs.is_empty() => {
                state = State::Reconnect;
            }
            Some(PublishRequest { message, res_tx }) = request_rx.recv() => {
                // 获取一个 addr
                let Some(conn) = conns.topic_conns.pop_front() else {
                    state = State::Reconnect;
                    continue;
                };
                if conn.is_closed() {
                    state = State::Reconnect;
                    continue;
                }
                producer.sequence_id += 1;
                let publish = Publish {
                    header: PublishHeader {
                        producer_id: producer.id,
                        topic_name: producer.topic_name.clone(),
                        sequence_id: producer.sequence_id,
                        publish_time: Utc::now(),
                        deliver_time: message.deliver_time,
                        payload_len: message.payload.len() as u64,
                    },
                    payload: message.payload
                };
                // 发送给 connection
                if let Err(crate::Error::Disconnect {..}) =  conn.publish(publish, res_tx) {
                    state = State::Reconnect;
                    continue
                }

                conns.topic_conns.push_back(conn);
            },
            else => break,
        }
    }

    while let Some(conn) = conns.topic_conns.pop_front() {
        conn.close_producer(producer.id).await.ok();
    }
}
