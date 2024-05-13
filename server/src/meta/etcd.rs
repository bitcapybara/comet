mod api;
mod lease;

use std::{
    cmp,
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use etcd::{
    EventType, GetOptions, PutOptions, WatchFilterType, WatchOptions, WatchStream, Watcher,
};
use etcd_client as etcd;
use futures::{stream::FuturesUnordered, Future, StreamExt, TryStreamExt};
use snafu::{ensure, Location, OptionExt, ResultExt, Snafu};
use sysinfo::{MemoryRefreshKind, System};
use tokio::{
    sync::{mpsc, Mutex, RwLock},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::broker::topic;

use self::lease::start_lease;
use comet_common::{
    addr::ConnectAddress,
    error::ResponsiveError,
    http,
    io::writer,
    protocol::{
        consumer::Subscribe,
        producer::CreateProducer,
        response::{Response, ReturnCode},
    },
    types::{AccessMode, InitialPosition, SubscriptionType},
};

use super::{
    mem::{self, SessionState},
    MetaConsumer, MetaProducer, MetaStorage, ProducerInfo,
};

const ELECTION_KEY: &str = "/comet/leader";

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("etcd grant lease error"))]
    GrantLease {
        #[snafu(source)]
        error: etcd::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("etcd connect error"))]
    EtcdConnect {
        #[snafu(source)]
        error: etcd::Error,
        #[snafu(implicit)]
        location: Location,
    },
    // #[snafu(display("election quit"))]
    // ElectionQuit {
    //     #[snafu(implicit)]
    //     location: Location,
    // },
    // #[snafu(display())]
    // LeaseQuit {
    //     #[snafu(implicit)]
    //     location: Location,
    // },
    #[snafu(display("etcd lease error"))]
    Lease {
        source: lease::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("codec error"))]
    Codec {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("etcd meta error"))]
    Etcd {
        #[snafu(source)]
        error: etcd::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("meta response error"))]
    Response {
        response: Response,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("meta http error"))]
    Http {
        source: comet_common::http::api::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("memory meta error"), context(false))]
    Memory {
        source: super::mem::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("etcd invalid key: {key}"))]
    InvalidKey {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("current etcd node not leader"))]
    NotLeader {
        #[snafu(implicit)]
        location: Location,
    },
}

impl ResponsiveError for Error {
    fn as_response(&self) -> comet_common::protocol::response::Response {
        match self {
            Error::Response { response, .. } => response.clone(),
            e => Response::new(ReturnCode::Internal, e.to_string()),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EtcdSubscriptionInfo {
    pub subscription_name: String,
    pub subscription_type: SubscriptionType,
    pub initial_position: InitialPosition,
    pub consumers: HashSet<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub node_id: u16,
    pub http_addr: ConnectAddress,
    pub broker_addr: ConnectAddress,
    pub etcd_addrs: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct NodeInfo {
    http_addr: ConnectAddress,
    broker_addr: ConnectAddress,
    memory: MemoryInfo,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MemoryInfo {
    total: u64,
    free: u64,
    available: u64,
    used: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct EtcdConsumer {
    permits: u32,
}

impl EtcdConsumer {
    pub fn new() -> Self {
        Self { permits: 0 }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum EtcdConsumers {
    // consumer_name, client_id
    Exclusive((String, HashSet<u64>)),
    // consumer_name -> client_id
    Shared(HashMap<String, HashSet<u64>>),
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct EtcdProducer;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum EtcdProducers {
    // producer_name, client_id
    Exclusive((String, HashSet<u64>)),
    // producer_name -> client_id
    Shared(HashMap<String, HashSet<u64>>),
}

#[derive(Clone)]
pub struct EtcdMeta {
    broker_addr: ConnectAddress,
    etcd: etcd::Client,
    lease_id: i64,
    role: RoleStatus,
    assign_watch_event_tx: mpsc::UnboundedSender<(String, u16)>,
    session: super::mem::SessionState<Producer, Consumer>,
    handle: Arc<Mutex<JoinSet<()>>>,
    token: CancellationToken,
}

impl EtcdMeta {
    #[tracing::instrument]
    pub async fn new(config: Config) -> Result<Self, Error> {
        let mut client = etcd::Client::connect(config.etcd_addrs, None)
            .await
            .context(EtcdConnectSnafu)?;
        let mut join_set = JoinSet::new();

        let lease_id = client
            .lease_grant(5, None)
            .await
            .context(GrantLeaseSnafu)?
            .id();

        // 注册
        let mut system = System::new();
        system.refresh_memory_specifics(MemoryRefreshKind::new().with_ram());
        let info = NodeInfo {
            http_addr: config.http_addr.clone(),
            broker_addr: config.broker_addr.clone(),
            memory: MemoryInfo {
                total: system.total_memory(),
                free: system.free_memory(),
                available: system.available_memory(),
                used: system.used_memory(),
            },
        };
        client
            .kv_client()
            .put(
                node_info_key(config.node_id),
                serde_json::to_vec(&info).context(CodecSnafu)?,
                Some(PutOptions::new().with_lease(lease_id)),
            )
            .await
            .context(EtcdSnafu)?;

        // 启动后台任务
        let token = CancellationToken::new();

        // lease loop
        join_set.spawn(start_lease(client.lease_client(), lease_id, token.clone()));

        // sysinfo
        join_set.spawn(upload_sysinfo(
            config.node_id,
            config.http_addr,
            config.broker_addr.clone(),
            client.kv_client(),
            lease_id,
            token.clone(),
        ));
        // main loop
        let role = RoleStatus(Arc::new(RwLock::new(Role::Follower)));
        let (assign_watch_event_tx, assign_watch_event_rx) = mpsc::unbounded_channel();
        join_set.spawn(start(
            config.broker_addr.clone(),
            client.clone(),
            lease_id,
            role.clone(),
            assign_watch_event_rx,
            token.clone(),
        ));

        Ok(Self {
            broker_addr: config.broker_addr,
            etcd: client,
            lease_id,
            role,
            assign_watch_event_tx,
            session: SessionState::new(),
            handle: Arc::new(Mutex::new(join_set)),
            token,
        })
    }

    /// leader 给各个节点分配 topic
    #[tracing::instrument(skip(self))]
    async fn assign_topic_addresses(
        &self,
        topic_name: &str,
        config: topic::Config,
    ) -> Result<Vec<ConnectAddress>, Error> {
        let resp = self
            .etcd
            .kv_client()
            .get(
                node_info_key_prefix(),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .context(EtcdSnafu)?;
        let mut sys_infos = Vec::with_capacity(resp.kvs().len());
        for kv in resp.kvs() {
            let sys_info: NodeInfo = serde_json::from_slice(kv.value()).context(CodecSnafu)?;
            sys_infos.push(sys_info);
        }
        // 倒序
        sys_infos.sort_unstable_by(|a, b| match b.memory.available.cmp(&a.memory.available) {
            p @ (cmp::Ordering::Less | cmp::Ordering::Greater) => p,
            cmp::Ordering::Equal => {
                (b.memory.available / b.memory.total).cmp(&(a.memory.available / a.memory.total))
            }
        });
        let mut addrs = Vec::with_capacity(config.partitions as usize);
        let mut node_addrs = sys_infos.into_iter().map(|info| info.broker_addr).cycle();
        for partition_id in 0..config.partitions {
            let Some(addr) = node_addrs.next() else {
                unreachable!()
            };
            addrs.push(addr);
            self.assign_watch_event_tx
                .send((topic_name.to_owned(), partition_id))
                .ok();
        }
        let value = serde_json::to_vec(&addrs).context(CodecSnafu)?;
        self.etcd
            .kv_client()
            .put(topic_assigned_addrs_key(topic_name), value, None)
            .await
            .context(EtcdSnafu)?;

        Ok(addrs)
    }

    async fn exec_with_lock<Fut, R>(&self, lock_key: &str, f: Fut) -> Result<R, Error>
    where
        Fut: Future<Output = Result<R, Error>>,
    {
        exec_with_lock(self.etcd.lock_client(), lock_key, f).await
    }
}

impl MetaStorage for EtcdMeta {
    type Error = Error;

    type Consumer = Consumer;

    type Producer = Producer;

    #[tracing::instrument(skip(self))]
    async fn create_topic(
        &self,
        topic_name: &str,
        config: topic::Config,
    ) -> Result<(), Self::Error> {
        let value = serde_json::to_vec(&config).context(CodecSnafu)?;
        self.etcd
            .kv_client()
            .put(topic_config_key(topic_name), value, None)
            .await
            .context(EtcdSnafu)?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn delete_topic(&self, topic_name: &str) -> Result<(), Self::Error> {
        self.etcd
            .kv_client()
            .delete(topic_config_key(topic_name), None)
            .await
            .context(EtcdSnafu)?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_topic_addresses(
        &self,
        topic_name: &str,
    ) -> Result<Vec<ConnectAddress>, Self::Error> {
        let mut kv_client = self.etcd.kv_client();

        // 查询到已分配的地址则直接返回
        let resp = kv_client
            .get(topic_assigned_addrs_key(topic_name), None)
            .await
            .context(EtcdSnafu)?;
        if let Some(kv) = resp.kvs().first() {
            return serde_json::from_slice(kv.value()).context(CodecSnafu);
        };

        let resp = kv_client
            .get(topic_config_key(topic_name), None)
            .await
            .context(EtcdSnafu)?;
        let Some(kv) = resp.kvs().first() else {
            return ResponseSnafu {
                response: ReturnCode::TopicNotFound,
            }
            .fail();
        };
        let config: topic::Config = serde_json::from_slice(kv.value()).context(CodecSnafu)?;

        match self.role.get_role().await {
            Role::Leader(_) => self.assign_topic_addresses(topic_name, config).await,
            Role::Follower => {
                let resp = self
                    .etcd
                    .election_client()
                    .leader(ELECTION_KEY)
                    .await
                    .context(EtcdSnafu)?;
                let Some(kv) = resp.kv() else { unreachable!() };
                let leader_addr = serde_json::from_slice(kv.value()).context(CodecSnafu)?;
                http::api::lookup_topic_address(&leader_addr, topic_name)
                    .await
                    .context(HttpSnafu)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn get_topic(
        &self,
        topic_name: &str,
    ) -> Result<Option<super::NodeTopicInfo>, Self::Error> {
        let mut kv_client = self.etcd.kv_client();
        // 查询到已分配的地址则直接返回
        let resp = kv_client
            .get(topic_assigned_addrs_key(topic_name), None)
            .await
            .context(EtcdSnafu)?;
        let Some(kv) = resp.kvs().first() else {
            return ResponseSnafu {
                response: ReturnCode::TopicNotFound,
            }
            .fail();
        };
        let addresses: Vec<ConnectAddress> =
            serde_json::from_slice(kv.value()).context(CodecSnafu)?;
        let partitions = addresses
            .into_iter()
            .enumerate()
            .filter(|(_, addr)| addr == &self.broker_addr)
            .map(|(id, _)| id as u16)
            .collect::<Vec<u16>>();
        if partitions.is_empty() {
            return Ok(None);
        }

        // 注册
        for partition_id in &partitions {
            let key = topic_partitioned_addr_key(topic_name, *partition_id);
            let value = serde_json::to_vec(&self.broker_addr).context(CodecSnafu)?;
            api::put_if_absent(self.etcd.clone(), &key, &value, self.lease_id)
                .await
                .context(EtcdSnafu)?;
        }

        // 获取配置
        let resp = kv_client
            .get(topic_config_key(topic_name), None)
            .await
            .context(EtcdSnafu)?;
        let Some(kv) = resp.kvs().first() else {
            return ResponseSnafu {
                response: ReturnCode::TopicNotFound,
            }
            .fail();
        };
        let config: topic::Config = serde_json::from_slice(kv.value()).context(CodecSnafu)?;

        Ok(Some(super::NodeTopicInfo { partitions, config }))
    }

    #[tracing::instrument(skip(self))]
    async fn get_server_http_addresses(&self) -> Result<Vec<ConnectAddress>, Self::Error> {
        let resp = self
            .etcd
            .kv_client()
            .get(
                node_info_key_prefix(),
                Some(GetOptions::new().with_prefix()),
            )
            .await
            .context(EtcdSnafu)?;
        let mut addrs = Vec::with_capacity(resp.kvs().len());
        for kv in resp.kvs() {
            let info: NodeInfo = serde_json::from_slice(kv.value()).context(CodecSnafu)?;
            addrs.push(info.http_addr);
        }
        Ok(addrs)
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
    ) -> Result<Self::Consumer, Self::Error> {
        // 直接存入内存
        let consumer = Consumer::new(
            self.etcd.clone(),
            mem::Consumer::new(consumer_id, client_id, subscribe.clone(), client_tx),
            self.lease_id,
        );
        self.session.add_consumer(consumer.clone()).await?;
        self.exec_with_lock(subscribes_info_lock_key(), async {
            let key = subscribe_info_key(&subscribe.topic_name, &subscribe.subscription_name);
            let get_resp = self
                .etcd
                .kv_client()
                .get(key.as_str(), None)
                .await
                .context(EtcdSnafu)?;
            let info = match get_resp.kvs().first() {
                Some(kv) => {
                    let mut info: EtcdSubscriptionInfo =
                        serde_json::from_slice(kv.value()).context(CodecSnafu)?;
                    if !info.consumers.insert(subscribe.consumer_name.clone()) {
                        return Ok(());
                    }
                    info
                }
                None => EtcdSubscriptionInfo {
                    subscription_name: subscribe.subscription_name.clone(),
                    subscription_type: subscribe.subscription_type,
                    initial_position: subscribe.initial_position,
                    consumers: {
                        let mut consumers = HashSet::new();
                        consumers.insert(subscribe.consumer_name.clone());
                        consumers
                    },
                },
            };

            let value = serde_json::to_vec(&info).context(CodecSnafu)?;
            self.etcd
                .kv_client()
                .put(key, value, None)
                .await
                .context(EtcdSnafu)?;
            Ok(())
        })
        .await?;
        self.exec_with_lock(consumers_lock_key(), async {
            let get_resp = self
                .etcd
                .kv_client()
                .get(
                    consumers_prefix_key(&subscribe.topic_name, &subscribe.subscription_name),
                    Some(GetOptions::new().with_prefix().with_keys_only()),
                )
                .await
                .context(EtcdSnafu)?;
            let mut consumers: Option<EtcdConsumers> = None;
            for kv in get_resp.kvs() {
                let key = kv.key_str().context(EtcdSnafu)?;
                let keys = key
                    .split('/')
                    .map(ToOwned::to_owned)
                    .collect::<Vec<String>>();
                let kv_subscription_type =
                    match keys.get(6).context(InvalidKeySnafu { key })?.as_str() {
                        "exclusive" => SubscriptionType::Exclusive,
                        "shared" => SubscriptionType::Shared,
                        _ => return InvalidKeySnafu { key }.fail(),
                    };
                let kv_consumer_name = keys.get(7).context(InvalidKeySnafu { key })?;

                let kv_client_id: u64 = keys
                    .get(8)
                    .context(InvalidKeySnafu { key })?
                    .parse()
                    .map_err(|_| InvalidKeySnafu { key }.build())?;
                match consumers.as_mut() {
                    Some(etcd_consumers) => match etcd_consumers {
                        EtcdConsumers::Exclusive((consumer_name, client_ids)) => {
                            ensure!(
                                matches!(kv_subscription_type, SubscriptionType::Exclusive),
                                InvalidKeySnafu { key }
                            );
                            ensure!(*consumer_name == *kv_consumer_name, InvalidKeySnafu { key });
                            ensure!(!client_ids.contains(&kv_client_id), InvalidKeySnafu { key });
                            client_ids.insert(kv_client_id);
                        }
                        EtcdConsumers::Shared(shared) => {
                            ensure!(
                                matches!(kv_subscription_type, SubscriptionType::Shared),
                                InvalidKeySnafu { key }
                            );
                            let client_ids = shared.entry(kv_consumer_name.clone()).or_default();
                            ensure!(!client_ids.contains(&kv_client_id), InvalidKeySnafu { key });
                            client_ids.insert(kv_client_id);
                        }
                    },
                    None => match kv_subscription_type {
                        SubscriptionType::Exclusive => {
                            let mut client_ids = HashSet::new();
                            client_ids.insert(kv_client_id);
                            consumers = Some(EtcdConsumers::Exclusive((
                                kv_consumer_name.clone(),
                                client_ids,
                            )));
                        }
                        SubscriptionType::Shared => {
                            let mut client_ids = HashSet::new();
                            client_ids.insert(kv_client_id);
                            let mut shared = HashMap::new();
                            shared.insert(kv_consumer_name.clone(), client_ids);
                            consumers = Some(EtcdConsumers::Shared(shared));
                        }
                    },
                }
            }
            // 存入 etcd
            let etcd_consumer = EtcdConsumer::new();
            let consumer_key = consumer_key(
                &subscribe.topic_name,
                &subscribe.subscription_name,
                subscribe.subscription_type,
                &subscribe.consumer_name,
                client_id,
            );
            match consumers {
                Some(consumers) => match consumers {
                    EtcdConsumers::Exclusive((consumer_name, client_ids)) => {
                        ensure!(
                            consumer_name == subscribe.consumer_name,
                            ResponseSnafu {
                                response: ReturnCode::ConsumerAlreadyExclusive,
                            }
                        );
                        ensure!(
                            !client_ids.contains(&client_id),
                            ResponseSnafu {
                                response: ReturnCode::ConsumerNameAlreadyExists,
                            }
                        );

                        let value = serde_json::to_vec(&etcd_consumer).context(CodecSnafu)?;
                        self.etcd
                            .kv_client()
                            .put(
                                consumer_key,
                                value,
                                Some(PutOptions::new().with_lease(self.lease_id)),
                            )
                            .await
                            .context(EtcdSnafu)?;
                    }
                    EtcdConsumers::Shared(mut shared) => {
                        match shared.get_mut(&subscribe.consumer_name) {
                            Some(client_ids) => {
                                ensure!(
                                    !client_ids.contains(&client_id),
                                    ResponseSnafu {
                                        response: ReturnCode::ConsumerNameAlreadyExists,
                                    }
                                );
                                let value =
                                    serde_json::to_vec(&etcd_consumer).context(CodecSnafu)?;
                                self.etcd
                                    .kv_client()
                                    .put(
                                        consumer_key,
                                        value,
                                        Some(PutOptions::new().with_lease(self.lease_id)),
                                    )
                                    .await
                                    .context(EtcdSnafu)?;
                            }
                            None => {
                                let value =
                                    serde_json::to_vec(&etcd_consumer).context(CodecSnafu)?;
                                self.etcd
                                    .kv_client()
                                    .put(
                                        consumer_key,
                                        value,
                                        Some(PutOptions::new().with_lease(self.lease_id)),
                                    )
                                    .await
                                    .context(EtcdSnafu)?;
                            }
                        }
                    }
                },
                None => match subscribe.subscription_type {
                    SubscriptionType::Exclusive => {
                        let value = serde_json::to_vec(&etcd_consumer).context(CodecSnafu)?;
                        self.etcd
                            .kv_client()
                            .put(
                                consumer_key,
                                value,
                                Some(PutOptions::new().with_lease(self.lease_id)),
                            )
                            .await
                            .context(EtcdSnafu)?;
                    }
                    SubscriptionType::Shared => {
                        let value = serde_json::to_vec(&etcd_consumer).context(CodecSnafu)?;
                        self.etcd
                            .kv_client()
                            .put(
                                consumer_key,
                                value,
                                Some(PutOptions::new().with_lease(self.lease_id)),
                            )
                            .await
                            .context(EtcdSnafu)?;
                    }
                },
            }
            Ok(consumer)
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn del_consumer(&self, consumer_id: u64) -> Result<Option<Self::Consumer>, Self::Error> {
        let deleted = self.session.del_consumer(consumer_id).await;

        let Some(deleted) = deleted else {
            return Ok(None);
        };

        let info = deleted.info();
        let consumer_key = consumer_key(
            &info.topic_name,
            &info.subscription_name,
            info.subscription_type,
            &info.name,
            info.client_id,
        );
        self.exec_with_lock(consumers_lock_key(), async {
            match deleted.info().subscription_type {
                SubscriptionType::Exclusive => {
                    self.etcd
                        .kv_client()
                        .delete(consumer_key, None)
                        .await
                        .context(EtcdSnafu)?;
                }
                SubscriptionType::Shared => {
                    self.etcd
                        .kv_client()
                        .delete(consumer_key, None)
                        .await
                        .context(EtcdSnafu)?;
                }
            }
            Ok(Some(deleted))
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn clear_consumer(&self, client_id: u64) -> Result<(), Self::Error> {
        self.session.clear_consumer(client_id).await;

        self.exec_with_lock(consumers_lock_key(), async {
            //
            let get_resp = self
                .etcd
                .kv_client()
                .get(
                    all_consumers_prefix_key(),
                    Some(GetOptions::new().with_prefix().with_keys_only()),
                )
                .await
                .context(EtcdSnafu)?;
            for kv in get_resp.kvs() {
                if kv.key().ends_with(format!("/{client_id}").as_bytes()) {
                    self.etcd
                        .kv_client()
                        .delete(kv.key(), None)
                        .await
                        .context(EtcdSnafu)?;
                }
            }
            Ok(())
        })
        .await
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
        let producer = Producer::new(mem::Producer::new(ProducerInfo::new(
            producer_id,
            client_id,
            packet.clone(),
        )));
        self.session.add_producer(producer.clone()).await?;

        self.exec_with_lock(producers_lock_key(), async {
            //
            let get_resp = self
                .etcd
                .kv_client()
                .get(
                    producers_prefix_key(&packet.topic_name),
                    Some(GetOptions::new().with_prefix().with_keys_only()),
                )
                .await
                .context(EtcdSnafu)?;
            let mut producers: Option<EtcdProducers> = None;
            for kv in get_resp.kvs() {
                let key = kv.key_str().context(EtcdSnafu)?;
                let keys = key
                    .split('/')
                    .map(ToOwned::to_owned)
                    .collect::<Vec<String>>();
                let kv_access_mode = match keys.get(4).context(InvalidKeySnafu { key })?.as_str() {
                    "exclusive" => AccessMode::Exclusive,
                    "shared" => AccessMode::Shared,
                    _ => return InvalidKeySnafu { key }.fail(),
                };
                let kv_producer_name = keys.get(4).context(InvalidKeySnafu { key })?;
                let kv_client_id: u64 = keys
                    .get(6)
                    .context(InvalidKeySnafu { key })?
                    .parse()
                    .map_err(|_| InvalidKeySnafu { key }.build())?;
                match producers.as_mut() {
                    Some(etcd_producers) => match etcd_producers {
                        EtcdProducers::Exclusive((producer_name, client_ids)) => {
                            ensure!(
                                matches!(kv_access_mode, AccessMode::Exclusive),
                                InvalidKeySnafu { key }
                            );
                            ensure!(*producer_name == *kv_producer_name, InvalidKeySnafu { key });
                            ensure!(!client_ids.contains(&kv_client_id), InvalidKeySnafu { key });
                            client_ids.insert(kv_client_id);
                        }
                        EtcdProducers::Shared(shared) => {
                            ensure!(
                                matches!(kv_access_mode, AccessMode::Shared),
                                InvalidKeySnafu { key }
                            );
                            let client_ids = shared.entry(kv_producer_name.clone()).or_default();
                            ensure!(!client_ids.contains(&kv_client_id), InvalidKeySnafu { key });
                            client_ids.insert(kv_client_id);
                        }
                    },
                    None => match kv_access_mode {
                        AccessMode::Exclusive => {
                            let mut client_ids = HashSet::new();
                            client_ids.insert(kv_client_id);
                            producers = Some(EtcdProducers::Exclusive((
                                kv_producer_name.clone(),
                                client_ids,
                            )));
                        }
                        AccessMode::Shared => {
                            let mut client_ids = HashSet::new();
                            client_ids.insert(kv_client_id);
                            let mut shared = HashMap::new();
                            shared.insert(kv_producer_name.clone(), client_ids);
                            producers = Some(EtcdProducers::Shared(shared));
                        }
                    },
                }
            }
            //
            let etcd_producer = EtcdProducer;
            let producer_key = producer_key(
                &packet.topic_name,
                packet.access_mode,
                &packet.producer_name,
                client_id,
            );
            match producers {
                Some(etcd_producers) => match etcd_producers {
                    EtcdProducers::Exclusive((producer_name, client_ids)) => {
                        ensure!(
                            producer_name == packet.producer_name,
                            ResponseSnafu {
                                response: ReturnCode::ProducerAlreadyExclusive,
                            }
                        );
                        ensure!(
                            !client_ids.contains(&client_id),
                            ResponseSnafu {
                                response: ReturnCode::ProducerNameAlreadyExists,
                            }
                        );

                        let value = serde_json::to_vec(&etcd_producer).context(CodecSnafu)?;
                        self.etcd
                            .kv_client()
                            .put(
                                producer_key,
                                value,
                                Some(PutOptions::new().with_lease(self.lease_id)),
                            )
                            .await
                            .context(EtcdSnafu)?;
                    }
                    EtcdProducers::Shared(mut shared) => {
                        match shared.get_mut(&packet.producer_name) {
                            Some(client_ids) => {
                                ensure!(
                                    !client_ids.contains(&client_id),
                                    ResponseSnafu {
                                        response: ReturnCode::ProducerNameAlreadyExists,
                                    }
                                );
                                let value =
                                    serde_json::to_vec(&etcd_producer).context(CodecSnafu)?;
                                self.etcd
                                    .kv_client()
                                    .put(
                                        producer_key,
                                        value,
                                        Some(PutOptions::new().with_lease(self.lease_id)),
                                    )
                                    .await
                                    .context(EtcdSnafu)?;
                            }
                            None => {
                                let value =
                                    serde_json::to_vec(&etcd_producer).context(CodecSnafu)?;
                                self.etcd
                                    .kv_client()
                                    .put(
                                        producer_key,
                                        value,
                                        Some(PutOptions::new().with_lease(self.lease_id)),
                                    )
                                    .await
                                    .context(EtcdSnafu)?;
                            }
                        }
                    }
                },
                None => match packet.access_mode {
                    AccessMode::Exclusive => {
                        let value = serde_json::to_vec(&etcd_producer).context(CodecSnafu)?;
                        self.etcd
                            .kv_client()
                            .put(
                                producer_key,
                                value,
                                Some(PutOptions::new().with_lease(self.lease_id)),
                            )
                            .await
                            .context(EtcdSnafu)?;
                    }
                    AccessMode::Shared => {
                        let value = serde_json::to_vec(&etcd_producer).context(CodecSnafu)?;
                        self.etcd
                            .kv_client()
                            .put(
                                producer_key,
                                value,
                                Some(PutOptions::new().with_lease(self.lease_id)),
                            )
                            .await
                            .context(EtcdSnafu)?;
                    }
                },
            }
            Ok(producer)
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn del_producer(&self, producer_id: u64) -> Result<Option<Producer>, Self::Error> {
        let deleted = self.session.del_producer(producer_id).await;

        let Some(deleted) = deleted else {
            return Ok(None);
        };

        let info = deleted.info();

        let producer_key = producer_key(
            &info.topic_name,
            info.access_mode,
            &info.producer_name,
            info.client_id,
        );
        self.exec_with_lock(producers_lock_key(), async {
            match deleted.info().access_mode {
                AccessMode::Exclusive => {
                    self.etcd
                        .kv_client()
                        .delete(producer_key, None)
                        .await
                        .context(EtcdSnafu)?;
                }
                AccessMode::Shared => {
                    self.etcd
                        .kv_client()
                        .delete(producer_key, None)
                        .await
                        .context(EtcdSnafu)?;
                }
            }
            Ok(Some(deleted))
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn clear_producer(&self, client_id: u64) -> Result<(), Self::Error> {
        self.session.clear_producer(client_id).await;

        self.exec_with_lock(producers_lock_key(), async {
            let get_resp = self
                .etcd
                .kv_client()
                .get(
                    all_producers_prefix_key(),
                    Some(GetOptions::new().with_prefix().with_keys_only()),
                )
                .await
                .context(EtcdSnafu)?;
            for kv in get_resp.kvs() {
                if kv.key().ends_with(format!("/{client_id}").as_bytes()) {
                    self.etcd
                        .kv_client()
                        .delete(kv.key(), None)
                        .await
                        .context(EtcdSnafu)?;
                }
            }
            Ok(())
        })
        .await
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
        self.token.cancel();
        let mut handle = self.handle.lock().await;
        while let Some(res) = handle.join_next().await {
            match res {
                Ok(_) => {}
                Err(e) => {
                    error!("meta server task panic! {e:?}")
                }
            }
        }
        Ok(())
    }

    async fn get_topic_subscriptions(
        &self,
        topic_name: &str,
    ) -> Result<Vec<super::SubscriptionInfo>, Self::Error> {
        self.exec_with_lock(subscribes_info_lock_key(), async {
            let get_resp = self
                .etcd
                .kv_client()
                .get(
                    subscribes_topic_prefix_key(topic_name),
                    Some(GetOptions::new().with_prefix()),
                )
                .await
                .context(EtcdSnafu)?;
            let mut infos = Vec::with_capacity(get_resp.kvs().len());
            for kv in get_resp.kvs() {
                let info: EtcdSubscriptionInfo =
                    serde_json::from_slice(kv.value()).context(CodecSnafu)?;
                infos.push(super::SubscriptionInfo {
                    subscription_name: info.subscription_name,
                    subscription_type: info.subscription_type,
                    initial_position: info.initial_position,
                })
            }
            Ok(infos)
        })
        .await
    }

    async fn unsubscribe(
        &self,
        topic_name: &str,
        subscription_name: &str,
        consumer_name: &str,
    ) -> Result<bool, Self::Error> {
        self.exec_with_lock(subscribes_info_lock_key(), async {
            let key = subscribe_info_key(topic_name, subscription_name);
            let get_resp = self
                .etcd
                .kv_client()
                .get(key.as_str(), None)
                .await
                .context(EtcdSnafu)?;
            let Some(kv) = get_resp.kvs().first() else {
                return Ok(false);
            };
            let mut info: EtcdSubscriptionInfo =
                serde_json::from_slice(kv.value()).context(CodecSnafu)?;
            if !info.consumers.remove(consumer_name) {
                return Ok(!info.consumers.is_empty());
            }
            let value = serde_json::to_vec(&info).context(CodecSnafu)?;
            self.etcd
                .kv_client()
                .put(key, value, None)
                .await
                .context(EtcdSnafu)?;
            Ok(!info.consumers.is_empty())
        })
        .await
    }
}

#[derive(Debug, Clone)]
pub struct RoleStatus(pub Arc<RwLock<Role>>);

impl RoleStatus {
    pub async fn get_role(&self) -> Role {
        self.0.read().await.clone()
    }

    pub async fn set_leader(&self, leader_key: etcd::LeaderKey) {
        let mut role = self.0.write().await;
        *role = Role::Leader(leader_key)
    }

    pub async fn set_follower(&self) {
        let mut role = self.0.write().await;
        *role = Role::Follower
    }
}

#[derive(Debug, Clone)]
#[repr(u8)]
pub enum Role {
    Leader(etcd::LeaderKey),
    Follower,
}

impl Role {
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader(_))
    }
}

async fn upload_sysinfo(
    node_id: u16,
    http_addr: ConnectAddress,
    broker_addr: ConnectAddress,
    mut etcd: etcd::KvClient,
    lease_id: i64,
    token: CancellationToken,
) {
    const SYSTEM_INFO_REPORT_INTERVAL: Duration = Duration::from_secs(5);
    let mut system = System::new();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(SYSTEM_INFO_REPORT_INTERVAL) => {
                system.refresh_memory_specifics(MemoryRefreshKind::new().with_ram());
                let info = NodeInfo {
                    http_addr: http_addr.clone(),
                    broker_addr: broker_addr.clone(),
                    memory: MemoryInfo {
                        total: system.total_memory(),
                        free: system.free_memory(),
                        available: system.available_memory(),
                        used: system.used_memory(),
                    },
                };
                if let Err(e) = etcd
                    .put(
                        node_info_key(node_id),
                        serde_json::to_vec(&info).expect("codec sysinfo error"),
                        Some(PutOptions::new().with_lease(lease_id)),
                    )
                    .await
                {
                    error!("upload sysinfo error: {e:?}")
                }
            }
            _ = token.cancelled() => break
        }
    }
}

async fn start(
    broker_addr: ConnectAddress,
    etcd: etcd::Client,
    lease_id: i64,
    role: RoleStatus,
    mut assign_watch_event_rx: mpsc::UnboundedReceiver<(String, u16)>,
    token: CancellationToken,
) {
    let mut election_client = etcd.election_client();
    let mut kv_client = etcd.kv_client();
    let mut watch_client = etcd.watch_client();
    let value = serde_json::to_vec(&broker_addr).expect("codec broker address error");
    let child_token = token.child_token();
    let mut futs = FuturesUnordered::new();
    loop {
        role.set_follower().await;
        tokio::select! {
            resp_res = election_client.campaign(ELECTION_KEY, value.clone(), lease_id) => match resp_res {
                Ok(mut resp) => {
                    let Some(leader_key) = resp.take_leader() else {
                        unreachable!()
                    };
                    role.set_leader(leader_key).await;
                    // 监听当前已存在的所有 topic 的 所有 partition
                    let resp = match kv_client
                        .get(
                            topic_partitioned_addr_key_prefix(),
                            Some(GetOptions::new().with_prefix()),
                        )
                        .await {
                            Ok(resp) => resp,
                            Err(e) => {
                                error!("get address of topics error: {e:?}");
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue
                            },
                        };
                    let mut watcher: Option<Watcher> = None;
                    for kv in resp.kvs() {
                        let Ok(key) = kv.key_str().context(EtcdSnafu) else {
                            unreachable!()
                        };
                        let keys = key
                            .split('/')
                            .map(ToOwned::to_owned)
                            .collect::<Vec<String>>();
                        let topic_name = match keys.get(3) {
                            Some(name) => name,
                            None => {
                                error!("invalid key: {key}, topic_name not found at index 3");
                                break
                            },
                        };
                        match watcher.as_mut() {
                            Some(wc) => {
                                if let Err(e) =  wc.watch(key, Some(WatchOptions::new().with_filters([WatchFilterType::NoPut]))).await {
                                    error!("watch follower error: {e:?}");
                                    break
                                }
                            },
                            None => {
                                match watch_client
                                .watch(
                                    key,
                                    Some(WatchOptions::new().with_filters([WatchFilterType::NoPut])),
                                )
                                .await {
                                    Ok((wc, ws)) => {
                                        watcher = Some(wc);
                                        futs.push(watch_follower(ws, etcd.clone(), topic_name.to_owned(), lease_id, child_token.clone()));
                                    },
                                    Err(e) => {
                                        error!("watch follower error:{e:?}");
                                        break
                                    },
                                };
                            },
                        }
                    }

                    loop {
                        tokio::select! {
                            biased;
                            _ = token.cancelled() => return,
                            _ = futs.next(), if !futs.is_empty() => continue,
                            Some((topic_name, partition_id)) = assign_watch_event_rx.recv() => {
                                let key = topic_partitioned_addr_key(&topic_name, partition_id);
                                match watcher.as_mut() {
                                    Some(wc) => {
                                        if let Err(e) =  wc.watch(key, Some(WatchOptions::new().with_filters([WatchFilterType::NoPut]))).await {
                                            error!("watch follower error: {e:?}");
                                            break
                                        }
                                    },
                                    None => {
                                        match etcd
                                        .watch_client()
                                        .watch(
                                            key,
                                            Some(WatchOptions::new().with_filters([WatchFilterType::NoPut])),
                                        )
                                        .await {
                                            Ok((wc, ws)) => {
                                                watcher = Some(wc);
                                                futs.push(watch_follower(ws, etcd.clone(), topic_name.to_owned(), lease_id, child_token.clone()));
                                            },
                                            Err(e) => {
                                                error!("watch follower error:{e:?}");
                                                break
                                            },
                                        };
                                    },
                                }
                            }
                            else => break
                        }
                    }
                }
                Err(e) => {
                    error!(lease_id, "election error: {e:?}");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }
            },
            _ = token.cancelled() => return,
        }
    }
}

#[tracing::instrument(skip(etcd))]
async fn watch_follower(
    mut ws: WatchStream,
    etcd: etcd::Client,
    topic_name: String,
    lease_id: i64,
    token: CancellationToken,
) -> Result<(), Error> {
    loop {
        tokio::select! {
            res = ws.try_next() => match res.context(EtcdSnafu)? {
                Some(resp) => {
                    for event in resp.events() {
                        if event.event_type() != EventType::Delete {
                            continue;
                        }
                        reassign_topic_addresses(etcd.clone(), &topic_name, lease_id).await?;
                    }
                }
                None => return Ok(()),
            },
            _ = token.cancelled() => return Ok(()),
        }
    }
}

#[tracing::instrument(skip(etcd))]
async fn reassign_topic_addresses(
    etcd: etcd::Client,
    topic_name: &str,
    lease_id: i64,
) -> Result<(), Error> {
    let resp = etcd
        .kv_client()
        .get(topic_config_key(topic_name), None)
        .await
        .context(EtcdSnafu)?;
    let Some(kv) = resp.kvs().first() else {
        return ResponseSnafu {
            response: ReturnCode::TopicNotFound,
        }
        .fail();
    };
    let config: topic::Config = serde_json::from_slice(kv.value()).context(CodecSnafu)?;
    let resp = etcd
        .kv_client()
        .get(
            node_info_key_prefix(),
            Some(GetOptions::new().with_prefix()),
        )
        .await
        .context(EtcdSnafu)?;
    let mut sys_infos = Vec::with_capacity(resp.kvs().len());
    for kv in resp.kvs() {
        let sys_info: NodeInfo = serde_json::from_slice(kv.value()).context(CodecSnafu)?;
        sys_infos.push(sys_info);
    }
    // 倒序
    sys_infos.sort_unstable_by(|a, b| match b.memory.available.cmp(&a.memory.available) {
        p @ (cmp::Ordering::Less | cmp::Ordering::Greater) => p,
        cmp::Ordering::Equal => {
            (b.memory.available / b.memory.total).cmp(&(a.memory.available / a.memory.total))
        }
    });
    let mut addrs = Vec::with_capacity(config.partitions as usize);
    let mut node_addrs = sys_infos.into_iter().map(|info| info.broker_addr).cycle();
    for _ in 0..config.partitions {
        let Some(addr) = node_addrs.next() else {
            unreachable!()
        };
        addrs.push(addr);
    }
    let value = serde_json::to_vec(&addrs).context(CodecSnafu)?;
    etcd.kv_client()
        .put(
            topic_assigned_addrs_key(topic_name),
            value,
            Some(PutOptions::new().with_lease(lease_id)),
        )
        .await
        .context(EtcdSnafu)?;
    Ok(())
}

impl std::fmt::Debug for EtcdMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EtcdMeta")
            .field("etcd", &"etcd client".to_string())
            .finish()
    }
}

#[derive(Clone)]
pub struct Consumer {
    etcd: etcd::Client,
    mem: mem::Consumer,
    lease_id: i64,
}

impl Consumer {
    pub fn new(etcd: etcd::Client, mem: mem::Consumer, lease_id: i64) -> Self {
        Self {
            etcd,
            mem,
            lease_id,
        }
    }

    async fn exec_with_lock<Fut, R>(&self, lock_key: impl Into<Vec<u8>>, f: Fut) -> Result<R, Error>
    where
        Fut: Future<Output = Result<R, Error>>,
    {
        exec_with_lock(self.etcd.lock_client(), lock_key, f).await
    }
}

impl MetaConsumer for Consumer {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    fn info(&self) -> super::ConsumerInfo {
        self.mem.info()
    }

    #[tracing::instrument(skip(self))]
    async fn ready(&self) -> Result<(), Self::Error> {
        Ok(self.mem.ready().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn is_ready(&self) -> Result<bool, Self::Error> {
        Ok(self.mem.is_ready().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn permits(&self) -> Result<u32, Self::Error> {
        let info = self.info();
        let key = consumer_key(
            &info.topic_name,
            &info.subscription_name,
            info.subscription_type,
            &info.name,
            info.client_id,
        );
        self.exec_with_lock(format!("{key}/lock"), async {
            let resp = self
                .etcd
                .kv_client()
                .get(key, None)
                .await
                .context(EtcdSnafu)?;
            let Some(kv) = resp.kvs().first() else {
                return Ok(0);
            };
            let consumer: EtcdConsumer = serde_json::from_slice(kv.value()).context(CodecSnafu)?;
            Ok(consumer.permits)
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn permits_add(&self, delta: u32) -> Result<(), Self::Error> {
        let info = self.info();
        let key = consumer_key(
            &info.topic_name,
            &info.subscription_name,
            info.subscription_type,
            &info.name,
            info.client_id,
        );
        self.exec_with_lock(format!("{key}/lock"), async {
            let resp = self
                .etcd
                .kv_client()
                .get(key.as_bytes(), None)
                .await
                .context(EtcdSnafu)?;
            let Some(kv) = resp.kvs().first() else {
                return Ok(());
            };
            let mut consumer: EtcdConsumer =
                serde_json::from_slice(kv.value()).context(CodecSnafu)?;
            consumer.permits = consumer.permits.saturating_add(delta);
            let value = serde_json::to_vec(&consumer).context(CodecSnafu)?;
            self.etcd
                .kv_client()
                .put(
                    key,
                    value,
                    Some(PutOptions::new().with_lease(self.lease_id)),
                )
                .await
                .context(EtcdSnafu)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn permits_sub(&self, delta: u32) -> Result<(), Self::Error> {
        let info = self.info();
        let key = consumer_key(
            &info.topic_name,
            &info.subscription_name,
            info.subscription_type,
            &info.name,
            info.client_id,
        );
        self.exec_with_lock(format!("{key}/lock"), async {
            let resp = self
                .etcd
                .kv_client()
                .get(key.as_bytes(), None)
                .await
                .context(EtcdSnafu)?;
            let Some(kv) = resp.kvs().first() else {
                return Ok(());
            };
            let mut consumer: EtcdConsumer =
                serde_json::from_slice(kv.value()).context(CodecSnafu)?;
            consumer.permits = consumer.permits.saturating_sub(delta);
            let value = serde_json::to_vec(&consumer).context(CodecSnafu)?;
            self.etcd
                .kv_client()
                .put(
                    key,
                    value,
                    Some(PutOptions::new().with_lease(self.lease_id)),
                )
                .await
                .context(EtcdSnafu)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<(), Self::Error> {
        self.mem.close().await?;
        let info = self.info();
        let key = consumer_key(
            &info.topic_name,
            &info.subscription_name,
            info.subscription_type,
            &info.name,
            info.client_id,
        );
        self.exec_with_lock(format!("{key}/lock"), async {
            self.etcd
                .kv_client()
                .delete(key, None)
                .await
                .context(EtcdSnafu)?;
            Ok(())
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn is_closed(&self) -> Result<bool, Self::Error> {
        Ok(self.mem.is_closed().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn send(&self, message: comet_common::protocol::send::Send) -> Result<(), Self::Error> {
        Ok(self.mem.send(message).await?)
    }
}

#[derive(Clone)]
pub struct Producer {
    mem: mem::Producer,
}

impl Producer {
    fn new(mem: mem::Producer) -> Self {
        Self { mem }
    }
}

impl MetaProducer for Producer {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    fn info(&self) -> super::ProducerInfo {
        self.mem.info()
    }
}

async fn exec_with_lock<Fut, R>(
    mut client: etcd::LockClient,
    lock_key: impl Into<Vec<u8>>,
    f: Fut,
) -> Result<R, Error>
where
    Fut: Future<Output = Result<R, Error>>,
{
    let lock_resp = client.lock(lock_key, None).await.context(EtcdSnafu)?;

    let res = f.await;

    // 解锁
    client.unlock(lock_resp.key()).await.context(EtcdSnafu)?;
    res
}

fn node_info_key(node_id: u16) -> String {
    format!("/comet/nodes/info/{node_id}")
}

const fn node_info_key_prefix() -> &'static str {
    "/comet/nodes/info/"
}

fn topic_assigned_addrs_key(topic_name: &str) -> String {
    format!("/comet/addrs/assigned/topics/{topic_name}")
}

fn topic_config_key(topic_name: &str) -> String {
    format!("/comet/config/topics/{topic_name}")
}

fn topic_partitioned_addr_key(topic_name: &str, partition_id: u16) -> String {
    format!("/comet/addrs/topics/{topic_name}/partitions/{partition_id}")
}

const fn topic_partitioned_addr_key_prefix() -> &'static str {
    "/comet/addrs/topics/"
}

const fn subscribes_info_lock_key() -> &'static str {
    "/comet/subscriptions/lock"
}

fn subscribe_info_key(topic_name: &str, subscription_name: &str) -> String {
    format!("/comet/subscriptions/{topic_name}/{subscription_name}",)
}

fn subscribes_topic_prefix_key(topic_name: &str) -> String {
    format!("/comet/subscriptions/{topic_name}")
}

const fn consumers_lock_key() -> &'static str {
    "/comet/consumers/lock"
}

fn consumers_prefix_key(topic_name: &str, subscription_name: &str) -> String {
    format!("/comet/consumers/topics/{topic_name}/subscriptions/{subscription_name}")
}

const fn all_consumers_prefix_key() -> &'static str {
    "/comet/consumers"
}

fn consumer_key(
    topic_name: &str,
    subscription_name: &str,
    subscription_type: SubscriptionType,
    consumer_name: &str,
    client_id: u64,
) -> String {
    format!("/comet/consumers/topics/{topic_name}/subscriptions/{subscription_name}/{subscription_type}/{consumer_name}/{client_id}")
}

const fn producers_lock_key() -> &'static str {
    "/comet/producers/lock"
}

fn producers_prefix_key(topic_name: &str) -> String {
    format!("/comet/producers/topics/{topic_name}")
}

fn producer_key(
    topic_name: &str,
    access_mode: AccessMode,
    producer_name: &str,
    client_id: u64,
) -> String {
    format!("/comet/producers/topics/{topic_name}/{access_mode}/{producer_name}/{client_id}")
}

const fn all_producers_prefix_key() -> &'static str {
    "/comet/producers/"
}
