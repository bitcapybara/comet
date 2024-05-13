use std::{collections::HashSet, path::PathBuf, sync::Arc};

use comet_common::{
    addr::ConnectAddress,
    error::ResponsiveError,
    io::writer,
    protocol::{
        consumer::Subscribe,
        producer::CreateProducer,
        response::{Response, ReturnCode},
        send,
    },
    types::{InitialPosition, SubscriptionType},
};
use redb::{ReadableTable, TableDefinition};
use snafu::{Location, ResultExt, Snafu};
use tokio::sync::mpsc;

use crate::broker::topic;

use super::{mem, MetaConsumer, MetaProducer, MetaStorage, SubscriptionInfo};

const TABLE: TableDefinition<&str, Vec<u8>> = TableDefinition::new("comet_meta");

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("response error: {response}"))]
    Response {
        response: Response,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("redb error"))]
    Redb {
        #[snafu(source)]
        error: redb::DatabaseError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("redb txn error"))]
    RedbTxn {
        #[snafu(source)]
        error: redb::TransactionError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("redb table error"))]
    RedbTable {
        #[snafu(source)]
        error: redb::TableError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("redb storage error"))]
    RedbStorage {
        #[snafu(source)]
        error: redb::StorageError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("redb txn commit error"))]
    RedbCommit {
        #[snafu(source)]
        error: redb::CommitError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("serde codec error"))]
    Serde {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(context(false), display("memory error"))]
    Memory {
        source: super::mem::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("create dir {} error", path.display()))]
    MakeDataDir {
        path: PathBuf,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("callback exec error"))]
    CallbackPanic {
        #[snafu(implicit)]
        location: Location,
    },
}

impl ResponsiveError for Error {
    fn as_response(&self) -> Response {
        match self {
            Error::Response { response, .. } => response.clone(),
            e => Response::new(ReturnCode::Internal, e.to_string()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub data_dir: PathBuf,
    pub mem: mem::Config,
}

#[derive(Clone)]
pub struct RedbMeta {
    db: Arc<redb::Database>,
    mem: mem::MemoryMeta,
}

impl RedbMeta {
    #[tracing::instrument]
    pub async fn new(config: Config) -> Result<Self, Error> {
        tokio::fs::create_dir_all(&config.data_dir)
            .await
            .context(MakeDataDirSnafu {
                path: &config.data_dir,
            })?;
        let db = asyncify(move || {
            redb::Database::create(config.data_dir.join("comet_meta.redb")).context(RedbSnafu)
        })
        .await?;
        Ok(Self {
            db: Arc::new(db),
            mem: mem::MemoryMeta::new(config.mem),
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct RedbSubscriptionInfo {
    pub subscription_name: String,
    pub subscription_type: SubscriptionType,
    pub initial_position: InitialPosition,
    pub consumers: HashSet<String>,
}

impl MetaStorage for RedbMeta {
    type Error = Error;

    type Consumer = Consumer;

    type Producer = Producer;

    #[tracing::instrument(skip(self))]
    async fn create_topic(
        &self,
        topic_name: &str,
        config: topic::Config,
    ) -> Result<(), Self::Error> {
        if config.partitions > 1 {
            return ResponseSnafu {
                response: ReturnCode::PartitionedTopicNotSupported,
            }
            .fail();
        }

        asyncify({
            let db = self.db.clone();
            let topic_name = topic_name.to_owned();
            let config = config.clone();
            move || -> Result<(), Error> {
                let txn = db.begin_write().context(RedbTxnSnafu)?;
                {
                    let mut table = txn.open_table(TABLE).context(RedbTableSnafu)?;
                    let config = serde_json::to_vec(&config).context(SerdeSnafu)?;
                    table
                        .insert(topic_key(topic_name).as_str(), config)
                        .context(RedbStorageSnafu)?;
                }
                txn.commit().context(RedbCommitSnafu)?;
                Ok(())
            }
        })
        .await?;

        self.mem.create_topic(topic_name, config).await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn delete_topic(&self, topic_name: &str) -> Result<(), Self::Error> {
        asyncify({
            let db = self.db.clone();
            let topic_name = topic_name.to_owned();
            move || -> Result<(), Error> {
                let txn = db.begin_write().context(RedbTxnSnafu)?;
                {
                    let mut table = txn.open_table(TABLE).context(RedbTableSnafu)?;
                    table
                        .remove(topic_key(topic_name).as_str())
                        .context(RedbStorageSnafu)?;
                }
                txn.commit().context(RedbCommitSnafu)?;
                Ok(())
            }
        })
        .await?;

        self.mem.delete_topic(topic_name).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_topic(
        &self,
        topic_name: &str,
    ) -> Result<Option<super::NodeTopicInfo>, Self::Error> {
        if let Some(info) = self.mem.get_topic(topic_name).await? {
            return Ok(Some(info));
        }

        let Some(config) = asyncify({
            let db = self.db.clone();
            let topic_name = topic_name.to_owned();
            move || -> Result<Option<topic::Config>, Error> {
                let txn = db.begin_read().context(RedbTxnSnafu)?;
                let table = txn.open_table(TABLE).context(RedbTableSnafu)?;
                let Some(config_bytes) = table
                    .get(topic_key(topic_name).as_str())
                    .context(RedbStorageSnafu)?
                else {
                    return Ok(None);
                };
                let config = serde_json::from_slice::<topic::Config>(&config_bytes.value())
                    .context(SerdeSnafu)?;
                Ok(Some(config))
            }
        })
        .await?
        else {
            return Ok(None);
        };

        Ok(Some(super::NodeTopicInfo {
            partitions: vec![0],
            config: config.clone(),
        }))
    }

    async fn get_topic_subscriptions(
        &self,
        topic_name: &str,
    ) -> Result<Vec<SubscriptionInfo>, Self::Error> {
        asyncify({
            let db = self.db.clone();
            let topic_name = topic_name.to_owned();
            move || {
                let key = subscribes_key_topic_prefix(topic_name);
                let txn = db.begin_read().context(RedbTxnSnafu)?;
                let table = txn.open_table(TABLE).context(RedbTableSnafu)?;
                let mut subscriptions = vec![];
                let mut range = table
                    .range(format!("{key}\0").as_str()..format!("{key}{}", char::MAX).as_str())
                    .context(RedbStorageSnafu)?;
                while let Some((_, v)) = range.next().transpose().context(RedbStorageSnafu)? {
                    let subscription: RedbSubscriptionInfo =
                        serde_json::from_slice(&v.value()).context(SerdeSnafu)?;
                    subscriptions.push(SubscriptionInfo {
                        subscription_name: subscription.subscription_name,
                        subscription_type: subscription.subscription_type,
                        initial_position: subscription.initial_position,
                    })
                }
                Ok(subscriptions)
            }
        })
        .await
    }

    #[tracing::instrument(skip(self))]
    async fn get_topic_addresses(
        &self,
        _topic_name: &str,
    ) -> Result<Vec<ConnectAddress>, Self::Error> {
        Ok(vec![self.mem.config.broker_addr.clone()])
    }

    #[tracing::instrument(skip(self))]
    async fn get_server_http_addresses(&self) -> Result<Vec<ConnectAddress>, Self::Error> {
        Ok(vec![self.mem.config.http_addr.clone()])
    }

    #[tracing::instrument(skip(self))]
    async fn get_available_consumers(
        &self,
        topic_name: &str,
        subscription_name: &str,
    ) -> Result<Vec<Self::Consumer>, Self::Error> {
        Ok(self
            .mem
            .get_available_consumers(topic_name, subscription_name)
            .await?
            .into_iter()
            .map(Consumer)
            .collect())
    }

    #[tracing::instrument(skip(self))]
    async fn get_consumer(&self, consumer_id: u64) -> Result<Option<Self::Consumer>, Self::Error> {
        Ok(self.mem.get_consumer(consumer_id).await?.map(Consumer))
    }

    #[tracing::instrument(skip(self))]
    async fn add_consumer(
        &self,
        client_id: u64,
        consumer_id: u64,
        subscribe: Subscribe,
        client_tx: mpsc::UnboundedSender<writer::Request>,
    ) -> Result<Self::Consumer, Self::Error> {
        asyncify({
            let db = self.db.clone();
            let topic_name = subscribe.topic_name.clone();
            let subscription_name = subscribe.subscription_name.clone();
            let consumer_name = subscribe.consumer_name.clone();
            move || -> Result<(), Error> {
                let key = subscribes_key(topic_name, subscription_name.clone());
                let txn = db.begin_write().context(RedbTxnSnafu)?;
                {
                    let mut table = txn.open_table(TABLE).context(RedbTableSnafu)?;
                    let value = {
                        match table.get(key.as_str()).context(RedbStorageSnafu)? {
                            Some(subscription) => {
                                let mut subscription: RedbSubscriptionInfo =
                                    serde_json::from_slice(&subscription.value())
                                        .context(SerdeSnafu)?;
                                if !subscription.consumers.insert(consumer_name) {
                                    return Ok(());
                                }
                                serde_json::to_vec(&subscription).context(SerdeSnafu)?
                            }
                            None => {
                                let subscription = RedbSubscriptionInfo {
                                    subscription_name: subscription_name.clone(),
                                    subscription_type: subscribe.subscription_type,
                                    initial_position: subscribe.initial_position,
                                    consumers: {
                                        let mut consumers = HashSet::new();
                                        consumers.insert(consumer_name);
                                        consumers
                                    },
                                };
                                serde_json::to_vec(&subscription).context(SerdeSnafu)?
                            }
                        }
                    };
                    table
                        .insert(key.as_str(), value)
                        .context(RedbStorageSnafu)?;
                }
                txn.commit().context(RedbCommitSnafu)?;
                Ok(())
            }
        })
        .await?;
        Ok(Consumer(
            self.mem
                .add_consumer(client_id, consumer_id, subscribe, client_tx)
                .await?,
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn del_consumer(&self, consumer_id: u64) -> Result<Option<Self::Consumer>, Self::Error> {
        Ok(self.mem.del_consumer(consumer_id).await?.map(Consumer))
    }

    #[tracing::instrument(skip(self))]
    async fn clear_consumer(&self, client_id: u64) -> Result<(), Self::Error> {
        self.mem.clear_consumer(client_id).await?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get_producer(&self, producer_id: u64) -> Result<Option<Producer>, Self::Error> {
        Ok(self.mem.get_producer(producer_id).await?.map(Producer))
    }

    #[tracing::instrument(skip(self))]
    async fn add_producer(
        &self,
        client_id: u64,
        producer_id: u64,
        packet: CreateProducer,
    ) -> Result<Producer, Self::Error> {
        Ok(Producer(
            self.mem
                .add_producer(client_id, producer_id, packet)
                .await?,
        ))
    }

    #[tracing::instrument(skip(self))]
    async fn del_producer(&self, producer_id: u64) -> Result<Option<Producer>, Self::Error> {
        Ok(self.mem.del_producer(producer_id).await?.map(Producer))
    }

    #[tracing::instrument(skip(self))]
    async fn clear_producer(&self, client_id: u64) -> Result<(), Self::Error> {
        Ok(self.mem.clear_producer(client_id).await?)
    }

    #[tracing::instrument(skip(self))]
    async fn close(self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn unsubscribe(
        &self,
        topic_name: &str,
        subscription_name: &str,
        consumer_name: &str,
    ) -> Result<bool, Self::Error> {
        asyncify({
            let db = self.db.clone();
            let topic_name = topic_name.to_owned();
            let subscription_name = subscription_name.to_owned();
            let consumer_name = consumer_name.to_owned();
            move || -> Result<bool, Error> {
                let mut res = false;
                let key = subscribes_key(topic_name, subscription_name);
                let txn = db.begin_write().context(RedbTxnSnafu)?;
                {
                    let mut table = txn.open_table(TABLE).context(RedbTableSnafu)?;
                    let value = {
                        let Some(consumers) = table.get(key.as_str()).context(RedbStorageSnafu)?
                        else {
                            return Ok(res);
                        };

                        let mut subscription: RedbSubscriptionInfo =
                            serde_json::from_slice(&consumers.value()).context(SerdeSnafu)?;
                        if !subscription.consumers.remove(&consumer_name) {
                            return Ok(!subscription.consumers.is_empty());
                        }
                        res = !subscription.consumers.is_empty();
                        serde_json::to_vec(&subscription).context(SerdeSnafu)?
                    };
                    table
                        .insert(key.as_str(), value)
                        .context(RedbStorageSnafu)?;
                }
                txn.commit().context(RedbCommitSnafu)?;
                Ok(res)
            }
        })
        .await
    }
}

#[derive(Clone)]
pub struct Consumer(super::mem::Consumer);

impl MetaConsumer for Consumer {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    fn info(&self) -> super::ConsumerInfo {
        self.0.info()
    }

    #[tracing::instrument(skip(self))]
    async fn ready(&self) -> Result<(), Self::Error> {
        Ok(self.0.ready().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn is_ready(&self) -> Result<bool, Self::Error> {
        Ok(self.0.is_ready().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn permits(&self) -> Result<u32, Self::Error> {
        Ok(self.0.permits().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn permits_add(&self, delta: u32) -> Result<(), Self::Error> {
        Ok(self.0.permits_add(delta).await?)
    }

    #[tracing::instrument(skip(self))]
    async fn permits_sub(&self, delta: u32) -> Result<(), Self::Error> {
        Ok(self.0.permits_sub(delta).await?)
    }

    #[tracing::instrument(skip(self))]
    async fn close(&self) -> Result<(), Self::Error> {
        Ok(self.0.close().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn is_closed(&self) -> Result<bool, Self::Error> {
        Ok(self.0.is_closed().await?)
    }

    #[tracing::instrument(skip(self))]
    async fn send(&self, message: send::Send) -> Result<(), Self::Error> {
        Ok(self.0.send(message).await?)
    }
}

#[derive(Clone)]
pub struct Producer(super::mem::Producer);

impl MetaProducer for Producer {
    type Error = Error;

    #[tracing::instrument(skip(self))]
    fn info(&self) -> super::ProducerInfo {
        self.0.info()
    }
}

fn topic_key(topic_name: String) -> String {
    format!("TOPICS/{topic_name}")
}

fn subscribes_key(topic_name: String, subscription_name: String) -> String {
    format!("SUBSCRIBES/{topic_name}/{subscription_name}")
}

fn subscribes_key_topic_prefix(topic_name: String) -> String {
    format!("SUBSCRIBES/{topic_name}/")
}

pub(crate) async fn asyncify<F, T>(f: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(res) => res,
        Err(_) => CallbackPanicSnafu.fail(),
    }
}
