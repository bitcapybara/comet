use chrono::{TimeZone, Utc};
use comet_common::id::distribute::DistributeId;

use crate::{
    meta::{
        self,
        etcd::{self, EtcdMeta},
    },
    storage::{
        self,
        postgres::{self, PostgresTopicStorage},
    },
};

use super::StorageScheme;

#[derive(Clone)]
pub struct Config {
    pub node_id: u16,
    pub meta: etcd::Config,
    pub storage: postgres::Config,
}

#[derive(Clone)]
pub struct Distributed(pub Config);

impl StorageScheme for Distributed {
    type MetaStorage = meta::etcd::EtcdMeta;
    type TopicStorage = storage::postgres::PostgresTopicStorage;
    type IdGenerator = DistributeId;

    #[tracing::instrument(skip(self))]
    fn create_id_generator(&self) -> Self::IdGenerator {
        DistributeId::new(
            self.0.node_id,
            Utc.with_ymd_and_hms(2024, 2, 28, 20, 0, 0).unwrap(),
        )
    }

    #[tracing::instrument(skip(self))]
    async fn create_meta_storage(&self) -> Result<Self::MetaStorage, meta::etcd::Error> {
        EtcdMeta::new(self.0.meta.clone()).await
    }

    #[tracing::instrument(skip(self))]
    async fn create_topic_storage(
        &self,
        topic_name: &str,
    ) -> Result<Self::TopicStorage, storage::postgres::Error> {
        PostgresTopicStorage::new(topic_name, self.0.storage.clone()).await
    }
}
