use comet_common::id::local::LocalId;

use crate::{
    meta::{self, redb::RedbMeta},
    storage::{self, sqlite::SqliteTopicStorage},
};

use super::StorageScheme;

#[derive(Clone)]
pub struct Config {
    pub(crate) meta: meta::redb::Config,
    pub(crate) storage: storage::sqlite::Config,
}

#[derive(Clone)]
pub struct LocalPersist(pub Config);

impl StorageScheme for LocalPersist {
    type MetaStorage = meta::redb::RedbMeta;
    type TopicStorage = storage::sqlite::SqliteTopicStorage;
    type IdGenerator = LocalId;

    #[tracing::instrument(skip(self))]
    fn create_id_generator(&self) -> Self::IdGenerator {
        LocalId::new()
    }

    #[tracing::instrument(skip(self))]
    async fn create_meta_storage(&self) -> Result<Self::MetaStorage, meta::redb::Error> {
        RedbMeta::new(self.0.meta.clone()).await
    }

    #[tracing::instrument(skip(self))]
    async fn create_topic_storage(
        &self,
        topic_name: &str,
    ) -> Result<Self::TopicStorage, storage::sqlite::Error> {
        SqliteTopicStorage::new(topic_name, self.0.storage.clone()).await
    }
}
