use comet_common::{addr::ConnectAddress, id::local::LocalId};

use crate::{meta, storage};

use super::StorageScheme;

#[derive(Clone)]
pub struct Config {
    pub http_connect_addr: ConnectAddress,
    pub broker_connect_addr: ConnectAddress,
}

#[derive(Clone)]
pub struct LocalMemory(pub Config);

impl StorageScheme for LocalMemory {
    type MetaStorage = meta::mem::MemoryMeta;
    type TopicStorage = storage::mem::MemoryTopicStorage;
    type IdGenerator = LocalId;

    #[tracing::instrument(skip(self))]
    fn create_id_generator(&self) -> Self::IdGenerator {
        LocalId::new()
    }

    #[tracing::instrument(skip(self))]
    async fn create_meta_storage(&self) -> Result<Self::MetaStorage, meta::mem::Error> {
        Ok(meta::mem::MemoryMeta::new(meta::mem::Config {
            broker_addr: self.0.broker_connect_addr.clone(),
            http_addr: self.0.http_connect_addr.clone(),
        }))
    }

    #[tracing::instrument(skip(self))]
    async fn create_topic_storage(
        &self,
        _topic_name: &str,
    ) -> Result<Self::TopicStorage, storage::mem::Error> {
        Ok(storage::mem::MemoryTopicStorage::new())
    }
}
