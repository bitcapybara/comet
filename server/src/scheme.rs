#[cfg(feature = "distributed")]
pub mod distributed;
#[cfg(feature = "local-memory")]
pub mod local_memory;
#[cfg(feature = "local-persist")]
pub mod local_persist;

use comet_common::id::IdGenerator;
#[cfg(feature = "distributed")]
pub use distributed::Distributed;
#[cfg(all(
    feature = "local-memory",
    not(any(feature = "local-persist", feature = "distributed"))
))]
pub use local_memory::LocalMemory;
#[cfg(feature = "local-persist")]
pub use local_persist::LocalPersist;

use std::future::Future;

use crate::{
    meta,
    storage::{self, TopicStorage},
};

pub trait StorageScheme: Clone + Send + Sync + 'static {
    type MetaStorage: meta::MetaStorage;

    type TopicStorage: storage::TopicStorage;

    type IdGenerator: IdGenerator;

    fn create_id_generator(&self) -> Self::IdGenerator;

    fn create_meta_storage(
        &self,
    ) -> impl Future<
        Output = Result<Self::MetaStorage, <Self::MetaStorage as meta::MetaStorage>::Error>,
    > + Send;

    /// 在数据库中创建 topic 相关配置，如果已存在则忽略
    fn create_topic_storage(
        &self,
        topic_name: &str,
    ) -> impl Future<Output = Result<Self::TopicStorage, <Self::TopicStorage as TopicStorage>::Error>>
           + Send;
}
