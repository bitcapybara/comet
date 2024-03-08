use std::net::SocketAddr;

use futures::Future;

use crate::storage::Storage;

mod etcd;
mod mem;

pub trait Meta: Clone + Send + Sync + 'static {
    type Error: std::error::Error;
    type Storage: Storage;

    fn get_storage(&self) -> impl Future<Output = Result<Self::Storage, Self::Error>> + Send;

    fn get_topic_nodes(
        &self,
        topic_name: &str,
    ) -> impl Future<Output = Result<Vec<SocketAddr>, Self::Error>>;
}
