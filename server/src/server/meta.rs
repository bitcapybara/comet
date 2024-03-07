use futures::Future;

use crate::storage::Storage;

mod etcd;
mod mem;

pub trait Meta: Clone + Send + Sync + 'static {
    type Error: std::error::Error;
    type Storage: Storage;

    fn get_storage(
        &self,
        topic_name: &str,
    ) -> impl Future<Output = Result<Self::Storage, Self::Error>> + Send;
}
