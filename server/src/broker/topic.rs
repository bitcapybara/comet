use comet_common::{
    protocol::{consumer::Subscribe, producer::CreateProducer, response::ReturnCode, Packet},
    types::{AccessMode, SubscriptionType},
};
use snafu::{ensure, Snafu};
use tokio::sync::{mpsc, oneshot};

use crate::storage::Storage;

use super::BrokerMessage;

#[derive(Debug, Snafu)]
pub enum Error {
    PacketResponse { code: ReturnCode },
}

pub struct Topic<S> {
    storage: S,
}

impl<S> Clone for Topic<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
        }
    }
}

impl<S> Topic<S>
where
    S: Storage,
{
    pub fn new(
        topic_name: &str,
        storage: S,
        client_tx: mpsc::UnboundedSender<BrokerMessage>,
    ) -> Self {
        todo!()
    }

    pub async fn del_producer(&self, producer_id: u64) {}

    pub async fn del_consumer(&self, subscription_name: &str, consumer_id: u64) {}

    pub async fn add_producer(
        &self,
        producer_id: u64,
        producer: CreateProducer,
    ) -> Result<(), Error> {
        ensure!(
            false,
            PacketResponseSnafu {
                code: ReturnCode::ProducerNameAlreadyExists
            }
        );
        Ok(())
    }

    pub async fn has_producer(&self, producer_id: u64) -> bool {
        false
    }

    pub async fn add_consumer(&self, consumer_id: u64, subscribe: Subscribe) -> Result<(), Error> {
        Ok(())
    }

    pub async fn has_consumer(&self, consumer_id: u64) -> bool {
        false
    }
}
