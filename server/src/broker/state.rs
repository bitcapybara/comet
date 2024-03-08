use std::{
    collections::{hash_map::Drain, HashMap},
    sync::Arc,
};

use tokio::sync::{mpsc, Mutex, RwLock};

use super::{topic::Topic, BrokerMessage};

#[derive(Clone)]
pub struct ConsumerInfo {
    pub topic_name: String,
    pub subscription_name: String,
    pub consumer_name: String,
}

#[derive(Clone)]
pub struct ProducerInfo {
    pub topic_name: String,
    pub producer_name: String,
}

/// One session per client
/// Save a client's connection information
#[derive(Clone)]
pub struct Session {
    /// send to client
    pub(crate) client_tx: mpsc::UnboundedSender<BrokerMessage>,
    /// consumer_id -> consumer_info
    consumers: HashMap<u64, ConsumerInfo>,
    // consumer_ids: HashMap<String, u64>,
    /// producer_id -> producer_info
    producers: HashMap<u64, ProducerInfo>,
    // producer_ids: HashMap<String, u64>,
}

impl Session {
    pub fn new(client_tx: mpsc::UnboundedSender<BrokerMessage>) -> Self {
        Self {
            client_tx,
            consumers: HashMap::new(),
            // consumer_ids: HashMap::new(),
            producers: HashMap::new(),
            // producer_ids: HashMap::new(),
        }
    }

    pub fn has_consumer(&self, consumer_name: &str) -> bool {
        self.consumers
            .values()
            .any(|info| info.consumer_name == consumer_name)
    }

    pub fn has_producer(&self, producer_name: &str) -> bool {
        self.producers
            .values()
            .any(|info| info.producer_name == producer_name)
    }

    pub fn get_consumer(&self, consumer_id: u64) -> Option<ConsumerInfo> {
        self.consumers.get(&consumer_id).cloned()
    }

    pub fn add_consumer(
        &mut self,
        consumer_id: u64,
        consumer_name: &str,
        topic_name: &str,
        subscription_name: &str,
    ) {
        self.consumers.insert(
            consumer_id,
            ConsumerInfo {
                subscription_name: subscription_name.to_string(),
                topic_name: topic_name.to_string(),
                consumer_name: consumer_name.to_string(),
            },
        );
    }

    pub fn del_consumer(&mut self, consumer_id: u64) -> Option<ConsumerInfo> {
        self.consumers.remove(&consumer_id)
    }

    pub fn drain_consumer(&mut self) -> Drain<'_, u64, ConsumerInfo> {
        self.consumers.drain()
    }

    pub fn drain_producer(&mut self) -> Drain<'_, u64, ProducerInfo> {
        self.producers.drain()
    }

    pub fn get_producer(&self, producer_id: u64) -> Option<ProducerInfo> {
        self.producers.get(&producer_id).cloned()
    }

    pub fn add_producer(&mut self, producer_id: u64, producer_name: &str, topic_name: &str) {
        self.producers.insert(
            producer_id,
            ProducerInfo {
                topic_name: topic_name.to_string(),
                producer_name: producer_name.to_string(),
            },
        );
    }

    pub fn del_producer(&mut self, producer_id: u64) -> Option<ProducerInfo> {
        self.producers.remove(&producer_id)
    }
}

/// parallel read, serial write
pub struct BrokerState<S> {
    /// key = client_id
    pub(crate) sessions: HashMap<u64, Session>,
    /// key = topic
    pub(crate) topics: HashMap<String, Topic<S>>,
}

// impl BrokerState {
//     pub async fn get_client(&self, id: u64) -> Option<Session> {
//         let sessions = self.sessions.read().await;
//         sessions.get(&id).cloned()
//     }

//     pub async fn get_topic(&self, name: &str) -> Option<Topic> {
//         let topics = self.topics.read().await;
//         topics.get(name).cloned()
//     }

//     pub async fn add_client(&self, id: u64, session: Session) {
//         let mut sessions = self.sessions.write().await;
//         sessions.insert(id, session);
//     }

//     pub async fn del_client(&self, id: u64) {
//         let mut sessions = self.sessions.write().await;
//         sessions.remove(&id);
//     }

//     pub async fn add_topic(&self, name: &str, topic: Topic) {
//         let mut topics = self.topics.write().await;
//         topics.insert(name.to_string(), topic);
//     }

//     pub async fn del_topic(&self, name: &str) {
//         let mut topics = self.topics.write().await;
//         topics.remove(name);
//     }
// }
