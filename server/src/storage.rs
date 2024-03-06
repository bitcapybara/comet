use chrono::{DateTime, Utc};
use futures::Future;

pub struct TopicMessage {
    topic_name: String,
    producer_name: String,
    sequence_id: u64,
    publish_time: DateTime<Utc>,
    payload: bytes::Bytes,
}

pub trait Storage {
    type Error;

    async fn create_topic(&self, topic_name: &str) -> Result<(), Self::Error>;

    async fn add_message(&self, message: &TopicMessage) -> Result<u64, Self::Error>;

    /// `from`: The next message id that the consumer is waiting to read
    /// `limit`: The maximum number of messages a consumer can accept
    async fn fetch_messages(
        &self,
        topic_name: &str,
        subscription_name: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<TopicMessage>, Self::Error>;
}
