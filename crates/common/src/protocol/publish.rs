use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublishHeader {
    pub producer_id: u64,
    /// pub subject
    pub topic_name: String,
    /// Ensure that the message sent by the producer is unique within this topic
    pub sequence_id: u64,
    /// produce time
    pub publish_time: DateTime<Utc>,
    /// deliver time
    pub deliver_time: Option<DateTime<Utc>>,
    /// payload len, help to read bytes block of payload
    pub payload_len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub header: PublishHeader,
    pub payload: bytes::Bytes,
}
