use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SendHeader {
    pub message_id: u64,
    pub topic_name: String,
    pub producer_name: String,
    pub consumer_id: u64,
    pub payload_len: u64,
    pub publish_time: DateTime<Utc>,
    pub deliver_time: Option<DateTime<Utc>>,
    pub handle_time: DateTime<Utc>,
    pub send_time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Send {
    pub header: SendHeader,
    pub payload: bytes::Bytes,
}
