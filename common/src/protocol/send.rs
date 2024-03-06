use chrono::{DateTime, Utc};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct SendHeader {
    pub message_id: u64,
    pub consumer_id: u64,
    pub payload_len: u64,
    pub publish_time: DateTime<Utc>,
    pub send_time: DateTime<Utc>,
}

pub struct Send {
    pub header: SendHeader,
    pub payload: bytes::Bytes,
}
