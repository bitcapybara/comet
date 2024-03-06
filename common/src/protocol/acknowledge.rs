#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Acknowledge {
    pub consumer_id: u64,
    pub message_ids: u64,
}
