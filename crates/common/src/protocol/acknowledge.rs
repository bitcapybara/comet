#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Acknowledge {
    pub consumer_id: u64,
    pub message_ids: Vec<u64>,
}
