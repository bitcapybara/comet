use crate::types::AccessMode;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateProducer {
    pub producer_name: String,
    pub topic_name: String,
    pub access_mode: AccessMode,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ProducerReceipt {
    pub producer_id: u64,
    pub last_sequence_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CloseProducer {
    pub producer_id: u64,
}
