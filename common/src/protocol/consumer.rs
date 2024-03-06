use crate::types::{InitialPostion, SubscriptionType};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Subscribe {
    /// consumer name
    pub consumer_name: String,
    /// subscribe topic
    pub topic_name: String,
    /// subscription id
    pub subscription_name: String,
    /// subscribe type
    pub subscription_type: SubscriptionType,
    /// consume init position
    pub initial_position: InitialPostion,
    /// default permits
    pub default_permits: u32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Unsubscribe {
    /// consumer id
    pub consumer_id: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct CloseConsumer {
    pub consumer_id: u64,
}
