#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ControlFlow {
    pub consumer_id: u64,
    /// number of additional messages requested
    pub permits: u32,
}
