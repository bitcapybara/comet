#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ControlFlow {
    pub consumer_id: u64,
    /// number of additional messages requested
    pub permits: u32,
}
