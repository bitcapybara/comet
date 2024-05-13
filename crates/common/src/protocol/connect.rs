#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Connect {
    /// keepalive(ms)
    pub keepalive: u16,
}
