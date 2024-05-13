use std::time::Duration;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    /// 分区数
    pub partitions: u16,
    /// 已确认消息的保留策略
    pub acked_retention: AckedRetentionPolicyConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            partitions: 1,
            acked_retention: Default::default(),
        }
    }
}

/// | Time limit | Num limit  | Message retention                                                                                                              |
/// | ---------- | ---------- | ------------------------------------------------------------------------------------------------------------------------------ |
/// | None       | None       | Infinite retention                                                                                                             |
/// | None       | >0         | Based on the num limit                                                                                                         |
/// | >0         | None       | Based on the time limit                                                                                                        |
/// | 0          | 0          | Disable message retention, which means messages are not reserved (**by default**)                                              |
/// | 0          | >0         | Invalid                                                                                                                        |
/// | >0         | 0          | Invalid                                                                                                                        |
/// | >0         | >0         | Acknowledged messages or messages with no active subscription will not be retained when either time or size reaches the limit. |
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AckedRetentionPolicyConfig {
    /// 消息保存的总字节数
    pub num_limit: Option<usize>,
    /// 消息保存的最大时长
    pub time_limit: Option<Duration>,
}

/// 默认情况下不保存任何已确认数据
impl Default for AckedRetentionPolicyConfig {
    fn default() -> Self {
        Self {
            num_limit: Some(0),
            time_limit: Some(Duration::ZERO),
        }
    }
}
