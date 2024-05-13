use std::time::Duration;

use chrono::{TimeDelta, Utc};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{broker::topic::AckedRetentionPolicyConfig, storage::SubscriptionStorage};

/// 按照保留策略删除 subscription 中已确认的消息
pub async fn start_acked_retention<S>(
    storage: S,
    config: AckedRetentionPolicyConfig,
    token: CancellationToken,
) where
    S: SubscriptionStorage,
{
    let (num_limit, time_limit) = match (config.num_limit, config.time_limit) {
        (None, None) => return,
        (Some(size_limit), Some(time_limit)) if size_limit == 0 && !time_limit.is_zero() => {
            error!(
                "invalid acked messages retention policy config: size_limit == 0 && time_limit != 0"
            );
            return;
        }
        (Some(size_limit), Some(time_limit)) if size_limit != 0 && time_limit.is_zero() => {
            error!(
                "invalid acked messages retention policy config: size_limit != 0 && time_limit == 0"
            );
            return;
        }
        (s, t) => (s, t),
    };

    // TODO: 随机选择一个时间段，避免对数据库造成压力。或者交给用户配置
    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if num_limit.is_some_and(|s| s == 0) && time_limit.is_some_and(|t| t.is_zero()) {
                    if let Err(e) = storage.drain_acked().await {
                        error!("drain acked subscription records error: {e:?}");
                    }
                } else {
                    let now = Utc::now();
                    let after = time_limit
                        .map(|d| TimeDelta::from_std(d).expect("duration out of range"))
                        .and_then(|t| now.checked_sub_signed(t));
                    if let Err(e) = storage.retain_acked(num_limit, after).await {
                        error!("retain acked subscription records error: {e:?}");
                    }
                }
            }
            _ = token.cancelled() => break,
        }
    }
}
