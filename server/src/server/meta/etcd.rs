mod election;
mod lease;

use etcd_client as etcd;
use futures::TryFutureExt;
use snafu::{IntoError, ResultExt, Snafu};
use tokio::{sync::watch, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::error;

use self::{
    election::{start_election, Role},
    lease::start_lease,
};
use crate::broker::Broker;
use comet_common::defer::defer;

#[derive(Debug, Snafu)]
pub enum Error {
    EtcdConnect { source: etcd::Error },
    ElectionQuitBeforeDistributed,
    Lease { source: lease::Error },
    Election { source: election::Error },
}

#[derive(Debug)]
pub struct PeerEvent {}

#[derive(Debug)]
pub struct ClientEvent {}

#[derive(Debug)]
pub struct Register {}

#[derive(Debug)]
pub struct SysInfoReport {}

pub struct MetaConfig {
    etcd: Vec<String>,
    connect_addr: String,
}

/// The leader's responsibility is to assign topics to brokers and postgres
pub async fn start_meta_server<M, S>(
    broker: Broker<M, S>,
    config: MetaConfig,
    token: CancellationToken,
) -> Result<(), Error> {
    let _guard = defer(|| token.cancel());
    let mut join_set = JoinSet::new();

    let client = etcd::Client::connect(config.etcd, None)
        .await
        .context(EtcdConnectSnafu)?;
    let (lease_notifier, lease_observer) = watch::channel(0);

    let token = token.child_token();
    join_set.spawn(
        start_lease(client.lease_client(), lease_notifier, token.clone())
            .map_err(|e| LeaseSnafu.into_error(e)),
    );
    let (election_notifier, mut election_observer) = watch::channel(Role::Leader);
    join_set.spawn(
        start_election(
            config.connect_addr,
            client.election_client(),
            lease_observer,
            election_notifier,
            token.clone(),
        )
        .map_err(|e| ElectionSnafu.into_error(e)),
    );
    join_set.spawn(start(election_observer));

    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Err(e)) => {
                error!("meta server task quit: {e}")
            }
            Err(e) => {
                error!("meta server task panic! {e}")
            }
            _ => {}
        }
    }
    Ok(())
}

async fn start(mut election_observer: watch::Receiver<Role>) -> Result<(), Error> {
    // get current role
    if election_observer.changed().await.is_err() {
        return ElectionQuitBeforeDistributedSnafu.fail();
    }
    let role = election_observer.borrow_and_update().clone();

    loop {
        match role.clone() {
            Role::Leader => {
                // 接收来自 follower 上报的消息，监控各个节点的状态
                // 接收来自客户端的消息，如 LoopupTopic 消息
                // 定期收集所有 pg 节点的 metrics，用于分配 Topic 使用
            }
            Role::Follower(leader_addr) => {
                // 上报自己节点的所有 Topic 信息
                // 定时上报节点系统状态
            }
        }
    }
}
