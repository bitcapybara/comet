use std::{
    pin::pin,
    sync::{
        atomic::{self, AtomicU8},
        Arc,
    },
    time::Duration,
};

use comet_common::defer::defer;
use etcd_client as etcd;
use futures::future::Either;
use snafu::{ResultExt, Snafu};
use tokio::{
    select,
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

const ELECTION_KEY: &str = "/comet/distributed/leader";

#[derive(Debug, Snafu)]
pub enum Error {
    ElectionCampaign { source: etcd::Error },
    FollowerWatch { source: etcd::Error },
    LeaseQuitBeforeElection,
}

#[derive(Debug, PartialEq, Clone)]
#[repr(u8)]
pub enum Role {
    Leader,
    Follower(String),
}

pub async fn start_election(
    connect_addr: String,
    client: etcd::ElectionClient,
    mut lease_observer: watch::Receiver<i64>,
    election_notifier: watch::Sender<Role>,
    token: CancellationToken,
) -> Result<(), Error> {
    let _guard = defer(|| token.cancel());
    // get new lease_id
    if lease_observer.changed().await.is_err() {
        return LeaseQuitBeforeElectionSnafu.fail();
    }
    let lease_id = *lease_observer.borrow_and_update();
    let token = token.child_token();
    loop {
        let mut join_set = JoinSet::new();
        let (campaign_tx, mut campaign_rx) = mpsc::channel(1);
        join_set.spawn(campaign_loop(
            client.clone(),
            connect_addr.to_string(),
            lease_id,
            campaign_tx,
            lease_observer.clone(),
            token.clone(),
        ));
        let (rolecheck_tx, mut rolecheck_rx) = mpsc::channel(1);
        join_set.spawn(rolecheck_loop(
            client.clone(),
            connect_addr.to_string(),
            rolecheck_tx,
            lease_observer.clone(),
            token.clone(),
        ));
        select! {
            Some(role) = campaign_rx.recv() => {
                election_notifier.send(role).ok();
            }
            Some(role) = rolecheck_rx.recv() => {
                election_notifier.send(role).ok();
            }
            _ = token.cancelled() => {
                return Ok(())
            }
        }
        // wait for task finished, then start new election
        while join_set.join_next().await.is_some() {}
    }
}

async fn campaign_loop(
    mut client: etcd::ElectionClient,
    connect_addr: String,
    mut lease_id: i64,
    notifier: mpsc::Sender<Role>,
    mut lease_id_observer: watch::Receiver<i64>,
    token: CancellationToken,
) {
    loop {
        select! {
            Ok(_resp) = client.campaign(ELECTION_KEY, connect_addr.as_str(), lease_id) => {
                // won the leader
                notifier.send(Role::Leader).await.ok();
                // once lease changed, we need to re-campaign
                select! {
                    Ok(_) = lease_id_observer.changed() => {
                        lease_id = *lease_id_observer.borrow_and_update();
                        continue;
                    }
                    _ = token.cancelled() => {
                        return
                    }
                    else => return
                }
            }
            _ = token.cancelled() => {
                return
            }
            else => return
        }
    }
}

async fn rolecheck_loop(
    mut client: etcd::ElectionClient,
    connect_addr: String,
    notifier: mpsc::Sender<Role>,
    mut lease_id_observer: watch::Receiver<i64>,
    token: CancellationToken,
) {
    loop {
        // check election role loop
        loop {
            match client.leader(ELECTION_KEY).await {
                Ok(resp) => match resp.kv() {
                    Some(kv) => {
                        let leader_addr = unsafe { kv.value_str_unchecked() };
                        if leader_addr == connect_addr {
                            notifier.send(Role::Leader).await.ok();
                        } else {
                            notifier
                                .send(Role::Follower(leader_addr.to_string()))
                                .await
                                .ok();
                        }
                        break;
                    }
                    None => {
                        unreachable!()
                    }
                },
                Err(e) => match e {
                    etcd::Error::GRpcStatus(status)
                        if status.code() as i32 == 2
                            && status.message() == "election: no leader" =>
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    _ => return,
                },
            }
        }

        // wait for lease_id change
        // once lease changed, we need to re-campaign
        select! {
            Ok(_) = lease_id_observer.changed() => {
                continue;
            }
            _ = token.cancelled() => {
                return
            }
            else => return
        }
    }
}
