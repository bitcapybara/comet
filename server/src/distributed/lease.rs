use std::{
    future::Future,
    sync::{
        atomic::{self, AtomicI64},
        Arc,
    },
    time::Duration,
};

use etcd_client as etcd;
use futures::StreamExt;
use snafu::{ResultExt, Snafu};
use tokio::{select, sync::watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

#[derive(Debug, Snafu)]
pub enum Error {
    GrantLease { source: etcd::Error },
    SendKeepalive { source: etcd::Error },
    KeepaliveServerEnded,
    FetchKeepaliveResponse { source: etcd::Error },
}

pub struct Lease {
    id: Arc<AtomicI64>,
}

impl Lease {
    fn new(id: i64) -> Self {
        Self {
            id: Arc::new(AtomicI64::new(id)),
        }
    }

    pub fn id(&self) -> i64 {
        self.id.load(atomic::Ordering::Relaxed)
    }

    pub fn set_id(&self, id: i64) {
        self.id.store(id, atomic::Ordering::Relaxed);
    }
}

pub async fn start_lease(
    mut client: etcd::LeaseClient,
    notifier: watch::Sender<i64>,
    token: CancellationToken,
) -> Result<(), Error> {
    let lease_id = client.grant(5, None).await.context(GrantLeaseSnafu)?.id();
    notifier.send(lease_id).ok();
    let lease = Lease::new(lease_id);
    let (mut keeper, mut stream) = client
        .keep_alive(lease.id())
        .await
        .context(SendKeepaliveSnafu)?;

    loop {
        // retry
        if lease.id() == 0 {
            match client.grant(5, None).await {
                Ok(resp) => {
                    debug!(lease_id = resp.id(), "retry grant lease success");
                    lease.set_id(resp.id());
                    // notifier all watchers new lease id
                    notifier.send(resp.id()).ok();
                    match client.keep_alive(resp.id()).await {
                        Ok((new_keeper, new_stream)) => (keeper, stream) = (new_keeper, new_stream),
                        Err(e) => {
                            lease.set_id(0);
                            error!("retry keep alive error: {e}");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    lease.set_id(0);
                    error!("retry grant lease error: {e}");
                    continue;
                }
            }
        }

        // keepalive ping-pong
        match tokio::time::timeout(Duration::from_secs(2), token.cancelled()).await {
            Ok(_) => return Ok(()),
            Err(_) => match keeper.keep_alive().await {
                Ok(_) => match stream.message().await {
                    Ok(Some(_)) => continue,
                    Ok(None) => {
                        return KeepaliveServerEndedSnafu.fail();
                    }
                    Err(e) => {
                        lease.set_id(0);
                        error!("fetch lease keep alive response error: {e}");
                    }
                },
                Err(e) => {
                    lease.set_id(0);
                    error!("send keep alive error: {e}");
                }
            },
        }

        // wait for retry
        if tokio::time::timeout(Duration::from_secs(3), token.cancelled())
            .await
            .is_ok()
        {
            return Ok(());
        }
    }
}
