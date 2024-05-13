use std::time::Duration;

use comet_common::utils::defer::defer;
use etcd::LeaseGrantOptions;
use etcd_client as etcd;
use snafu::{Location, Snafu};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::error;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("etcd send keep alive error"))]
    SendKeepalive {
        #[snafu(source)]
        error: etcd::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("keep alive shutdown"))]
    KeepaliveServerEnded {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("fetch keep alive response error"))]
    FetchKeepaliveResponse {
        #[snafu(source)]
        error: etcd::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub async fn start_lease(mut client: etcd::LeaseClient, lease_id: i64, token: CancellationToken) {
    let _guard = defer(|| token.cancel());

    loop {
        match client.keep_alive(lease_id).await {
            Ok((mut keeper, mut stream)) => loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => match keeper.keep_alive().await {
                        Ok(_) => tokio::select! {
                            resp = stream.message() => match resp {
                                Ok(Some(_)) => continue,
                                Ok(None) => return,
                                Err(e) => {
                                    error!("fetch lease keep alive response error: {e:?}");
                                }
                            },
                            _ = token.cancelled() => return
                        },
                        Err(e) => {
                            error!("send keep alive error: {e:?}");
                            break;
                        }
                    },
                    _ = token.cancelled() => return
                }
            },
            Err(_) => loop {
                match timeout(Duration::from_millis(500), token.cancelled()).await {
                    Ok(_) => return,
                    Err(_) => match client
                        .grant(5, Some(LeaseGrantOptions::new().with_id(lease_id)))
                        .await
                    {
                        Ok(_) => break,
                        Err(e) => {
                            error!(lease_id, "grant lease error: {e:?}")
                        }
                    },
                }
            },
        };
    }
}
