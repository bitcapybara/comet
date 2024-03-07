use std::{io, net::SocketAddr};

use axum::{routing::get, Router};
use comet_common::defer::defer;
use snafu::{ResultExt, Snafu};
use tokio_util::sync::CancellationToken;

use crate::{broker::Broker, storage::Storage};

use super::meta::Meta;

#[derive(Debug, Snafu)]
pub enum Error {
    HttpListenerBind { source: io::Error },
}

pub struct HttpConfig {
    connect_addr: String,
    listen_addr: SocketAddr,
}

pub async fn start_http_server<M, S>(
    config: HttpConfig,
    broker: Broker<M, S>,
    token: CancellationToken,
) -> Result<(), Error>
where
    M: Meta,
    S: Storage,
{
    let _gurad = defer(|| token.cancel());
    let app = Router::new()
        .route("/comet/api/peer/v1/topic", get(|| async { "hello, world" }))
        .with_state(broker);
    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .context(HttpListenerBindSnafu)?;
    axum::serve(listener, app)
        .with_graceful_shutdown({
            let token = token.clone();
            token.cancelled_owned()
        })
        .await
        .unwrap();
    Ok(())
}
