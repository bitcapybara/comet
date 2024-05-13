use std::{io, net::SocketAddr};

use axum::{
    routing::{delete, get, post},
    Router,
};
use comet_common::{addr::ConnectAddress, utils::defer::defer};
use snafu::{Location, ResultExt, Snafu};
use tokio_util::sync::CancellationToken;

use crate::{broker::Broker, scheme::StorageScheme};

use self::router::{create_topic, delete_topic, lookup_http, lookup_topic};

mod router;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("http listener bind port error"))]
    HttpListenerBind {
        #[snafu(source)]
        error: io::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub struct HttpConfig {
    /// http(s)://localhost:6888
    pub connect_addr: ConnectAddress,
    /// 0.0.0.0:6888
    pub listen_addr: SocketAddr,
}

#[derive(Clone)]
struct ServerState<S>
where
    S: StorageScheme,
{
    meta: S::MetaStorage,
    broker: Broker<S>,
}

pub async fn start_http_server<S>(
    config: HttpConfig,
    broker: Broker<S>,
    meta: S::MetaStorage,
    token: CancellationToken,
) -> Result<(), Error>
where
    S: StorageScheme,
{
    let _guard = defer(|| token.cancel());

    let app = Router::new()
        .route(
            "/comet/api/public/topic/:topic_name/broker_addresses",
            get(lookup_topic),
        )
        .route("/comet/api/public/topic/:topic_name", post(create_topic))
        .route("/comet/api/public/addresses", get(lookup_http))
        .route("/comet/api/public/topic/:topic_name", delete(delete_topic))
        .with_state(ServerState { meta, broker });
    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .context(HttpListenerBindSnafu)?;

    // TODO: 等待 https://github.com/tokio-rs/axum/pull/2479 合并后集成 s2n-tls
    axum::serve(listener, app)
        .with_graceful_shutdown({
            let token = token.clone();
            async move { token.cancelled().await }
        })
        .await
        .unwrap();
    Ok(())
}
