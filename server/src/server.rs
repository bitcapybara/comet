use futures::TryFutureExt;
use snafu::{IntoError, ResultExt, Snafu};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::broker::{self, Broker};

use self::{
    http::{start_http_server, HttpConfig},
    quic::{start_quic_server, QuicConfig},
};

mod http;
mod meta;
mod quic;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("init broker error"))]
    Broker {
        source: broker::Error,
    },
    QuicServer {
        source: quic::Error,
    },
    HttpServer {
        source: http::Error,
    },
}

pub struct ServerConfig {
    quic: QuicConfig,
    http: HttpConfig,
}

pub async fn start_server(config: ServerConfig, token: CancellationToken) -> Result<(), Error> {
    let broker = Broker::new().context(BrokerSnafu)?;

    let child_token = token.child_token();
    let mut join_set = JoinSet::new();
    join_set.spawn(
        start_quic_server(config.quic, broker.clone(), child_token.clone())
            .map_err(|e| QuicServerSnafu.into_error(e)),
    );
    join_set.spawn(
        start_http_server(config.http, broker.clone(), child_token.clone())
            .map_err(|e| HttpServerSnafu.into_error(e)),
    );
    while join_set.join_next().await.is_some() {}
    Ok(())
}
