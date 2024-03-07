use std::{io, net::SocketAddr};

use futures::stream::StreamExt;
use s2n_quic::provider::tls::default as tls;
use snafu::{ResultExt, Snafu};
use tokio::{select, sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use comet_common::{defer::defer, mtls};

use crate::{
    broker::{start_broker, Broker},
    client::start_client,
    storage::Storage,
};

use super::meta::Meta;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("init tls server error: {message}"))]
    InitTlsServer {
        message: String,
        source: tls::error::Error,
    },
    #[snafu(display("set listen addr error"))]
    SetListenAddr { source: io::Error },
    #[snafu(display("start quic server error"))]
    StartQuicServer {
        source: s2n_quic::provider::StartError,
    },
}

pub struct QuicConfig {
    pub certs: mtls::CertsFile,
    /// comet://localhost:5888
    pub connect_addr: String,
    pub listen_addr: SocketAddr,
}

pub async fn start_quic_server<M, S>(
    config: QuicConfig,
    broker: Broker<M, S>,
    token: CancellationToken,
) -> Result<(), Error>
where
    M: Meta<Storage = S>,
    S: Storage,
{
    let _gurad = defer(|| token.cancel());
    let tls = tls::Server::builder()
        .with_trusted_certificate(config.certs.ca_cert_file.as_path())
        .context(InitTlsServerSnafu {
            message: "add ca cert file error",
        })?
        .with_certificate(
            config.certs.cert_file.as_path(),
            config.certs.key_file.as_path(),
        )
        .context(InitTlsServerSnafu {
            message: "add server cert file error",
        })?
        .with_client_authentication()
        .context(InitTlsServerSnafu {
            message: "set client auth error",
        })?
        .build()
        .context(InitTlsServerSnafu {
            message: "build quic server error",
        })?;
    let server = s2n_quic::Server::builder()
        .with_tls(tls)
        .unwrap()
        .with_io(config.listen_addr)
        .context(SetListenAddrSnafu)?
        .start()
        .context(StartQuicServerSnafu)?;

    accept_loop(server, broker, token.clone()).await;

    Ok(())
}

async fn accept_loop<M, S>(
    mut server: s2n_quic::Server,
    broker: Broker<M, S>,
    token: CancellationToken,
) where
    M: Meta<Storage = S>,
    S: Storage,
{
    let mut futs = JoinSet::new();
    let mut latest_client_id: u64 = 0;

    loop {
        select! {
            Some(conn) = server.next() => {
                // get client id
                let id = latest_client_id;
                latest_client_id = latest_client_id.wrapping_add(1);

                // get broker
                let broker = broker.clone();
                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                // token for this conn
                let child_token = token.child_token();

                // start tasks
                futs.spawn(
                    futures::future::join(
                        tokio::spawn(start_broker(broker, inbound_rx, child_token.clone())),
                        tokio::spawn(start_client(id, conn, inbound_tx, child_token.clone())),
                    ),
                );
            }
            _ = token.cancelled() => {
                while futs.join_next().await.is_some() {}
                break
            }
            else => break
        }
    }

    // waiting for all connections end
    token.cancel();
    while futs.join_next().await.is_some() {}
}
