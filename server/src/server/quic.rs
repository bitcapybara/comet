use std::{io, net::SocketAddr};

use futures::stream::{FuturesUnordered, StreamExt};
use s2n_quic::provider::tls::default as tls;
use snafu::{Location, ResultExt, Snafu};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use comet_common::{
    addr::ConnectAddress, codec::Codec, id::IdGenerator, mtls, utils::defer::defer,
};

use crate::{broker::Broker, client::start_client, scheme::StorageScheme};

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("init tls server error: {message}"))]
    InitTlsServer {
        message: String,
        #[snafu(source)]
        error: tls::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("set listen addr error"))]
    SetListenAddr {
        #[snafu(source)]
        error: io::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("start quic server error"))]
    StartQuicServer {
        #[snafu(source)]
        error: s2n_quic::provider::StartError,
        #[snafu(implicit)]
        location: Location,
    },
}

pub struct QuicConfig {
    pub certs: mtls::CertsFile,
    /// localhost:5888
    pub connect_addr: ConnectAddress,
    /// 0.0.0.0:5888
    pub listen_addr: SocketAddr,
}

pub async fn start_quic_server<S, C>(
    config: QuicConfig,
    id_generator: S::IdGenerator,
    broker: Broker<S>,
    token: CancellationToken,
) -> Result<(), Error>
where
    S: StorageScheme,
    C: Codec,
{
    let _guard = defer(|| token.cancel());
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

    accept_loop::<S, C>(server, broker, id_generator, token.clone()).await;

    Ok(())
}

async fn accept_loop<S, C>(
    mut server: s2n_quic::Server,
    broker: Broker<S>,
    id_generator: S::IdGenerator,
    token: CancellationToken,
) where
    S: StorageScheme,
    C: Codec,
{
    let _guard = defer(|| token.cancel());
    let mut futs = FuturesUnordered::new();

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => break,
            Some(_) = futs.next(), if !futs.is_empty() => {}
            Some(conn) = server.next() => {
                // get client id
                let client_id = id_generator.next_id();

                let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
                // token for this conn
                let child_token = token.child_token();

                // start tasks
                futs.push(
                    futures::future::join(
                        tokio::spawn(broker.clone().start_handler(client_id, inbound_rx, child_token.clone())),
                        tokio::spawn(start_client::<C>(conn, inbound_tx, child_token.clone())),
                    ),
                );
            }
            else => {
                token.cancel();
                break;
            }
        }
    }

    // waiting for all connections end
    while futs.next().await.is_some() {}
}
