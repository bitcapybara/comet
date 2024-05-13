use comet_common::{codec::Codec, error::BoxedError};
use futures::{Future, TryFutureExt};
use snafu::{IntoError, Location, Snafu};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{broker::Broker, meta::MetaStorage, scheme::StorageScheme};

pub use self::{
    http::{start_http_server, HttpConfig},
    quic::{start_quic_server, QuicConfig},
};

mod http;
mod quic;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("start quic server error"))]
    QuicServer {
        source: quic::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("start http server error"))]
    HttpServer {
        source: http::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("meta server error"))]
    MetaServer {
        source: comet_common::error::BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("meta server error"))]
    GetMeta {
        source: comet_common::error::BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
}

#[cfg(all(
    feature = "local-memory",
    not(any(feature = "local-persist", feature = "distributed"))
))]
pub type Config = ServerConfig;

#[cfg(all(
    feature = "local-memory",
    not(any(feature = "local-persist", feature = "distributed"))
))]
pub async fn start_local_memory_server<F, C>(config: Config, shutdown: F) -> Result<(), Error>
where
    F: Future<Output = ()>,
    C: Codec,
{
    use crate::scheme;

    let scheme = scheme::LocalMemory(scheme::local_memory::Config {
        http_connect_addr: config.http.connect_addr.clone(),
        broker_connect_addr: config.quic.connect_addr.clone(),
    });
    start_server::<_, _, C>(config, scheme, shutdown).await
}

#[cfg(feature = "local-persist")]
pub struct Config {
    pub server: ServerConfig,
    pub meta_dir: std::path::PathBuf,
    pub storage_dir: std::path::PathBuf,
    pub storage_mem_message_num_limit: Option<usize>,
}

#[cfg(feature = "local-persist")]
pub async fn start_local_persist_server<F, C>(config: Config, shutdown: F) -> Result<(), Error>
where
    F: Future<Output = ()>,
    C: Codec,
{
    use crate::{meta, scheme, storage};

    let mem = meta::mem::Config {
        broker_addr: config.server.quic.connect_addr.clone(),
        http_addr: config.server.http.connect_addr.clone(),
    };

    start_server::<_, _, C>(
        config.server,
        scheme::LocalPersist(scheme::local_persist::Config {
            meta: meta::redb::Config {
                data_dir: config.meta_dir,
                mem,
            },
            storage: storage::sqlite::Config {
                data_dir: config.storage_dir,
                mem_messages_num_limit: config.storage_mem_message_num_limit,
            },
        }),
        shutdown,
    )
    .await
}

#[cfg(feature = "distributed")]
pub struct Config {
    pub node_id: u16,
    pub server: ServerConfig,
    pub meta: crate::meta::etcd::Config,
    pub storage: crate::storage::postgres::Config,
}

#[cfg(feature = "distributed")]
pub async fn start_distributed_server<F, C>(config: Config, shutdown: F) -> Result<(), Error>
where
    F: Future<Output = ()>,
    C: Codec,
{
    use crate::scheme;

    let node_id = config.node_id;
    start_server::<_, _, C>(
        config.server,
        scheme::Distributed(scheme::distributed::Config {
            node_id,
            meta: config.meta,
            storage: config.storage,
        }),
        shutdown,
    )
    .await
}

pub struct ServerConfig {
    pub quic: QuicConfig,
    pub http: HttpConfig,
}

pub async fn start_server<S, F, C>(
    config: ServerConfig,
    scheme: S,
    shutdown: F,
) -> Result<(), Error>
where
    S: StorageScheme,
    F: Future<Output = ()>,
    C: Codec,
{
    let token = CancellationToken::new();

    let meta = scheme
        .create_meta_storage()
        .await
        .map_err(|e| GetMetaSnafu.into_error(BoxedError::new(e)))?;

    let broker = Broker::<S>::new(scheme.clone(), meta.clone(), token.clone());

    let mut join_set: JoinSet<Result<(), Error>> = JoinSet::new();

    // quic server
    join_set.spawn(
        start_quic_server::<S, C>(
            config.quic,
            scheme.create_id_generator(),
            broker.clone(),
            token.clone(),
        )
        .map_err(|e| QuicServerSnafu.into_error(e)),
    );
    // http server
    join_set.spawn(
        start_http_server(config.http, broker.clone(), meta.clone(), token.clone())
            .map_err(|e| HttpServerSnafu.into_error(e)),
    );

    tokio::select! {
        _ = shutdown => {
            token.cancel();
        }
        _ = token.cancelled() => {}
    };
    meta.close().await.ok();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                error!("server task quit with error: {e:?}")
            }
            Err(e) => {
                error!("server task failed to execute to completion: {e:?}")
            }
        }
    }

    Ok(())
}
