use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use comet_common::{
    addr::ConnectAddress,
    codec::{bincode::Bincode, Codec},
    http::{api, topic},
    mtls::CertsFile,
};
use snafu::ResultExt;
use tokio::{sync::Mutex, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    connection::manager::ConnectionManager,
    dns::{DnsResolver, LocalDnsResolver},
    HttpRequestSnafu,
};

use self::{consumer::ConsumerBuilder, producer::ProducerBuilder};

pub mod consumer;
pub mod producer;

pub struct Config {
    pub server_http_addrs: Vec<ConnectAddress>,
    pub certs: CertsFile,
    pub keepalive: u16,
}

impl Config {
    pub fn builder(server_http_addrs: Vec<ConnectAddress>, certs: CertsFile) -> ConfigBuilder {
        ConfigBuilder {
            server_http_addrs,
            certs,
            keepalive: 5,
        }
    }
}

pub struct ConfigBuilder {
    server_http_addrs: Vec<ConnectAddress>,
    certs: CertsFile,
    /// unit: seconds
    keepalive: u16,
}

impl ConfigBuilder {
    pub fn keepalive(mut self, keepalive: u16) -> Self {
        self.keepalive = keepalive;
        self
    }

    pub async fn build(self) -> Result<Config, crate::Error> {
        let server_http_addrs = api::get_server_http_addresses(&self.server_http_addrs)
            .await
            .context(HttpRequestSnafu)?;
        Ok(Config {
            server_http_addrs,
            certs: self.certs,
            keepalive: self.keepalive,
        })
    }
}

pub struct Client<D, C>
where
    D: DnsResolver,
{
    pub(crate) server_http_addrs: Arc<Mutex<Vec<ConnectAddress>>>,
    pub(crate) conn_manager: ConnectionManager<D>,
    pub(crate) tasks: Arc<Mutex<JoinSet<()>>>,
    pub(crate) token: CancellationToken,
    _p: PhantomData<C>,
}

impl<D, C> Clone for Client<D, C>
where
    D: DnsResolver + Clone,
    C: Codec,
{
    fn clone(&self) -> Self {
        Self {
            server_http_addrs: self.server_http_addrs.clone(),
            conn_manager: self.conn_manager.clone(),
            tasks: self.tasks.clone(),
            token: self.token.clone(),
            _p: PhantomData,
        }
    }
}

impl<C> Client<LocalDnsResolver, C>
where
    C: Codec,
{
    pub fn with_default_dns(config: Config) -> Self {
        Client::<_, C>::new(config, LocalDnsResolver)
    }
}

impl<D> Client<D, Bincode>
where
    D: DnsResolver,
{
    pub fn with_default_codec(config: Config, dns_resolver: D) -> Self {
        Client::<D, _>::new(config, dns_resolver)
    }
}

impl Client<LocalDnsResolver, Bincode> {
    pub fn with_default(config: Config) -> Self {
        Client::new(config, LocalDnsResolver)
    }
}

impl<D, C> Client<D, C>
where
    D: DnsResolver,
    C: Codec,
{
    pub fn new(config: Config, dns_resolver: D) -> Self {
        let token = CancellationToken::new();
        Self {
            server_http_addrs: Arc::new(Mutex::new(config.server_http_addrs)),
            conn_manager: ConnectionManager::new(
                dns_resolver,
                config.certs,
                config.keepalive,
                token.clone(),
            ),
            tasks: Arc::new(Mutex::new(JoinSet::new())),
            token,
            _p: PhantomData,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn create_topic(
        &self,
        topic_name: &str,
        config: topic::Config,
    ) -> Result<(), crate::Error> {
        let addrs = self.refresh_http_addrs().await?;
        api::create_topic(&addrs, topic_name, config)
            .await
            .context(HttpRequestSnafu)?;
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub fn new_producer_builder<T>(
        &self,
        name: impl Into<String> + Debug,
        topic_name: impl Into<String> + Debug,
    ) -> Result<ProducerBuilder<D, C, T>, crate::Error> {
        ProducerBuilder::new(name.into(), topic_name.into(), self.clone())
    }

    #[tracing::instrument(skip(self))]
    pub fn new_consumer_builder<T>(
        &self,
        name: impl Into<String> + Debug,
        topic_name: impl Into<String> + Debug,
        subscription_name: impl Into<String> + Debug,
    ) -> Result<ConsumerBuilder<D, C, T>, crate::Error> {
        ConsumerBuilder::new(
            name.into(),
            topic_name.into(),
            subscription_name.into(),
            self.clone(),
        )
    }

    #[tracing::instrument(skip(self))]
    pub async fn close(self) {
        self.conn_manager.close().await;
        self.token.cancel();
        let mut tasks = self.tasks.lock().await;
        while tasks.join_next().await.is_some() {}
    }

    #[tracing::instrument(skip(self))]
    pub(crate) async fn refresh_http_addrs(&self) -> Result<Vec<ConnectAddress>, crate::Error> {
        let mut addrs = self.server_http_addrs.lock().await;
        let new_addrs = api::get_server_http_addresses(&addrs)
            .await
            .context(HttpRequestSnafu)?;
        addrs.clone_from(&new_addrs);
        Ok(new_addrs)
    }
}
