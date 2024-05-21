use std::fmt::Debug;
use std::future::Future;
use std::io;
use std::net::SocketAddr;

use snafu::{OptionExt, ResultExt, Snafu};
use tokio::net::lookup_host;

pub trait DnsResolver: Clone + Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    fn resolve(
        &self,
        host: &str,
        port: u16,
    ) -> impl Future<Output = Result<SocketAddr, Self::Error>> + Send;
}

#[derive(Debug, Snafu)]
pub enum LocalDnsError {
    #[snafu(display("IO error for host {host}"))]
    Io { host: String, source: io::Error },
    #[snafu(display("No valid address found for host {host}"))]
    NotFound { host: String },
}

#[derive(Clone)]
pub struct LocalDnsResolver;

impl DnsResolver for LocalDnsResolver {
    type Error = LocalDnsError;

    #[tracing::instrument(skip(self))]
    async fn resolve(&self, host: &str, port: u16) -> Result<SocketAddr, Self::Error> {
        lookup_host((host, port))
            .await
            .context(IoSnafu { host })?
            .find(|addr| !addr.ip().is_multicast() && !addr.ip().is_unspecified() && addr.is_ipv4())
            .context(NotFoundSnafu { host })
    }
}
