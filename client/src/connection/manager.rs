use std::{collections::HashMap, sync::Arc};

use comet_common::{addr::ConnectAddress, codec::Codec, mtls::CertsFile};
use futures::FutureExt;
use s2n_quic::{client as s2n_client, provider::tls::default as tls};
use snafu::{IntoError, ResultExt};
use tokio::{
    sync::{mpsc, oneshot, Mutex},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::{
    connection::{reader::reader_loop, writer::writer_loop},
    dns::DnsResolver,
    BindClientAddrSnafu, ClientConnectSnafu, ConnectCanceledSnafu, DnsResolverSnafu, InitTlsSnafu,
    ShutdownSnafu, StartQuicClientSnafu,
};

use super::TopicConnection;

enum ConnectionState {
    Connected(TopicConnection),
    Connecting(Vec<oneshot::Sender<Result<TopicConnection, crate::Error>>>),
}

pub struct ConnectionManager<D>
where
    D: DnsResolver,
{
    keepalive: u16,
    dns_resolver: D,
    certs: CertsFile,
    conns: Arc<Mutex<HashMap<ConnectAddress, ConnectionState>>>,
    tasks: Arc<Mutex<JoinSet<()>>>,
    token: CancellationToken,
}

impl<D> Clone for ConnectionManager<D>
where
    D: DnsResolver + Clone,
{
    fn clone(&self) -> Self {
        Self {
            dns_resolver: self.dns_resolver.clone(),
            certs: self.certs.clone(),
            conns: self.conns.clone(),
            tasks: self.tasks.clone(),
            keepalive: self.keepalive,
            token: self.token.clone(),
        }
    }
}

impl<D> ConnectionManager<D>
where
    D: DnsResolver,
{
    pub fn new(
        dns_resolver: D,
        certs: CertsFile,
        keepalive: u16,
        token: CancellationToken,
    ) -> Self {
        Self {
            keepalive,
            dns_resolver,
            certs,
            conns: Arc::new(Mutex::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(JoinSet::new())),
            token,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn get_connection<C>(
        &self,
        addr: &ConnectAddress,
    ) -> Result<TopicConnection, crate::Error>
    where
        C: Codec,
    {
        // 返回 None 代表需要新建连接
        let waiter = {
            let mut conns = self.conns.lock().await;
            match conns.remove(addr) {
                Some(conn_state) => match conn_state {
                    ConnectionState::Connected(conn) => {
                        if !conn.is_closed() {
                            conns.insert(addr.to_owned(), ConnectionState::Connected(conn.clone()));
                            return Ok(conn);
                        }
                        conns.insert(addr.to_owned(), ConnectionState::Connecting(vec![]));
                        None
                    }
                    ConnectionState::Connecting(mut senders) => {
                        let (tx, rx) = oneshot::channel();
                        senders.push(tx);
                        conns.insert(addr.to_owned(), ConnectionState::Connecting(senders));
                        Some(rx)
                    }
                },
                None => {
                    conns.insert(addr.to_owned(), ConnectionState::Connecting(vec![]));
                    None
                }
            }
        };
        match waiter {
            Some(rx) => match rx.await {
                Ok(conn) => conn,
                Err(_) => ShutdownSnafu.fail(),
            },
            None => self.connect::<C>(addr).await,
        }
    }

    #[tracing::instrument(skip(self))]
    async fn connect<C>(&self, addr: &ConnectAddress) -> Result<TopicConnection, crate::Error>
    where
        C: Codec,
    {
        match self.create_connection::<C>(addr).await {
            Ok(tc) => {
                let mut conns = self.conns.lock().await;
                if let Some(ConnectionState::Connecting(mut senders)) = conns.remove(addr) {
                    for sender in senders.drain(..) {
                        sender.send(Ok(tc.clone())).ok();
                    }
                }
                conns.insert(addr.to_owned(), ConnectionState::Connected(tc.clone()));
                Ok(tc)
            }
            Err(e) => {
                let mut conns = self.conns.lock().await;
                if let Some(ConnectionState::Connecting(mut senders)) = conns.remove(addr) {
                    for sender in senders.drain(..) {
                        sender.send(ConnectCanceledSnafu.fail()).ok();
                    }
                }
                Err(e)
            }
        }
    }

    #[tracing::instrument(skip(self))]
    async fn create_connection<C>(
        &self,
        addr: &ConnectAddress,
    ) -> Result<TopicConnection, crate::Error>
    where
        C: Codec,
    {
        let tls = tls::Client::builder()
            .with_certificate(self.certs.ca_cert_file.as_path())
            .context(InitTlsSnafu {
                message: "add ca cert file error",
            })?
            .with_client_identity(
                self.certs.cert_file.as_path(),
                self.certs.key_file.as_path(),
            )
            .context(InitTlsSnafu {
                message: "add client cert file error",
            })?
            .build()
            .unwrap();

        let client: s2n_client::Client = s2n_client::Client::builder()
            .with_tls(tls)
            .unwrap()
            .with_io("0.0.0.0:0")
            .context(BindClientAddrSnafu)?
            .start()
            .context(StartQuicClientSnafu)?;

        let dns_addr = addr.addr();
        let socket_addr = self
            .dns_resolver
            .resolve(&dns_addr)
            .await
            .map_err(|e| DnsResolverSnafu.into_error(Box::new(e)))?;

        let connect = s2n_client::Connect::new(socket_addr).with_server_name(dns_addr.0);
        let connection = client.connect(connect).await.context(ClientConnectSnafu)?;
        // connection.keep_alive(true);

        let token = self.token.child_token();
        let (handle, acceptor) = connection.split();
        let (register_tx, register_rx) = mpsc::unbounded_channel();
        let (request_tx, request_rx) = mpsc::unbounded_channel();

        let mut tasks = self.tasks.lock().await;
        tasks.spawn(
            futures::future::join(
                reader_loop::<C>(acceptor, register_rx, token.clone()),
                writer_loop::<C>(self.keepalive, handle, request_rx, token.clone()),
            )
            .map(|_| ()),
        );

        let conn = TopicConnection {
            addr: addr.to_owned(),
            keepalive: self.keepalive,
            request_tx,
            register_tx,
            token,
        };

        conn.handshake().await?;

        Ok(conn)
    }

    #[tracing::instrument(skip(self))]
    pub async fn close(self) {
        {
            let mut conns = self.conns.lock().await;
            for (_, conn_state) in conns.drain() {
                if let ConnectionState::Connected(conn) = conn_state {
                    let addr = conn.addr.clone();
                    if let Err(e) = conn.disconnect().await {
                        error!(%addr, "send disconnect error: {e:?}");
                    }
                }
            }
        }

        {
            self.token.cancel();
            let mut tasks = self.tasks.lock().await;
            while tasks.join_next().await.is_some() {}
        }
    }
}
