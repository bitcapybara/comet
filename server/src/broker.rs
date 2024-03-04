use etcd_client as etcd;
use snafu::Snafu;
use tokio::{select, sync::mpsc};
use tokio_util::sync::CancellationToken;

use comet_common::{defer::defer, protocol};

#[derive(Debug, Snafu)]
pub enum Error {
    SendLeaseGrant { source: etcd::Error },
    SendLeaseKeepalive { source: etcd::Error },
}

#[derive(Clone)]
pub struct Broker {
    connect_addr: String,
    etcd: etcd::Client,
    pg: sqlx::postgres::PgPool,
}

impl Broker {
    pub fn new() -> Result<Self, Error> {
        todo!()
    }

    /// used by `broker::run` and `http server`
    pub async fn handle_xx_packet() -> Result<(), Error> {
        todo!()
    }
}

pub async fn start_broker(
    mut broker: Broker,
    mut inbound_rx: mpsc::UnboundedReceiver<protocol::Packet>,
    token: CancellationToken,
) -> Result<(), Error> {
    let _gurad = defer(|| token.cancel());
    loop {
        select! {
            packet = inbound_rx.recv() => {
                // handle packets
            }
        }
    }
}
