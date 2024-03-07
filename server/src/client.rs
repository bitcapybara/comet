use s2n_quic::Connection;
use tokio::sync::mpsc;

use comet_common::{defer::defer, protocol};
use tokio_util::sync::CancellationToken;

use crate::broker::{BrokerMessage, ClientMessage};

pub struct Client {
    id: u64,
    conn: Connection,
    packet_tx: mpsc::UnboundedSender<protocol::Packet>,
}

pub async fn start_client(
    id: u64,
    conn: Connection,
    packet_tx: mpsc::UnboundedSender<ClientMessage>,
    token: CancellationToken,
) {
    let _gurad = defer(|| token.cancel());
    todo!()
}
