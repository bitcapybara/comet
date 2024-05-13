mod reader;

use s2n_quic::Connection;
use tokio::sync::mpsc;

use comet_common::{codec::Codec, io::writer::start_writer, utils::defer::defer};
use tokio_util::sync::CancellationToken;

use crate::{broker::ClientPacket, client::reader::reader_loop};

/// broker_tx 发送给 broker的消息
pub async fn start_client<C>(
    conn: Connection,
    broker_tx: mpsc::UnboundedSender<ClientPacket>,
    token: CancellationToken,
) where
    C: Codec,
{
    let _guard = defer(|| token.cancel());

    let (handle, acceptor) = conn.split();

    let (client_tx, client_rx) = mpsc::unbounded_channel();

    tokio::join!(
        reader_loop::<C>(acceptor, broker_tx, client_tx, token.clone()),
        start_writer::<C>(handle, client_rx, token.clone()),
    );
}
