use s2n_quic::stream::{BidirectionalStream, ReceiveStream, SendStream};
use snafu::Snafu;
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    codec::bincode::{Bincode, BincodeCodec},
    protocol::{self, PacketCodec},
};

mod reader;
mod writer;

pub type DefaultPacketCodec = PacketCodec<DefaultCodec>;
pub type DefaultCodec = Bincode;

#[derive(Debug, Snafu)]
pub enum Error {
    StreamWrite {
        source: protocol::Error<DefaultCodec>,
    },
    StreamRead {
        source: protocol::Error<DefaultCodec>,
    },
}

pub fn split_quic_stream(
    stream: BidirectionalStream,
) -> (
    FramedRead<ReceiveStream, BincodeCodec>,
    FramedWrite<SendStream, BincodeCodec>,
) {
    let (recv_quic_stream, send_quic_stream) = stream.split();
    (
        FramedRead::new(recv_quic_stream, DefaultPacketCodec::new()),
        FramedWrite::new(send_quic_stream, DefaultPacketCodec::new()),
    )
}
