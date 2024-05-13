use s2n_quic::stream::{BidirectionalStream, ReceiveStream, SendStream};
use snafu::{Location, Snafu};
use tokio_util::codec::{FramedRead, FramedWrite};

use crate::{
    codec::Codec,
    protocol::{self, PacketCodec},
};

pub mod reader;
pub mod writer;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("stream write error"))]
    StreamWrite {
        source: protocol::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("stream read error"))]
    StreamRead {
        source: protocol::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

pub fn split_quic_stream<C>(
    stream: BidirectionalStream,
) -> (
    FramedRead<ReceiveStream, PacketCodec<C>>,
    FramedWrite<SendStream, PacketCodec<C>>,
)
where
    C: Codec,
{
    let (recv_quic_stream, send_quic_stream) = stream.split();
    (
        FramedRead::new(recv_quic_stream, PacketCodec::<C>::new()),
        FramedWrite::new(send_quic_stream, PacketCodec::<C>::new()),
    )
}
