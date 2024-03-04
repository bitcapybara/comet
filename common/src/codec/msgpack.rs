use bytes::{Buf, BufMut};
use snafu::{ResultExt, Snafu};

use crate::protocol::PacketCodec;

use super::Codec;

pub type MsgPackCodec = PacketCodec<MsgPack>;

#[derive(Debug, Snafu)]
pub enum Error {
    Serialize { source: rmp_serde::encode::Error },
    Deserialize { source: rmp_serde::decode::Error },
}

pub struct MsgPack;

impl Codec for MsgPack {
    type Error = Error;

    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        rmp_serde::encode::write(&mut buf.writer(), item).context(SerializeSnafu)
    }

    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        rmp_serde::decode::from_read(buf.reader()).context(DeserializeSnafu)
    }
}
