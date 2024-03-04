use bytes::{Buf, BufMut};

use crate::protocol::PacketCodec;

use super::Codec;

pub type BincodeCodec = PacketCodec<Bincode>;

#[derive(Debug)]
pub struct Bincode;

impl Codec for Bincode {
    type Error = bincode::Error;

    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        bincode::serialize_into(buf.writer(), item)
    }

    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        bincode::deserialize_from(buf.reader())
    }
}
