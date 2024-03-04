use bytes::{Buf, BufMut};

use crate::{codec::Codec, protocol::PacketCodec};

pub type JsonCodec = PacketCodec<Json>;

pub struct Json;

impl Codec for Json {
    type Error = serde_json::Error;

    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        serde_json::to_writer(buf.writer(), item)
    }

    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        serde_json::from_reader(buf.reader())
    }
}
