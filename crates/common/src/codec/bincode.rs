use bytes::{Buf, BufMut};

use super::Codec;

#[derive(Debug)]
pub struct Bincode;

impl Codec for Bincode {
    type Error = bincode::Error;

    #[tracing::instrument(skip_all)]
    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        bincode::serialize_into(buf.writer(), item)
    }

    #[tracing::instrument(skip_all)]
    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        bincode::deserialize_from(buf.reader())
    }

    #[tracing::instrument(skip_all)]
    fn size<T>(item: &T) -> Result<u64, Self::Error>
    where
        T: serde::Serialize,
    {
        bincode::serialized_size(item)
    }
}
