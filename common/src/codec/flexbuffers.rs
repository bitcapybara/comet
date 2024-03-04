use snafu::{ResultExt, Snafu};

use crate::protocol::PacketCodec;

use super::Codec;

pub type FlexBuffersCodec = PacketCodec<FlexBuffers>;

#[derive(Debug, Snafu)]
pub enum Error {
    Serialize {
        source: flexbuffers::SerializationError,
    },
    Deserialize {
        source: flexbuffers::DeserializationError,
    },
    DeserializeReader {
        source: flexbuffers::ReaderError,
    },
}

pub struct FlexBuffers;

impl Codec for FlexBuffers {
    type Error = Error;

    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        let mut s = flexbuffers::FlexbufferSerializer::new();
        item.serialize(&mut s).context(SerializeSnafu)?;
        buf.extend_from_slice(s.view());
        Ok(())
    }

    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        let reader = flexbuffers::Reader::get_root(buf.as_ref()).context(DeserializeReaderSnafu)?;
        T::deserialize(reader).context(DeserializeSnafu)
    }
}
