pub mod bincode;
pub mod flexbuffers;
pub mod json;
mod msgpack;

pub trait Codec {
    type Error;

    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize;

    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>;
}
