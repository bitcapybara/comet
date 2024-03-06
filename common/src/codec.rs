pub mod bincode;

pub trait Codec {
    type Error;

    fn size<T>(item: &T) -> Result<u64, Self::Error>
    where
        T: serde::Serialize;

    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize;

    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>;
}
