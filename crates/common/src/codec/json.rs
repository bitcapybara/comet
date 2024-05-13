use std::io::{self, Write};

use bytes::{Buf, BufMut};

use super::Codec;

#[derive(Debug)]
pub struct Json;

impl Codec for Json {
    type Error = serde_json::Error;

    #[tracing::instrument(skip_all)]
    fn encode<T>(item: &T, buf: &mut bytes::BytesMut) -> Result<(), Self::Error>
    where
        T: serde::Serialize,
    {
        serde_json::to_writer(buf.writer(), item)
    }

    #[tracing::instrument(skip_all)]
    fn decode<T>(buf: bytes::Bytes) -> Result<T, Self::Error>
    where
        T: for<'a> serde::Deserialize<'a>,
    {
        serde_json::from_reader(buf.reader())
    }

    #[tracing::instrument(skip_all)]
    fn size<T>(item: &T) -> Result<u64, Self::Error>
    where
        T: serde::Serialize,
    {
        let mut ser = serde_json::Serializer::new(ByteCount(0));
        item.serialize(&mut ser)?;
        Ok(ser.into_inner().0)
    }
}

struct ByteCount(u64);

impl Write for ByteCount {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
