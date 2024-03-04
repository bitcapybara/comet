pub mod connect;
pub mod publish;
pub mod response;

use std::{io, marker::PhantomData, slice::Iter};

use bytes::BufMut;
use snafu::{ResultExt, Snafu};
use tokio_util::codec::{Decoder, Encoder};

use crate::codec::Codec;

use self::{connect::Connect, response::Response};

#[derive(Debug, Snafu)]
pub enum Error<T>
where
    T: Codec,
    T::Error: std::error::Error + 'static + std::fmt::Debug,
{
    #[snafu(display("io error during packet codec"), context(false))]
    CodecIo {
        source: io::Error,
    },
    #[snafu(display("codec error"))]
    ProtocolEncode {
        source: T::Error,
    },
    ProtocolDecode {
        source: T::Error,
    },
}

pub enum Packet {
    Connect(Connect),
    Response(Response),
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        todo!()
    }
}

#[derive(Debug)]
#[repr(u8)]
pub enum PacketType {
    Connect = 0,
}

impl std::fmt::Display for PacketType {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

pub struct PacketCodec<T>(PhantomData<T>);

impl<T> PacketCodec<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Default for PacketCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Decoder for PacketCodec<T>
where
    T: Codec + std::fmt::Debug,
    T::Error: std::error::Error + 'static,
{
    type Item = Packet;

    type Error = Error<T>;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let (header, header_len) = Header::read(src.iter())?;
        // header + body + other
        let bytes = src
            .split_to(header.remain_len + header_len) // header + body
            .split_off(header_len) // body
            .freeze();
        Ok(Some(match header.packet_type()? {
            PacketType::Connect => {
                Packet::Connect(T::decode::<Connect>(bytes).context(ProtocolDecodeSnafu)?)
            }
        }))
    }
}

impl<T> Encoder<Packet> for PacketCodec<T>
where
    T: Codec + std::fmt::Debug,
    T::Error: std::error::Error + 'static,
{
    type Error = Error<T>;

    fn encode(&mut self, item: Packet, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::Connect(connect) => T::encode(&connect, dst).context(ProtocolEncodeSnafu)?,
            Packet::Response(_) => todo!(),
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct Header {
    /// 8 bits
    type_byte: u8,
    /// mqtt remain len algorithm
    remain_len: usize,
}

impl Header {
    fn new(packet_type: PacketType, remain_len: usize) -> Self {
        Self {
            type_byte: packet_type as u8,
            remain_len,
        }
    }
    fn read<T>(mut buf: Iter<u8>) -> Result<(Self, usize), Error<T>>
    where
        T: Codec,
        T::Error: std::error::Error,
    {
        // TODO error handling
        let type_byte = buf.next().unwrap().to_owned();

        let mut remain_len = 0usize;
        let mut header_len = 1; // init with type_byte bit
        let mut done = false;
        let mut shift = 0;

        for byte in buf.map(|b| *b as usize) {
            header_len += 1;
            remain_len += (byte & 0x7F) << shift;

            done = (byte & 0x80) == 0;
            if done {
                break;
            }
            shift += 7;

            if shift > 21 {
                unreachable!()
            }
        }

        if !done {
            unreachable!()
        }

        Ok((
            Header {
                remain_len,
                type_byte,
            },
            header_len,
        ))
    }

    fn write<T>(&self, buf: &mut bytes::BytesMut) -> Result<(), Error<T>>
    where
        T: Codec,
        T::Error: std::error::Error,
    {
        buf.put_u8(self.type_byte);

        let mut done = false;
        let mut x = self.remain_len;

        while !done {
            let mut byte = (x % 128) as u8;
            x /= 128;
            if x > 0 {
                byte |= 128;
            }

            buf.put_u8(byte);
            done = x == 0;
        }

        Ok(())
    }

    fn packet_type<T>(&self) -> Result<PacketType, Error<T>>
    where
        T: Codec,
        T::Error: std::error::Error,
    {
        Ok(match self.type_byte {
            1 => PacketType::Connect,
            _ => unreachable!(),
        })
    }
}
