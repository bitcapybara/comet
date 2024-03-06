mod acknowledge;
pub mod connect;
mod consumer;
mod control_flow;
mod producer;
pub mod publish;
pub mod response;
mod send;

use std::{io, marker::PhantomData, slice::Iter};

use bytes::BufMut;
use snafu::{ResultExt, Snafu};
use tokio_util::codec::{Decoder, Encoder};

use crate::codec::Codec;

use self::{
    acknowledge::Acknowledge,
    connect::Connect,
    consumer::{CloseConsumer, Subscribe, Unsubscribe},
    control_flow::ControlFlow,
    producer::{CloseProducer, CreateProducer, ProducerReceipt},
    publish::{Publish, PublishHeader},
    response::Response,
    send::{Send, SendHeader},
};

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
    HeaderRemainLenTooLarge,
    HeaderRemainLenNotEnough,
}

#[derive(Debug)]
#[repr(u8)]
pub enum PacketType {
    Connect = 0,
    CreateProducer,
    ProducerReceipt,
    Publish,
    CloseProducer,
    Subscribe,
    ControlFlow,
    Send,
    Acknowledge,
    Unsubscribe,
    CloseConsumer,
    Response,
}

pub enum Packet {
    Connect(Connect),
    CreateProducer(CreateProducer),
    ProducerReceipt(ProducerReceipt),
    Publish(Publish),
    CloseProducer(CloseProducer),
    Subscribe(Subscribe),
    ControlFlow(ControlFlow),
    Send(Send),
    Acknowledge(Acknowledge),
    Unsubscribe(Unsubscribe),
    CloseConsumer(CloseConsumer),
    Response(Response),
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        todo!()
    }
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

macro_rules! decode {
    ($packet: expr, $buf: expr) => {
        $packet(T::decode($buf).context(ProtocolDecodeSnafu)?)
    };
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
        let body = src
            .split_to((header_len + header.remain_len) as usize) // header + body
            .split_off(header_len as usize) // body
            .freeze();
        Ok(Some(match header.packet_type()? {
            PacketType::Connect => decode!(Packet::Connect, body),
            PacketType::Publish => {
                let header = T::decode::<PublishHeader>(body).context(ProtocolDecodeSnafu)?;
                let payload = src.split_to(header.payload_len as usize).freeze();
                Packet::Publish(Publish { header, payload })
            }
            PacketType::Acknowledge => decode!(Packet::Acknowledge, body),
            PacketType::ControlFlow => decode!(Packet::Connect, body),
            PacketType::CreateProducer => decode!(Packet::CreateProducer, body),
            PacketType::ProducerReceipt => decode!(Packet::ProducerReceipt, body),
            PacketType::CloseProducer => decode!(Packet::CloseProducer, body),
            PacketType::Send => {
                let header = T::decode::<SendHeader>(body).context(ProtocolDecodeSnafu)?;
                let payload = src.split_to(header.payload_len as usize).freeze();
                Packet::Send(Send { header, payload })
            }
            PacketType::Response => decode!(Packet::Response, body),
            PacketType::Subscribe => decode!(Packet::Subscribe, body),
            PacketType::Unsubscribe => decode!(Packet::Unsubscribe, body),
            PacketType::CloseConsumer => decode!(Packet::CloseConsumer, body),
        }))
    }
}

macro_rules! encode {
    ($item: expr, $buf: expr) => {
        Header::new(
            PacketType::Acknowledge,
            T::size(&$item).context(ProtocolEncodeSnafu)?,
        )
        .write($buf)?;
        T::encode(&$item, $buf).context(ProtocolEncodeSnafu)?;
    };
}

impl<T> Encoder<Packet> for PacketCodec<T>
where
    T: Codec + std::fmt::Debug,
    T::Error: std::error::Error + 'static,
{
    type Error = Error<T>;

    fn encode(&mut self, item: Packet, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::Connect(connect) => {
                encode!(connect, dst);
            }
            Packet::Response(response) => {
                encode!(response, dst);
            }
            Packet::Publish(publish) => {
                encode!(publish.header, dst);
                dst.extend(publish.payload);
            }
            Packet::Acknowledge(ack) => {
                encode!(ack, dst);
            }
            Packet::ControlFlow(cf) => {
                encode!(cf, dst);
            }
            Packet::CreateProducer(cp) => {
                encode!(cp, dst);
            }
            Packet::ProducerReceipt(pr) => {
                encode!(pr, dst);
            }
            Packet::CloseProducer(cp) => {
                encode!(cp, dst);
            }
            Packet::Send(send) => {
                encode!(send.header, dst);
                dst.extend(send.payload);
            }
            Packet::Subscribe(subscribe) => {
                encode!(subscribe, dst);
            }
            Packet::Unsubscribe(unsubscribe) => {
                encode!(unsubscribe, dst);
            }
            Packet::CloseConsumer(cc) => {
                encode!(cc, dst);
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct Header {
    /// 8 bits
    type_byte: u8,
    /// mqtt remain len algorithm
    remain_len: u64,
}

impl Header {
    fn new(packet_type: PacketType, remain_len: u64) -> Self {
        Self {
            type_byte: packet_type as u8,
            remain_len,
        }
    }

    fn read<T>(mut buf: Iter<u8>) -> Result<(Self, u64), Error<T>>
    where
        T: Codec,
        T::Error: std::error::Error,
    {
        // buf is not empty, so we can unwrap here
        let type_byte = buf.next().unwrap().to_owned();

        let mut remain_len = 0u64;
        let mut header_len = 1; // init with type_byte bit
        let mut done = false;
        let mut shift = 0;

        for byte in buf.map(|b| *b as u64) {
            header_len += 1;
            remain_len += (byte & 0x7F) << shift;

            done = (byte & 0x80) == 0;
            if done {
                break;
            }
            shift += 7;

            if shift > 21 {
                return HeaderRemainLenTooLargeSnafu.fail();
            }
        }

        if !done {
            return HeaderRemainLenNotEnoughSnafu.fail();
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
