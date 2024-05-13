use std::{
    io,
    marker::{self, PhantomData},
    slice::Iter,
};

use bytes::BufMut;
use snafu::{IntoError, Location, Snafu};
use tokio_util::codec::{Decoder, Encoder};

use crate::codec::Codec;

use self::{
    acknowledge::Acknowledge,
    connect::Connect,
    consumer::{CloseConsumer, Subscribe, SubscribeReceipt, Unsubscribe},
    control_flow::ControlFlow,
    producer::{CloseProducer, CreateProducer, ProducerReceipt},
    publish::{Publish, PublishHeader},
    response::{Response, ReturnCode},
    send::{Send, SendHeader},
};

pub mod acknowledge;
pub mod connect;
pub mod consumer;
pub mod control_flow;
pub mod producer;
pub mod publish;
pub mod response;
pub mod send;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("io error during packet codec"), context(false))]
    CodecIo {
        #[snafu(source)]
        error: io::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("codec error"))]
    ProtocolEncode {
        #[snafu(source)]
        error: Box<dyn std::error::Error + marker::Send + Sync + 'static>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("protocol decode error"))]
    ProtocolDecode {
        #[snafu(source)]
        error: Box<dyn std::error::Error + marker::Send + Sync + 'static>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("header remain len too large"))]
    HeaderRemainLenTooLarge {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("header remain len not enough"))]
    HeaderRemainLenNotEnough {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("unrecognized packet type"))]
    UnrecognizedPacketType {
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Clone)]
#[repr(u8)]
pub enum PacketType {
    Connect = 0,
    CreateProducer,
    ProducerReceipt,
    Publish,
    CloseProducer,
    Subscribe,
    SubscribeReceipt,
    ControlFlow,
    Send,
    Acknowledge,
    Unsubscribe,
    CloseConsumer,
    Response,
    Ping,
    Pong,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    CreateProducer(CreateProducer),
    ProducerReceipt(ProducerReceipt),
    Publish(Publish),
    CloseProducer(CloseProducer),
    Subscribe(Subscribe),
    SubscribeReceipt(SubscribeReceipt),
    ControlFlow(ControlFlow),
    Send(Send),
    Acknowledge(Acknowledge),
    Unsubscribe(Unsubscribe),
    CloseConsumer(CloseConsumer),
    Response(Response),
    Ping,
    Pong,
    Disconnect,
}

impl Packet {
    pub fn packet_type(&self) -> PacketType {
        match self {
            Packet::Connect(_) => PacketType::Connect,
            Packet::CreateProducer(_) => PacketType::CreateProducer,
            Packet::ProducerReceipt(_) => PacketType::ProducerReceipt,
            Packet::Publish(_) => PacketType::Publish,
            Packet::CloseProducer(_) => PacketType::CloseProducer,
            Packet::Subscribe(_) => PacketType::Subscribe,
            Packet::SubscribeReceipt(_) => PacketType::SubscribeReceipt,
            Packet::ControlFlow(_) => PacketType::ControlFlow,
            Packet::Send(_) => PacketType::Send,
            Packet::Acknowledge(_) => PacketType::Acknowledge,
            Packet::Unsubscribe(_) => PacketType::Unsubscribe,
            Packet::CloseConsumer(_) => PacketType::CloseConsumer,
            Packet::Response(_) => PacketType::Response,
            Packet::Ping => PacketType::Ping,
            Packet::Pong => PacketType::Pong,
            Packet::Disconnect => PacketType::Disconnect,
        }
    }

    pub fn err(code: ReturnCode) -> Packet {
        Packet::Response(Response {
            code,
            message: None,
        })
    }

    pub fn err_with_message(code: ReturnCode, message: String) -> Packet {
        Packet::Response(Response {
            code,
            message: Some(message),
        })
    }

    pub fn ok() -> Packet {
        Packet::Response(Response {
            code: ReturnCode::Success,
            message: None,
        })
    }
}

impl std::fmt::Display for PacketType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PacketType::Connect => "CONNECT",
            PacketType::CreateProducer => "CREATE_PRODUCER",
            PacketType::ProducerReceipt => "PRODUCER_RECEIPT",
            PacketType::Publish => "PUBLISH",
            PacketType::CloseProducer => "CLOSE_PRODUCER",
            PacketType::Subscribe => "SUBSCRIBE",
            PacketType::SubscribeReceipt => "SUBSCRIBE_RECEIPT",
            PacketType::ControlFlow => "CONTROL_FLOW",
            PacketType::Send => "SEND",
            PacketType::Acknowledge => "ACKNOWLEDGE",
            PacketType::Unsubscribe => "UNSUBSCRIBE",
            PacketType::CloseConsumer => "CLOSE_CONSUMER",
            PacketType::Response => "RESPONSE",
            PacketType::Ping => "PING",
            PacketType::Pong => "PONG",
            PacketType::Disconnect => "DISCONNECT",
        };
        write!(f, "{s}")
    }
}

pub struct PacketCodec<T>(PhantomData<T>);

impl<T> PacketCodec<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<T> Clone for PacketCodec<T> {
    fn clone(&self) -> Self {
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
        $packet(T::decode($buf).map_err(|e| ProtocolDecodeSnafu.into_error(Box::new(e)))?)
    };
}

impl<T> Decoder for PacketCodec<T>
where
    T: Codec,
{
    type Item = Packet;

    type Error = Error;

    #[tracing::instrument(skip(self, src))]
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
                let header = T::decode::<PublishHeader>(body)
                    .map_err(|e| ProtocolDecodeSnafu.into_error(Box::new(e)))?;
                let payload = src.split_to(header.payload_len as usize).freeze();
                Packet::Publish(Publish { header, payload })
            }
            PacketType::Acknowledge => decode!(Packet::Acknowledge, body),
            PacketType::ControlFlow => decode!(Packet::ControlFlow, body),
            PacketType::CreateProducer => decode!(Packet::CreateProducer, body),
            PacketType::ProducerReceipt => decode!(Packet::ProducerReceipt, body),
            PacketType::CloseProducer => decode!(Packet::CloseProducer, body),
            PacketType::Send => {
                let header = T::decode::<SendHeader>(body)
                    .map_err(|e| ProtocolDecodeSnafu.into_error(Box::new(e)))?;
                let payload = src.split_to(header.payload_len as usize).freeze();
                Packet::Send(Send { header, payload })
            }
            PacketType::Response => decode!(Packet::Response, body),
            PacketType::Subscribe => decode!(Packet::Subscribe, body),
            PacketType::SubscribeReceipt => decode!(Packet::SubscribeReceipt, body),
            PacketType::Unsubscribe => decode!(Packet::Unsubscribe, body),
            PacketType::CloseConsumer => decode!(Packet::CloseConsumer, body),
            PacketType::Ping => Packet::Ping,
            PacketType::Pong => Packet::Pong,
            PacketType::Disconnect => Packet::Disconnect,
        }))
    }
}

macro_rules! encode {
    ($item: expr, $type: expr, $buf: expr) => {
        Header::new(
            $type,
            T::size(&$item).map_err(|e| ProtocolEncodeSnafu.into_error(Box::new(e)))?,
        )
        .write($buf)?;
        T::encode(&$item, $buf).map_err(|e| ProtocolEncodeSnafu.into_error(Box::new(e)))?;
    };
}

impl<T> Encoder<Packet> for PacketCodec<T>
where
    T: Codec,
{
    type Error = Error;

    #[tracing::instrument(skip(self, dst))]
    fn encode(&mut self, item: Packet, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
            Packet::Connect(connect) => {
                encode!(connect, PacketType::Connect, dst);
            }
            Packet::Response(response) => {
                encode!(response, PacketType::Response, dst);
            }
            Packet::Publish(publish) => {
                encode!(publish.header, PacketType::Publish, dst);
                dst.extend(publish.payload);
            }
            Packet::Acknowledge(ack) => {
                encode!(ack, PacketType::Acknowledge, dst);
            }
            Packet::ControlFlow(cf) => {
                encode!(cf, PacketType::ControlFlow, dst);
            }
            Packet::CreateProducer(cp) => {
                encode!(cp, PacketType::CreateProducer, dst);
            }
            Packet::ProducerReceipt(pr) => {
                encode!(pr, PacketType::ProducerReceipt, dst);
            }
            Packet::CloseProducer(cp) => {
                encode!(cp, PacketType::CloseProducer, dst);
            }
            Packet::Send(send) => {
                encode!(send.header, PacketType::Send, dst);
                dst.extend(send.payload);
            }
            Packet::Subscribe(subscribe) => {
                encode!(subscribe, PacketType::Subscribe, dst);
            }
            Packet::SubscribeReceipt(receipt) => {
                encode!(receipt, PacketType::SubscribeReceipt, dst);
            }
            Packet::Unsubscribe(unsubscribe) => {
                encode!(unsubscribe, PacketType::Unsubscribe, dst);
            }
            Packet::CloseConsumer(cc) => {
                encode!(cc, PacketType::CloseConsumer, dst);
            }
            Packet::Ping => Header::new(PacketType::Ping, 0).write(dst)?,
            Packet::Pong => Header::new(PacketType::Pong, 0).write(dst)?,
            Packet::Disconnect => Header::new(PacketType::Disconnect, 0).write(dst)?,
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct Header {
    /// 8 bits
    type_byte: u8,
    /// remain len algorithm
    remain_len: u64,
}

impl Header {
    fn new(packet_type: PacketType, remain_len: u64) -> Self {
        Self {
            type_byte: packet_type as u8,
            remain_len,
        }
    }

    #[tracing::instrument(skip(buf))]
    fn read(mut buf: Iter<u8>) -> Result<(Self, u64), Error> {
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

    #[tracing::instrument(skip(buf))]
    fn write(&self, buf: &mut bytes::BytesMut) -> Result<(), Error> {
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

    fn packet_type(&self) -> Result<PacketType, Error> {
        Ok(match self.type_byte {
            0 => PacketType::Connect,
            1 => PacketType::CreateProducer,
            2 => PacketType::ProducerReceipt,
            3 => PacketType::Publish,
            4 => PacketType::CloseProducer,
            5 => PacketType::Subscribe,
            6 => PacketType::SubscribeReceipt,
            7 => PacketType::ControlFlow,
            8 => PacketType::Send,
            9 => PacketType::Acknowledge,
            10 => PacketType::Unsubscribe,
            11 => PacketType::CloseConsumer,
            12 => PacketType::Response,
            13 => PacketType::Ping,
            14 => PacketType::Pong,
            15 => PacketType::Disconnect,
            _ => UnrecognizedPacketTypeSnafu.fail()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use crate::codec::bincode::Bincode;

    use super::*;

    #[test]
    fn connect() {
        codec_works(Packet::Connect(Connect { keepalive: 1000 }))
    }

    fn codec_works(packet: Packet) {
        let mut bytes = BytesMut::new();
        let mut codec = PacketCodec::<Bincode>::new();
        codec.encode(packet.clone(), &mut bytes).unwrap();
        println!("{}: {:?}", packet.packet_type(), bytes.as_ref());
        assert_eq!(packet, codec.decode(&mut bytes).unwrap().unwrap());
    }
}
