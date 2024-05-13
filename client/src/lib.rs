#![forbid(unsafe_code)]

use comet_common::protocol::{response::Response, PacketType};
use s2n_quic::provider::tls::default as tls;
use snafu::{Location, Snafu};

pub mod client;
mod connection;
pub mod dns;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("disconnected"))]
    Disconnect {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("client has shutdown"))]
    Shutdown {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Unexpected response packet, request: {request}, response: {response}"))]
    UnexpectedResponsePacket {
        request: PacketType,
        response: PacketType,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("http request error"))]
    HttpRequest {
        source: comet_common::http::api::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("cluster is unreachable"))]
    ClusterUnreachable {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("connection read write error"))]
    ConnectionReadWrite {
        source: comet_common::io::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("Error Response from server: {response}"))]
    ServerResponse {
        response: Response,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("init tls error: {message}"))]
    InitTls {
        message: String,
        #[snafu(source)]
        error: tls::error::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("bind client addr error"))]
    BindClientAddr {
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("start quic client error"))]
    StartQuicClient {
        #[snafu(source)]
        error: s2n_quic::provider::StartError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("dns resolver error"))]
    DnsResolver {
        #[snafu(source)]
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("quic client handshake error"))]
    ClientConnect {
        #[snafu(source)]
        error: s2n_quic::connection::Error,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("get connection cancelled"))]
    ConnectCanceled {
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("codec error"))]
    Codec {
        #[snafu(source)]
        error: Box<dyn std::error::Error + Send + Sync + 'static>,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("invalid name: {name}"))]
    InvalidName {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },
}
