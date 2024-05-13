use std::{fmt::Display, str::FromStr};

use snafu::{Location, ResultExt, Snafu};
use url::Url;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display(
        "wrong connect address format: {addr}, expected: 'quic://example.com:8999' for quic or 'https://example.com:7999' for https"
    ))]
    WrongFormat {
        addr: String,
        #[snafu(source)]
        error: url::ParseError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("unexpected scheme: {input}"))]
    UnexpectedScheme {
        input: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("port number not found in {addr}"))]
    PortNotFound {
        addr: String,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("domain not found in {addr}"))]
    DomainNotFound {
        addr: String,
        #[snafu(implicit)]
        location: Location,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum Scheme {
    Quic,
    Https,
    Http,
}

impl Display for Scheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scheme::Quic => write!(f, "quic"),
            Scheme::Https => write!(f, "https"),
            Scheme::Http => write!(f, "http"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Addr(pub String, pub u16);

impl std::fmt::Display for Addr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.0, self.1)
    }
}

impl From<&Addr> for String {
    fn from(addr: &Addr) -> Self {
        addr.to_string()
    }
}

impl From<&Addr> for Addr {
    fn from(value: &Addr) -> Self {
        Addr(value.0.clone(), value.1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ConnectAddress {
    scheme: Scheme,
    domain: String,
    port: u16,
}

impl ConnectAddress {
    pub fn addr(&self) -> Addr {
        Addr(self.domain.clone(), self.port)
    }
}

impl FromStr for ConnectAddress {
    type Err = Error;

    #[tracing::instrument]
    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        let addr_url = Url::parse(addr).context(WrongFormatSnafu { addr })?;

        let scheme = match addr_url.scheme() {
            "quic" => Scheme::Quic,
            "https" => Scheme::Https,
            "http" => Scheme::Http,
            input => return UnexpectedSchemeSnafu { input }.fail(),
        };

        let Some(port) = addr_url.port() else {
            return PortNotFoundSnafu { addr }.fail();
        };

        let Some(domain) = addr_url.domain() else {
            return DomainNotFoundSnafu { addr }.fail();
        };

        Ok(Self {
            scheme: scheme.to_owned(),
            domain: domain.to_owned(),
            port,
        })
    }
}

impl Display for ConnectAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.domain, self.port)
    }
}

impl From<&ConnectAddress> for ConnectAddress {
    fn from(value: &ConnectAddress) -> Self {
        value.clone()
    }
}
