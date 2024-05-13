use axum::http::StatusCode;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Response {
    pub code: ReturnCode,
    pub message: Option<String>,
}

impl Response {
    pub fn new<S>(code: ReturnCode, message: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            code,
            message: Some(message.into()),
        }
    }

    pub fn is_success(&self) -> bool {
        self.code.is_success()
    }

    pub fn with<S>(mut self, message: S) -> Self
    where
        S: Into<String>,
    {
        self.message = Some(message.into());
        self
    }
}

impl From<ReturnCode> for Response {
    fn from(code: ReturnCode) -> Self {
        Self {
            code,
            message: None,
        }
    }
}

impl std::fmt::Display for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "response code: {}, message: {:?}",
            self.code, self.message
        )
    }
}

#[derive(Debug, PartialEq, Eq, Clone, serde::Serialize, serde::Deserialize)]
pub enum ReturnCode {
    Success,
    HandshakeExpected,
    DuplicatedConnect,
    UnexpectedPacket,
    ProducerNameAlreadyExists,
    ConsumerNameAlreadyExists,
    ProducerNotFound,
    ConsumerNotFound,
    TopicNotFound,
    ProducerAlreadyExclusive,
    ProducerAccessModeMismatched,
    SubscriptionNotFound,
    ConsumerAlreadyExclusive,
    ConsumerSubscriptionTypeMismatched,
    PartitionedTopicNotSupported,
    BrokerUnavailable,
    InvalidName,
    Internal,
}

impl ReturnCode {
    pub fn is_success(&self) -> bool {
        *self == ReturnCode::Success
    }
}

impl From<ReturnCode> for axum::http::StatusCode {
    fn from(code: ReturnCode) -> Self {
        match code {
            ReturnCode::Success => StatusCode::OK,
            ReturnCode::HandshakeExpected => StatusCode::FORBIDDEN,
            ReturnCode::DuplicatedConnect => StatusCode::BAD_REQUEST,
            ReturnCode::UnexpectedPacket => StatusCode::NOT_ACCEPTABLE,
            ReturnCode::ProducerNameAlreadyExists => StatusCode::BAD_REQUEST,
            ReturnCode::ConsumerNameAlreadyExists => StatusCode::BAD_REQUEST,
            ReturnCode::ProducerNotFound => StatusCode::BAD_REQUEST,
            ReturnCode::ConsumerNotFound => StatusCode::BAD_REQUEST,
            ReturnCode::TopicNotFound => StatusCode::BAD_REQUEST,
            ReturnCode::ProducerAlreadyExclusive => StatusCode::BAD_REQUEST,
            ReturnCode::ProducerAccessModeMismatched => StatusCode::BAD_REQUEST,
            ReturnCode::SubscriptionNotFound => StatusCode::BAD_REQUEST,
            ReturnCode::ConsumerAlreadyExclusive => StatusCode::BAD_REQUEST,
            ReturnCode::ConsumerSubscriptionTypeMismatched => StatusCode::BAD_REQUEST,
            ReturnCode::PartitionedTopicNotSupported => StatusCode::BAD_REQUEST,
            ReturnCode::BrokerUnavailable => StatusCode::INTERNAL_SERVER_ERROR,
            ReturnCode::InvalidName => StatusCode::BAD_REQUEST,
            ReturnCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl std::fmt::Display for ReturnCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReturnCode::Success => write!(f, "SUCCESS"),
            ReturnCode::HandshakeExpected => write!(f, "HANDSHAKE_EXPECTED"),
            ReturnCode::DuplicatedConnect => write!(f, "DUPLICATED_CONNECT"),
            ReturnCode::UnexpectedPacket => write!(f, "UNEXPECTED_PACKET"),
            ReturnCode::ProducerNameAlreadyExists => write!(f, "PRODUCER_NAME_ALREADY_EXISTS"),
            ReturnCode::ConsumerNameAlreadyExists => write!(f, "CONSUMER_NAME_ALREADY_EXISTS"),
            ReturnCode::ProducerNotFound => write!(f, "PRODUCER_NOT_FOUND"),
            ReturnCode::ConsumerNotFound => write!(f, "CONSUMER_NOT_FOUND"),
            ReturnCode::TopicNotFound => write!(f, "TOPIC_NOT_FOUND"),
            ReturnCode::ProducerAlreadyExclusive => write!(f, "PRODUCER_ALREADY_EXCLUSIVE"),
            ReturnCode::ProducerAccessModeMismatched => {
                write!(f, "PRODUCER_ACCESS_MODE_MISMATCHED")
            }
            ReturnCode::SubscriptionNotFound => write!(f, "SUBSCRIPTION_NOT_FOUND"),
            ReturnCode::ConsumerAlreadyExclusive => write!(f, "CONSUMER_ALREADY_EXCLUSIVE"),
            ReturnCode::ConsumerSubscriptionTypeMismatched => {
                write!(f, "CONSUMER_SUBSCRIPTION_TYPE_MISMATCHED")
            }
            ReturnCode::PartitionedTopicNotSupported => {
                write!(f, "PARTITIONED_TOPIC_NOT_SUPPORTED")
            }
            ReturnCode::BrokerUnavailable => write!(f, "BROKER_UNAVAILABLE"),
            ReturnCode::InvalidName => write!(f, "INVALID_NAME"),
            ReturnCode::Internal => write!(f, "INTERNAL"),
        }
    }
}
