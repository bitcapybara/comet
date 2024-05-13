use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    Json,
};
use comet_common::{
    addr::ConnectAddress,
    error::{BoxedError, ResponsiveError},
    http::{ErrResponse, OkResponse},
};

use snafu::{IntoError, Location, ResultExt, Snafu};

use crate::{
    broker::{self, topic},
    meta::MetaStorage,
    scheme::StorageScheme,
};

use super::ServerState;

#[common_macro::stack_error_debug]
#[derive(Snafu)]
pub enum Error {
    #[snafu(display("meta error"))]
    Meta {
        source: comet_common::error::BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("storage error"))]
    Storage {
        source: comet_common::error::BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
    #[snafu(display("broker error"))]
    Broker {
        source: broker::Error,
        #[snafu(implicit)]
        location: Location,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        Into::<ErrResponse>::into(self).into_response()
    }
}

impl From<Error> for ErrResponse {
    fn from(error: Error) -> Self {
        match error {
            Error::Meta { source, .. } => source.as_response().into(),
            Error::Storage { source, .. } => source.as_response().into(),
            Error::Broker { source, .. } => source.as_response().into(),
        }
    }
}

#[tracing::instrument(skip(state))]
pub async fn lookup_topic<S>(
    Path(topic_name): Path<String>,
    State(state): State<ServerState<S>>,
) -> Result<OkResponse<Vec<ConnectAddress>>, Error>
where
    S: StorageScheme,
{
    let addresses = state
        .meta
        .get_topic_addresses(&topic_name)
        .await
        .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?;
    Ok(addresses.into())
}

#[tracing::instrument(skip(state))]
pub async fn create_topic<S>(
    State(state): State<ServerState<S>>,
    Path(topic_name): Path<String>,
    Json(config): Json<topic::Config>,
) -> Result<OkResponse, Error>
where
    S: StorageScheme,
{
    state
        .meta
        .create_topic(&topic_name, config)
        .await
        .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?;
    Ok(().into())
}

#[tracing::instrument(skip(state))]
pub async fn delete_topic<S>(
    Path(topic_name): Path<String>,
    State(state): State<ServerState<S>>,
) -> Result<OkResponse, Error>
where
    S: StorageScheme,
{
    // meta 中删除
    state
        .meta
        .delete_topic(&topic_name)
        .await
        .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?;

    // 存储中删除
    state
        .broker
        .delete_topic(&topic_name)
        .await
        .context(BrokerSnafu)?;
    Ok(().into())
}

#[tracing::instrument(skip(state))]
pub async fn lookup_http<SS>(
    State(state): State<ServerState<SS>>,
) -> Result<OkResponse<Vec<ConnectAddress>>, Error>
where
    SS: StorageScheme,
{
    let addresses = state
        .meta
        .get_server_http_addresses()
        .await
        .map_err(|e| MetaSnafu.into_error(BoxedError::new(e)))?;
    Ok(addresses.into())
}
