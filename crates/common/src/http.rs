pub mod api;
pub mod topic;

use axum::{http::StatusCode, response::IntoResponse, Json};

use crate::protocol::response::{Response, ReturnCode};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ErrResponse {
    pub code: ReturnCode,
    pub message: Option<String>,
}

impl From<Response> for ErrResponse {
    fn from(value: Response) -> Self {
        Self {
            code: value.code,
            message: value.message,
        }
    }
}

impl IntoResponse for ErrResponse {
    fn into_response(self) -> axum::response::Response {
        (Into::<StatusCode>::into(self.code.clone()), Json(self)).into_response()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct OkResponse<T = ()> {
    pub code: ReturnCode,
    pub data: T,
}

impl<T: serde::Serialize> From<T> for OkResponse<T> {
    fn from(data: T) -> Self {
        Self {
            code: ReturnCode::Success,
            data,
        }
    }
}

impl<T: serde::Serialize> IntoResponse for OkResponse<T> {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Debug)]
pub struct StatusOkResponse<T = ()> {
    pub code: StatusCode,
    pub data: T,
}

impl<T: serde::Serialize> IntoResponse for StatusOkResponse<T> {
    fn into_response(self) -> axum::response::Response {
        (
            self.code,
            Json(OkResponse {
                code: ReturnCode::Success,
                data: self.data,
            }),
        )
            .into_response()
    }
}

impl<T> From<(StatusCode, T)> for StatusOkResponse<T> {
    fn from((code, data): (StatusCode, T)) -> Self {
        Self { code, data }
    }
}

impl From<StatusCode> for StatusOkResponse<()> {
    fn from(code: StatusCode) -> Self {
        Self { code, data: () }
    }
}
