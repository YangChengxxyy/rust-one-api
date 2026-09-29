use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Application error rendered as an OpenAI-style error body.
#[derive(Debug)]
pub struct AppError {
    pub status: StatusCode,
    pub message: String,
    pub kind: &'static str,
}

impl AppError {
    pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self { status, message: message.into(), kind: "internal_error" }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self { status: StatusCode::UNAUTHORIZED, message: message.into(), kind: "authentication_error" }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self { status: StatusCode::FORBIDDEN, message: message.into(), kind: "permission_error" }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: message.into(), kind: "invalid_request_error" }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: message.into(), kind: "not_found_error" }
    }

    pub fn upstream(message: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_GATEWAY, message: message.into(), kind: "upstream_error" }
    }

    pub fn quota_exceeded(message: impl Into<String>) -> Self {
        Self { status: StatusCode::FORBIDDEN, message: message.into(), kind: "quota_exceeded" }
    }
}

impl From<anyhow::Error> for AppError {
    fn from(e: anyhow::Error) -> Self {
        AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        AppError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let body = json!({
            "error": {
                "message": self.message,
                "type": self.kind,
            }
        });
        (self.status, Json(body)).into_response()
    }
}
