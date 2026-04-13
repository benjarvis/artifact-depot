// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Core error type for depot. HTTP/OpenAPI integration is feature-gated
//! so downstream crates that don't need axum or utoipa can avoid those deps.

use serde::Serialize;

/// Whether a storage error is likely transient and safe to retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryability {
    /// Permanent failure — retrying will not help.
    Permanent,
    /// Transient failure — caller may retry with backoff.
    Transient,
}

#[derive(Debug, thiserror::Error)]
pub enum DepotError {
    #[error("storage error: {0}")]
    Storage(
        #[source] Box<dyn std::error::Error + Send + Sync>,
        Retryability,
    ),

    #[error("{0}")]
    BadRequest(String),

    #[error("{0}")]
    Unauthorized(String),

    #[error("{0}")]
    Forbidden(String),

    #[error("{0}")]
    Conflict(String),

    #[error("{0}")]
    NotFound(String),

    #[error("{0}")]
    NotAllowed(String),

    #[error("upstream error: {0}")]
    Upstream(String),

    #[error("data integrity error: {0}")]
    DataIntegrity(String),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, DepotError>;

#[derive(Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

impl DepotError {
    #[cfg(feature = "http")]
    fn error_code(&self) -> &'static str {
        match self {
            DepotError::Storage(_, _) => "STORAGE_ERROR",
            DepotError::BadRequest(_) => "BAD_REQUEST",
            DepotError::Unauthorized(_) => "UNAUTHORIZED",
            DepotError::Forbidden(_) => "FORBIDDEN",
            DepotError::Conflict(_) => "CONFLICT",
            DepotError::NotFound(_) => "NOT_FOUND",
            DepotError::NotAllowed(_) => "NOT_ALLOWED",
            DepotError::Upstream(_) => "UPSTREAM_ERROR",
            DepotError::DataIntegrity(_) => "DATA_INTEGRITY_ERROR",
            DepotError::Internal(_) => "INTERNAL_ERROR",
        }
    }

    /// Returns `true` if this error is likely transient and the operation
    /// may succeed on retry.
    pub fn is_transient(&self) -> bool {
        matches!(self, DepotError::Storage(_, Retryability::Transient))
    }

    /// Wrap any `Error` as a permanent storage error.
    ///
    /// Use in crates that can't add `From` impls due to the orphan rule.
    pub fn storage_permanent(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Storage(Box::new(e), Retryability::Permanent)
    }

    /// Wrap any `Error` as a transient storage error.
    pub fn storage_transient(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Storage(Box::new(e), Retryability::Transient)
    }

    /// Wrap any displayable value as an upstream error.
    pub fn upstream(e: impl std::fmt::Display) -> Self {
        Self::Upstream(e.to_string())
    }
}

#[cfg(feature = "http")]
impl axum::response::IntoResponse for DepotError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        let status = match &self {
            DepotError::Storage(_, Retryability::Transient) => StatusCode::SERVICE_UNAVAILABLE,
            DepotError::Storage(_, Retryability::Permanent)
            | DepotError::DataIntegrity(_)
            | DepotError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            DepotError::BadRequest(_) => StatusCode::BAD_REQUEST,
            DepotError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            DepotError::Forbidden(_) => StatusCode::FORBIDDEN,
            DepotError::Conflict(_) => StatusCode::CONFLICT,
            DepotError::NotFound(_) => StatusCode::NOT_FOUND,
            DepotError::NotAllowed(_) => StatusCode::METHOD_NOT_ALLOWED,
            DepotError::Upstream(_) => StatusCode::BAD_GATEWAY,
        };
        if status.is_server_error() {
            tracing::warn!(status = %status, error = ?self, "server error response");
        }
        let body = ErrorResponse {
            error: ErrorBody {
                code: self.error_code().to_string(),
                message: self.to_string(),
            },
        };
        (status, axum::Json(body)).into_response()
    }
}

// --- Classification helpers ---

fn io_retryability(e: &std::io::Error) -> Retryability {
    use std::io::ErrorKind;
    match e.kind() {
        ErrorKind::ConnectionReset
        | ErrorKind::ConnectionAborted
        | ErrorKind::TimedOut
        | ErrorKind::Interrupted
        | ErrorKind::BrokenPipe
        | ErrorKind::WouldBlock => Retryability::Transient,
        _ => Retryability::Permanent,
    }
}

// --- From implementations (unconditional — always-available deps) ---

impl From<std::io::Error> for DepotError {
    fn from(e: std::io::Error) -> Self {
        let r = io_retryability(&e);
        DepotError::Storage(Box::new(e), r)
    }
}

macro_rules! impl_storage_error {
    ($($t:ty),+ $(,)?) => {
        $(impl From<$t> for DepotError {
            fn from(e: $t) -> Self {
                DepotError::Storage(Box::new(e), Retryability::Permanent)
            }
        })+
    };
}

impl_storage_error!(
    serde_json::Error,
    rmp_serde::encode::Error,
    rmp_serde::decode::Error,
    tokio::task::JoinError,
);

// --- From implementations (feature-gated — optional deps) ---

#[cfg(feature = "reqwest-errors")]
impl From<reqwest::Error> for DepotError {
    fn from(e: reqwest::Error) -> Self {
        DepotError::Upstream(e.to_string())
    }
}
