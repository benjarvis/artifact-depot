// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::http::StatusCode;
use axum::response::IntoResponse;
use http_body_util::BodyExt;

use depot_core::error::{DepotError, Retryability};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_all_error_status_codes() {
    let cases: Vec<(DepotError, StatusCode)> = vec![
        (
            DepotError::Storage(
                Box::new(std::io::Error::other("disk failure")),
                Retryability::Permanent,
            ),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            DepotError::BadRequest("bad input".into()),
            StatusCode::BAD_REQUEST,
        ),
        (
            DepotError::Unauthorized("no token".into()),
            StatusCode::UNAUTHORIZED,
        ),
        (
            DepotError::Forbidden("access denied".into()),
            StatusCode::FORBIDDEN,
        ),
        (
            DepotError::Conflict("duplicate".into()),
            StatusCode::CONFLICT,
        ),
        (
            DepotError::NotFound("missing".into()),
            StatusCode::NOT_FOUND,
        ),
        (
            DepotError::NotAllowed("wrong method".into()),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            DepotError::Upstream("remote down".into()),
            StatusCode::BAD_GATEWAY,
        ),
        (
            DepotError::DataIntegrity("hash mismatch".into()),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        (
            DepotError::Internal("something broke".into()),
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];

    for (error, expected_status) in cases {
        let desc = format!("{:?}", error);
        let resp = error.into_response();
        assert_eq!(resp.status(), expected_status, "wrong status for {}", desc);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_all_error_codes_in_body() {
    let cases: Vec<(DepotError, &str)> = vec![
        (
            DepotError::Storage(
                Box::new(std::io::Error::other("x")),
                Retryability::Permanent,
            ),
            "STORAGE_ERROR",
        ),
        (DepotError::BadRequest("x".into()), "BAD_REQUEST"),
        (DepotError::Unauthorized("x".into()), "UNAUTHORIZED"),
        (DepotError::Forbidden("x".into()), "FORBIDDEN"),
        (DepotError::Conflict("x".into()), "CONFLICT"),
        (DepotError::NotFound("x".into()), "NOT_FOUND"),
        (DepotError::NotAllowed("x".into()), "NOT_ALLOWED"),
        (DepotError::Upstream("x".into()), "UPSTREAM_ERROR"),
        (
            DepotError::DataIntegrity("x".into()),
            "DATA_INTEGRITY_ERROR",
        ),
        (DepotError::Internal("x".into()), "INTERNAL_ERROR"),
    ];

    for (error, expected_code) in cases {
        let resp = error.into_response();
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(
            json["error"]["code"].as_str().unwrap(),
            expected_code,
            "wrong code for {}",
            expected_code
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_error_message_preserved() {
    let cases: Vec<(DepotError, &str)> = vec![
        (
            DepotError::BadRequest("the request was bad".into()),
            "the request was bad",
        ),
        (
            DepotError::NotFound("item xyz not found".into()),
            "item xyz not found",
        ),
        (
            DepotError::Upstream("connection refused".into()),
            "connection refused",
        ),
    ];

    for (error, expected_msg) in cases {
        let resp = error.into_response();
        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        let msg = json["error"]["message"].as_str().unwrap();
        assert!(
            msg.contains(expected_msg),
            "message '{}' should contain '{}'",
            msg,
            expected_msg
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_transient_storage_returns_503() {
    let err = DepotError::Storage(
        Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")),
        Retryability::Transient,
    );
    let resp = err.into_response();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_permanent_storage_returns_500() {
    let err = DepotError::Storage(
        Box::new(std::io::Error::other("disk failure")),
        Retryability::Permanent,
    );
    let resp = err.into_response();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[test]
fn test_is_transient() {
    let transient = DepotError::Storage(
        Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")),
        Retryability::Transient,
    );
    assert!(transient.is_transient());

    let permanent = DepotError::Storage(
        Box::new(std::io::Error::other("disk")),
        Retryability::Permanent,
    );
    assert!(!permanent.is_transient());

    assert!(!DepotError::NotFound("x".into()).is_transient());
}
