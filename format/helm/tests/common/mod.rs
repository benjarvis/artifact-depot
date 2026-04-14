// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Helm-specific request builders used by integration tests.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::{header, Method, Request};

/// Build a multipart file upload request with a single file field.
fn multipart_file_upload_request(
    url_path: &str,
    field_name: &str,
    filename: &str,
    data: &[u8],
    token: &str,
) -> Request<Body> {
    let boundary = "----TestBoundary12345";
    let mut body = Vec::new();

    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{field_name}\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    Request::builder()
        .method(Method::POST)
        .uri(url_path)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body))
        .unwrap()
}

/// Build a Helm chart upload multipart request.
pub fn helm_upload_request(repo: &str, filename: &str, data: &[u8], token: &str) -> Request<Body> {
    multipart_file_upload_request(
        &format!("/repository/{}/upload", repo),
        "chart",
        filename,
        data,
        token,
    )
}
