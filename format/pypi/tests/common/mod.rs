// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PyPI-specific request builders used by integration tests.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::{header, Method, Request};

use depot_test_support::TestApp;

/// Build a PyPI multipart upload request.
pub fn pypi_upload_request(
    app: &TestApp,
    repo: &str,
    name: &str,
    version: &str,
    filename: &str,
    data: &[u8],
    extras: &[(&str, &str)],
) -> Request<Body> {
    let token = app.admin_token();
    let boundary = "----TestBoundary12345";
    let mut body = Vec::new();

    // Helper to add a text field
    fn add_field(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }

    add_field(&mut body, boundary, ":action", "file_upload");
    add_field(&mut body, boundary, "name", name);
    add_field(&mut body, boundary, "version", version);
    for (k, v) in extras {
        add_field(&mut body, boundary, k, v);
    }

    // File field
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    Request::builder()
        .method(Method::POST)
        .uri(format!("/repository/{}/", repo))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body))
        .unwrap()
}
