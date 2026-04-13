// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! APT-specific request builders used by integration tests.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::{header, Method, Request};

/// Build an APT .deb upload multipart request.
pub fn apt_upload_request(
    repo: &str,
    filename: &str,
    data: &[u8],
    component: Option<&str>,
    token: &str,
) -> Request<Body> {
    apt_upload_request_dist(repo, filename, data, None, component, token)
}

pub fn apt_upload_request_dist(
    repo: &str,
    filename: &str,
    data: &[u8],
    distribution: Option<&str>,
    component: Option<&str>,
    token: &str,
) -> Request<Body> {
    let boundary = "----TestBoundary12345";
    let mut body = Vec::new();

    fn add_field(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }

    if let Some(dist) = distribution {
        add_field(&mut body, boundary, "distribution", dist);
    }
    if let Some(comp) = component {
        add_field(&mut body, boundary, "component", comp);
    }

    // File field
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    Request::builder()
        .method(Method::POST)
        .uri(format!("/apt/{}/upload", repo))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body))
        .unwrap()
}

/// Build a Nexus-compatible component upload request for APT.
pub fn nexus_apt_upload_request(
    repo: &str,
    filename: &str,
    distribution: &str,
    component: Option<&str>,
    data: &[u8],
    token: &str,
) -> Request<Body> {
    let boundary = "----TestBoundary12345";
    let mut body = Vec::new();

    // apt.asset.distribution text field
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"apt.asset.distribution\"\r\n\r\n{distribution}\r\n"
        )
        .as_bytes(),
    );
    // apt.asset.component text field (optional)
    if let Some(comp) = component {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"apt.asset.component\"\r\n\r\n{comp}\r\n"
            )
            .as_bytes(),
        );
    }
    // apt.asset file field
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"apt.asset\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(data);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    Request::builder()
        .method(Method::POST)
        .uri(format!("/service/rest/v1/components?repository={}", repo))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body))
        .unwrap()
}
