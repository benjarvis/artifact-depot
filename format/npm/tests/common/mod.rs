// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! NPM-specific request builders used by integration tests.

#![allow(dead_code)]

use axum::body::Body;
use axum::http::{header, Method, Request};
use base64::Engine;
use serde_json::{json, Value};

use depot_test_support::TestApp;

/// Build an npm publish request body (PUT JSON with base64-encoded tarball).
pub fn build_npm_publish_body(name: &str, version: &str, tarball_data: &[u8]) -> Value {
    let b64 = base64::engine::general_purpose::STANDARD.encode(tarball_data);
    // For scoped packages (@scope/pkg), filename uses just the pkg part
    let bare_name = name.rsplit('/').next().unwrap_or(name);
    let filename = format!("{}-{}.tgz", bare_name, version);
    json!({
        "name": name,
        "versions": {
            version: {
                "name": name,
                "version": version,
                "dist": {
                    "tarball": format!("placeholder/{filename}"),
                }
            }
        },
        "dist-tags": {
            "latest": version,
        },
        "_attachments": {
            &filename: {
                "data": b64,
                "length": tarball_data.len(),
            }
        }
    })
}

/// Build and send an npm publish request.
pub fn npm_publish_request(
    app: &TestApp,
    repo: &str,
    name: &str,
    version: &str,
    tarball_data: &[u8],
) -> Request<Body> {
    let token = app.admin_token();
    let body = build_npm_publish_body(name, version, tarball_data);
    let path = format!("/repository/{}/{}", repo, name);
    Request::builder()
        .method(Method::PUT)
        .uri(&path)
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}
