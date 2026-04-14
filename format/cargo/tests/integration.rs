// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::body::Body;
use axum::http::{header, Method, StatusCode};
use http_body_util::BodyExt;

use depot_test_support as helpers;
use depot_test_support::*;

// --- Helpers ---

fn build_cargo_publish_body(meta_json: &[u8], crate_data: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(meta_json.len() as u32).to_le_bytes());
    body.extend_from_slice(meta_json);
    body.extend_from_slice(&(crate_data.len() as u32).to_le_bytes());
    body.extend_from_slice(crate_data);
    body
}

async fn create_cargo_repo(app: &TestApp, name: &str) {
    app.create_repo(serde_json::json!({
        "name": name,
        "repo_type": "hosted",
        "format": "cargo",
    }))
    .await;
}

async fn create_cargo_proxy_repo(app: &TestApp, name: &str, members: Vec<&str>) {
    app.create_repo(serde_json::json!({
        "name": name,
        "repo_type": "proxy",
        "format": "cargo",
        "members": members,
    }))
    .await;
}

async fn publish_crate(app: &TestApp, repo: &str, name: &str, version: &str, data: &[u8]) {
    let meta = serde_json::json!({
        "name": name,
        "vers": version,
        "deps": [],
        "features": {},
        "links": null,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();

    // Build wire format: the publish handler computes sha256 itself,
    // but the CargoStore checks it matches meta.cksum which gets set by the handler.
    // Actually the handler sets cksum from computing sha256 of crate_bytes.
    // Let's build metadata without cksum and let the handler add it.
    // Wait — looking at the handler code more carefully: the handler computes sha256,
    // builds CrateVersionMeta with cksum = computed sha256, and then CargoStore::publish_crate
    // verifies meta.cksum == computed sha256. So it always matches for the publish handler.
    // We just need to provide valid wire format.

    let body = build_cargo_publish_body(&meta_bytes, data);
    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri(format!("/repository/{repo}/api/v1/crates/new"))
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap();
    let (status, resp_body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK, "publish failed: {resp_body}");
}

// ===========================================================================
// Validation
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nonexistent_repo_404() {
    let app = TestApp::new().await;
    helpers::assert_nonexistent_repo_404(&app, "/repository/no-such-repo/index/config.json").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_non_cargo_repo_400() {
    // Under the unified `/repository/{repo}/...` router, a cargo-shaped URL on
    // a raw repo falls through to the generic artifact lookup and returns 404
    // rather than 400 — URL shape is no longer tied to format.
    let app = TestApp::new().await;
    app.create_hosted_repo("raw-repo").await;
    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/raw-repo/index/config.json", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// config.json
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_config_json() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-test").await;
    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/cargo-test/index/config.json", &token);
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["dl"].as_str().unwrap().contains("cargo-test"));
    assert!(body["api"].as_str().unwrap().contains("cargo-test"));
}

// ===========================================================================
// Publish and Download
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_and_download() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-hosted").await;

    let crate_data = b"fake-crate-tarball-content";
    publish_crate(&app, "cargo-hosted", "my-crate", "1.0.0", crate_data).await;

    // Download
    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-hosted/api/v1/crates/my-crate/1.0.0/download",
        &token,
    );
    let (status, downloaded) = app.call_raw(req).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "download failed: {}",
        String::from_utf8_lossy(&downloaded)
    );
    assert_eq!(downloaded, crate_data);
}

// ===========================================================================
// Sparse Index
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sparse_index() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-idx").await;

    let crate_data = b"index-test-crate";
    publish_crate(&app, "cargo-idx", "serde", "1.0.0", crate_data).await;

    let token = app.admin_token();
    // serde -> prefix se/rd
    let req = app.auth_request(Method::GET, "/repository/cargo-idx/index/se/rd/serde", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&body_bytes);
    assert!(body.contains("\"name\":\"serde\""));
    assert!(body.contains("\"vers\":\"1.0.0\""));
    assert!(body.contains("\"cksum\":"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sparse_index_1char() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-1ch").await;

    publish_crate(&app, "cargo-1ch", "a", "0.1.0", b"one-char-crate").await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/cargo-1ch/index/1/a", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sparse_index_2char() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-2ch").await;

    publish_crate(&app, "cargo-2ch", "ab", "0.1.0", b"two-char-crate").await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/cargo-2ch/index/2/ab", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_sparse_index_3char() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-3ch").await;

    publish_crate(&app, "cargo-3ch", "abc", "0.1.0", b"three-char-crate").await;

    let token = app.admin_token();
    // abc -> prefix 3/a
    let req = app.auth_request(Method::GET, "/repository/cargo-3ch/index/3/a/abc", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Yank / Unyank
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_yank_unyank() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-yank").await;

    publish_crate(&app, "cargo-yank", "my-crate", "1.0.0", b"yank-test").await;

    let token = app.admin_token();

    // Yank
    let req = axum::http::Request::builder()
        .method(Method::DELETE)
        .uri("/repository/cargo-yank/api/v1/crates/my-crate/1.0.0/yank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);

    // Check index shows yanked=true
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-yank/index/my/cr/my-crate",
        &token,
    );
    let resp = app.call_resp(req).await;
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&body_bytes);
    assert!(
        body.contains("\"yanked\":true"),
        "expected yanked:true in: {body}"
    );

    // Unyank
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-yank/api/v1/crates/my-crate/1.0.0/unyank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);

    // Check index shows yanked=false
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-yank/index/my/cr/my-crate",
        &token,
    );
    let resp = app.call_resp(req).await;
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = String::from_utf8_lossy(&body_bytes);
    assert!(
        body.contains("\"yanked\":false"),
        "expected yanked:false in: {body}"
    );
}

// ===========================================================================
// Publish Validation
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_body_too_short() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-val").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-val/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(vec![0u8; 2]))
        .unwrap();
    let (status, body) = app.call(req).await;
    assert!(status.is_client_error() || status.is_server_error());
    assert!(body["errors"][0]["detail"].as_str().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_invalid_json() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-badjson").await;

    let token = app.admin_token();
    let bad_json = b"not valid json";
    let crate_data = b"crate";
    let mut body = Vec::new();
    body.extend_from_slice(&(bad_json.len() as u32).to_le_bytes());
    body.extend_from_slice(bad_json);
    body.extend_from_slice(&(crate_data.len() as u32).to_le_bytes());
    body.extend_from_slice(crate_data);

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-badjson/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(body))
        .unwrap();
    let (status, resp_body) = app.call(req).await;
    assert!(status.is_client_error());
    assert!(resp_body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("invalid metadata JSON"));
}

// ===========================================================================
// Search
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_search() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-search").await;

    publish_crate(&app, "cargo-search", "tokio", "1.0.0", b"tokio-crate").await;
    publish_crate(&app, "cargo-search", "serde", "1.0.0", b"serde-crate").await;

    let token = app.admin_token();

    // Search all
    let req = app.auth_request(Method::GET, "/repository/cargo-search/api/v1/crates", &token);
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["total"], 2);

    // Search with query
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-search/api/v1/crates?q=tok",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["meta"]["total"], 1);
}

// ===========================================================================
// Proxy repo
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_download() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-member").await;
    create_cargo_proxy_repo(&app, "cargo-proxy", vec!["cargo-member"]).await;

    let crate_data = b"proxy-test-crate";
    publish_crate(&app, "cargo-member", "proxy-crate", "0.1.0", crate_data).await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-proxy/api/v1/crates/proxy-crate/0.1.0/download",
        &token,
    );
    let (status, downloaded) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(downloaded, crate_data);
}

// ===========================================================================
// Publish with dependencies
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_with_deps() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-deps").await;

    let meta = serde_json::json!({
        "name": "my-lib",
        "vers": "0.2.0",
        "deps": [
            {
                "name": "serde",
                "version_req": "^1.0",
                "features": ["derive"],
                "optional": false,
                // default_features omitted — should default to true
                "kind": "normal",
            },
            {
                "name": "tokio",
                "version_req": "^1",
                "features": [],
                "optional": true,
                "default_features": false,
                // kind omitted — should default to "normal"
            },
            {
                "name": "criterion",
                "version_req": "^0.5",
                "features": [],
                "optional": false,
                "default_features": true,
                "kind": "dev",
                "target": "cfg(not(target_arch = \"wasm32\"))",
            },
            {
                // explicit_name_in_toml: the package is "serde_json" but exposed as "json"
                "name": "serde_json",
                "version_req": "^1",
                "features": [],
                "optional": false,
                "default_features": true,
                "kind": "normal",
                "explicit_name_in_toml": "json",
            },
        ],
        "features": {"default": ["serde"]},
        "links": null,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();
    let crate_data = b"deps-test-crate";
    let body = build_cargo_publish_body(&meta_bytes, crate_data);

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-deps/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap();
    let (status, resp_body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK, "publish failed: {resp_body}");

    // Verify deps appear in the sparse index
    let req = app.auth_request(Method::GET, "/repository/cargo-deps/index/my/li/my-lib", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);

    // The explicit_name_in_toml dep should have name="json" and package="serde_json"
    assert!(
        body_str.contains("\"name\":\"json\""),
        "expected renamed dep 'json' in: {body_str}"
    );
    assert!(
        body_str.contains("\"package\":\"serde_json\""),
        "expected package field in: {body_str}"
    );
    // Check that deps are present
    assert!(
        body_str.contains("\"name\":\"serde\""),
        "expected serde dep"
    );
    assert!(
        body_str.contains("\"name\":\"tokio\""),
        "expected tokio dep"
    );
    assert!(
        body_str.contains("\"kind\":\"dev\""),
        "expected dev dep kind"
    );
}

// ===========================================================================
// Proxy write paths (publish, yank, unyank through proxy)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_through_proxy() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-prx-member").await;
    create_cargo_proxy_repo(&app, "cargo-prx-pub", vec!["cargo-prx-member"]).await;

    let crate_data = b"proxy-publish-crate";
    // Publish through the proxy
    publish_crate(&app, "cargo-prx-pub", "proxy-pub", "1.0.0", crate_data).await;

    // Verify it landed in the hosted member
    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-prx-member/api/v1/crates/proxy-pub/1.0.0/download",
        &token,
    );
    let (status, downloaded) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(downloaded, crate_data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_yank_through_proxy() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-yk-member").await;
    create_cargo_proxy_repo(&app, "cargo-yk-proxy", vec!["cargo-yk-member"]).await;

    publish_crate(&app, "cargo-yk-member", "yk-crate", "1.0.0", b"yank-proxy").await;

    let token = app.admin_token();
    // Yank through the proxy
    let req = axum::http::Request::builder()
        .method(Method::DELETE)
        .uri("/repository/cargo-yk-proxy/api/v1/crates/yk-crate/1.0.0/yank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);

    // Verify yanked in hosted member's index
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-yk-member/index/yk/cr/yk-crate",
        &token,
    );
    let resp = app.call_resp(req).await;
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(
        body_str.contains("\"yanked\":true"),
        "expected yanked:true in: {body_str}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unyank_through_proxy() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-uyk-member").await;
    create_cargo_proxy_repo(&app, "cargo-uyk-proxy", vec!["cargo-uyk-member"]).await;

    publish_crate(
        &app,
        "cargo-uyk-member",
        "uyk-crate",
        "1.0.0",
        b"unyank-proxy",
    )
    .await;

    let token = app.admin_token();

    // Yank first (directly on hosted)
    let req = axum::http::Request::builder()
        .method(Method::DELETE)
        .uri("/repository/cargo-uyk-member/api/v1/crates/uyk-crate/1.0.0/yank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Unyank through the proxy
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-uyk-proxy/api/v1/crates/uyk-crate/1.0.0/unyank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["ok"], true);

    // Verify unyank in hosted member's index
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-uyk-member/index/uy/kc/uyk-crate",
        &token,
    );
    let resp = app.call_resp(req).await;
    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(
        body_str.contains("\"yanked\":false"),
        "expected yanked:false in: {body_str}"
    );
}

// ===========================================================================
// Proxy index entry
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_index_entry() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-pidx-member").await;
    create_cargo_proxy_repo(&app, "cargo-pidx-proxy", vec!["cargo-pidx-member"]).await;

    publish_crate(
        &app,
        "cargo-pidx-member",
        "pidx-crate",
        "2.0.0",
        b"proxy-index-test",
    )
    .await;

    let token = app.admin_token();
    // Fetch index through proxy (pidx-crate -> prefix pi/dx)
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-pidx-proxy/index/pi/dx/pidx-crate",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body_str = String::from_utf8_lossy(&body_bytes);
    assert!(body_str.contains("\"name\":\"pidx-crate\""));
    assert!(body_str.contains("\"vers\":\"2.0.0\""));
}

// ===========================================================================
// Error edge cases
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_truncated_crate() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-trunc").await;

    let meta = serde_json::json!({
        "name": "trunc-crate",
        "vers": "1.0.0",
        "deps": [],
        "features": {},
        "links": null,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();

    // Build wire format where json is valid but crate data is truncated:
    // json_len (4 bytes) + json + crate_len (4 bytes, claiming 1000 bytes) + only 2 bytes of data
    let mut body = Vec::new();
    body.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
    body.extend_from_slice(&meta_bytes);
    body.extend_from_slice(&(1000u32).to_le_bytes()); // claim 1000 bytes
    body.extend_from_slice(b"AB"); // only 2 bytes

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-trunc/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap();
    let (status, resp_body) = app.call(req).await;
    assert!(status.is_client_error(), "expected 4xx, got {status}");
    assert!(resp_body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("too short for crate data"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_download_not_found() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-dl404").await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-dl404/api/v1/crates/no-such-crate/1.0.0/download",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_yank_not_found() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-ynf").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::DELETE)
        .uri("/repository/cargo-ynf/api/v1/crates/no-such-crate/1.0.0/yank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

// ===========================================================================
// Cache repo rejection paths
// ===========================================================================

async fn create_cargo_cache_repo(app: &TestApp, name: &str, upstream_url: &str) {
    app.create_repo(serde_json::json!({
        "name": name,
        "repo_type": "cache",
        "format": "cargo",
        "upstream_url": upstream_url,
        "cache_ttl_secs": 3600,
    }))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_to_cache_rejected() {
    let app = TestApp::new().await;
    create_cargo_cache_repo(&app, "cargo-cache-pub", "https://example.com").await;

    let meta = serde_json::json!({
        "name": "bad-publish",
        "vers": "1.0.0",
        "deps": [],
        "features": {},
        "links": null,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();
    let body = build_cargo_publish_body(&meta_bytes, b"cache-reject");

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-cache-pub/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("only hosted and proxy repos accept uploads"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_yank_cache_rejected() {
    let app = TestApp::new().await;
    create_cargo_cache_repo(&app, "cargo-cache-yk", "https://example.com").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::DELETE)
        .uri("/repository/cargo-cache-yk/api/v1/crates/some-crate/1.0.0/yank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("only hosted and proxy repos accept uploads"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unyank_cache_rejected() {
    let app = TestApp::new().await;
    create_cargo_cache_repo(&app, "cargo-cache-uyk", "https://example.com").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-cache-uyk/api/v1/crates/some-crate/1.0.0/unyank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("only hosted and proxy repos accept uploads"));
}

// ===========================================================================
// Unyank not-found, proxy-download not-found, proxy no write member
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_unyank_not_found() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-uynf").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-uynf/api/v1/crates/no-such-crate/1.0.0/unyank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_download_not_found() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-pdl-member").await;
    create_cargo_proxy_repo(&app, "cargo-pdl-proxy", vec!["cargo-pdl-member"]).await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-pdl-proxy/api/v1/crates/nonexistent/1.0.0/download",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("not found in any member"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_no_write_member() {
    let app = TestApp::new().await;
    // Create a proxy whose only member is a raw repo (not cargo), so find_write_member fails
    app.create_hosted_repo("raw-nwm").await;
    create_cargo_proxy_repo(&app, "cargo-nwm", vec!["raw-nwm"]).await;

    let meta = serde_json::json!({
        "name": "no-target",
        "vers": "1.0.0",
        "deps": [],
        "features": {},
        "links": null,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();
    let body = build_cargo_publish_body(&meta_bytes, b"no-member-publish");

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-nwm/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(body))
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("no hosted cargo member"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_index_not_found() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-pinf-member").await;
    create_cargo_proxy_repo(&app, "cargo-pinf-proxy", vec!["cargo-pinf-member"]).await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-pinf-proxy/index/no/su/no-such-crate",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["errors"][0]["detail"]
        .as_str()
        .unwrap()
        .contains("not found"));
}

// ===========================================================================
// Auth required
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_write_requires_auth() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-auth").await;

    // Create a read-only user
    app.create_user_with_roles("reader", "pass", vec!["read-only"])
        .await;
    let reader_token = app.token_for("reader").await;

    let crate_data = b"auth-test";
    let meta = serde_json::json!({
        "name": "test-crate",
        "vers": "1.0.0",
        "deps": [],
        "features": {},
        "links": null,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();
    let body = build_cargo_publish_body(&meta_bytes, crate_data);

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-auth/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {reader_token}"))
        .body(Body::from(body))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// ===========================================================================
// Multi-version index entries
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_index_entry_multiple_versions() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-multi").await;

    publish_crate(&app, "cargo-multi", "mypkg", "0.1.0", b"crate-v1").await;
    publish_crate(&app, "cargo-multi", "mypkg", "0.2.0", b"crate-v2").await;
    publish_crate(&app, "cargo-multi", "mypkg", "1.0.0", b"crate-v3").await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/cargo-multi/index/my/pk/mypkg", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).unwrap();
    // Each version is a separate JSON line.
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3, "should have 3 index lines for 3 versions");
    assert!(text.contains("\"vers\":\"0.1.0\""));
    assert!(text.contains("\"vers\":\"0.2.0\""));
    assert!(text.contains("\"vers\":\"1.0.0\""));
}

// ===========================================================================
// Publish with features and links metadata
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_publish_with_features_and_links() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-feat").await;
    let token = app.admin_token();

    let meta = serde_json::json!({
        "name": "feat-crate",
        "vers": "1.0.0",
        "deps": [{
            "name": "serde",
            "version_req": "^1.0",
            "features": ["derive"],
            "optional": false,
            "default_features": true,
            "target": null,
            "kind": "normal",
        }],
        "features": {"default": ["serde/derive"], "full": ["default"]},
        "features2": {"unstable": ["dep:tokio"]},
        "links": "openssl",
        "rust_version": "1.70.0",
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();
    let crate_data = b"feat-crate-data";
    let body = build_cargo_publish_body(&meta_bytes, crate_data);

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-feat/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(body))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Verify the index entry contains features, links, and rust_version.
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-feat/index/fe/at/feat-crate",
        &token,
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).unwrap();
    let entry: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(entry["links"], "openssl");
    assert_eq!(entry["rust_version"], "1.70.0");
    assert!(entry["features"]["default"].is_array());
    assert!(entry["deps"].as_array().unwrap().len() == 1);
}

// ===========================================================================
// Publish through proxy, download + verify
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_publish_and_download() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-prx-m").await;
    create_cargo_proxy_repo(&app, "cargo-prx", vec!["cargo-prx-m"]).await;
    let token = app.admin_token();

    // Publish through the proxy.
    let crate_data = b"proxy-published-crate";
    let meta = serde_json::json!({
        "name": "prx-crate",
        "vers": "1.0.0",
        "deps": [],
        "features": {},
        "links": null,
    });
    let meta_bytes = serde_json::to_vec(&meta).unwrap();
    let body = build_cargo_publish_body(&meta_bytes, crate_data);

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/cargo-prx/api/v1/crates/new")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(body))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Download through the proxy.
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-prx/api/v1/crates/prx-crate/1.0.0/download",
        &token,
    );
    let (status, downloaded) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(downloaded, crate_data);

    // Verify index through proxy.
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-prx/index/pr/x-/prx-crate",
        &token,
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("prx-crate"));
}

// ===========================================================================
// Yank and verify download no longer works
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_yank_blocks_download() {
    let app = TestApp::new().await;
    create_cargo_repo(&app, "cargo-yank-dl").await;
    let token = app.admin_token();

    let crate_data = b"yankable-crate";
    publish_crate(&app, "cargo-yank-dl", "yankme", "1.0.0", crate_data).await;

    // Yank it.
    let req = axum::http::Request::builder()
        .method(Method::DELETE)
        .uri("/repository/cargo-yank-dl/api/v1/crates/yankme/1.0.0/yank")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Yanking is advisory in Cargo — downloads still work, but the index marks yanked=true.
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-yank-dl/api/v1/crates/yankme/1.0.0/download",
        &token,
    );
    let (status, downloaded) = app.call_raw(req).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "yanked crates are still downloadable"
    );
    assert_eq!(downloaded, crate_data);

    // Verify index shows yanked=true.
    let req = app.auth_request(
        Method::GET,
        "/repository/cargo-yank-dl/index/ya/nk/yankme",
        &token,
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body.to_vec()).unwrap();
    let entry: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(entry["yanked"], true);
}
