// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::body::Body;
use axum::http::{header, Method, StatusCode};

use depot_format_golang::store::{decode_module_path, encode_module_path, GolangStore};

use depot_test_support::*;

// ===========================================================================
// Path encoding
// ===========================================================================

#[test]
fn test_encode_module_path() {
    assert_eq!(
        encode_module_path("github.com/Azure/sdk"),
        "github.com/!azure/sdk"
    );
    assert_eq!(encode_module_path("example.com/foo"), "example.com/foo");
    assert_eq!(
        encode_module_path("github.com/BurntSushi/toml"),
        "github.com/!burnt!sushi/toml"
    );
}

#[test]
fn test_decode_module_path() {
    assert_eq!(
        decode_module_path("github.com/!azure/sdk"),
        "github.com/Azure/sdk"
    );
    assert_eq!(decode_module_path("example.com/foo"), "example.com/foo");
    assert_eq!(
        decode_module_path("github.com/!burnt!sushi/toml"),
        "github.com/BurntSushi/toml"
    );
}

#[test]
fn test_encode_decode_roundtrip() {
    let paths = [
        "github.com/Azure/sdk",
        "example.com/foo",
        "github.com/BurntSushi/toml",
        "golang.org/x/tools",
    ];
    for path in &paths {
        assert_eq!(decode_module_path(&encode_module_path(path)), *path);
    }
}

// ===========================================================================
// Validation
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_format_validation() {
    let app = TestApp::new().await;
    app.create_hosted_repo("raw-repo").await;

    // Under the unified `/repository/{repo}/...` router, a golang-shaped URL on
    // a raw repo falls through to the generic artifact lookup and returns 404
    // rather than 400 — URL shape is no longer tied to format.
    let req = app.auth_request(
        Method::GET,
        "/repository/raw-repo/example.com/foo/@v/list",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_nonexistent_repo_404() {
    let app = TestApp::new().await;
    let req = app.auth_request(
        Method::GET,
        "/repository/no-such-repo/example.com/foo/@v/list",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Upload and download
// ===========================================================================

fn make_golang_upload_request(
    app: &TestApp,
    repo: &str,
    module: &str,
    version: &str,
    info: &[u8],
    gomod: &[u8],
    zip: &[u8],
) -> axum::http::Request<Body> {
    let token = app.admin_token();
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

    fn add_file_field(body: &mut Vec<u8>, boundary: &str, name: &str, filename: &str, data: &[u8]) {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(data);
        body.extend_from_slice(b"\r\n");
    }

    add_field(&mut body, boundary, "module", module);
    add_field(&mut body, boundary, "version", version);
    add_file_field(&mut body, boundary, "info", "info", info);
    add_file_field(&mut body, boundary, "mod", "go.mod", gomod);
    add_file_field(&mut body, boundary, "zip", "source.zip", zip);
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    axum::http::Request::builder()
        .method(Method::POST)
        .uri(format!("/repository/{}/upload", repo))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_upload_and_download() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-hosted").await;

    let module = "example.com/hello";
    let version = "v1.0.0";
    let info = br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#;
    let gomod = b"module example.com/hello\n\ngo 1.21\n";
    let zip = b"fake-zip-content";

    // Upload
    let req = make_golang_upload_request(&app, "go-hosted", module, version, info, gomod, zip);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Download .info
    let req = app.auth_request(
        Method::GET,
        "/repository/go-hosted/example.com/hello/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info);

    // Download .mod
    let req = app.auth_request(
        Method::GET,
        "/repository/go-hosted/example.com/hello/@v/v1.0.0.mod",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, gomod);

    // Download .zip
    let req = app.auth_request(
        Method::GET,
        "/repository/go-hosted/example.com/hello/@v/v1.0.0.zip",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, zip);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_list_versions() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-list").await;

    let module = "example.com/hello";

    // Upload two versions
    let req = make_golang_upload_request(
        &app,
        "go-list",
        module,
        "v1.0.0",
        br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#,
        b"module example.com/hello\n\ngo 1.21\n",
        b"zip-v1",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    let req = make_golang_upload_request(
        &app,
        "go-list",
        module,
        "v1.1.0",
        br#"{"Version":"v1.1.0","Time":"2024-02-01T00:00:00Z"}"#,
        b"module example.com/hello\n\ngo 1.21\n",
        b"zip-v2",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // List versions
    let req = app.auth_request(
        Method::GET,
        "/repository/go-list/example.com/hello/@v/list",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("v1.0.0"), "should contain v1.0.0: {}", text);
    assert!(text.contains("v1.1.0"), "should contain v1.1.0: {}", text);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_latest() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-latest").await;

    let module = "example.com/hello";
    let info_v2 = br#"{"Version":"v1.1.0","Time":"2024-02-01T00:00:00Z"}"#;

    // Upload v1.0.0
    let req = make_golang_upload_request(
        &app,
        "go-latest",
        module,
        "v1.0.0",
        br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#,
        b"module example.com/hello\n\ngo 1.21\n",
        b"zip-v1",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Upload v1.1.0
    let req = make_golang_upload_request(
        &app,
        "go-latest",
        module,
        "v1.1.0",
        info_v2,
        b"module example.com/hello\n\ngo 1.21\n",
        b"zip-v2",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // @latest should return v1.1.0 info
    let req = app.auth_request(
        Method::GET,
        "/repository/go-latest/example.com/hello/@latest",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info_v2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_case_encoding() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-case").await;

    let module = "github.com/Azure/sdk";
    let info = br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#;

    let req = make_golang_upload_request(
        &app,
        "go-case",
        module,
        "v1.0.0",
        info,
        b"module github.com/Azure/sdk\n\ngo 1.21\n",
        b"zip",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Download using encoded path (lowercase with ! prefix)
    let req = app.auth_request(
        Method::GET,
        "/repository/go-case/github.com/!azure/sdk/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info);
}

// ===========================================================================
// Upload error paths
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_upload_missing_module_400() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-err").await;

    let token = app.admin_token();
    let boundary = "----TestBoundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"version\"\r\n\r\nv1.0.0\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let req = axum::http::Request::builder()
        .method(Method::POST)
        .uri("/repository/go-err/upload")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_upload_missing_version_400() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-err-ver").await;

    let token = app.admin_token();
    let boundary = "----TestBoundary";
    let mut body = Vec::new();
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"module\"\r\n\r\nexample.com/foo\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let req = axum::http::Request::builder()
        .method(Method::POST)
        .uri("/repository/go-err-ver/upload")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_upload_to_cache_400() {
    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-noup", "http://localhost:1234", 300)
        .await;

    let req = make_golang_upload_request(
        &app,
        "go-cache-noup",
        "example.com/foo",
        "v1.0.0",
        b"{}",
        b"module example.com/foo",
        b"zip",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_upload_proxy_no_hosted_member_400() {
    let app = TestApp::new().await;
    // Create cache member (not hosted).
    app.create_golang_cache_repo("go-cache-only", "http://localhost:1234", 300)
        .await;
    // Create proxy with only cache member.
    app.create_golang_proxy_repo("go-proxy-noh", vec!["go-cache-only"])
        .await;

    let req = make_golang_upload_request(
        &app,
        "go-proxy-noh",
        "example.com/foo",
        "v1.0.0",
        b"{}",
        b"module",
        b"zip",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ===========================================================================
// Path parsing edge cases
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_invalid_path_returns_404() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-badpath").await;

    // No @v/ or @latest in path.
    let req = app.auth_request(
        Method::GET,
        "/repository/go-badpath/example.com/foo/bar",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_hosted_latest_no_versions_404() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-empty-latest").await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-empty-latest/example.com/foo/@latest",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_not_found_returns_404() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-empty").await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-empty/example.com/nonexistent/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Hosted repo (already tested above)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_repo() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-upstream").await;

    let module = "example.com/cached";
    let info = br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#;

    let req = make_golang_upload_request(
        &app,
        "go-upstream",
        module,
        "v1.0.0",
        info,
        b"module example.com/cached\n\ngo 1.21\n",
        b"zip-data",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    let req = app.auth_request(
        Method::GET,
        "/repository/go-upstream/example.com/cached/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info);
}

// ===========================================================================
// Proxy repo
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_proxy_repo() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-member").await;
    app.create_golang_proxy_repo("go-proxy", vec!["go-member"])
        .await;

    let module = "example.com/proxied";
    let info = br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#;

    let req = make_golang_upload_request(
        &app,
        "go-member",
        module,
        "v1.0.0",
        info,
        b"module example.com/proxied\n\ngo 1.21\n",
        b"zip-proxy",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Access via proxy.
    let req = app.auth_request(
        Method::GET,
        "/repository/go-proxy/example.com/proxied/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info);

    // List via proxy.
    let req = app.auth_request(
        Method::GET,
        "/repository/go-proxy/example.com/proxied/@v/list",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("v1.0.0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_upload_proxy_routes_to_hosted() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-proxy-member").await;
    app.create_golang_proxy_repo("go-proxy-up", vec!["go-proxy-member"])
        .await;

    let req = make_golang_upload_request(
        &app,
        "go-proxy-up",
        "example.com/through",
        "v1.0.0",
        br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#,
        b"module example.com/through\n\ngo 1.21\n",
        b"zip-through",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Verify it landed on the member.
    let req = app.auth_request(
        Method::GET,
        "/repository/go-proxy-member/example.com/through/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
}

// ===========================================================================
// Cache repo with wiremock upstream
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_info_fetches_upstream() {
    let upstream = wiremock::MockServer::start().await;
    let info_data = br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/example.com/foo/@v/v1.0.0.info"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(info_data.to_vec()))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-info", &upstream.uri(), 300)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-info/example.com/foo/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info_data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_mod_fetches_upstream() {
    let upstream = wiremock::MockServer::start().await;
    let mod_data = b"module example.com/foo\n\ngo 1.21\n";

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/example.com/foo/@v/v1.0.0.mod"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(mod_data.to_vec()))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-mod", &upstream.uri(), 300)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-mod/example.com/foo/@v/v1.0.0.mod",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, mod_data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_zip_fetches_upstream() {
    let upstream = wiremock::MockServer::start().await;
    let zip_data = b"fake-zip-bytes-for-testing";

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/example.com/foo/@v/v1.0.0.zip"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(zip_data.to_vec()))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-zip", &upstream.uri(), 300)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-zip/example.com/foo/@v/v1.0.0.zip",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, zip_data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_serves_from_local_cache() {
    let upstream = wiremock::MockServer::start().await;

    // Mock that should NOT be called (we expect local cache hit).
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/example.com/cached/@v/v1.0.0.info",
        ))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("upstream"))
        .expect(0)
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-local", &upstream.uri(), 300)
        .await;

    // Pre-populate cache directly via GolangStore.
    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = GolangStore {
        repo: "go-cache-local",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
    };
    let info_data = br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#;
    store
        .put_module_file("example.com/cached", "v1.0.0", "info", info_data)
        .await
        .unwrap();

    // Request should be served from local cache, not upstream.
    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-local/example.com/cached/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info_data);
    // wiremock will verify expect(0) on drop.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_upstream_404_returns_not_found() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/example.com/missing/@v/v1.0.0.info",
        ))
        .respond_with(wiremock::ResponseTemplate::new(404))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-404", &upstream.uri(), 300)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-404/example.com/missing/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_upstream_error() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/example.com/broken/@v/v1.0.0.info",
        ))
        .respond_with(wiremock::ResponseTemplate::new(500))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-500", &upstream.uri(), 300)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-500/example.com/broken/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    // Should return a 502 (upstream error).
    assert!(
        status == StatusCode::BAD_GATEWAY || status == StatusCode::INTERNAL_SERVER_ERROR,
        "expected 502 or 500, got {}",
        status
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_list_from_upstream() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/example.com/foo/@v/list"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("v1.0.0\nv1.1.0\n"))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-list", &upstream.uri(), 300)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-list/example.com/foo/@v/list",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("v1.0.0"));
    assert!(text.contains("v1.1.0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_list_fallback_to_local() {
    let app = TestApp::new().await;
    // Unreachable upstream.
    app.create_golang_cache_repo("go-cache-listfb", "http://127.0.0.1:1", 300)
        .await;

    // Pre-populate local cache.
    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = GolangStore {
        repo: "go-cache-listfb",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
    };
    store
        .put_module_file(
            "example.com/local",
            "v2.0.0",
            "info",
            br#"{"Version":"v2.0.0","Time":"2024-01-01T00:00:00Z"}"#,
        )
        .await
        .unwrap();

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-listfb/example.com/local/@v/list",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("v2.0.0"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_latest_from_upstream() {
    let upstream = wiremock::MockServer::start().await;
    let latest_json = br#"{"Version":"v2.0.0","Time":"2024-06-01T00:00:00Z"}"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/example.com/foo/@latest"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(latest_json.to_vec()))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-lat", &upstream.uri(), 300)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-lat/example.com/foo/@latest",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, latest_json);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_cache_latest_fallback_to_local() {
    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-latfb", "http://127.0.0.1:1", 300)
        .await;

    // Pre-populate local cache with a version.
    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = GolangStore {
        repo: "go-cache-latfb",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
    };
    let info = br#"{"Version":"v3.0.0","Time":"2024-01-01T00:00:00Z"}"#;
    store
        .put_module_file("example.com/local", "v3.0.0", "info", info)
        .await
        .unwrap();

    let req = app.auth_request(
        Method::GET,
        "/repository/go-cache-latfb/example.com/local/@latest",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info);
}

// ===========================================================================
// Proxy with cache member
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_proxy_with_cache_member() {
    let upstream = wiremock::MockServer::start().await;
    let info = br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/example.com/proxycache/@v/v1.0.0.info",
        ))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(info.to_vec()))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_golang_cache_repo("go-cache-mem", &upstream.uri(), 300)
        .await;
    app.create_golang_proxy_repo("go-proxy-cm", vec!["go-cache-mem"])
        .await;

    let req = app.auth_request(
        Method::GET,
        "/repository/go-proxy-cm/example.com/proxycache/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, info);
}

// ===========================================================================
// Delete module version (repo layer, called directly)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_delete_module_version() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-del").await;

    // Upload a module.
    let req = make_golang_upload_request(
        &app,
        "go-del",
        "example.com/del",
        "v1.0.0",
        br#"{"Version":"v1.0.0","Time":"2024-01-01T00:00:00Z"}"#,
        b"module example.com/del\n\ngo 1.21\n",
        b"zip-del",
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Verify it exists.
    let req = app.auth_request(
        Method::GET,
        "/repository/go-del/example.com/del/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Delete via GolangStore directly.
    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = GolangStore {
        repo: "go-del",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
    };
    let deleted = store
        .delete_module_version("example.com/del", "v1.0.0")
        .await
        .unwrap();
    assert!(deleted);

    // Verify it's gone.
    let req = app.auth_request(
        Method::GET,
        "/repository/go-del/example.com/del/@v/v1.0.0.info",
        &app.admin_token(),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_delete_nonexistent_returns_false() {
    let app = TestApp::new().await;
    app.create_golang_repo("go-del-ne").await;

    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = GolangStore {
        repo: "go-del-ne",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
    };
    let deleted = store
        .delete_module_version("example.com/nope", "v1.0.0")
        .await
        .unwrap();
    assert!(!deleted);
}

// ===========================================================================
// fetch_upstream_file unit tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_fetch_upstream_file_success() {
    let upstream = wiremock::MockServer::start().await;
    let data = b"some-upstream-data";

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/example.com/mod/@v/v1.0.0.info"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_bytes(data.to_vec()))
        .mount(&upstream)
        .await;

    let http = reqwest::Client::new();
    let result = depot_format_golang::store::fetch_upstream_file(
        &http,
        &upstream.uri(),
        "example.com/mod",
        "@v/v1.0.0.info",
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, Some(data.to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_fetch_upstream_file_not_found() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/example.com/missing/@v/v1.0.0.info",
        ))
        .respond_with(wiremock::ResponseTemplate::new(404))
        .mount(&upstream)
        .await;

    let http = reqwest::Client::new();
    let result = depot_format_golang::store::fetch_upstream_file(
        &http,
        &upstream.uri(),
        "example.com/missing",
        "@v/v1.0.0.info",
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_fetch_upstream_file_gone() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/example.com/old/@v/v0.1.0.info"))
        .respond_with(wiremock::ResponseTemplate::new(410))
        .mount(&upstream)
        .await;

    let http = reqwest::Client::new();
    let result = depot_format_golang::store::fetch_upstream_file(
        &http,
        &upstream.uri(),
        "example.com/old",
        "@v/v0.1.0.info",
        None,
    )
    .await
    .unwrap();

    assert_eq!(result, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_golang_fetch_upstream_file_server_error() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/example.com/broken/@v/v1.0.0.info",
        ))
        .respond_with(wiremock::ResponseTemplate::new(500))
        .mount(&upstream)
        .await;

    let http = reqwest::Client::new();
    let result = depot_format_golang::store::fetch_upstream_file(
        &http,
        &upstream.uri(),
        "example.com/broken",
        "@v/v1.0.0.info",
        None,
    )
    .await;

    assert!(result.is_err());
}
