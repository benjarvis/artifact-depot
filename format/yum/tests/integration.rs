// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::http::{Method, StatusCode};
use http_body_util::BodyExt;
use serde_json::json;

use depot_format_apt::store::generate_gpg_keypair;
use depot_format_yum::store::build_synthetic_rpm;

mod common;

use common::*;
use depot_test_support as helpers;
use depot_test_support::*;

// ===========================================================================
// Validation
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nonexistent_repo_404() {
    let app = TestApp::new().await;
    helpers::assert_nonexistent_repo_404(&app, "/yum/no-such-repo/repodata/repomd.xml").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_non_yum_format_rejected() {
    let app = TestApp::new().await;
    helpers::assert_wrong_format_rejected(&app, "/yum/raw-repo/repodata/repomd.xml").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_to_cache_rejected() {
    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache", "http://localhost:19999/yum/fake", 300)
        .await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request("yum-cache", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ===========================================================================
// Hosted upload + metadata (core happy path)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_and_repomd() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-hosted").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "A test package").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request("yum-hosted", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch repomd.xml
    let req = app.auth_request(Method::GET, "/yum/yum-hosted/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let repomd = String::from_utf8(body.to_vec()).unwrap();

    assert!(repomd.contains("<repomd"), "missing repomd root element");
    assert!(
        repomd.contains("type=\"primary\""),
        "missing primary data entry"
    );
    assert!(
        repomd.contains("type=\"filelists\""),
        "missing filelists data entry"
    );
    assert!(
        repomd.contains("type=\"other\""),
        "missing other data entry"
    );
    assert!(repomd.contains("<revision>"), "missing revision element");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_and_primary_xml() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-primary").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "A test package").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request("yum-primary", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Get repomd.xml to find primary filename
    let req = app.auth_request(Method::GET, "/yum/yum-primary/repodata/repomd.xml", &token);
    let (_, repomd_body) = app.call_raw(req).await;
    let repomd = String::from_utf8(repomd_body).unwrap();

    // Extract primary filename from repomd
    let primary_href = repomd
        .lines()
        .find(|l| l.contains("primary.xml.gz"))
        .and_then(|l| {
            l.split("href=\"repodata/")
                .nth(1)
                .and_then(|s| s.split('"').next())
        })
        .unwrap();

    // Fetch primary.xml.gz
    let req = app.auth_request(
        Method::GET,
        &format!("/yum/yum-primary/repodata/{primary_href}"),
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let gz_bytes = resp.into_body().collect().await.unwrap().to_bytes();

    // Decompress and verify
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(&gz_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).unwrap();

    assert!(
        decompressed.contains("<name>hello</name>"),
        "missing package name in primary.xml"
    );
    assert!(
        decompressed.contains("ver=\"1.0\""),
        "missing version in primary.xml"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_package_download() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-dl").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request("yum-dl", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Download package (no directory → stored at root)
    let req = app.auth_request(Method::GET, "/yum/yum-dl/hello-1.0-1.x86_64.rpm", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        downloaded.to_vec(),
        rpm,
        "downloaded RPM should match uploaded"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_multiple_packages() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-multi").await;

    let token = app.admin_token();

    let rpm1 = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "Hello package").unwrap();
    let req = yum_upload_request("yum-multi", "hello-1.0-1.x86_64.rpm", &rpm1, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    let rpm2 = build_synthetic_rpm("goodbye", "2.1", "1", "x86_64", "Goodbye package").unwrap();
    let req = yum_upload_request("yum-multi", "goodbye-2.1-1.x86_64.rpm", &rpm2, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Check repomd.xml references primary with both packages
    let req = app.auth_request(Method::GET, "/yum/yum-multi/repodata/repomd.xml", &token);
    let (_, repomd_body) = app.call_raw(req).await;
    let repomd = String::from_utf8(repomd_body).unwrap();

    let primary_href = repomd
        .lines()
        .find(|l| l.contains("primary.xml.gz"))
        .and_then(|l| {
            l.split("href=\"repodata/")
                .nth(1)
                .and_then(|s| s.split('"').next())
        })
        .unwrap();

    let req = app.auth_request(
        Method::GET,
        &format!("/yum/yum-multi/repodata/{primary_href}"),
        &token,
    );
    let (_, gz_bytes) = app.call_raw(req).await;

    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(&gz_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).unwrap();

    assert!(
        decompressed.contains("<name>hello</name>"),
        "missing hello package"
    );
    assert!(
        decompressed.contains("<name>goodbye</name>"),
        "missing goodbye package"
    );
    assert!(
        decompressed.contains("packages=\"2\""),
        "package count should be 2"
    );
}

// ===========================================================================
// Snapshots
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_create_and_access() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-snap").await;

    let token = app.admin_token();

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-snap", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Trigger metadata build by fetching repomd
    let req = app.auth_request(Method::GET, "/yum/yum-snap/repodata/repomd.xml", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Create snapshot
    let req = app.json_request(
        Method::POST,
        "/yum/yum-snap/snapshots",
        &token,
        serde_json::json!({ "name": "v1" }),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Access snapshot repomd.xml
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-snap/snapshots/v1/repodata/repomd.xml",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let snap_repomd = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        snap_repomd.contains("<repomd"),
        "snapshot repomd should be valid"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_immutability() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-immut").await;

    let token = app.admin_token();

    let rpm1 = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-immut", "hello-1.0-1.x86_64.rpm", &rpm1, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Trigger metadata build
    let req = app.auth_request(Method::GET, "/yum/yum-immut/repodata/repomd.xml", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Create snapshot
    let req = app.json_request(
        Method::POST,
        "/yum/yum-immut/snapshots",
        &token,
        serde_json::json!({ "name": "v1" }),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Get snapshot primary filename
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-immut/snapshots/v1/repodata/repomd.xml",
        &token,
    );
    let (_, snap_repomd_bytes) = app.call_raw(req).await;
    let snap_repomd = String::from_utf8(snap_repomd_bytes).unwrap();

    // Upload another package
    let rpm2 = build_synthetic_rpm("goodbye", "2.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-immut", "goodbye-2.0-1.x86_64.rpm", &rpm2, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Snapshot should be unchanged
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-immut/snapshots/v1/repodata/repomd.xml",
        &token,
    );
    let (_, snap_repomd_bytes2) = app.call_raw(req).await;
    let snap_repomd2 = String::from_utf8(snap_repomd_bytes2).unwrap();

    assert_eq!(
        snap_repomd, snap_repomd2,
        "snapshot repomd should not change after new upload"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_empty_repo_fails() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-empty").await;

    let token = app.admin_token();

    // Try to create snapshot of repo with no metadata
    let req = app.json_request(
        Method::POST,
        "/yum/yum-empty/snapshots",
        &token,
        serde_json::json!({ "name": "v1" }),
    );
    let (status, _) = app.call(req).await;
    // Should fail because no metadata exists
    assert_ne!(status, StatusCode::CREATED);
}

// ===========================================================================
// Proxy
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_searches_members() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-h1").await;
    app.create_yum_proxy_repo("yum-proxy", vec!["yum-h1"]).await;

    let token = app.admin_token();

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-h1", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Download via proxy (no directory → stored at root)
    let req = app.auth_request(Method::GET, "/yum/yum-proxy/hello-1.0-1.x86_64.rpm", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.to_vec(), rpm);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_upload_routes_to_hosted() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-h2").await;
    app.create_yum_proxy_repo("yum-proxy2", vec!["yum-h2"])
        .await;

    let token = app.admin_token();

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-proxy2", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Verify it's in the hosted member (no directory → stored at root)
    let req = app.auth_request(Method::GET, "/yum/yum-h2/hello-1.0-1.x86_64.rpm", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Public key
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_public_key_not_found() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-nokey").await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-nokey/repodata/repomd.xml.key",
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Replace package
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_replace_package_updates_metadata() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-replace").await;

    let token = app.admin_token();

    // Upload v1
    let rpm1 = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "Version 1").unwrap();
    let req = yum_upload_request("yum-replace", "hello-1.0-1.x86_64.rpm", &rpm1, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch metadata to build it
    let req = app.auth_request(Method::GET, "/yum/yum-replace/repodata/repomd.xml", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Upload v2 with same filename
    let rpm2 = build_synthetic_rpm("hello", "2.0", "1", "x86_64", "Version 2").unwrap();
    let req = yum_upload_request("yum-replace", "hello-1.0-1.x86_64.rpm", &rpm2, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch updated metadata
    let req = app.auth_request(Method::GET, "/yum/yum-replace/repodata/repomd.xml", &token);
    let (_, repomd_body) = app.call_raw(req).await;
    let repomd = String::from_utf8(repomd_body).unwrap();

    let primary_href = repomd
        .lines()
        .find(|l| l.contains("primary.xml.gz"))
        .and_then(|l| {
            l.split("href=\"repodata/")
                .nth(1)
                .and_then(|s| s.split('"').next())
        })
        .unwrap();

    let req = app.auth_request(
        Method::GET,
        &format!("/yum/yum-replace/repodata/{primary_href}"),
        &token,
    );
    let (_, gz_bytes) = app.call_raw(req).await;

    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(&gz_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).unwrap();

    // Should have version 2.0 since we replaced
    assert!(
        decompressed.contains("ver=\"2.0\""),
        "should contain updated version"
    );
    assert!(
        decompressed.contains("packages=\"1\""),
        "should still have 1 package"
    );
}

// ===========================================================================
// Cache repo (wiremock upstream)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_serves_upstream_repomd() {
    let upstream = wiremock::MockServer::start().await;

    // Build a real repomd.xml to serve from the mock upstream
    let repomd = r#"<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo">
  <revision>1000</revision>
  <data type="primary">
    <checksum type="sha256">abc123</checksum>
    <location href="repodata/abc123-primary.xml.gz"/>
    <timestamp>1000</timestamp>
    <size>100</size>
  </data>
</repomd>
"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(repomd)
                .insert_header("Content-Type", "application/xml"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-up", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/yum/yum-cache-up/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("<repomd"), "should serve upstream repomd.xml");
    assert!(text.contains("<revision>1000</revision>"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_serves_cached_repomd_within_ttl() {
    let upstream = wiremock::MockServer::start().await;

    let repomd = r#"<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo">
  <revision>2000</revision>
</repomd>"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(repomd)
                .insert_header("Content-Type", "application/xml"),
        )
        .expect(1) // Should only be called once; second request is cached
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-ttl", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();

    // First fetch — hits upstream
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-ttl/repodata/repomd.xml",
        &token,
    );
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Second fetch — should be served from cache (wiremock expect(1) verifies)
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-ttl/repodata/repomd.xml",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("<revision>2000</revision>"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_stale_fallback_on_upstream_failure() {
    let upstream = wiremock::MockServer::start().await;

    let repomd = r#"<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo">
  <revision>3000</revision>
</repomd>"#;

    // First request succeeds
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(repomd)
                .insert_header("Content-Type", "application/xml"),
        )
        .up_to_n_times(1)
        .mount(&upstream)
        .await;

    // Subsequent requests return 500 (upstream down)
    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(wiremock::ResponseTemplate::new(500))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-stale", &upstream.uri(), 0)
        .await;

    let token = app.admin_token();

    // First fetch — seeds cache
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-stale/repodata/repomd.xml",
        &token,
    );
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Second fetch — upstream returns 500, should serve stale cache
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-stale/repodata/repomd.xml",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK, "should serve stale cache");
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("<revision>3000</revision>"),
        "stale cache should contain original data"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_serves_upstream_gz_file() {
    let upstream = wiremock::MockServer::start().await;

    // Build a real gzipped primary.xml
    let primary_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<metadata xmlns="http://linux.duke.edu/metadata/common" packages="0">
</metadata>"#;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut encoder, primary_xml.as_bytes()).unwrap();
    let gz_data = encoder.finish().unwrap();

    // Cache now fetches repomd.xml first, which references the .gz file.
    let repomd_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo">
  <data type="primary">
    <location href="repodata/abc123-primary.xml.gz"/>
  </data>
</repomd>"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(repomd_xml)
                .insert_header("Content-Type", "application/xml"),
        )
        .mount(&upstream)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/abc123-primary.xml.gz"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_bytes(gz_data.clone())
                .insert_header("Content-Type", "application/gzip"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-gz", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-gz/repodata/abc123-primary.xml.gz",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap(),
        "application/gzip"
    );
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.to_vec(), gz_data, "should serve upstream gz file");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_download_package_from_upstream() {
    let upstream = wiremock::MockServer::start().await;

    let rpm_data = build_synthetic_rpm("cached-pkg", "1.0", "1", "x86_64", "cached").unwrap();

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/Packages/c/cached-pkg-1.0-1.x86_64.rpm",
        ))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_bytes(rpm_data.clone())
                .insert_header("Content-Type", "application/x-rpm"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-dl", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-dl/Packages/c/cached-pkg-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.to_vec(), rpm_data, "should fetch RPM from upstream");

    // Second fetch should serve from cache (upstream only called once)
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-dl/Packages/c/cached-pkg-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "second fetch should serve from cache"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_download_package_not_found() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/Packages/m/missing-1.0-1.x86_64.rpm",
        ))
        .respond_with(wiremock::ResponseTemplate::new(404))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-404", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-404/Packages/m/missing-1.0-1.x86_64.rpm",
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Proxy metadata
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_serves_member_metadata() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-pm1").await;
    app.create_yum_proxy_repo("yum-pmeta", vec!["yum-pm1"])
        .await;

    let token = app.admin_token();

    let rpm = build_synthetic_rpm("proxy-meta", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-pm1", "proxy-meta-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch repomd.xml through proxy
    let req = app.auth_request(Method::GET, "/yum/yum-pmeta/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(text.contains("<repomd"), "proxy should serve member repomd");
    assert!(
        text.contains("type=\"primary\""),
        "proxy repomd should have primary"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_serves_member_repodata_file() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-prf1").await;
    app.create_yum_proxy_repo("yum-prfp", vec!["yum-prf1"])
        .await;

    let token = app.admin_token();

    let rpm = build_synthetic_rpm("proxy-file", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-prf1", "proxy-file-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Trigger metadata build via proxy
    let req = app.auth_request(Method::GET, "/yum/yum-prfp/repodata/repomd.xml", &token);
    let (_, repomd_body) = app.call_raw(req).await;
    let repomd = String::from_utf8(repomd_body).unwrap();

    // Extract primary filename
    let primary_href = repomd
        .lines()
        .find(|l| l.contains("primary.xml.gz"))
        .and_then(|l| {
            l.split("href=\"repodata/")
                .nth(1)
                .and_then(|s| s.split('"').next())
        })
        .unwrap();

    // Fetch primary.xml.gz through proxy
    let req = app.auth_request(
        Method::GET,
        &format!("/yum/yum-prfp/repodata/{primary_href}"),
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let gz_bytes = resp.into_body().collect().await.unwrap().to_bytes();

    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(&gz_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).unwrap();
    assert!(
        decompressed.contains("<name>proxy-file</name>"),
        "proxy should serve member primary.xml.gz"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_metadata_not_found_no_members() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-empty-member").await;
    app.create_yum_proxy_repo("yum-pnf", vec!["yum-empty-member"])
        .await;

    let token = app.admin_token();

    // No packages uploaded — proxy generates empty but valid merged metadata.
    let req = app.auth_request(Method::GET, "/yum/yum-pnf/repodata/repomd.xml", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).unwrap();
    assert!(
        text.contains("<repomd"),
        "empty proxy should generate valid repomd.xml"
    );
}

// ===========================================================================
// GPG signing
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_store_and_retrieve_signing_key() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-gpg").await;

    // Generate a key pair and store it via the YumStore
    let (priv_key, pub_key) = generate_gpg_keypair("yum-gpg").unwrap();

    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = depot_format_yum::store::YumStore {
        repo: "yum-gpg",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
        repodata_depth: 0,
    };
    store.store_signing_key(&priv_key, &pub_key).await.unwrap();

    // Retrieve signing key
    let retrieved = store.get_signing_key().await.unwrap();
    assert!(retrieved.is_some(), "signing key should be retrievable");
    assert_eq!(retrieved.unwrap(), priv_key);

    // Retrieve public key via API
    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/yum/yum-gpg/repodata/repomd.xml.key", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let key_text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        key_text.contains("PGP PUBLIC KEY"),
        "should return PGP public key"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_signed_metadata_has_asc() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-signed").await;

    // Store GPG key pair
    let (priv_key, pub_key) = generate_gpg_keypair("yum-signed").unwrap();
    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = depot_format_yum::store::YumStore {
        repo: "yum-signed",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
        repodata_depth: 0,
    };
    store.store_signing_key(&priv_key, &pub_key).await.unwrap();

    // Upload a package and trigger metadata rebuild
    let token = app.admin_token();
    let rpm = build_synthetic_rpm("signed-pkg", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-signed", "signed-pkg-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch repomd.xml (triggers rebuild with signing)
    let req = app.auth_request(Method::GET, "/yum/yum-signed/repodata/repomd.xml", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch repomd.xml.asc
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-signed/repodata/repomd.xml.asc",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let asc = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        asc.contains("PGP SIGNATURE"),
        "repomd.xml.asc should contain PGP signature, got: {}",
        &asc[..asc.len().min(100)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_with_gpg_signing() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-snap-gpg").await;

    // Store GPG key pair
    let (priv_key, pub_key) = generate_gpg_keypair("yum-snap-gpg").unwrap();
    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = depot_format_yum::store::YumStore {
        repo: "yum-snap-gpg",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
        repodata_depth: 0,
    };
    store.store_signing_key(&priv_key, &pub_key).await.unwrap();

    // Upload and trigger metadata build
    let token = app.admin_token();
    let rpm = build_synthetic_rpm("snap-gpg", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-snap-gpg", "snap-gpg-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    let req = app.auth_request(Method::GET, "/yum/yum-snap-gpg/repodata/repomd.xml", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Create snapshot (should re-sign)
    let req = app.json_request(
        Method::POST,
        "/yum/yum-snap-gpg/snapshots",
        &token,
        json!({ "name": "signed-v1" }),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Check snapshot repomd.xml.asc exists
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-snap-gpg/snapshots/signed-v1/repodata/repomd.xml.asc",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let asc = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        asc.contains("PGP SIGNATURE"),
        "snapshot repomd.xml.asc should be signed"
    );
}

// ===========================================================================
// Snapshot repodata file access
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_repodata_file_access() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-snap-file").await;

    let token = app.admin_token();
    let rpm = build_synthetic_rpm("snap-file", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-snap-file", "snap-file-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Build metadata
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-snap-file/repodata/repomd.xml",
        &token,
    );
    let (_, repomd_body) = app.call_raw(req).await;
    let repomd = String::from_utf8(repomd_body).unwrap();

    // Get primary filename
    let primary_href = repomd
        .lines()
        .find(|l| l.contains("primary.xml.gz"))
        .and_then(|l| {
            l.split("href=\"repodata/")
                .nth(1)
                .and_then(|s| s.split('"').next())
        })
        .unwrap();

    // Create snapshot
    let req = app.json_request(
        Method::POST,
        "/yum/yum-snap-file/snapshots",
        &token,
        json!({ "name": "file-v1" }),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Access primary.xml.gz through snapshot path
    let req = app.auth_request(
        Method::GET,
        &format!("/yum/yum-snap-file/snapshots/file-v1/repodata/{primary_href}"),
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let gz_bytes = resp.into_body().collect().await.unwrap().to_bytes();

    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(&gz_bytes[..]);
    let mut decompressed = String::new();
    decoder.read_to_string(&mut decompressed).unwrap();
    assert!(
        decompressed.contains("<name>snap-file</name>"),
        "snapshot primary.xml.gz should contain package data"
    );
}

// ===========================================================================
// Hosted repomd.xml.asc (unsigned)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repomd_asc_unsigned() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-asc").await;

    let token = app.admin_token();
    let rpm = build_synthetic_rpm("asc-pkg", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-asc", "asc-pkg-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Trigger metadata build
    let req = app.auth_request(Method::GET, "/yum/yum-asc/repodata/repomd.xml", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch .asc — should exist but be empty (no signing key)
    let req = app.auth_request(Method::GET, "/yum/yum-asc/repodata/repomd.xml.asc", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Cache repomd.xml.asc
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_repomd_asc() {
    let upstream = wiremock::MockServer::start().await;

    // Cache now fetches repomd.xml first, then repomd.xml.asc.
    let repomd_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo">
</repomd>"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(repomd_xml)
                .insert_header("Content-Type", "application/xml"),
        )
        .mount(&upstream)
        .await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml.asc"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string("-----BEGIN PGP SIGNATURE-----\nfake\n-----END PGP SIGNATURE-----")
                .insert_header("Content-Type", "application/pgp-signature"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-asc", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-cache-asc/repodata/repomd.xml.asc",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("PGP SIGNATURE"),
        "cache should serve upstream .asc"
    );
}

// ===========================================================================
// set_metadata_stale (direct call)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_set_metadata_stale_directly() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-stale-direct").await;

    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = depot_format_yum::store::YumStore {
        repo: "yum-stale-direct",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
        repodata_depth: 0,
    };

    // Initially not stale
    assert!(
        !store.is_metadata_stale().await.unwrap(),
        "should not be stale initially"
    );

    // Set stale explicitly
    store.set_metadata_stale().await.unwrap();
    assert!(
        store.is_metadata_stale().await.unwrap(),
        "should be stale after set_metadata_stale"
    );
}

// ===========================================================================
// Upload error paths
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_invalid_rpm() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-invalid").await;

    let token = app.admin_token();
    let garbage = b"this is not an rpm file at all";
    let req = yum_upload_request("yum-invalid", "garbage-1.0-1.x86_64.rpm", garbage, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_missing_file_field() {
    use axum::body::Body;
    use axum::http::{header, Request};

    let app = TestApp::new().await;
    app.create_yum_repo("yum-nofile").await;
    let token = app.admin_token();

    let boundary = "----TestBoundary12345";
    let body_data = format!("--{boundary}--\r\n");

    let req = Request::builder()
        .method(Method::POST)
        .uri("/yum/yum-nofile/upload")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            format!("multipart/form-data; boundary={}", boundary),
        )
        .body(Body::from(body_data))
        .unwrap();

    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ===========================================================================
// Proxy .asc route
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_repomd_asc() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-pasc-h").await;
    app.create_yum_proxy_repo("yum-pasc", vec!["yum-pasc-h"])
        .await;

    let token = app.admin_token();

    // Store signing key on the proxy (proxy generates its own .asc).
    let (priv_key, pub_key) = generate_gpg_keypair("yum-pasc").unwrap();
    let blobs = app.state.repo.blob_store("default").await.unwrap();
    let store = depot_format_yum::store::YumStore {
        repo: "yum-pasc",
        kv: app.state.repo.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: "default",
        updater: &app.state.repo.updater,
        repodata_depth: 0,
    };
    store.store_signing_key(&priv_key, &pub_key).await.unwrap();

    // Upload package and trigger metadata build
    let rpm = build_synthetic_rpm("pasc-pkg", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request("yum-pasc-h", "pasc-pkg-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Access .asc through proxy
    let req = app.auth_request(Method::GET, "/yum/yum-pasc/repodata/repomd.xml.asc", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let asc = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        asc.contains("PGP SIGNATURE"),
        "proxy should serve member .asc"
    );
}

// ===========================================================================
// Snapshot when metadata is stale (triggers rebuild before snapshot)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_rebuilds_stale_metadata() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-snap-stale").await;

    let token = app.admin_token();

    let rpm = build_synthetic_rpm("stale-snap", "1.0", "1", "x86_64", "test").unwrap();
    let req = yum_upload_request(
        "yum-snap-stale",
        "stale-snap-1.0-1.x86_64.rpm",
        &rpm,
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Do NOT fetch repomd.xml first — metadata is stale.
    // Create snapshot directly — should trigger rebuild internally.
    let req = app.json_request(
        Method::POST,
        "/yum/yum-snap-stale/snapshots",
        &token,
        json!({ "name": "auto-rebuild" }),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Snapshot metadata should be available
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-snap-stale/snapshots/auto-rebuild/repodata/repomd.xml",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Cache metadata not found
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_metadata_not_found() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(wiremock::ResponseTemplate::new(404))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_cache_repo("yum-cache-nf", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/yum/yum-cache-nf/repodata/repomd.xml", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Package not found in hosted repo
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hosted_package_not_found() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-pkg-nf").await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-pkg-nf/Packages/n/noexist-1.0-1.x86_64.rpm",
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Filelists and other.xml metadata
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_filelists_contains_file_entries() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-fl").await;

    let rpm = build_synthetic_rpm("filelists-test", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request("yum-fl", "filelists-test-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch repomd.xml to find filelists filename
    let req = app.auth_request(Method::GET, "/yum/yum-fl/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let repomd = String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    // Find the filelists href
    let fl_href = repomd
        .lines()
        .find(|l| l.contains("filelists.xml.gz"))
        .and_then(|l| l.split("href=\"repodata/").nth(1))
        .and_then(|s| s.split('"').next())
        .unwrap();

    // Fetch and decompress filelists
    let url = format!("/yum/yum-fl/repodata/{}", fl_href);
    let req = app.auth_request(Method::GET, &url, &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let compressed = resp.into_body().collect().await.unwrap().to_bytes();
    let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut xml = String::new();
    std::io::Read::read_to_string(&mut decoder, &mut xml).unwrap();
    assert!(
        xml.contains("<file>") || xml.contains("<file "),
        "filelists.xml should contain file entries, got: {}",
        &xml[..xml.len().min(500)]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_other_contains_changelogs() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-cl").await;

    let rpm = build_synthetic_rpm("changelog-test", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request("yum-cl", "changelog-test-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch repomd.xml to find other.xml filename
    let req = app.auth_request(Method::GET, "/yum/yum-cl/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    let repomd = String::from_utf8(
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let other_href = repomd
        .lines()
        .find(|l| l.contains("other.xml.gz"))
        .and_then(|l| l.split("href=\"repodata/").nth(1))
        .and_then(|s| s.split('"').next())
        .unwrap();

    // Fetch and decompress other.xml
    let url = format!("/yum/yum-cl/repodata/{}", other_href);
    let req = app.auth_request(Method::GET, &url, &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let compressed = resp.into_body().collect().await.unwrap().to_bytes();
    let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
    let mut xml = String::new();
    std::io::Read::read_to_string(&mut decoder, &mut xml).unwrap();
    assert!(
        xml.contains("<changelog"),
        "other.xml should contain changelog entries, got: {}",
        &xml[..xml.len().min(500)]
    );
}

// ===========================================================================
// Proxy with cache member — download RPM through group from cache upstream
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_downloads_rpm_from_cache_member() {
    let upstream = wiremock::MockServer::start().await;

    let rpm_data = build_synthetic_rpm("grp-cached", "1.0", "1", "x86_64", "group cache").unwrap();

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/Packages/g/grp-cached-1.0-1.x86_64.rpm",
        ))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_bytes(rpm_data.clone())
                .insert_header("Content-Type", "application/x-rpm"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_repo("yum-grp-h").await;
    app.create_yum_cache_repo("yum-grp-c", &upstream.uri(), 300)
        .await;
    app.create_yum_proxy_repo("yum-grp", vec!["yum-grp-h", "yum-grp-c"])
        .await;

    let token = app.admin_token();

    // Download through proxy — should find it via cache member's upstream.
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-grp/Packages/g/grp-cached-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.to_vec(), rpm_data);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_metadata_from_cache_member() {
    let upstream = wiremock::MockServer::start().await;

    let repomd = r#"<?xml version="1.0" encoding="UTF-8"?>
<repomd xmlns="http://linux.duke.edu/metadata/repo">
  <revision>9999</revision>
</repomd>"#;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repodata/repomd.xml"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string(repomd)
                .insert_header("Content-Type", "application/xml"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_yum_repo("yum-meta-h").await;
    app.create_yum_cache_repo("yum-meta-c", &upstream.uri(), 300)
        .await;

    let token = app.admin_token();

    // Prime the cache member's metadata so it's locally available.
    let req = app.auth_request(Method::GET, "/yum/yum-meta-c/repodata/repomd.xml", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Now create a proxy over both — cache member has metadata, hosted is empty.
    app.create_yum_proxy_repo("yum-meta-grp", vec!["yum-meta-h", "yum-meta-c"])
        .await;

    // Proxy generates merged metadata from all members.
    let req = app.auth_request(Method::GET, "/yum/yum-meta-grp/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        text.contains("<repomd"),
        "proxy should generate valid merged repomd.xml"
    );
}

// ===========================================================================
// Directory-based uploads (Nexus-style)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_with_directory() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-dir").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request_with_dir(
        "yum-dir",
        "hello-1.0-1.x86_64.rpm",
        "7/x86_64",
        &rpm,
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Download via /yum/ catch-all
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-dir/7/x86_64/hello-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.to_vec(), rpm);

    // Download via /repository/ path
    let req = app.auth_request(
        Method::GET,
        "/repository/yum-dir/7/x86_64/hello-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.to_vec(), rpm);

    // Verify metadata location href
    let req = app.auth_request(Method::GET, "/yum/yum-dir/repodata/repomd.xml", &token);
    let (_, repomd_body) = app.call_raw(req).await;
    let repomd = String::from_utf8(repomd_body).unwrap();
    let primary_href = repomd
        .lines()
        .find(|l| l.contains("primary.xml.gz"))
        .and_then(|l| {
            l.split("href=\"repodata/")
                .nth(1)
                .and_then(|s| s.split('"').next())
        })
        .unwrap();
    let req = app.auth_request(
        Method::GET,
        &format!("/yum/yum-dir/repodata/{primary_href}"),
        &token,
    );
    let resp = app.call_resp(req).await;
    let gz_bytes = resp.into_body().collect().await.unwrap().to_bytes();
    use flate2::read::GzDecoder;
    use std::io::Read;
    let mut decoder = GzDecoder::new(&gz_bytes[..]);
    let mut primary_xml = String::new();
    decoder.read_to_string(&mut primary_xml).unwrap();
    assert!(
        primary_xml.contains("href=\"7/x86_64/hello-1.0-1.x86_64.rpm\""),
        "location href should use user-specified directory, got: {}",
        primary_xml
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_without_directory_stores_at_root() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-nodir").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    // Upload without directory field
    let req = yum_upload_request("yum-nodir", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Download at root path (no directory prefix)
    let req = app.auth_request(Method::GET, "/yum/yum-nodir/hello-1.0-1.x86_64.rpm", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.to_vec(), rpm);
}

// ===========================================================================
// Nexus component upload
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_component_upload() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-nexus").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    let req = nexus_yum_upload_request(
        "yum-nexus",
        "hello-1.0-1.x86_64.rpm",
        "el9/x86_64",
        &rpm,
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Download via /repository/ path
    let req = app.auth_request(
        Method::GET,
        "/repository/yum-nexus/el9/x86_64/hello-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let downloaded = resp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(downloaded.to_vec(), rpm);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_component_upload_no_directory() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-nexus2").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();
    let req = nexus_yum_upload_request("yum-nexus2", "hello-1.0-1.x86_64.rpm", "", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Download at root
    let req = app.auth_request(
        Method::GET,
        "/repository/yum-nexus2/hello-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// PUT: non-RPM files blocked, RPM files accepted
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_non_rpm_blocked_for_yum_repo() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-noput").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/yum-noput/some/path/file.txt")
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", token),
        )
        .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
        .body(axum::body::Body::from(b"not an rpm".to_vec()))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

// ===========================================================================
// PUT upload tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_rpm_upload() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-put").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test put").unwrap();
    let token = app.admin_token();

    // PUT an RPM via /repository/{repo}/{path}
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/yum-put/p1/7/x86_64/hello-1.0-1.x86_64.rpm")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/x-rpm")
        .body(axum::body::Body::from(rpm))
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Verify repodata was generated
    let req = app.auth_request(Method::GET, "/yum/yum-put/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify primary.xml has the package with correct location href
    let req = app.auth_request(Method::GET, "/yum/yum-put/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let repomd = String::from_utf8(body.to_vec()).unwrap();
    let locations = depot_format_yum::store::parse_repomd_locations(&repomd);
    assert!(
        !locations.is_empty(),
        "repomd should reference metadata files"
    );

    // Download the package via the expected path
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-put/p1/7/x86_64/hello-1.0-1.x86_64.rpm",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_non_rpm_rejected() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-put-reject").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/yum-put-reject/README.txt")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "text/plain")
        .body(axum::body::Body::from("hello"))
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

// ===========================================================================
// Repodata depth tests
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repodata_depth_3() {
    let app = TestApp::new().await;
    app.create_yum_repo_with_depth("yum-depth3", 3).await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test depth").unwrap();
    let token = app.admin_token();

    // Upload via PUT at a 3-deep path
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/yum-depth3/p1/7/x86_64/hello-1.0-1.x86_64.rpm")
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/x-rpm")
        .body(axum::body::Body::from(rpm))
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Repodata should be at the prefixed path, not root
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-depth3/p1/7/x86_64/repodata/repomd.xml",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let repomd = String::from_utf8(body.to_vec()).unwrap();
    assert!(repomd.contains("<repomd"), "should be valid repomd XML");

    // Primary.xml should have relative hrefs (just filename, no prefix)
    let locations = depot_format_yum::store::parse_repomd_locations(&repomd);
    let primary_loc = locations.iter().find(|l| l.contains("primary")).unwrap();
    let prefixed_loc = format!("p1/7/x86_64/{primary_loc}");
    let req = app.auth_request(
        Method::GET,
        &format!("/yum/yum-depth3/{prefixed_loc}"),
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    // Decompress gzip
    let mut decoder = flate2::read::GzDecoder::new(body.as_ref());
    let mut primary_xml = String::new();
    std::io::Read::read_to_string(&mut decoder, &mut primary_xml).unwrap();
    // With depth=3, href should be just the filename (no p1/7/x86_64/ prefix)
    assert!(
        primary_xml.contains("href=\"hello-1.0-1.x86_64.rpm\""),
        "location href should be relative to prefix, got: {primary_xml}"
    );

    // Root repodata should NOT exist for depth=3 (only prefixed)
    let req = app.auth_request(Method::GET, "/yum/yum-depth3/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repodata_depth_0_default() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-depth0").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();

    let req = yum_upload_request("yum-depth0", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Root repodata should exist
    let req = app.auth_request(Method::GET, "/yum/yum-depth0/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repodata_depth_change() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-dchange").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();

    // Upload with default depth=0
    let req = yum_upload_request_with_dir(
        "yum-dchange",
        "hello-1.0-1.x86_64.rpm",
        "p1/7/x86_64",
        &rpm,
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Verify root repodata exists
    let req = app.auth_request(Method::GET, "/yum/yum-dchange/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Change depth to 3
    let req = app.json_request(
        Method::PUT,
        "/api/v1/repositories/yum-dchange",
        &token,
        json!({ "repodata_depth": 3 }),
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Wait for the background rebuild task to complete.  The depth-change
    // handler spawns the rebuild asynchronously, so poll until the new
    // prefixed metadata appears rather than relying on a fixed sleep.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let req = app.auth_request(
            Method::GET,
            "/yum/yum-dchange/p1/7/x86_64/repodata/repomd.xml",
            &token,
        );
        let resp = app.call_resp(req).await;
        if resp.status() == StatusCode::OK {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "prefixed repodata did not appear within 5s after depth change"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Old root repodata should be gone (deleted by depth change)
    let req = app.auth_request(Method::GET, "/yum/yum-dchange/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repodata_depth_fewer_segments() {
    let app = TestApp::new().await;
    app.create_yum_repo_with_depth("yum-fewer", 3).await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();

    // Upload with only 1 directory segment (fewer than depth=3)
    let req = yum_upload_request_with_dir("yum-fewer", "hello-1.0-1.x86_64.rpm", "a", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Repodata should be at a/repodata/ (using all available segments)
    let req = app.auth_request(Method::GET, "/yum/yum-fewer/a/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_repodata_depth_multiple_prefixes() {
    let app = TestApp::new().await;
    app.create_yum_repo_with_depth("yum-multi", 2).await;

    let rpm1 = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "test").unwrap();
    let rpm2 = build_synthetic_rpm("world", "2.0", "1", "x86_64", "test").unwrap();
    let token = app.admin_token();

    // Upload to two different 2-deep prefixes
    let req =
        yum_upload_request_with_dir("yum-multi", "hello-1.0-1.x86_64.rpm", "p1/7", &rpm1, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    let req =
        yum_upload_request_with_dir("yum-multi", "world-2.0-1.x86_64.rpm", "p2/7", &rpm2, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Both prefixes should have independent repodata
    let req = app.auth_request(
        Method::GET,
        "/yum/yum-multi/p1/7/repodata/repomd.xml",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    let req = app.auth_request(
        Method::GET,
        "/yum/yum-multi/p2/7/repodata/repomd.xml",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Metadata browse visibility
// ===========================================================================

/// After uploading an RPM and triggering a metadata rebuild, the `repodata`
/// directory must appear in the browse listing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_metadata_visible_in_browse() {
    let app = TestApp::new().await;
    app.create_yum_repo("yum-browse").await;

    let rpm = build_synthetic_rpm("hello", "1.0", "1", "x86_64", "A test package").unwrap();
    let token = app.admin_token();
    let req = yum_upload_request("yum-browse", "hello-1.0-1.x86_64.rpm", &rpm, &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Fetch repomd.xml to trigger metadata rebuild.
    let req = app.auth_request(Method::GET, "/yum/yum-browse/repodata/repomd.xml", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Poll the browse API until the `repodata` directory appears.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let req = app.auth_request(
            Method::GET,
            "/api/v1/repositories/yum-browse/artifacts",
            &token,
        );
        let (status, body) = app.call(req).await;
        assert_eq!(status, StatusCode::OK);

        let dirs = body["dirs"].as_array().unwrap();
        if dirs.iter().any(|d| d["name"].as_str() == Some("repodata")) {
            // Verify non-zero counts.
            let repodata = dirs
                .iter()
                .find(|d| d["name"].as_str() == Some("repodata"))
                .unwrap();
            assert!(
                repodata["artifact_count"].as_u64().unwrap() > 0,
                "repodata should have non-zero artifact_count"
            );
            break;
        }

        if tokio::time::Instant::now() > deadline {
            panic!("repodata directory did not appear in browse within 5s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
