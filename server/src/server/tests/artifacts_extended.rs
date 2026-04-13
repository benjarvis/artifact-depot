// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::body::Body;
use axum::http::{header, Method, StatusCode};
use serde_json::json;

use depot_test_support::*;

// ===========================================================================
// Download variations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_download_nonexistent_repo_404() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/no-such-repo/file.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_download_nonexistent_artifact_404() {
    let app = TestApp::new().await;
    app.create_hosted_repo("dl-repo").await;
    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/dl-repo/missing.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_download_from_proxy_repo_searches_members() {
    let app = TestApp::new().await;
    app.create_hosted_repo("proxy-member-1").await;
    app.create_hosted_repo("proxy-member-2").await;

    // Upload to member-2 only
    let token = app.admin_token();
    app.upload_artifact("proxy-member-2", "data.bin", b"found-it", &token)
        .await;

    // Create proxy
    app.create_repo(json!({
        "name": "raw-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["proxy-member-1", "proxy-member-2"],
    }))
    .await;

    let req = app.auth_request(Method::GET, "/repository/raw-proxy/data.bin", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"found-it");
}

// ===========================================================================
// Upload variations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_to_hosted_repo() {
    let app = TestApp::new().await;
    app.create_hosted_repo("up-hosted").await;
    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/up-hosted/test.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from(b"hello".to_vec()))
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["path"].as_str().unwrap(), "test.txt");
    assert_eq!(body["size"].as_u64().unwrap(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_to_proxy_without_write_member_blocked() {
    let app = TestApp::new().await;
    app.create_hosted_repo("proxy-target").await;
    app.create_repo(json!({
        "name": "raw-proxy-w",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["proxy-target"],
    }))
    .await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/raw-proxy-w/file.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(b"proxy-write".to_vec()))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_to_proxy_with_write_member() {
    let app = TestApp::new().await;
    app.create_hosted_repo("proxy-wm-target").await;
    app.create_repo(json!({
        "name": "raw-proxy-wm",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["proxy-wm-target"],
        "write_member": "proxy-wm-target",
    }))
    .await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/raw-proxy-wm/file.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(b"proxy-write".to_vec()))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Verify it landed on the hosted member.
    let req = app.auth_request(Method::GET, "/repository/proxy-wm-target/file.txt", &token);
    let (status, data) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(data, b"proxy-write");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_to_cache_returns_405() {
    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "raw-cache",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": "http://localhost:1234",
    }))
    .await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/raw-cache/file.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(b"data".to_vec()))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_to_proxy_no_hosted_member_405() {
    let app = TestApp::new().await;
    // Create proxy with a cache member (no hosted).
    app.create_repo(json!({
        "name": "cache-only",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": "http://localhost:1234",
    }))
    .await;
    app.create_repo(json!({
        "name": "proxy-no-hosted",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["cache-only"],
    }))
    .await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/proxy-no-hosted/file.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(b"data".to_vec()))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_nonexistent_repo_404() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/ghost-repo/file.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::from(b"data".to_vec()))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_upload_preserves_content_type() {
    let app = TestApp::new().await;
    app.create_hosted_repo("ct-repo").await;
    let token = app.admin_token();

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/ct-repo/doc.pdf")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/pdf")
        .body(Body::from(b"fakepdf".to_vec()))
        .unwrap();
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["content_type"].as_str().unwrap(), "application/pdf");

    // Verify download content-type header
    let req = app.auth_request(Method::GET, "/repository/ct-repo/doc.pdf", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap(),
        "application/pdf"
    );
}

// ===========================================================================
// Delete variations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_from_hosted_204() {
    let app = TestApp::new().await;
    app.create_hosted_repo("del-hosted").await;
    let token = app.admin_token();
    app.upload_artifact("del-hosted", "file.txt", b"data", &token)
        .await;
    let req = app.auth_request(Method::DELETE, "/repository/del-hosted/file.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_from_cache_succeeds() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/repository/del-cache-up/file.txt",
        ))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string("cached data")
                .insert_header("Content-Type", "text/plain"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "del-cache",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/del-cache-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;
    let token = app.admin_token();

    // Populate the cache.
    let req = app.auth_request(Method::GET, "/repository/del-cache/file.txt", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"cached data");

    // Delete the cached artifact.
    let req = app.auth_request(Method::DELETE, "/repository/del-cache/file.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Deleting again gives 404.
    let req = app.auth_request(Method::DELETE, "/repository/del-cache/file.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_from_proxy_delegates_to_write_member() {
    let app = TestApp::new().await;
    app.create_hosted_repo("del-proxy-member").await;
    app.create_repo(json!({
        "name": "del-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["del-proxy-member"],
    }))
    .await;
    let token = app.admin_token();

    // Upload to the hosted member, then delete via the proxy.
    app.upload_artifact("del-proxy-member", "file.txt", b"data", &token)
        .await;
    let req = app.auth_request(Method::DELETE, "/repository/del-proxy/file.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Deleting again gives 404.
    let req = app.auth_request(Method::DELETE, "/repository/del-proxy/file.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_from_proxy_removes_from_all_members() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/repository/del-multi-up/file.txt",
        ))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string("upstream data")
                .insert_header("Content-Type", "text/plain"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    let token = app.admin_token();

    // Two hosted members and one cache member, all with the same artifact.
    app.create_hosted_repo("del-multi-h1").await;
    app.create_hosted_repo("del-multi-h2").await;
    app.create_repo(json!({
        "name": "del-multi-cache",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/del-multi-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    app.upload_artifact("del-multi-h1", "file.txt", b"data1", &token)
        .await;
    app.upload_artifact("del-multi-h2", "file.txt", b"data2", &token)
        .await;

    // Populate the cache member.
    let req = app.auth_request(Method::GET, "/repository/del-multi-cache/file.txt", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Create a proxy over all three.
    app.create_repo(json!({
        "name": "del-multi-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["del-multi-h1", "del-multi-h2", "del-multi-cache"],
    }))
    .await;

    // Delete via the proxy.
    let req = app.auth_request(
        Method::DELETE,
        "/repository/del-multi-proxy/file.txt",
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify the artifact is gone from every member.
    for member in ["del-multi-h1", "del-multi-h2", "del-multi-cache"] {
        let req = app.auth_request(
            Method::HEAD,
            &format!("/repository/{}/file.txt", member),
            &token,
        );
        let (status, _) = app.call(req).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "artifact still in {}",
            member
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_nonexistent_artifact_404() {
    let app = TestApp::new().await;
    app.create_hosted_repo("del-miss").await;
    let token = app.admin_token();
    let req = app.auth_request(Method::DELETE, "/repository/del-miss/ghost.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_delete_nonexistent_repo_404() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = app.auth_request(Method::DELETE, "/repository/nope-repo/file.txt", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// List / search artifacts
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_browse_mode_dirs_and_artifacts() {
    let app = TestApp::new().await;
    app.create_hosted_repo("browse-repo").await;
    let token = app.admin_token();
    app.upload_artifact("browse-repo", "dir/file1.txt", b"a", &token)
        .await;
    app.upload_artifact("browse-repo", "root.txt", b"b", &token)
        .await;

    // Poll until the update worker has processed the dir entry events.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let req = app.auth_request(
            Method::GET,
            "/api/v1/repositories/browse-repo/artifacts",
            &token,
        );
        let (status, body) = app.call(req).await;
        assert_eq!(status, StatusCode::OK);

        let dirs = body["dirs"].as_array().unwrap();
        let artifacts = body["artifacts"].as_array().unwrap();
        if dirs.iter().any(|d| d["name"].as_str() == Some("dir"))
            && artifacts
                .iter()
                .any(|a| a["path"].as_str() == Some("root.txt"))
        {
            break; // Dir entries are up to date.
        }

        if tokio::time::Instant::now() > deadline {
            panic!("dir entries did not appear within 5s");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_with_prefix() {
    let app = TestApp::new().await;
    app.create_hosted_repo("prefix-repo").await;
    let token = app.admin_token();
    app.upload_artifact("prefix-repo", "sub/file1.txt", b"a", &token)
        .await;
    app.upload_artifact("prefix-repo", "sub/file2.txt", b"b", &token)
        .await;
    app.upload_artifact("prefix-repo", "other.txt", b"c", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/prefix-repo/artifacts?prefix=sub/",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_search_mode() {
    let app = TestApp::new().await;
    app.create_hosted_repo("search-repo").await;
    let token = app.admin_token();
    app.upload_artifact("search-repo", "alpha.txt", b"a", &token)
        .await;
    app.upload_artifact("search-repo", "beta.txt", b"b", &token)
        .await;
    app.upload_artifact("search-repo", "gamma.txt", b"c", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/search-repo/artifacts?q=beta",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0]["path"].as_str().unwrap(), "beta.txt");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_nonexistent_repo_404() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/api/v1/repositories/ghost/artifacts", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Repository CRUD gaps
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_repo_empty_name_400() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = app.json_request(
        Method::POST,
        "/api/v1/repositories",
        &token,
        json!({"name": "", "repo_type": "hosted", "format": "raw", "store": "default"}),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_repo_cache_no_upstream_400() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = app.json_request(
        Method::POST,
        "/api/v1/repositories",
        &token,
        json!({"name": "bad-cache", "repo_type": "cache", "format": "raw", "store": "default"}),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_repo_proxy_no_members_400() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = app.json_request(
        Method::POST,
        "/api/v1/repositories",
        &token,
        json!({"name": "bad-proxy", "repo_type": "proxy", "format": "raw", "store": "default", "members": []}),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_create_repo_duplicate_409() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    app.create_hosted_repo("dup-repo").await;
    let req = app.json_request(
        Method::POST,
        "/api/v1/repositories",
        &token,
        json!({"name": "dup-repo", "repo_type": "hosted", "format": "raw", "store": "default"}),
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_get_repo_not_found_404() {
    let app = TestApp::new().await;
    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/api/v1/repositories/nonexistent", &token);
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_repos_permission_filtering() {
    let app = TestApp::new().await;
    app.create_hosted_repo("visible").await;
    app.create_hosted_repo("hidden").await;
    app.create_role_with_caps(
        "vis-reader",
        vec![depot_core::store::kv::CapabilityGrant {
            capability: depot_core::store::kv::Capability::Read,
            repo: "visible".to_string(),
        }],
    )
    .await;
    app.create_user_with_roles("filtered-user", "pass", vec!["vis-reader"])
        .await;
    let token = app.token_for("filtered-user").await;
    let req = app.auth_request(Method::GET, "/api/v1/repositories", &token);
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let names: Vec<&str> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"visible"));
    assert!(!names.contains(&"hidden"));
}

// ===========================================================================
// List on cache/proxy repos
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_cache_repo() {
    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "list-cache",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": "http://localhost:1234",
    }))
    .await;
    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/list-cache/artifacts",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    // Empty cache should return empty lists.
    assert_eq!(body["dirs"].as_array().unwrap().len(), 0);
    assert_eq!(body["artifacts"].as_array().unwrap().len(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_proxy_repo() {
    let app = TestApp::new().await;
    app.create_hosted_repo("proxy-list-m1").await;
    app.create_hosted_repo("proxy-list-m2").await;
    let token = app.admin_token();
    app.upload_artifact("proxy-list-m1", "a.txt", b"a", &token)
        .await;
    app.upload_artifact("proxy-list-m2", "b.txt", b"b", &token)
        .await;

    app.create_repo(json!({
        "name": "list-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["proxy-list-m1", "proxy-list-m2"],
    }))
    .await;

    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/list-proxy/artifacts",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    // Proxy should aggregate members' artifacts.
    let artifacts = body["artifacts"].as_array().unwrap();
    assert!(artifacts.len() >= 2);
}

// ===========================================================================
// Proxy search
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_search_proxy_repo() {
    let app = TestApp::new().await;
    app.create_hosted_repo("search-m1").await;
    app.create_hosted_repo("search-m2").await;
    let token = app.admin_token();
    app.upload_artifact("search-m1", "needle.txt", b"x", &token)
        .await;
    app.upload_artifact("search-m2", "haystack.txt", b"y", &token)
        .await;

    app.create_repo(json!({
        "name": "search-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["search-m1", "search-m2"],
    }))
    .await;

    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/search-proxy/artifacts?q=needle",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0]["path"].as_str().unwrap(), "needle.txt");
}

// ===========================================================================
// Path normalization and traversal protection
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_path_normalization_collapses_slashes() {
    let app = TestApp::new().await;
    app.create_hosted_repo("norm-repo").await;
    let token = app.admin_token();
    app.upload_artifact("norm-repo", "a//b/c.txt", b"hello", &token)
        .await;
    // Fetch with normalized path
    let req = app.auth_request(Method::GET, "/repository/norm-repo/a/b/c.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_path_traversal_rejected() {
    let app = TestApp::new().await;
    app.create_hosted_repo("trav-repo").await;
    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/repository/trav-repo/../escape/file.txt",
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_dot_segments_normalized() {
    let app = TestApp::new().await;
    app.create_hosted_repo("dot-repo").await;
    let token = app.admin_token();
    app.upload_artifact("dot-repo", "a/./b/c.txt", b"dotty", &token)
        .await;
    let req = app.auth_request(Method::GET, "/repository/dot-repo/a/b/c.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_etag_header_present() {
    let app = TestApp::new().await;
    app.create_hosted_repo("etag-repo").await;
    let token = app.admin_token();
    app.upload_artifact("etag-repo", "file.txt", b"etag-content", &token)
        .await;
    let req = app.auth_request(Method::GET, "/repository/etag-repo/file.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let etag = resp.headers().get(header::ETAG);
    assert!(etag.is_some(), "ETag header should be present");
    let etag_str = etag.unwrap().to_str().unwrap();
    assert!(etag_str.starts_with('"') && etag_str.ends_with('"'));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_last_modified_header_present() {
    let app = TestApp::new().await;
    app.create_hosted_repo("lm-repo").await;
    let token = app.admin_token();
    app.upload_artifact("lm-repo", "file.txt", b"lm-content", &token)
        .await;
    let req = app.auth_request(Method::GET, "/repository/lm-repo/file.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers().get(header::LAST_MODIFIED).is_some(),
        "Last-Modified header should be present"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_conditional_get_304_if_none_match() {
    let app = TestApp::new().await;
    app.create_hosted_repo("cond-repo").await;
    let token = app.admin_token();
    app.upload_artifact("cond-repo", "file.txt", b"conditional", &token)
        .await;

    // First GET to obtain ETag
    let req = app.auth_request(Method::GET, "/repository/cond-repo/file.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let etag = resp
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Conditional GET with matching ETag
    let req = axum::http::Request::builder()
        .method(Method::GET)
        .uri("/repository/cond-repo/file.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::IF_NONE_MATCH, &etag)
        .body(Body::empty())
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_conditional_get_304_if_modified_since() {
    let app = TestApp::new().await;
    app.create_hosted_repo("ims-repo").await;
    let token = app.admin_token();
    app.upload_artifact("ims-repo", "file.txt", b"since-test", &token)
        .await;

    // First GET to obtain Last-Modified
    let req = app.auth_request(Method::GET, "/repository/ims-repo/file.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let last_mod = resp
        .headers()
        .get(header::LAST_MODIFIED)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Conditional GET with matching If-Modified-Since
    let req = axum::http::Request::builder()
        .method(Method::GET)
        .uri("/repository/ims-repo/file.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::IF_MODIFIED_SINCE, &last_mod)
        .body(Body::empty())
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_head_has_etag_and_last_modified() {
    let app = TestApp::new().await;
    app.create_hosted_repo("head-cache-repo").await;
    let token = app.admin_token();
    app.upload_artifact("head-cache-repo", "file.txt", b"head-test", &token)
        .await;
    let req = app.auth_request(Method::HEAD, "/repository/head-cache-repo/file.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get(header::ETAG).is_some());
    assert!(resp.headers().get(header::LAST_MODIFIED).is_some());
}

// ===========================================================================
// Proxy — search through proxy repo
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_search_finds_artifacts_from_members() {
    let app = TestApp::new().await;
    app.create_hosted_repo("search-m1").await;
    app.create_hosted_repo("search-m2").await;
    app.create_repo(serde_json::json!({
        "name": "search-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["search-m1", "search-m2"],
    }))
    .await;
    let token = app.admin_token();

    app.upload_artifact("search-m1", "alpha.txt", b"a1", &token)
        .await;
    app.upload_artifact("search-m2", "beta.txt", b"b2", &token)
        .await;

    // Search for "alpha" through proxy
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/search-proxy/artifacts?q=alpha",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert!(
        artifacts
            .iter()
            .any(|a| a["path"].as_str().is_some_and(|p| p.contains("alpha"))),
        "proxy search should find alpha from member"
    );

    // Search for "beta" through proxy
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/search-proxy/artifacts?q=beta",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert!(
        artifacts
            .iter()
            .any(|a| a["path"].as_str().is_some_and(|p| p.contains("beta"))),
        "proxy search should find beta from member"
    );
}

// ===========================================================================
// Proxy — list / browse through proxy repo
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_list_children_merges_members() {
    let app = TestApp::new().await;
    app.create_hosted_repo("list-m1").await;
    app.create_hosted_repo("list-m2").await;
    app.create_repo(serde_json::json!({
        "name": "list-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["list-m1", "list-m2"],
    }))
    .await;
    let token = app.admin_token();

    app.upload_artifact("list-m1", "file1.txt", b"data1", &token)
        .await;
    app.upload_artifact("list-m2", "file2.txt", b"data2", &token)
        .await;

    // Browse root of proxy repo
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/list-proxy/artifacts",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    // Should have artifacts from both members.
    assert!(
        artifacts.len() >= 2,
        "proxy browse should merge artifacts from both members"
    );
}

// ===========================================================================
// Pagination — limit and cursor
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_with_limit() {
    let app = TestApp::new().await;
    app.create_hosted_repo("limit-repo").await;
    let token = app.admin_token();

    for i in 0..5 {
        app.upload_artifact(
            "limit-repo",
            &format!("file{i}.txt"),
            format!("data{i}").as_bytes(),
            &token,
        )
        .await;
    }

    // List with limit=2
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/limit-repo/artifacts?q=file&limit=2",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 2, "should respect limit parameter");
    assert!(
        body["next_cursor"].is_string(),
        "should return a continuation cursor"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_list_with_cursor_pagination() {
    let app = TestApp::new().await;
    app.create_hosted_repo("cursor-repo").await;
    let token = app.admin_token();

    for i in 0..5 {
        app.upload_artifact(
            "cursor-repo",
            &format!("item{i}.txt"),
            format!("data{i}").as_bytes(),
            &token,
        )
        .await;
    }

    // Page 1
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/cursor-repo/artifacts?q=item&limit=2",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let page1 = body["artifacts"].as_array().unwrap().clone();
    assert_eq!(page1.len(), 2);
    let cursor = body["next_cursor"].as_str().unwrap();

    // Page 2
    let req = app.auth_request(
        Method::GET,
        &format!(
            "/api/v1/repositories/cursor-repo/artifacts?q=item&limit=2&cursor={}",
            cursor
        ),
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let page2 = body["artifacts"].as_array().unwrap().clone();
    assert!(
        !page2.is_empty(),
        "second page should have at least 1 result"
    );

    // Pages should not overlap.
    let page1_paths: Vec<&str> = page1.iter().map(|a| a["path"].as_str().unwrap()).collect();
    let page2_paths: Vec<&str> = page2.iter().map(|a| a["path"].as_str().unwrap()).collect();
    for p in &page2_paths {
        assert!(
            !page1_paths.contains(p),
            "pages should not overlap: {p} appears in both"
        );
    }
}

// ===========================================================================
// Proxy — search with pagination
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_search_with_limit() {
    let app = TestApp::new().await;
    app.create_hosted_repo("ps-m1").await;
    app.create_hosted_repo("ps-m2").await;
    app.create_repo(serde_json::json!({
        "name": "ps-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["ps-m1", "ps-m2"],
    }))
    .await;
    let token = app.admin_token();

    for i in 0..4 {
        app.upload_artifact(
            "ps-m1",
            &format!("doc{i}.txt"),
            format!("content{i}").as_bytes(),
            &token,
        )
        .await;
    }
    for i in 4..8 {
        app.upload_artifact(
            "ps-m2",
            &format!("doc{i}.txt"),
            format!("content{i}").as_bytes(),
            &token,
        )
        .await;
    }

    // Search across proxy with limit
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/ps-proxy/artifacts?q=doc&limit=3",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 3, "proxy search should respect limit");
}

// ===========================================================================
// Cache repo — raw artifact operations
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_raw_get_from_upstream() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/repository/upstream-raw/data.txt",
        ))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string("cached content")
                .insert_header("Content-Type", "text/plain"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "raw-cache",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/upstream-raw", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    let token = app.admin_token();

    // GET from cache — should fetch from upstream.
    let req = app.auth_request(Method::GET, "/repository/raw-cache/data.txt", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"cached content");

    // Second GET — should serve from cache (upstream only called once).
    let req = app.auth_request(Method::GET, "/repository/raw-cache/data.txt", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"cached content");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_raw_head() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repository/head-up/file.bin"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_bytes(b"head content".to_vec())
                .insert_header("Content-Type", "application/octet-stream"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "raw-cache-head",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/head-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    let token = app.admin_token();

    // Prime the cache.
    let req = app.auth_request(Method::GET, "/repository/raw-cache-head/file.bin", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // HEAD on cached artifact.
    let req = app.auth_request(Method::HEAD, "/repository/raw-cache-head/file.bin", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_raw_get_content_length_on_miss() {
    let upstream = wiremock::MockServer::start().await;

    let body = b"content-length test payload";

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repository/cl-up/file.bin"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_bytes(body.to_vec())
                .insert_header("Content-Type", "application/octet-stream"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "raw-cache-cl",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/cl-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    let token = app.admin_token();

    // First GET is a cache miss — should still have Content-Length.
    let req = app.auth_request(Method::GET, "/repository/raw-cache-cl/file.bin", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cl = resp
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .expect("Content-Length missing on cache-miss GET")
        .to_str()
        .unwrap();
    assert_eq!(cl, body.len().to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_raw_list_cached_artifacts() {
    let upstream = wiremock::MockServer::start().await;

    for name in &["a.txt", "b.txt"] {
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(format!(
                "/repository/list-up/{name}"
            )))
            .respond_with(
                wiremock::ResponseTemplate::new(200).set_body_string(format!("content of {name}")),
            )
            .mount(&upstream)
            .await;
    }

    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "raw-cache-list",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/list-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    let token = app.admin_token();

    // Prime the cache with two files.
    for name in &["a.txt", "b.txt"] {
        let req = app.auth_request(
            Method::GET,
            &format!("/repository/raw-cache-list/{name}"),
            &token,
        );
        let (status, _) = app.call_raw(req).await;
        assert_eq!(status, StatusCode::OK);
    }

    // List artifacts.
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/raw-cache-list/artifacts",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert!(artifacts.len() >= 2, "should list cached artifacts");
}

// ===========================================================================
// Proxy repo — HEAD resolves across members
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_head_resolves_across_members() {
    let app = TestApp::new().await;
    app.create_hosted_repo("head-m1").await;
    app.create_hosted_repo("head-m2").await;
    let token = app.admin_token();

    app.upload_artifact("head-m1", "only-in-m1.txt", b"hello", &token)
        .await;
    app.upload_artifact("head-m2", "only-in-m2.txt", b"world", &token)
        .await;

    app.create_repo(json!({
        "name": "head-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["head-m1", "head-m2"],
    }))
    .await;

    // HEAD for artifact in member 1.
    let req = app.auth_request(
        Method::HEAD,
        "/repository/head-proxy/only-in-m1.txt",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // HEAD for artifact in member 2.
    let req = app.auth_request(
        Method::HEAD,
        "/repository/head-proxy/only-in-m2.txt",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // HEAD for non-existent artifact.
    let req = app.auth_request(
        Method::HEAD,
        "/repository/head-proxy/no-such-file.txt",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ===========================================================================
// Proxy repo — GET resolves across members
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_get_from_second_member() {
    let app = TestApp::new().await;
    app.create_hosted_repo("get-m1").await;
    app.create_hosted_repo("get-m2").await;
    let token = app.admin_token();

    // Only upload to member 2.
    app.upload_artifact("get-m2", "fallback.bin", b"from second member", &token)
        .await;

    app.create_repo(json!({
        "name": "get-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["get-m1", "get-m2"],
    }))
    .await;

    // GET should find it in member 2.
    let req = app.auth_request(Method::GET, "/repository/get-proxy/fallback.bin", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"from second member");
}

// ===========================================================================
// Proxy browse — list_children at root
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_browse_root() {
    let app = TestApp::new().await;
    app.create_hosted_repo("brw-m1").await;
    app.create_hosted_repo("brw-m2").await;
    let token = app.admin_token();

    app.upload_artifact("brw-m1", "alpha.txt", b"a", &token)
        .await;
    app.upload_artifact("brw-m2", "beta.txt", b"b", &token)
        .await;

    app.create_repo(json!({
        "name": "brw-proxy",
        "repo_type": "proxy",
        "format": "raw",
        "members": ["brw-m1", "brw-m2"],
    }))
    .await;

    // Browse root — should see artifacts from both members.
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/brw-proxy/artifacts",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    let paths: Vec<&str> = artifacts
        .iter()
        .map(|a| a["path"].as_str().unwrap())
        .collect();
    assert!(
        paths.contains(&"alpha.txt"),
        "should have m1 artifact: {paths:?}"
    );
    assert!(
        paths.contains(&"beta.txt"),
        "should have m2 artifact: {paths:?}"
    );
}

// ===========================================================================
// Cache browse — list_children at root
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_browse_root() {
    let upstream = wiremock::MockServer::start().await;

    for name in &["file1.txt", "file2.txt"] {
        wiremock::Mock::given(wiremock::matchers::method("GET"))
            .and(wiremock::matchers::path(format!(
                "/repository/brw-up/{name}"
            )))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("content"))
            .mount(&upstream)
            .await;
    }

    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "cache-browse",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/brw-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    let token = app.admin_token();

    // Prime cache.
    for name in &["file1.txt", "file2.txt"] {
        let req = app.auth_request(
            Method::GET,
            &format!("/repository/cache-browse/{name}"),
            &token,
        );
        let (status, _) = app.call_raw(req).await;
        assert_eq!(status, StatusCode::OK);
    }

    // Browse at root.
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/cache-browse/artifacts",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert!(
        artifacts.len() >= 2,
        "should list cached artifacts: got {}",
        artifacts.len()
    );
}

// ===========================================================================
// Search on cache repo
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_cache_search() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/repository/search-up/searchable.txt",
        ))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("find me"))
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "cache-search",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/search-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    let token = app.admin_token();

    // Prime cache.
    let req = app.auth_request(
        Method::GET,
        "/repository/cache-search/searchable.txt",
        &token,
    );
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // Search.
    let req = app.auth_request(
        Method::GET,
        "/api/v1/repositories/cache-search/artifacts?q=searchable",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let artifacts = body["artifacts"].as_array().unwrap();
    assert!(
        !artifacts.is_empty(),
        "should find cached artifact by search"
    );
}

// ===========================================================================
// Proxy with cache member — download raw artifact from cache upstream
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_downloads_from_cache_member() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path(
            "/repository/upstream-raw/data/from-cache.txt",
        ))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_string("upstream cached content")
                .insert_header("Content-Type", "text/plain"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    let token = app.admin_token();

    // Hosted member with a different file.
    app.create_hosted_repo("raw-grp-h").await;
    app.upload_artifact("raw-grp-h", "data/hosted.txt", b"hosted content", &token)
        .await;

    // Cache member pointing to wiremock upstream.
    app.create_repo(json!({
        "name": "raw-grp-c",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/upstream-raw", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;

    // Proxy over both.
    app.create_format_proxy_repo("raw-grp", "raw", vec!["raw-grp-h", "raw-grp-c"])
        .await;

    // Download hosted content through proxy.
    let req = app.auth_request(Method::GET, "/repository/raw-grp/data/hosted.txt", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"hosted content");

    // Download cache content through proxy — fetched from upstream.
    let req = app.auth_request(
        Method::GET,
        "/repository/raw-grp/data/from-cache.txt",
        &token,
    );
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"upstream cached content");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_proxy_head_from_cache_member() {
    let upstream = wiremock::MockServer::start().await;

    wiremock::Mock::given(wiremock::matchers::method("GET"))
        .and(wiremock::matchers::path("/repository/head-up/file.bin"))
        .respond_with(
            wiremock::ResponseTemplate::new(200)
                .set_body_bytes(b"head cache content".to_vec())
                .insert_header("Content-Type", "application/octet-stream"),
        )
        .mount(&upstream)
        .await;

    let app = TestApp::new().await;
    let token = app.admin_token();

    app.create_hosted_repo("raw-hgrp-h").await;
    app.create_repo(json!({
        "name": "raw-hgrp-c",
        "repo_type": "cache",
        "format": "raw",
        "upstream_url": format!("{}/repository/head-up", upstream.uri()),
        "cache_ttl_secs": 300,
    }))
    .await;
    app.create_format_proxy_repo("raw-hgrp", "raw", vec!["raw-hgrp-h", "raw-hgrp-c"])
        .await;

    // GET first to prime the cache.
    let req = app.auth_request(Method::GET, "/repository/raw-hgrp/file.bin", &token);
    let (status, _) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);

    // HEAD through proxy should find the cached artifact.
    let req = axum::http::Request::builder()
        .method(Method::HEAD)
        .uri("/repository/raw-hgrp/file.bin")
        .header(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {}", token),
        )
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Content-Disposition for raw repos
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_content_disposition_default_is_attachment() {
    let app = TestApp::new().await;
    app.create_hosted_repo("cd-default").await;
    let token = app.admin_token();
    app.upload_artifact("cd-default", "readme.txt", b"hello", &token)
        .await;

    let req = app.auth_request(Method::GET, "/repository/cd-default/readme.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cd = resp
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .expect("Content-Disposition header missing")
        .to_str()
        .unwrap();
    assert!(
        cd.starts_with("attachment"),
        "expected attachment, got: {cd}"
    );
    assert!(
        cd.contains("readme.txt"),
        "expected filename in header: {cd}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_content_disposition_inline() {
    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "cd-inline",
        "repo_type": "hosted",
        "format": "raw",
        "content_disposition": "inline",
    }))
    .await;
    let token = app.admin_token();
    app.upload_artifact("cd-inline", "page.html", b"<h1>hi</h1>", &token)
        .await;

    let req = app.auth_request(Method::GET, "/repository/cd-inline/page.html", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cd = resp
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .expect("Content-Disposition header missing")
        .to_str()
        .unwrap();
    assert!(cd.starts_with("inline"), "expected inline, got: {cd}");
    assert!(
        cd.contains("page.html"),
        "expected filename in header: {cd}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_content_disposition_head_returns_header() {
    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "cd-head",
        "repo_type": "hosted",
        "format": "raw",
        "content_disposition": "inline",
    }))
    .await;
    let token = app.admin_token();
    app.upload_artifact("cd-head", "doc.txt", b"data", &token)
        .await;

    let req = app.auth_request(Method::HEAD, "/repository/cd-head/doc.txt", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cd = resp
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .expect("Content-Disposition header missing on HEAD")
        .to_str()
        .unwrap();
    assert!(cd.starts_with("inline"), "expected inline, got: {cd}");
    assert!(cd.contains("doc.txt"), "expected filename in header: {cd}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_content_disposition_nested_path_filename() {
    let app = TestApp::new().await;
    app.create_hosted_repo("cd-nested").await;
    let token = app.admin_token();
    app.upload_artifact("cd-nested", "a/b/deep.bin", b"bytes", &token)
        .await;

    let req = app.auth_request(Method::GET, "/repository/cd-nested/a/b/deep.bin", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let cd = resp
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cd.contains("deep.bin"), "expected leaf filename, got: {cd}");
    assert!(
        !cd.contains("a/b"),
        "should not contain directory path: {cd}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_content_disposition_update_setting() {
    let app = TestApp::new().await;
    app.create_hosted_repo("cd-update").await;
    let token = app.admin_token();
    app.upload_artifact("cd-update", "file.dat", b"stuff", &token)
        .await;

    // Default should be attachment.
    let req = app.auth_request(Method::GET, "/repository/cd-update/file.dat", &token);
    let resp = app.call_resp(req).await;
    let cd = resp
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cd.starts_with("attachment"), "initial: {cd}");

    // Update to inline.
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/api/v1/repositories/cd-update")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"content_disposition": "inline"})).unwrap(),
        ))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);

    // Should now be inline.
    let req = app.auth_request(Method::GET, "/repository/cd-update/file.dat", &token);
    let resp = app.call_resp(req).await;
    let cd = resp
        .headers()
        .get(header::CONTENT_DISPOSITION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(cd.starts_with("inline"), "after update: {cd}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_raw_content_disposition_api_roundtrip() {
    let app = TestApp::new().await;
    app.create_repo(json!({
        "name": "cd-api",
        "repo_type": "hosted",
        "format": "raw",
        "content_disposition": "inline",
    }))
    .await;
    let token = app.admin_token();

    let req = app.auth_request(Method::GET, "/api/v1/repositories/cd-api", &token);
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["content_disposition"].as_str().unwrap(),
        "inline",
        "API response should reflect the setting"
    );
}

// ===========================================================================
// Helm PUT via /repository/{repo}/{path}
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_helm_chart_tgz() {
    use depot_format_helm::store::build_synthetic_chart;

    let app = TestApp::new().await;
    app.create_helm_repo("helm-put").await;

    let chart = build_synthetic_chart("nginx", "1.0.0", "A web server").unwrap();
    let token = app.admin_token();

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/helm-put/nginx-1.0.0.tgz")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/gzip")
        .body(Body::from(chart))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Verify index.yaml contains the chart.
    let req = app.auth_request(Method::GET, "/helm/helm-put/index.yaml", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let index = String::from_utf8(body).unwrap();
    assert!(index.contains("nginx"), "index should contain nginx");
    assert!(index.contains("1.0.0"), "index should contain version");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_helm_non_tgz_rejected() {
    let app = TestApp::new().await;
    app.create_helm_repo("helm-put-reject").await;

    let token = app.admin_token();
    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/helm-put-reject/readme.txt")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from(b"hello".to_vec()))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_helm_chart_via_proxy_with_write_member() {
    use depot_format_helm::store::build_synthetic_chart;

    let app = TestApp::new().await;
    app.create_helm_repo("helm-proxy-member").await;
    app.create_repo(json!({
        "name": "helm-proxy-put",
        "repo_type": "proxy",
        "format": "helm",
        "members": ["helm-proxy-member"],
        "write_member": "helm-proxy-member",
    }))
    .await;

    let chart = build_synthetic_chart("grafana", "2.0.0", "Dashboards").unwrap();
    let token = app.admin_token();

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/helm-proxy-put/grafana-2.0.0.tgz")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/gzip")
        .body(Body::from(chart))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::CREATED);

    // Verify it landed on the hosted member's index.
    let req = app.auth_request(Method::GET, "/helm/helm-proxy-member/index.yaml", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    let index = String::from_utf8(body).unwrap();
    assert!(index.contains("grafana"), "index should contain grafana");
    assert!(index.contains("2.0.0"), "index should contain version");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_put_helm_chart_to_proxy_without_write_member_blocked() {
    use depot_format_helm::store::build_synthetic_chart;

    let app = TestApp::new().await;
    app.create_helm_repo("helm-proxy-no-wm-member").await;
    app.create_helm_proxy_repo("helm-proxy-no-wm", vec!["helm-proxy-no-wm-member"])
        .await;

    let chart = build_synthetic_chart("nginx", "1.0.0", "A web server").unwrap();
    let token = app.admin_token();

    let req = axum::http::Request::builder()
        .method(Method::PUT)
        .uri("/repository/helm-proxy-no-wm/nginx-1.0.0.tgz")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/gzip")
        .body(Body::from(chart))
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
}
