// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use http_body_util::BodyExt;

use depot_test_support::*;

fn sha256_digest(data: &[u8]) -> String {
    format!(
        "sha256:{:x}",
        sha2::Digest::finalize(sha2::Digest::chain_update(sha2::Sha256::default(), data))
    )
}

// ===========================================================================
// Nexus search/assets API
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_assets() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-raw").await;

    let token = app.admin_token();
    app.upload_artifact("nexus-raw", "dir/file.txt", b"hello", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets?repository=nexus-raw&name=*file*",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert!(!items.is_empty(), "should find at least one asset");
    assert!(items[0]["downloadUrl"]
        .as_str()
        .unwrap()
        .contains("nexus-raw"));
    assert!(items[0]["path"].as_str().is_some());
    assert!(items[0]["repository"].as_str().unwrap() == "nexus-raw");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_assets_empty() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-empty").await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets?repository=nexus-empty&name=*nonexistent*",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["items"].as_array().unwrap().is_empty());
    assert!(body["continuationToken"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_assets_download_redirect() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-dl").await;

    let token = app.admin_token();
    app.upload_artifact("nexus-dl", "report.txt", b"data", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets/download?repository=nexus-dl&name=*report*",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::FOUND);
    let loc = resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        loc.contains("/repository/nexus-dl/"),
        "redirect should point to artifact: {loc}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_assets_download_not_found() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-dl404").await;

    let token = app.admin_token();
    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets/download?repository=nexus-dl404&name=*missing*",
        &token,
    );
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_components() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-comp").await;

    let token = app.admin_token();
    app.upload_artifact("nexus-comp", "pkg/v1/archive.tar.gz", b"bytes", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search?repository=nexus-comp&name=*archive*",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert!(!items.is_empty());
    assert!(!items[0]["assets"].as_array().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_status() {
    let app = TestApp::new().await;
    let req = Request::builder()
        .method(Method::GET)
        .uri("/service/rest/v1/status")
        .body(Body::empty())
        .unwrap();
    let (status, _) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_list_repositories() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-listrepo").await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/service/rest/v1/repositories", &token);
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let repos = body.as_array().unwrap();
    let names: Vec<&str> = repos.iter().map(|r| r["name"].as_str().unwrap()).collect();
    assert!(
        names.contains(&"nexus-listrepo"),
        "should list created repo"
    );
}

// ===========================================================================
// Nexus /assets and /components list endpoints (non-search)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_list_assets() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-list").await;

    let token = app.admin_token();
    app.upload_artifact("nexus-list", "a/one.txt", b"1", &token)
        .await;
    app.upload_artifact("nexus-list", "b/two.txt", b"2", &token)
        .await;

    // GET /service/rest/v1/assets (no /search/ prefix) must return JSON, not HTML.
    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/assets?repository=nexus-list",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "should list both uploaded assets");
    assert!(items[0]["downloadUrl"].as_str().is_some());
    assert!(items[0]["path"].as_str().is_some());
    assert!(body["continuationToken"].is_null());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_list_components() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-listcomp").await;

    let token = app.admin_token();
    app.upload_artifact("nexus-listcomp", "pkg/lib.tar.gz", b"data", &token)
        .await;

    // GET /service/rest/v1/components (no /search/ prefix).
    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/components?repository=nexus-listcomp",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert!(!items.is_empty());
    assert!(!items[0]["assets"].as_array().unwrap().is_empty());
    assert!(items[0]["name"].as_str().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_assets_limit_parameter() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-limit").await;

    let token = app.admin_token();
    for i in 0..5 {
        app.upload_artifact(
            "nexus-limit",
            &format!("file{i}.txt"),
            format!("data{i}").as_bytes(),
            &token,
        )
        .await;
    }

    // Request only 2 items.
    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets?repository=nexus-limit&limit=2",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2, "should return exactly 2 items");
    assert!(
        body["continuationToken"].as_str().is_some(),
        "should have continuationToken since more items remain"
    );

    // Paginate with the token.
    let token_val = body["continuationToken"].as_str().unwrap();
    let req = app.auth_request(
        Method::GET,
        &format!(
            "/service/rest/v1/search/assets?repository=nexus-limit&limit=2&continuationToken={}",
            token_val
        ),
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items2 = body["items"].as_array().unwrap();
    assert_eq!(items2.len(), 2, "second page should have 2 items");

    // Paths from page 1 and page 2 should not overlap.
    let paths1: Vec<&str> = items.iter().map(|i| i["path"].as_str().unwrap()).collect();
    let paths2: Vec<&str> = items2.iter().map(|i| i["path"].as_str().unwrap()).collect();
    for p in &paths2 {
        assert!(!paths1.contains(p), "page 2 should not repeat page 1 paths");
    }
}

// ===========================================================================
// Search against Proxy (group) repositories: must interleave member results
// across a single resumable continuationToken cursor.
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_proxy_interleaves_members() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nxp-m1").await;
    app.create_hosted_repo("nxp-m2").await;
    app.create_format_proxy_repo("nxp-grp", "raw", vec!["nxp-m1", "nxp-m2"])
        .await;

    let token = app.admin_token();
    app.upload_artifact("nxp-m1", "dir/a.txt", b"a", &token)
        .await;
    app.upload_artifact("nxp-m2", "dir/b.txt", b"b", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets?repository=nxp-grp&name=*.txt",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    let paths: Vec<&str> = items.iter().map(|i| i["path"].as_str().unwrap()).collect();
    assert!(
        paths.contains(&"dir/a.txt"),
        "proxy search should surface member m1 artifact: paths={paths:?}"
    );
    assert!(
        paths.contains(&"dir/b.txt"),
        "proxy search should surface member m2 artifact: paths={paths:?}"
    );
    for item in items {
        assert_eq!(item["repository"].as_str().unwrap(), "nxp-grp");
        assert!(item["downloadUrl"]
            .as_str()
            .unwrap()
            .contains("/repository/nxp-grp/"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_components_proxy() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nxc-m1").await;
    app.create_hosted_repo("nxc-m2").await;
    app.create_format_proxy_repo("nxc-grp", "raw", vec!["nxc-m1", "nxc-m2"])
        .await;

    let token = app.admin_token();
    app.upload_artifact("nxc-m1", "pkg/alpha.tar.gz", b"a", &token)
        .await;
    app.upload_artifact("nxc-m2", "pkg/beta.tar.gz", b"b", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search?repository=nxc-grp&name=*tar.gz",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    let names: Vec<&str> = items.iter().map(|i| i["name"].as_str().unwrap()).collect();
    assert!(names.contains(&"alpha.tar.gz"), "names={names:?}");
    assert!(names.contains(&"beta.tar.gz"), "names={names:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_proxy_download_redirects_to_member() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nxd-m1").await;
    app.create_hosted_repo("nxd-m2").await;
    app.create_format_proxy_repo("nxd-grp", "raw", vec!["nxd-m1", "nxd-m2"])
        .await;

    let token = app.admin_token();
    app.upload_artifact("nxd-m2", "report.txt", b"only-in-m2", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets/download?repository=nxd-grp&name=*report*",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::FOUND);
    let loc = resp
        .headers()
        .get(header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap();
    // The redirect points at the proxy, which will resolve the artifact via
    // its members on read.
    assert!(
        loc.contains("/repository/nxd-grp/report.txt"),
        "redirect should target the proxy URL: {loc}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_proxy_pagination_resumable() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nxpag-m1").await;
    app.create_hosted_repo("nxpag-m2").await;
    app.create_format_proxy_repo("nxpag-grp", "raw", vec!["nxpag-m1", "nxpag-m2"])
        .await;

    let token = app.admin_token();
    for i in 0..5 {
        app.upload_artifact(
            "nxpag-m1",
            &format!("m1-{i:02}.txt"),
            format!("m1-{i}").as_bytes(),
            &token,
        )
        .await;
        app.upload_artifact(
            "nxpag-m2",
            &format!("m2-{i:02}.txt"),
            format!("m2-{i}").as_bytes(),
            &token,
        )
        .await;
    }

    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        pages += 1;
        assert!(pages < 20, "pagination did not terminate");
        let url = match &cursor {
            Some(c) => format!(
                "/service/rest/v1/search/assets?repository=nxpag-grp&name=*.txt&limit=3&continuationToken={}",
                c
            ),
            None => {
                "/service/rest/v1/search/assets?repository=nxpag-grp&name=*.txt&limit=3".to_string()
            }
        };
        let req = app.auth_request(Method::GET, &url, &token);
        let (status, body) = app.call(req).await;
        assert_eq!(status, StatusCode::OK);
        let items = body["items"].as_array().unwrap();
        for item in items {
            let path = item["path"].as_str().unwrap().to_string();
            assert!(
                seen.insert(path.clone()),
                "duplicate path {path} across pages"
            );
        }
        match body["continuationToken"].as_str() {
            Some(t) => cursor = Some(t.to_string()),
            None => break,
        }
    }

    let expected: std::collections::BTreeSet<String> = (0..5)
        .flat_map(|i| [format!("m1-{i:02}.txt"), format!("m2-{i:02}.txt")])
        .collect();
    assert_eq!(
        seen, expected,
        "pagination must surface every member artifact exactly once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_search_proxy_dedup_first_member_wins() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nxdup-m1").await;
    app.create_hosted_repo("nxdup-m2").await;
    app.create_format_proxy_repo("nxdup-grp", "raw", vec!["nxdup-m1", "nxdup-m2"])
        .await;

    let token = app.admin_token();
    // Same path in both members — m1 comes first and should win.
    app.upload_artifact("nxdup-m1", "same.txt", b"from-m1", &token)
        .await;
    app.upload_artifact("nxdup-m2", "same.txt", b"from-m2", &token)
        .await;

    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets?repository=nxdup-grp&name=*same*",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let items = body["items"].as_array().unwrap();
    assert_eq!(
        items.len(),
        1,
        "duplicate paths across members must collapse to one entry: {items:?}"
    );
    assert_eq!(items[0]["path"].as_str().unwrap(), "same.txt");
    // The size reflects the blob chosen by first-member-wins; m1's payload is
    // "from-m1" (7 bytes).
    assert_eq!(
        items[0]["fileSize"].as_u64().unwrap(),
        b"from-m1".len() as u64
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_assets_limit_clamped() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nexus-clamp").await;

    let token = app.admin_token();
    app.upload_artifact("nexus-clamp", "test.txt", b"x", &token)
        .await;

    // limit=999999 should succeed (clamped to MAX_PAGE_SIZE).
    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets?repository=nexus-clamp&limit=999999",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body["items"].as_array().unwrap().is_empty());

    // limit=0 should fall back to default (still returns results).
    let req = app.auth_request(
        Method::GET,
        "/service/rest/v1/search/assets?repository=nexus-clamp&limit=0",
        &token,
    );
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body["items"].as_array().unwrap().is_empty());
}

// ===========================================================================
// Docker V2 via /repository/{repo}/v2/... (Nexus-compatible path)
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_docker_v2_check() {
    let app = TestApp::new().await;
    app.create_docker_repo("nxd-check").await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/nxd-check/v2/", &token);
    let resp = app.call_resp(req).await;
    // V2 check should succeed (200 for authenticated user).
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Docker V2 check via /repository/ should return 200"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_docker_push_pull_via_repository_path() {
    let app = TestApp::new().await;
    app.create_docker_repo("nxd-pushpull").await;

    let token = app.admin_token();

    // Push blob via /repository/{repo}/v2/{image}/blobs/uploads/
    let config_data = b"nexus docker config";
    let config_digest = sha256_digest(config_data);
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/repository/nxd-pushpull/v2/myimage/blobs/uploads/?digest={}",
            config_digest
        ))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(header::CONTENT_TYPE, "application/octet-stream")
        .header(header::CONTENT_LENGTH, config_data.len().to_string())
        .body(Body::from(config_data.to_vec()))
        .unwrap();
    let resp = app.call_resp(req).await;
    assert!(
        resp.status() == StatusCode::CREATED || resp.status() == StatusCode::ACCEPTED,
        "blob push via /repository/ failed: {}",
        resp.status()
    );

    // Push manifest via /repository/{repo}/v2/{image}/manifests/{tag}
    let manifest = TestApp::make_manifest(&config_digest, &[]);
    let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
    let req = Request::builder()
        .method(Method::PUT)
        .uri("/repository/nxd-pushpull/v2/myimage/manifests/latest")
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .header(
            header::CONTENT_TYPE,
            "application/vnd.docker.distribution.manifest.v2+json",
        )
        .body(Body::from(manifest_bytes))
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "manifest push via /repository/ failed"
    );

    // GET manifest via /repository/{repo}/v2/{image}/manifests/{tag}
    let req = app.auth_request(
        Method::GET,
        "/repository/nxd-pushpull/v2/myimage/manifests/latest",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().get("Docker-Content-Digest").is_some());

    // Also verify it's accessible via the standard /v2/ path.
    let req = app.auth_request(
        Method::GET,
        "/v2/nxd-pushpull/myimage/manifests/latest",
        &token,
    );
    let resp = app.call_resp(req).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "manifest should also be accessible via standard /v2/ path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_docker_head_blob_via_repository_path() {
    let app = TestApp::new().await;
    app.create_docker_repo("nxd-headblob").await;

    // Push blob via standard path.
    let data = b"nexus head blob test";
    let digest = app.push_docker_blob("nxd-headblob", data).await;

    // HEAD blob via /repository/ path.
    let token = app.admin_token();
    let req = Request::builder()
        .method(Method::HEAD)
        .uri(format!(
            "/repository/nxd-headblob/v2/nxd-headblob/blobs/{}",
            digest
        ))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "HEAD blob via /repository/ should work"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_nexus_docker_non_docker_repo_returns_error() {
    let app = TestApp::new().await;
    app.create_hosted_repo("nxd-raw").await;

    let token = app.admin_token();
    let req = app.auth_request(Method::GET, "/repository/nxd-raw/v2/", &token);
    let (status, _body) = app.call(req).await;
    // Should return an error, not serve Docker V2 for a raw repo.
    assert_ne!(status, StatusCode::OK);
}

// ===========================================================================
// Verify standard Docker V2 routes still work unchanged
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_standard_docker_v2_unaffected() {
    let app = TestApp::new().await;
    app.create_docker_repo("std-docker").await;

    // Push and pull via standard /v2/ path.
    let config_digest = app.push_docker_blob("std-docker", b"{}").await;
    let layer_digest = app.push_docker_blob("std-docker", b"layer data").await;
    let manifest = TestApp::make_manifest(&config_digest, &[&layer_digest]);
    let _manifest_digest = app
        .push_docker_manifest("std-docker", "v1.0", &manifest)
        .await;

    let token = app.admin_token();

    // GET manifest via standard path.
    let req = app.auth_request(Method::GET, "/v2/std-docker/manifests/v1.0", &token);
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // HEAD blob via standard path.
    let req = Request::builder()
        .method(Method::HEAD)
        .uri(format!("/v2/std-docker/blobs/{}", layer_digest))
        .header(header::AUTHORIZATION, format!("Bearer {}", token))
        .body(Body::empty())
        .unwrap();
    let resp = app.call_resp(req).await;
    assert_eq!(resp.status(), StatusCode::OK);

    // Tags list via standard path.
    let req = app.auth_request(Method::GET, "/v2/std-docker/tags/list", &token);
    let (status, body) = app.call(req).await;
    assert_eq!(status, StatusCode::OK);
    let tags = body["tags"].as_array().unwrap();
    assert!(tags.iter().any(|t| t == "v1.0"));
}

// ===========================================================================
// Verify standard /repository/ artifact routes still work for non-Docker
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_standard_raw_repository_unaffected() {
    let app = TestApp::new().await;
    app.create_hosted_repo("std-raw").await;

    let token = app.admin_token();
    app.upload_artifact("std-raw", "path/to/file.bin", b"binary data", &token)
        .await;

    // GET artifact via /repository/ path — should still work.
    let req = app.auth_request(Method::GET, "/repository/std-raw/path/to/file.bin", &token);
    let (status, body) = app.call_raw(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"binary data");
}
