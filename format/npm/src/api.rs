// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! npm HTTP API implementation.
//!
//! Implements the npm registry protocol for hosted, cache, and proxy repositories.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use serde::Deserialize;
use std::collections::HashMap;
use tokio_util::io::ReaderStream;

use depot_core::store_from_config_fn;

use crate::store::{self as npm, NpmStore};
use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;
use depot_core::format_state::FormatState;
use depot_core::service;
use depot_core::store::kv::{
    ArtifactFormat, Capability, RepoConfig, RepoKind, RepoType, CURRENT_RECORD_VERSION,
};

store_from_config_fn!(npm_store_from_config, NpmStore);

// =============================================================================
// /repository/{repo}/ dispatch for npm repos
// =============================================================================

pub async fn try_handle_repository_path(
    state: &FormatState,
    config: &RepoConfig,
    path: &str,
) -> Option<Response> {
    // Scoped package: @scope/package or @scope/package/-/filename
    if let Some(rest) = path.strip_prefix('@') {
        let parts: Vec<&str> = rest.splitn(4, '/').collect();
        if let [scope, pkg] = parts.as_slice() {
            // @scope/package → packument
            let package = format!("@{scope}/{pkg}");
            return Some(get_packument_inner_from_config(state, config, &package).await);
        }
        if let [scope, pkg, "-", filename] = parts.as_slice() {
            // @scope/package/-/filename → tarball download
            let package = format!("@{scope}/{pkg}");
            return Some(
                download_tarball_inner_from_config(state, config, &package, filename).await,
            );
        }
        return None;
    }

    // Unscoped package/-/filename.tgz → tarball download
    if let Some((package, rest)) = path.split_once("/-/") {
        if !package.is_empty() && !package.contains('/') && !rest.is_empty() {
            return Some(download_tarball_inner_from_config(state, config, package, rest).await);
        }
    }

    // Unscoped package → packument (must not contain '/')
    if !path.is_empty() && !path.contains('/') {
        return Some(get_packument_inner_from_config(state, config, path).await);
    }

    None
}

async fn get_packument_inner_from_config(
    state: &FormatState,
    config: &RepoConfig,
    package: &str,
) -> Response {
    match config.repo_type() {
        RepoType::Hosted => hosted_packument(state, &config.name, package).await,
        RepoType::Cache => cache_packument(state, config, package).await,
        RepoType::Proxy => proxy_packument(state, config, package).await,
    }
}

async fn download_tarball_inner_from_config(
    state: &FormatState,
    config: &RepoConfig,
    package: &str,
    filename: &str,
) -> Response {
    let version = match extract_version_from_filename(filename, package) {
        Some(v) => v,
        None => {
            return DepotError::BadRequest("invalid tarball filename".into()).into_response();
        }
    };

    match config.repo_type() {
        RepoType::Hosted => hosted_download(state, &config.name, package, &version, filename).await,
        RepoType::Cache => cache_download(state, config, package, &version, filename).await,
        RepoType::Proxy => proxy_download(state, config, package, &version, filename).await,
    }
}

// =============================================================================
// GET /-/whoami — npm whoami
// =============================================================================

pub async fn whoami(Extension(user): Extension<AuthenticatedUser>) -> Result<Response, DepotError> {
    let body = serde_json::json!({ "username": user.0 });
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&body).unwrap_or_default(),
    )
        .into_response())
}

// =============================================================================
// PUT /-/user/org.couchdb.user:{username} — npm login/adduser
// =============================================================================

pub async fn npm_login(
    State(state): State<FormatState>,
    body: axum::body::Bytes,
) -> Result<Response, DepotError> {
    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            return Err(DepotError::BadRequest(format!("invalid JSON: {e}")));
        }
    };

    let name = match payload.get("name").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => {
            return Err(DepotError::BadRequest("missing 'name' field".into()));
        }
    };
    let password = match payload.get("password").and_then(|v| v.as_str()) {
        Some(p) => p,
        None => {
            return Err(DepotError::BadRequest("missing 'password' field".into()));
        }
    };

    match state.auth.authenticate(name, password).await? {
        Some(user) => {
            let token = state.auth.create_user_token(&user).await?;
            let body = serde_json::json!({ "ok": true, "token": token });
            Ok((
                StatusCode::CREATED,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&body).unwrap_or_default(),
            )
                .into_response())
        }
        None => Ok(StatusCode::UNAUTHORIZED.into_response()),
    }
}

// =============================================================================
// GET /npm/{repo}/{package} — get packument (unscoped)
// =============================================================================

pub async fn get_packument(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, package)): Path<(String, String)>,
) -> Result<Response, DepotError> {
    Ok(get_packument_inner(&state, &user, &repo_name, &package).await)
}

// =============================================================================
// GET /npm/{repo}/@{scope}/{package} — get packument (scoped)
// =============================================================================

pub async fn get_packument_scoped(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, scope, package)): Path<(String, String, String)>,
) -> Result<Response, DepotError> {
    let full_name = format!("@{scope}/{package}");
    Ok(get_packument_inner(&state, &user, &repo_name, &full_name).await)
}

async fn get_packument_inner(
    state: &FormatState,
    user: &AuthenticatedUser,
    repo_name: &str,
    package: &str,
) -> Response {
    let config =
        match depot_core::api_helpers::validate_format_repo(state, repo_name, ArtifactFormat::Npm)
            .await
        {
            Ok(c) => c,
            Err(e) => return e.into_response(),
        };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, repo_name, Capability::Read)
        .await
    {
        return e.into_response();
    }

    match config.repo_type() {
        RepoType::Hosted => hosted_packument(state, repo_name, package).await,
        RepoType::Cache => cache_packument(state, &config, package).await,
        RepoType::Proxy => proxy_packument(state, &config, package).await,
    }
}

async fn hosted_packument(state: &FormatState, repo_name: &str, package: &str) -> Response {
    let (blobs, blob_config) =
        match depot_core::api_helpers::resolve_repo_blob_store(state, repo_name).await {
            Ok(b) => b,
            Err(e) => return e.into_response(),
        };
    let store_name = blob_config.store.clone();
    let store = NpmStore {
        repo: repo_name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &store_name,
        updater: &state.updater,
    };

    let base_url = format!("/npm/{repo_name}");
    match store.get_packument(package, &base_url).await {
        Ok(Some(packument)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_string(&packument).unwrap_or_default(),
        )
            .into_response(),
        Ok(None) => DepotError::NotFound("package not found".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn cache_packument(state: &FormatState, config: &RepoConfig, package: &str) -> Response {
    let RepoKind::Cache {
        ref upstream_url,
        ref upstream_auth,
        cache_ttl_secs,
        ..
    } = config.kind
    else {
        return DepotError::Internal("expected cache repo kind".into()).into_response();
    };

    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };

    // Check local cache freshness
    let store = NpmStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    // Try to fetch from upstream
    match npm::fetch_upstream_packument(&state.http, upstream_url, package, upstream_auth.as_ref())
        .await
    {
        Ok(Some(upstream_packument)) => {
            // Cache version metadata and dist-tags from upstream
            cache_upstream_packument(state, config, package, &upstream_packument).await;

            // Rewrite tarball URLs and serve
            let base_url = format!("/npm/{}", config.name);
            let rewritten = rewrite_packument_urls(&upstream_packument, &base_url);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&rewritten).unwrap_or_default(),
            )
                .into_response()
        }
        Ok(None) => DepotError::NotFound("package not found upstream".into()).into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "upstream fetch failed, serving stale cache");
            // Serve stale cache
            let base_url = format!("/npm/{}", config.name);
            match store.get_packument(package, &base_url).await {
                Ok(Some(packument)) => (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json")],
                    serde_json::to_string(&packument).unwrap_or_default(),
                )
                    .into_response(),
                _ => {
                    let _ = cache_ttl_secs; // suppress unused warning
                    DepotError::Upstream(format!("upstream error: {e}")).into_response()
                }
            }
        }
    }
}

/// Cache version metadata from an upstream packument.
async fn cache_upstream_packument(
    state: &FormatState,
    config: &RepoConfig,
    package: &str,
    packument: &serde_json::Value,
) {
    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(_) => return,
    };
    let now = depot_core::repo::now_utc();

    // Cache dist-tags
    if let Some(tags_obj) = packument.get("dist-tags").and_then(|v| v.as_object()) {
        let tags: HashMap<String, String> = tags_obj
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();
        let tags_json = serde_json::to_string(&tags).unwrap_or_default();
        let tags_bytes = tags_json.as_bytes();
        let tags_blob_id = uuid::Uuid::new_v4().to_string();
        let tags_hash = blake3::hash(tags_bytes).to_hex().to_string();
        if blobs.put(&tags_blob_id, tags_bytes).await.is_ok() {
            let blob_rec = depot_core::store::kv::BlobRecord {
                schema_version: CURRENT_RECORD_VERSION,
                blob_id: tags_blob_id.clone(),
                hash: tags_hash.clone(),
                size: tags_bytes.len() as u64,
                created_at: now,
                store: config.store.clone(),
            };
            let tags_record = depot_core::store::kv::ArtifactRecord {
                schema_version: CURRENT_RECORD_VERSION,
                id: String::new(),
                size: tags_bytes.len() as u64,
                content_type: "application/json".to_string(),
                kind: depot_core::store::kv::ArtifactKind::NpmMetadata,
                blob_id: Some(tags_blob_id.clone()),
                content_hash: Some(tags_hash.clone()),
                etag: Some(tags_hash.clone()),
                created_at: now,
                updated_at: now,
                last_accessed_at: now,
                path: String::new(),
                internal: true,
            };
            let dt_path = npm::dist_tags_path(package);
            let cn = config.name.clone();
            let store = config.store.clone();
            let _ = service::put_dedup_record(state.kv.as_ref(), &store, &blob_rec).await;
            let _ = service::put_artifact(state.kv.as_ref(), &cn, &dt_path, &tags_record).await;
        }
    }

    // Cache each version's metadata
    if let Some(versions) = packument.get("versions").and_then(|v| v.as_object()) {
        for (version, ver_obj) in versions {
            let ver_json = serde_json::to_string(ver_obj).unwrap_or_default();
            let ver_bytes = ver_json.as_bytes();
            let ver_blob_id = uuid::Uuid::new_v4().to_string();
            let ver_hash = blake3::hash(ver_bytes).to_hex().to_string();
            if blobs.put(&ver_blob_id, ver_bytes).await.is_ok() {
                let blob_rec = depot_core::store::kv::BlobRecord {
                    schema_version: CURRENT_RECORD_VERSION,
                    blob_id: ver_blob_id.clone(),
                    hash: ver_hash.clone(),
                    size: ver_bytes.len() as u64,
                    created_at: now,
                    store: config.store.clone(),
                };
                let ver_record = depot_core::store::kv::ArtifactRecord {
                    schema_version: CURRENT_RECORD_VERSION,
                    id: String::new(),
                    size: ver_bytes.len() as u64,
                    content_type: "application/json".to_string(),
                    kind: depot_core::store::kv::ArtifactKind::NpmMetadata,
                    blob_id: Some(ver_blob_id.clone()),
                    content_hash: Some(ver_hash.clone()),
                    etag: Some(ver_hash.clone()),
                    created_at: now,
                    updated_at: now,
                    last_accessed_at: now,
                    path: String::new(),
                    internal: true,
                };
                let ver_path = npm::version_path(package, version);
                let cn = config.name.clone();
                let store = config.store.clone();
                let _ = service::put_dedup_record(state.kv.as_ref(), &store, &blob_rec).await;
                let _ = service::put_artifact(state.kv.as_ref(), &cn, &ver_path, &ver_record).await;
            }
        }
    }
}

/// Rewrite tarball URLs in a packument to point to our server.
fn rewrite_packument_urls(packument: &serde_json::Value, base_url: &str) -> serde_json::Value {
    let mut result = packument.clone();
    if let Some(versions) = result.get_mut("versions").and_then(|v| v.as_object_mut()) {
        for (version, ver_obj) in versions.iter_mut() {
            // Extract name before mutable borrow of dist
            let name = ver_obj
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let bare_name = name.rsplit('/').next().unwrap_or(&name);
            let filename = format!("{}-{}.tgz", npm::normalize_name(bare_name), version);
            let tarball_url = format!(
                "{base_url}/{name}/-/{filename}",
                base_url = base_url.trim_end_matches('/'),
            );
            if let Some(dist) = ver_obj.get_mut("dist").and_then(|v| v.as_object_mut()) {
                dist.insert(
                    "tarball".to_string(),
                    serde_json::Value::String(tarball_url),
                );
            }
        }
    }
    result
}

async fn proxy_packument(state: &FormatState, config: &RepoConfig, package: &str) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return DepotError::Internal("expected proxy repo kind".into()).into_response();
    };

    let proxy_blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let proxy_store = npm_store_from_config(config, state, proxy_blobs.as_ref());

    // Check if cached packument is fresh (newer than stale watermark).
    let stale_ts = proxy_store
        .get_stale_flag()
        .await
        .ok()
        .flatten()
        .map(|r| r.updated_at);
    if let Ok(Some((json, record))) = proxy_store.get_cached_packument(package).await {
        let is_fresh = match stale_ts {
            Some(ts) => record.updated_at >= ts,
            None => true, // no stale flag means nothing changed
        };
        if is_fresh {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                json,
            )
                .into_response();
        }
    }

    // Re-merge from members.
    let base_url = format!("/npm/{}", config.name);
    let mut merged_versions = serde_json::Map::new();
    let mut merged_dist_tags: HashMap<String, String> = HashMap::new();
    let mut merged_time = serde_json::Map::new();

    for member_name in members {
        let mn = member_name.clone();
        let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
            Ok(Some(c)) if c.format() == ArtifactFormat::Npm => c,
            _ => continue,
        };

        let member_blobs = match state.blob_store(&member_config.store).await {
            Ok(b) => b,
            Err(_) => continue,
        };

        let store = NpmStore {
            repo: member_name,
            kv: state.kv.as_ref(),
            blobs: member_blobs.as_ref(),
            store: &member_config.store,
            updater: &state.updater,
        };

        if let Ok(Some(packument)) = store.get_packument(package, &base_url).await {
            if let Some(versions) = packument.get("versions").and_then(|v| v.as_object()) {
                for (ver, obj) in versions {
                    if !merged_versions.contains_key(ver) {
                        merged_versions.insert(ver.clone(), obj.clone());
                    }
                }
            }
            if let Some(tags) = packument.get("dist-tags").and_then(|v| v.as_object()) {
                for (k, v) in tags {
                    if !merged_dist_tags.contains_key(k) {
                        if let Some(s) = v.as_str() {
                            merged_dist_tags.insert(k.clone(), s.to_string());
                        }
                    }
                }
            }
            if let Some(time) = packument.get("time").and_then(|v| v.as_object()) {
                for (k, v) in time {
                    if !merged_time.contains_key(k) {
                        merged_time.insert(k.clone(), v.clone());
                    }
                }
            }
        }
    }

    if merged_versions.is_empty() {
        return DepotError::NotFound("package not found in any member".into()).into_response();
    }

    let packument = serde_json::json!({
        "name": package,
        "dist-tags": merged_dist_tags,
        "versions": merged_versions,
        "time": merged_time,
    });

    let json = serde_json::to_string(&packument).unwrap_or_default();

    // Cache the merged result on the proxy.
    let _ = proxy_store.store_cached_packument(package, &json).await;

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        json,
    )
        .into_response()
}

// =============================================================================
// GET /npm/{repo}/{package}/-/{filename} — download tarball (unscoped)
// =============================================================================

pub async fn download_tarball(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, package, filename)): Path<(String, String, String)>,
) -> Result<Response, DepotError> {
    Ok(download_tarball_inner(&state, &user, &repo_name, &package, &filename).await)
}

// =============================================================================
// GET /npm/{repo}/@{scope}/{package}/-/{filename} — download tarball (scoped)
// =============================================================================

pub async fn download_tarball_scoped(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, scope, package, filename)): Path<(String, String, String, String)>,
) -> Result<Response, DepotError> {
    let full_name = format!("@{scope}/{package}");
    Ok(download_tarball_inner(&state, &user, &repo_name, &full_name, &filename).await)
}

async fn download_tarball_inner(
    state: &FormatState,
    user: &AuthenticatedUser,
    repo_name: &str,
    package: &str,
    filename: &str,
) -> Response {
    let config =
        match depot_core::api_helpers::validate_format_repo(state, repo_name, ArtifactFormat::Npm)
            .await
        {
            Ok(c) => c,
            Err(e) => return e.into_response(),
        };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, repo_name, Capability::Read)
        .await
    {
        return e.into_response();
    }

    // Extract version from filename: {bare_name}-{version}.tgz
    let version = match extract_version_from_filename(filename, package) {
        Some(v) => v,
        None => {
            return DepotError::BadRequest("invalid tarball filename".into()).into_response();
        }
    };

    match config.repo_type() {
        RepoType::Hosted => hosted_download(state, repo_name, package, &version, filename).await,
        RepoType::Cache => cache_download(state, &config, package, &version, filename).await,
        RepoType::Proxy => proxy_download(state, &config, package, &version, filename).await,
    }
}

/// Extract version from an npm tarball filename.
/// Format: `{name}-{version}.tgz`
///
/// Primary: strip the known package name prefix + `-` from the stem.
/// Fallback: scan from the end for the rightmost `-` followed by a digit.
pub fn extract_version_from_filename(filename: &str, package_name: &str) -> Option<String> {
    let stem = filename.strip_suffix(".tgz")?;

    // Primary: strip known package name prefix
    let bare_name = package_name.rsplit('/').next().unwrap_or(package_name);
    let norm = npm::normalize_name(bare_name);
    let prefix = format!("{norm}-");
    if let Some(version) = stem.strip_prefix(&prefix) {
        if !version.is_empty() {
            return Some(version.to_string());
        }
    }

    // Fallback: scan from the end for the rightmost `-` followed by a digit
    for (i, c) in stem.char_indices().rev() {
        if c == '-' && i + 1 < stem.len() {
            let (_, version) = stem.split_at(i + 1);
            if version.starts_with(|ch: char| ch.is_ascii_digit()) && !version.is_empty() {
                return Some(version.to_string());
            }
        }
    }
    None
}

fn npm_stream_response(
    reader: depot_core::store::blob::BlobReader,
    size: u64,
    filename: &str,
) -> Response {
    let stream = ReaderStream::with_capacity(reader, 256 * 1024);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        "application/octet-stream"
            .parse()
            .unwrap_or(header::HeaderValue::from_static("application/octet-stream")),
    );
    if let Ok(v) = format!("attachment; filename=\"{filename}\"").parse() {
        headers.insert(header::CONTENT_DISPOSITION, v);
    }
    headers.insert(header::CONTENT_LENGTH, size.into());
    (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
}

async fn hosted_download(
    state: &FormatState,
    repo_name: &str,
    name: &str,
    version: &str,
    filename: &str,
) -> Response {
    let (blobs, blob_config) =
        match depot_core::api_helpers::resolve_repo_blob_store(state, repo_name).await {
            Ok(b) => b,
            Err(e) => return e.into_response(),
        };
    let store_name = blob_config.store.clone();
    let store = NpmStore {
        repo: repo_name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &store_name,
        updater: &state.updater,
    };

    match store.get_tarball(name, version, filename).await {
        Ok(Some((reader, size, _record))) => npm_stream_response(reader, size, filename),
        Ok(None) => DepotError::NotFound("tarball not found".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn cache_download(
    state: &FormatState,
    config: &RepoConfig,
    name: &str,
    version: &str,
    filename: &str,
) -> Response {
    let RepoKind::Cache {
        ref upstream_url,
        ref upstream_auth,
        ..
    } = config.kind
    else {
        return DepotError::Internal("expected cache repo kind".into()).into_response();
    };
    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = NpmStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    // Check local cache first
    if let Ok(Some((reader, size, _record))) = store.get_tarball(name, version, filename).await {
        return npm_stream_response(reader, size, filename);
    }

    // Fetch from upstream — we need to find the tarball URL from cached version metadata
    let ver_meta = store
        .get_version_metadata(name, version)
        .await
        .ok()
        .flatten();
    let upstream_tarball_url = ver_meta.and_then(|meta| {
        let obj: serde_json::Value = serde_json::from_str(&meta).ok()?;
        obj.get("dist")?
            .get("tarball")?
            .as_str()
            .map(|s| s.to_string())
    });

    // If no cached metadata, construct the tarball URL from the upstream base URL
    let tarball_url = upstream_tarball_url
        .unwrap_or_else(|| format!("{}/{name}/-/{filename}", upstream_url.trim_end_matches('/')));

    // Resolve relative URLs against the upstream base
    let url = if tarball_url.starts_with("http://") || tarball_url.starts_with("https://") {
        tarball_url
    } else if let Some((scheme, after_scheme)) = upstream_url.split_once("://") {
        // Extract origin from upstream_url
        let host = after_scheme
            .split_once('/')
            .map_or(after_scheme, |(h, _)| h);
        let origin = format!("{scheme}://{host}");
        format!("{origin}{tarball_url}")
    } else {
        tarball_url
    };

    {
        match npm::fetch_upstream_tarball(&state.http, &url, upstream_auth.as_ref()).await {
            Ok(resp) => {
                let data = match resp.bytes().await {
                    Ok(b) => b.to_vec(),
                    Err(e) => {
                        return DepotError::Upstream(format!("failed to read tarball: {e}"))
                            .into_response()
                    }
                };

                // Cache it locally
                let _ = store
                    .put_package_from_upstream(
                        name,
                        version,
                        filename,
                        &data,
                        "", // sha1 unknown
                        "", // sha512 unknown
                        &serde_json::json!({"name": name, "version": version}).to_string(),
                        &url,
                    )
                    .await;

                // Serve it
                if let Ok(Some((reader, size, _))) =
                    store.get_tarball(name, version, filename).await
                {
                    return npm_stream_response(reader, size, filename);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to fetch tarball from upstream");
            }
        }
    }

    DepotError::NotFound("tarball not found".into()).into_response()
}

async fn proxy_download(
    state: &FormatState,
    config: &RepoConfig,
    name: &str,
    version: &str,
    filename: &str,
) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return DepotError::Internal("expected proxy repo kind".into()).into_response();
    };

    for member_name in members {
        let mn = member_name.clone();
        let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
            Ok(Some(c)) if c.format() == ArtifactFormat::Npm => c,
            _ => continue,
        };

        match member_config.repo_type() {
            RepoType::Hosted => {
                let resp = hosted_download(state, member_name, name, version, filename).await;
                if resp.status() == StatusCode::OK {
                    return resp;
                }
            }
            RepoType::Cache => {
                let resp = cache_download(state, &member_config, name, version, filename).await;
                if resp.status() == StatusCode::OK {
                    return resp;
                }
            }
            RepoType::Proxy => {
                let resp = hosted_download(state, member_name, name, version, filename).await;
                if resp.status() == StatusCode::OK {
                    return resp;
                }
            }
        }
    }

    DepotError::NotFound("tarball not found in any member".into()).into_response()
}

// =============================================================================
// PUT /npm/{repo}/{package} — publish (unscoped)
// =============================================================================

pub async fn publish(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, package)): Path<(String, String)>,
    body: axum::body::Bytes,
) -> Result<Response, DepotError> {
    Ok(publish_inner(&state, &user, &repo_name, &package, &body).await)
}

// =============================================================================
// PUT /npm/{repo}/@{scope}/{package} — publish (scoped)
// =============================================================================

pub async fn publish_scoped(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, scope, package)): Path<(String, String, String)>,
    body: axum::body::Bytes,
) -> Result<Response, DepotError> {
    let full_name = format!("@{scope}/{package}");
    Ok(publish_inner(&state, &user, &repo_name, &full_name, &body).await)
}

async fn publish_inner(
    state: &FormatState,
    user: &AuthenticatedUser,
    repo_name: &str,
    package: &str,
    body: &[u8],
) -> Response {
    let (target_repo, target_config, blobs) = match depot_core::api_helpers::upload_preamble(
        state,
        &user.0,
        repo_name,
        ArtifactFormat::Npm,
    )
    .await
    {
        Ok(v) => v,
        Err(e) => return e.into_response(),
    };

    // Parse the npm publish JSON body
    let payload: serde_json::Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(e) => {
            return DepotError::BadRequest(format!("invalid JSON: {e}")).into_response();
        }
    };

    // Extract versions
    let versions = match payload.get("versions").and_then(|v| v.as_object()) {
        Some(v) => v,
        None => {
            return DepotError::BadRequest("missing 'versions' field".into()).into_response();
        }
    };

    // Extract dist-tags
    let mut dist_tags: HashMap<String, String> = payload
        .get("dist-tags")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    // Auto-create "latest" dist-tag if not present
    if !dist_tags.contains_key("latest") {
        // Use the last version from the versions map
        if let Some(last_version) = versions.keys().next_back() {
            dist_tags.insert("latest".to_string(), last_version.clone());
        }
    }

    // Extract _attachments
    let attachments = match payload.get("_attachments").and_then(|v| v.as_object()) {
        Some(a) => a,
        None => {
            return DepotError::BadRequest("missing '_attachments' field".into()).into_response();
        }
    };

    // Process each version
    for (version, ver_obj) in versions {
        let version_metadata_json = serde_json::to_string(ver_obj).unwrap_or_default();

        // Determine attachment filename
        let expected_filename = format!("{}-{}.tgz", package, version);
        // Try the expected filename first, then try normalized
        let norm_filename = format!("{}-{}.tgz", npm::normalize_name(package), version);
        let attachment_key = if attachments.contains_key(&expected_filename) {
            expected_filename.clone()
        } else if attachments.contains_key(&norm_filename) {
            norm_filename.clone()
        } else {
            // Try to find any matching attachment
            match attachments.keys().next() {
                Some(k) => k.clone(),
                None => {
                    return DepotError::BadRequest(format!(
                        "no attachment found for version {version}"
                    ))
                    .into_response();
                }
            }
        };

        // Normalize the stored filename to match what packument URLs produce
        let bare_name = package.rsplit('/').next().unwrap_or(package);
        let stored_filename = format!("{}-{}.tgz", npm::normalize_name(bare_name), version);

        let attachment = &attachments[&attachment_key];
        let data_b64 = match attachment.get("data").and_then(|v| v.as_str()) {
            Some(d) => d,
            None => {
                return DepotError::BadRequest("attachment missing 'data' field".into())
                    .into_response();
            }
        };

        use base64::Engine;
        let tarball_data = match base64::engine::general_purpose::STANDARD.decode(data_b64) {
            Ok(d) => d,
            Err(e) => {
                return DepotError::BadRequest(format!("invalid base64 in attachment: {e}"))
                    .into_response();
            }
        };

        let store = NpmStore {
            repo: &target_repo,
            kv: state.kv.as_ref(),
            blobs: blobs.as_ref(),
            store: &target_config.store,
            updater: &state.updater,
        };

        match store
            .put_package(
                package,
                version,
                &stored_filename,
                &tarball_data,
                &version_metadata_json,
                &dist_tags,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::error!(error = %e, "npm publish failed");
                return e.into_response();
            }
        }
    }

    // Set stale and propagate to parent proxies.
    let store = NpmStore {
        repo: &target_repo,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &target_config.store,
        updater: &state.updater,
    };
    let _ = store.set_stale_flag().await;
    depot_core::api_helpers::propagate_staleness_to_parents(
        state,
        repo_name,
        ArtifactFormat::Npm,
        "_npm/metadata_stale",
    )
    .await;

    StatusCode::OK.into_response()
}

// =============================================================================
// GET /npm/{repo}/-/v1/search — search packages
// =============================================================================

#[derive(Deserialize)]
pub struct SearchQuery {
    text: Option<String>,
    #[serde(default = "default_search_size")]
    size: usize,
}

fn default_search_size() -> usize {
    20
}

pub async fn search(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(repo_name): Path<String>,
    Query(query): Query<SearchQuery>,
) -> Result<Response, DepotError> {
    let config =
        depot_core::api_helpers::validate_format_repo(&state, &repo_name, ArtifactFormat::Npm)
            .await?;

    state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await?;

    let blobs = state.blob_store(&config.store).await?;
    let store = NpmStore {
        repo: &repo_name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    let packages = store.list_packages().await?;

    let text = query.text.unwrap_or_default().to_lowercase();
    let filtered: Vec<_> = packages
        .into_iter()
        .filter(|name| text.is_empty() || name.contains(&text))
        .take(query.size)
        .collect();

    let mut objects: Vec<serde_json::Value> = Vec::new();
    for name in &filtered {
        // Look up dist-tags to find "latest" version
        let version = match store.get_dist_tags(name).await {
            Ok(tags) => tags
                .get("latest")
                .cloned()
                .unwrap_or_else(|| "0.0.0".to_string()),
            Err(_) => "0.0.0".to_string(),
        };
        objects.push(serde_json::json!({
            "package": {
                "name": name,
                "version": version,
                "description": "",
            }
        }));
    }

    let result = serde_json::json!({ "objects": objects });

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&result).unwrap_or_default(),
    )
        .into_response())
}
