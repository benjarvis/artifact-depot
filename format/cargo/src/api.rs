// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cargo registry HTTP API implementation.
//!
//! Implements the sparse registry protocol (RFC 2789) and the Cargo publish/download API
//! for hosted, cache, and proxy repositories.

use axum::{
    body::Body,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_util::io::ReaderStream;

use depot_core::auth::AuthenticatedUser;
use depot_core::format_state::FormatState;
use depot_core::repo;
use depot_core::service;
use depot_core::store::kv::{
    ArtifactFormat, Capability, CrateDep, CrateVersionMeta, RepoConfig, RepoKind, RepoType,
};

use crate::store::CargoStore;

// --- Helpers ---

/// Return a Cargo-protocol error response.
fn cargo_error(status: StatusCode, msg: &str) -> Response {
    let body = serde_json::json!({
        "errors": [{"detail": msg}]
    });
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&body).unwrap_or_default(),
    )
        .into_response()
}

/// Return a Cargo-protocol success response.
fn cargo_ok() -> Response {
    let body = serde_json::json!({"ok": true});
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&body).unwrap_or_default(),
    )
        .into_response()
}

// =============================================================================
// /repository/{repo}/ dispatch for Cargo repos
// =============================================================================

pub async fn try_handle_repository_path(
    state: &FormatState,
    config: &RepoConfig,
    path: &str,
    query: Option<&str>,
) -> Option<Response> {
    // index/config.json → config
    if path == "index/config.json" {
        let body = serde_json::json!({
            "dl": format!("/repository/{}/api/v1/crates/{{crate}}/{{version}}/download", config.name),
            "api": format!("/repository/{}", config.name),
        });
        return Some(
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&body).unwrap_or_default(),
            )
                .into_response(),
        );
    }

    // index/{...}/{name} → sparse index entry
    if let Some(rest) = path.strip_prefix("index/") {
        if !rest.is_empty() {
            // The crate name is the last path segment
            let crate_name = rest.rsplit('/').next().unwrap_or(rest);
            if !crate_name.is_empty() {
                return Some(match config.repo_type() {
                    RepoType::Hosted => hosted_index_entry(state, config, crate_name).await,
                    RepoType::Proxy => proxy_index_entry(state, config, crate_name).await,
                    RepoType::Cache => hosted_index_entry(state, config, crate_name).await,
                });
            }
        }
    }

    // api/v1/crates/{name}/{version}/download → crate tarball
    if let Some(rest) = path.strip_prefix("api/v1/crates/") {
        if let Some(tail) = rest.strip_suffix("/download") {
            if let Some((crate_name, version)) = tail.rsplit_once('/') {
                if !crate_name.is_empty() && !version.is_empty() {
                    return Some(match config.repo_type() {
                        RepoType::Hosted | RepoType::Cache => {
                            hosted_download(state, config, crate_name, version).await
                        }
                        RepoType::Proxy => proxy_download(state, config, crate_name, version).await,
                    });
                }
            }
        }
    }

    // api/v1/crates[?q={filter}] → search.
    if path == "api/v1/crates" {
        let blobs = match state.blob_store(&config.store).await {
            Ok(b) => b,
            Err(e) => return Some(e.into_response()),
        };
        let store = CargoStore {
            repo: &config.name,
            kv: state.kv.as_ref(),
            blobs: blobs.as_ref(),
            store: &config.store,
            updater: &state.updater,
        };
        let crates = match store.list_crates().await {
            Ok(c) => c,
            Err(e) => return Some(e.into_response()),
        };
        // Optional `?q=...` substring filter.
        let filter = query
            .and_then(|q| {
                q.split('&').find_map(|p| {
                    p.strip_prefix("q=")
                        .map(|v| urlencoding::decode(v).unwrap_or_default().into_owned())
                })
            })
            .unwrap_or_default();
        let filter_lc = filter.to_lowercase();
        let results: Vec<serde_json::Value> = crates
            .iter()
            .filter(|name| filter.is_empty() || name.to_lowercase().contains(&filter_lc))
            .map(|name| {
                serde_json::json!({
                    "name": name,
                    "max_version": "0.0.0",
                    "description": "",
                })
            })
            .collect();
        let body = serde_json::json!({
            "crates": results,
            "meta": { "total": results.len() },
        });
        return Some(
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::to_string(&body).unwrap_or_default(),
            )
                .into_response(),
        );
    }

    // dl/ downloads fall through to raw handler
    None
}

// =============================================================================
// /repository/{repo}/ dispatch for Cargo repos (writes)
// =============================================================================

/// Handle a write request under `/repository/{repo}/<path>` for a Cargo repo.
/// Returns `None` if the (method, path) combination is not a Cargo write.
pub async fn try_handle_repository_write(
    state: &FormatState,
    user: &AuthenticatedUser,
    config: &RepoConfig,
    method: &axum::http::Method,
    path: &str,
    _headers: &axum::http::HeaderMap,
    body: axum::body::Body,
) -> Option<axum::response::Response> {
    // PUT api/v1/crates/new — publish
    if method == axum::http::Method::PUT && path == "api/v1/crates/new" {
        let body_bytes = match depot_core::api_helpers::body_to_bytes(body, usize::MAX).await {
            Ok(b) => b,
            Err(r) => return Some(r),
        };
        return Some(handle_publish(state, user, config, body_bytes).await);
    }

    // DELETE api/v1/crates/{crate_name}/{version}/yank
    if method == axum::http::Method::DELETE {
        if let Some(rest) = path.strip_prefix("api/v1/crates/") {
            if let Some(rest) = rest.strip_suffix("/yank") {
                let segments: Vec<&str> = rest.split('/').collect();
                if segments.len() == 2 {
                    let crate_name = segments[0];
                    let version = segments[1];
                    if !crate_name.is_empty() && !version.is_empty() {
                        return Some(
                            handle_set_yanked(state, user, config, crate_name, version, true).await,
                        );
                    }
                }
            }
        }
    }

    // PUT api/v1/crates/{crate_name}/{version}/unyank
    if method == axum::http::Method::PUT {
        if let Some(rest) = path.strip_prefix("api/v1/crates/") {
            if let Some(rest) = rest.strip_suffix("/unyank") {
                let segments: Vec<&str> = rest.split('/').collect();
                if segments.len() == 2 {
                    let crate_name = segments[0];
                    let version = segments[1];
                    if !crate_name.is_empty() && !version.is_empty() {
                        return Some(
                            handle_set_yanked(state, user, config, crate_name, version, false)
                                .await,
                        );
                    }
                }
            }
        }
    }

    None
}

async fn hosted_index_entry(state: &FormatState, config: &RepoConfig, name: &str) -> Response {
    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = CargoStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    match store.build_index_entry(name).await {
        Ok(body) if body.is_empty() => cargo_error(StatusCode::NOT_FOUND, "crate not found"),
        Ok(body) => {
            let etag = format!("\"{}\"", blake3::hash(body.as_bytes()).to_hex());
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "text/plain".to_string()),
                    (header::ETAG, etag),
                ],
                body,
            )
                .into_response()
        }
        Err(e) => e.into_response(),
    }
}

async fn proxy_index_entry(state: &FormatState, config: &RepoConfig, name: &str) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return cargo_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "expected proxy repo kind",
        );
    };

    for member_name in members {
        let mn = member_name.clone();
        let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
            Ok(Some(c)) if c.format() == ArtifactFormat::Cargo => c,
            _ => continue,
        };

        let resp = hosted_index_entry(state, &member_config, name).await;
        if resp.status() == StatusCode::OK {
            return resp;
        }
    }

    cargo_error(StatusCode::NOT_FOUND, "crate not found in any member")
}

// =============================================================================
// PUT /cargo/{repo}/api/v1/crates/new — publish
// =============================================================================

/// Metadata JSON from the cargo publish wire format.
#[derive(Debug, Deserialize)]
struct PublishMeta {
    name: String,
    vers: String,
    #[serde(default)]
    deps: Vec<PublishDep>,
    #[serde(default)]
    features: serde_json::Value,
    #[serde(default)]
    features2: Option<serde_json::Value>,
    #[serde(default)]
    links: Option<String>,
    #[serde(default)]
    rust_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublishDep {
    name: String,
    version_req: String,
    #[serde(default)]
    features: Vec<String>,
    #[serde(default)]
    optional: bool,
    #[serde(default = "default_true")]
    default_features: bool,
    #[serde(default)]
    target: Option<String>,
    #[serde(default = "default_kind")]
    kind: String,
    #[serde(default)]
    registry: Option<String>,
    #[serde(default)]
    explicit_name_in_toml: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_kind() -> String {
    "normal".to_string()
}

/// Shared guts for cargo publish. Parses the [json_len][json][crate_len][crate]
/// wire format, stores the crate blob, and records the version metadata.
async fn handle_publish(
    state: &FormatState,
    user: &AuthenticatedUser,
    config: &RepoConfig,
    body_bytes: axum::body::Bytes,
) -> Response {
    if let Err(e) = state
        .auth
        .check_permission(&user.0, &config.name, Capability::Write)
        .await
    {
        return e.into_response();
    }

    let (target_repo, _target_config) =
        match repo::resolve_format_write_target(state.kv.as_ref(), config, ArtifactFormat::Cargo)
            .await
        {
            Ok(v) => v,
            Err(e) => return cargo_error(StatusCode::BAD_REQUEST, &e.to_string()),
        };

    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };

    // Parse header: [4-byte LE json_len][json][4-byte LE crate_len][crate bytes]
    let buf = body_bytes.as_ref();
    if buf.len() < 4 {
        return cargo_error(StatusCode::BAD_REQUEST, "request body too short");
    }
    #[allow(clippy::indexing_slicing)]
    let json_len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let mut offset = 4usize;

    if buf.len() < offset + json_len {
        return cargo_error(
            StatusCode::BAD_REQUEST,
            "request body too short for metadata",
        );
    }
    #[allow(clippy::indexing_slicing)]
    let json_bytes = &buf[offset..offset + json_len];
    offset += json_len;

    let meta: PublishMeta = match serde_json::from_slice(json_bytes) {
        Ok(m) => m,
        Err(e) => {
            return cargo_error(
                StatusCode::BAD_REQUEST,
                &format!("invalid metadata JSON: {e}"),
            );
        }
    };

    if buf.len() < offset + 4 {
        return cargo_error(
            StatusCode::BAD_REQUEST,
            "request body too short for crate length",
        );
    }
    #[allow(clippy::indexing_slicing)]
    let crate_len = u32::from_le_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ]) as u64;
    offset += 4;

    if (buf.len() as u64) < (offset as u64) + crate_len {
        return cargo_error(
            StatusCode::BAD_REQUEST,
            &format!(
                "request body too short for crate data: expected {} bytes, got {}",
                crate_len,
                buf.len().saturating_sub(offset),
            ),
        );
    }
    #[allow(clippy::indexing_slicing)]
    let crate_bytes = &buf[offset..offset + crate_len as usize];

    // Write the crate bytes to BlobWriter, computing hashes.
    let blob_id = uuid::Uuid::new_v4().to_string();
    let mut blob_writer = match blobs.create_writer(&blob_id, Some(crate_len)).await {
        Ok(w) => w,
        Err(e) => return e.into_response(),
    };
    let mut blake3_hasher = blake3::Hasher::new();
    let mut sha256_hasher = Sha256::new();

    blake3_hasher.update(crate_bytes);
    Digest::update(&mut sha256_hasher, crate_bytes);
    if let Err(e) = blob_writer.write_chunk(crate_bytes).await {
        let _ = blob_writer.abort().await;
        return cargo_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("blob write failed: {e}"),
        );
    }

    if let Err(e) = blob_writer.finish().await {
        return cargo_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("blob finish failed: {e}"),
        );
    }

    let blake3_hash = blake3_hasher.finalize().to_hex().to_string();
    let sha256 = hex_encode(Digest::finalize(sha256_hasher));
    let size = crate_len;

    // Build CrateVersionMeta from publish metadata.
    let deps: Vec<CrateDep> = meta
        .deps
        .iter()
        .map(|d| CrateDep {
            name: d
                .explicit_name_in_toml
                .clone()
                .unwrap_or_else(|| d.name.clone()),
            req: d.version_req.clone(),
            features: d.features.clone(),
            optional: d.optional,
            default_features: d.default_features,
            target: d.target.clone(),
            kind: d.kind.clone(),
            registry: d.registry.clone(),
            package: d.explicit_name_in_toml.as_ref().map(|_| d.name.clone()),
        })
        .collect();

    let features_str = if meta.features.is_null() {
        "{}".to_string()
    } else {
        serde_json::to_string(&meta.features).unwrap_or_else(|_| "{}".to_string())
    };
    let features2_str = meta
        .features2
        .map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "{}".to_string()));

    let version_meta = CrateVersionMeta {
        name: meta.name.clone(),
        vers: meta.vers.clone(),
        deps,
        cksum: sha256,
        features: features_str,
        features2: features2_str,
        yanked: false,
        links: meta.links,
        rust_version: meta.rust_version,
    };

    let store = CargoStore {
        repo: &target_repo,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    match store
        .commit_crate(&version_meta, &blob_id, &blake3_hash, size)
        .await
    {
        Ok(_) => cargo_ok(),
        Err(e) => {
            tracing::error!(error = %e, "cargo publish failed");
            cargo_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string())
        }
    }
}

async fn hosted_download(
    state: &FormatState,
    config: &RepoConfig,
    name: &str,
    version: &str,
) -> Response {
    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = CargoStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    match store.get_crate(name, version).await {
        Ok(Some((reader, size, _record))) => {
            let stream = ReaderStream::with_capacity(reader, 256 * 1024);
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                "application/x-tar"
                    .parse()
                    .unwrap_or(header::HeaderValue::from_static("application/octet-stream")),
            );
            headers.insert(header::CONTENT_LENGTH, size.into());
            (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
        }
        Ok(None) => cargo_error(StatusCode::NOT_FOUND, "crate not found"),
        Err(e) => cargo_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

async fn proxy_download(
    state: &FormatState,
    config: &RepoConfig,
    name: &str,
    version: &str,
) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return cargo_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "expected proxy repo kind",
        );
    };

    for member_name in members {
        let mn = member_name.clone();
        let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
            Ok(Some(c)) if c.format() == ArtifactFormat::Cargo => c,
            _ => continue,
        };

        let resp = hosted_download(state, &member_config, name, version).await;
        if resp.status() == StatusCode::OK {
            return resp;
        }
    }

    cargo_error(StatusCode::NOT_FOUND, "crate not found in any member")
}

/// Shared guts for yank/unyank. Flips the `yanked` flag on a crate version.
async fn handle_set_yanked(
    state: &FormatState,
    user: &AuthenticatedUser,
    config: &RepoConfig,
    crate_name: &str,
    version: &str,
    yanked: bool,
) -> Response {
    if let Err(e) = state
        .auth
        .check_permission(&user.0, &config.name, Capability::Write)
        .await
    {
        return e.into_response();
    }

    let (target_repo, _target_config) =
        match repo::resolve_format_write_target(state.kv.as_ref(), config, ArtifactFormat::Cargo)
            .await
        {
            Ok(v) => v,
            Err(e) => return cargo_error(StatusCode::BAD_REQUEST, &e.to_string()),
        };

    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = CargoStore {
        repo: &target_repo,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    match store.set_yanked(crate_name, version, yanked).await {
        Ok(true) => cargo_ok(),
        Ok(false) => cargo_error(StatusCode::NOT_FOUND, "crate version not found"),
        Err(e) => cargo_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

fn hex_encode(data: impl AsRef<[u8]>) -> String {
    data.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}
