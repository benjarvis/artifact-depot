// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cargo registry HTTP API implementation.
//!
//! Implements the sparse registry protocol (RFC 2789) and the Cargo publish/download API
//! for hosted, cache, and proxy repositories.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_util::io::ReaderStream;

use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;
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

/// Validate that a repo exists and has Cargo format, mapping errors to Cargo protocol format.
async fn validate_cargo_repo(state: &FormatState, repo_name: &str) -> Result<RepoConfig, Response> {
    depot_core::api_helpers::validate_format_repo(state, repo_name, ArtifactFormat::Cargo)
        .await
        .map_err(|e| match e {
            DepotError::NotFound(_) => cargo_error(StatusCode::NOT_FOUND, "repository not found"),
            DepotError::BadRequest(_) => cargo_error(
                StatusCode::BAD_REQUEST,
                "repository is not a cargo repository",
            ),
            _ => cargo_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
        })
}

// =============================================================================
// /repository/{repo}/ dispatch for Cargo repos
// =============================================================================

pub async fn try_handle_repository_path(
    state: &FormatState,
    config: &RepoConfig,
    path: &str,
) -> Option<Response> {
    // index/config.json → config
    if path == "index/config.json" {
        let body = serde_json::json!({
            "dl": format!("/cargo/{}/api/v1/crates/{{crate}}/{{version}}/download", config.name),
            "api": format!("/cargo/{}", config.name),
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

    // api/v1/crates → search (no query params available here, returns all)
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
        let results: Vec<serde_json::Value> = crates
            .iter()
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
// GET /cargo/{repo}/index/config.json
// =============================================================================

pub async fn config_json(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(repo_name): Path<String>,
) -> Response {
    let _config = match validate_cargo_repo(&state, &repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await
    {
        return e.into_response();
    }

    let body = serde_json::json!({
        "dl": format!("/cargo/{repo_name}/api/v1/crates/{{crate}}/{{version}}/download"),
        "api": format!("/cargo/{repo_name}"),
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&body).unwrap_or_default(),
    )
        .into_response()
}

// =============================================================================
// GET /cargo/{repo}/index/... — sparse index entry
// =============================================================================

/// All sparse index route variants extract the crate name from the last URI segment.
/// Axum route params differ per pattern, so we parse the raw URI instead.
pub async fn get_index_entry(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    req: axum::extract::Request,
) -> Response {
    let uri_path = req.uri().path().to_string();
    let segments: Vec<&str> = uri_path.split('/').filter(|s| !s.is_empty()).collect();

    // Path pattern: /cargo/{repo}/index/.../{name}
    // segments[0] = "cargo", segments[1] = repo, segments[2] = "index", last = name
    if segments.len() < 4 {
        return cargo_error(StatusCode::BAD_REQUEST, "invalid index path");
    }

    let repo_name = match segments.get(1) {
        Some(s) => *s,
        None => return cargo_error(StatusCode::BAD_REQUEST, "invalid index path"),
    };
    let crate_name = match segments.last() {
        Some(s) => *s,
        None => return cargo_error(StatusCode::BAD_REQUEST, "invalid index path"),
    };

    let config = match validate_cargo_repo(&state, repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, repo_name, Capability::Read)
        .await
    {
        return e.into_response();
    }

    match config.repo_type() {
        RepoType::Hosted => hosted_index_entry(&state, &config, crate_name).await,
        RepoType::Proxy => proxy_index_entry(&state, &config, crate_name).await,
        RepoType::Cache => hosted_index_entry(&state, &config, crate_name).await,
    }
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

pub async fn publish(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(repo_name): Path<String>,
    body: Body,
) -> Response {
    let config = match validate_cargo_repo(&state, &repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Write)
        .await
    {
        return e.into_response();
    }

    let (target_repo, _target_config) =
        match repo::resolve_format_write_target(state.kv.as_ref(), &config, ArtifactFormat::Cargo)
            .await
        {
            Ok(v) => v,
            Err(e) => return cargo_error(StatusCode::BAD_REQUEST, &e.to_string()),
        };

    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };

    // Stream the body incrementally, buffering only the small header.
    let mut reader = StreamReader::new(body);

    // Parse header: [4-byte LE json_len][json][4-byte LE crate_len]
    let len_bytes = match reader.read_exact(4).await {
        Ok(b) => b,
        Err(_) => return cargo_error(StatusCode::BAD_REQUEST, "request body too short"),
    };
    #[allow(clippy::indexing_slicing)]
    let json_len =
        u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;

    let json_bytes = match reader.read_exact(json_len).await {
        Ok(b) => b,
        Err(_) => {
            return cargo_error(
                StatusCode::BAD_REQUEST,
                "request body too short for metadata",
            )
        }
    };
    let meta: PublishMeta = match serde_json::from_slice(&json_bytes) {
        Ok(m) => m,
        Err(e) => {
            return cargo_error(
                StatusCode::BAD_REQUEST,
                &format!("invalid metadata JSON: {e}"),
            );
        }
    };

    let len_bytes = match reader.read_exact(4).await {
        Ok(b) => b,
        Err(_) => {
            return cargo_error(
                StatusCode::BAD_REQUEST,
                "request body too short for crate length",
            )
        }
    };
    #[allow(clippy::indexing_slicing)]
    let crate_len =
        u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as u64;

    // Stream the crate bytes to BlobWriter, computing hashes incrementally.
    let blob_id = uuid::Uuid::new_v4().to_string();
    let mut blob_writer = match blobs.create_writer(&blob_id, Some(crate_len)).await {
        Ok(w) => w,
        Err(e) => return e.into_response(),
    };
    let mut blake3_hasher = blake3::Hasher::new();
    let mut sha256_hasher = Sha256::new();
    let mut size = 0u64;

    while size < crate_len {
        let chunk = match reader.next_chunk().await {
            Some(Ok(c)) => c,
            Some(Err(e)) => {
                let _ = blob_writer.abort().await;
                return cargo_error(StatusCode::BAD_REQUEST, &format!("error reading body: {e}"));
            }
            None => break,
        };
        if chunk.is_empty() {
            continue;
        }
        blake3_hasher.update(&chunk);
        Digest::update(&mut sha256_hasher, &chunk);
        if let Err(e) = blob_writer.write_chunk(&chunk).await {
            let _ = blob_writer.abort().await;
            return cargo_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("blob write failed: {e}"),
            );
        }
        size += chunk.len() as u64;
    }

    if size != crate_len {
        let _ = blob_writer.abort().await;
        return cargo_error(
            StatusCode::BAD_REQUEST,
            &format!(
                "request body too short for crate data: expected {} bytes, got {}",
                crate_len, size
            ),
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

/// Helper for reading exact byte counts from a streaming body.
struct StreamReader {
    stream:
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<bytes::Bytes, axum::Error>> + Send>>,
    buffer: bytes::BytesMut,
}

impl StreamReader {
    fn new(body: Body) -> Self {
        Self {
            stream: Box::pin(body.into_data_stream()),
            buffer: bytes::BytesMut::new(),
        }
    }

    /// Read exactly `n` bytes, buffering from the stream as needed.
    async fn read_exact(&mut self, n: usize) -> Result<bytes::Bytes, String> {
        use futures::StreamExt;
        while self.buffer.len() < n {
            match self.stream.next().await {
                Some(Ok(chunk)) => self.buffer.extend_from_slice(&chunk),
                Some(Err(e)) => return Err(e.to_string()),
                None => return Err("unexpected end of stream".to_string()),
            }
        }
        Ok(self.buffer.split_to(n).freeze())
    }

    /// Return the next chunk (draining internal buffer first).
    async fn next_chunk(&mut self) -> Option<Result<bytes::Bytes, String>> {
        use futures::StreamExt;
        if !self.buffer.is_empty() {
            return Some(Ok(self.buffer.split().freeze()));
        }
        match self.stream.next().await {
            Some(Ok(chunk)) => Some(Ok(chunk)),
            Some(Err(e)) => Some(Err(e.to_string())),
            None => None,
        }
    }
}

// =============================================================================
// GET /cargo/{repo}/api/v1/crates/{name}/{version}/download
// =============================================================================

pub async fn download(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, crate_name, version)): Path<(String, String, String)>,
) -> Response {
    let config = match validate_cargo_repo(&state, &repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await
    {
        return e.into_response();
    }

    match config.repo_type() {
        RepoType::Hosted => hosted_download(&state, &config, &crate_name, &version).await,
        RepoType::Cache => hosted_download(&state, &config, &crate_name, &version).await,
        RepoType::Proxy => proxy_download(&state, &config, &crate_name, &version).await,
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

// =============================================================================
// DELETE /cargo/{repo}/api/v1/crates/{name}/{version}/yank
// =============================================================================

pub async fn yank(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, crate_name, version)): Path<(String, String, String)>,
) -> Response {
    let config = match validate_cargo_repo(&state, &repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Write)
        .await
    {
        return e.into_response();
    }

    let (target_repo, _target_config) =
        match repo::resolve_format_write_target(state.kv.as_ref(), &config, ArtifactFormat::Cargo)
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

    match store.set_yanked(&crate_name, &version, true).await {
        Ok(true) => cargo_ok(),
        Ok(false) => cargo_error(StatusCode::NOT_FOUND, "crate version not found"),
        Err(e) => cargo_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// =============================================================================
// PUT /cargo/{repo}/api/v1/crates/{name}/{version}/unyank
// =============================================================================

pub async fn unyank(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, crate_name, version)): Path<(String, String, String)>,
) -> Response {
    let config = match validate_cargo_repo(&state, &repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Write)
        .await
    {
        return e.into_response();
    }

    let (target_repo, _target_config) =
        match repo::resolve_format_write_target(state.kv.as_ref(), &config, ArtifactFormat::Cargo)
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

    match store.set_yanked(&crate_name, &version, false).await {
        Ok(true) => cargo_ok(),
        Ok(false) => cargo_error(StatusCode::NOT_FOUND, "crate version not found"),
        Err(e) => cargo_error(StatusCode::INTERNAL_SERVER_ERROR, &e.to_string()),
    }
}

// =============================================================================
// GET /cargo/{repo}/api/v1/crates?q=term — search
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    q: Option<String>,
}

pub async fn search(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(repo_name): Path<String>,
    Query(query): Query<SearchQuery>,
) -> Response {
    let config = match validate_cargo_repo(&state, &repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    if let Err(e) = state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await
    {
        return e.into_response();
    }

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

    let crates = match store.list_crates().await {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };

    let term = query.q.unwrap_or_default().to_lowercase();
    let filtered: Vec<&String> = if term.is_empty() {
        crates.iter().collect()
    } else {
        crates.iter().filter(|c| c.contains(&term)).collect()
    };

    let results: Vec<serde_json::Value> = filtered
        .iter()
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
        "meta": {
            "total": results.len(),
        },
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_string(&body).unwrap_or_default(),
    )
        .into_response()
}

fn hex_encode(data: impl AsRef<[u8]>) -> String {
    data.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}
