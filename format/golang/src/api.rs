// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Go module proxy HTTP API implementation.
//!
//! Implements the GOPROXY protocol plus a custom upload endpoint for
//! hosted, cache, and proxy repositories.

use axum::{
    body::Body,
    extract::{Multipart, Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Extension,
};
use std::sync::Arc;
use tokio_util::io::ReaderStream;

use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;
use depot_core::format_state::FormatState;
use depot_core::service;
use depot_core::store::kv::{
    ArtifactFormat, Capability, RepoConfig, RepoKind, RepoType, UpstreamAuth,
};

use crate::store::{self as golang, GolangStore};

// =============================================================================
// POST /golang/{repo}/upload — custom upload endpoint
// =============================================================================

pub async fn upload(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(repo_name): Path<String>,
    mut multipart: Multipart,
) -> Result<Response, DepotError> {
    let (target_repo, target_config, blobs) = depot_core::api_helpers::upload_preamble(
        &state,
        &user.0,
        &repo_name,
        ArtifactFormat::Golang,
    )
    .await?;

    // Parse multipart fields: module, version, plus file fields (info, mod, zip).
    // info and mod are tiny (<1KB) — buffer them. zip can be large — stream to BlobWriter.
    let mut module_name = None;
    let mut version = None;
    let mut info_data: Option<Vec<u8>> = None;
    let mut mod_data: Option<Vec<u8>> = None;
    let mut zip_blob: Option<(String, String, String, u64)> = None; // (blob_id, blake3, sha256, size)

    while let Ok(Some(mut field)) = multipart.next_field().await {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "module" => {
                module_name = field.text().await.ok();
            }
            "version" => {
                version = field.text().await.ok();
            }
            "info" => {
                info_data = field.bytes().await.ok().map(|b| b.to_vec());
            }
            "mod" => {
                mod_data = field.bytes().await.ok().map(|b| b.to_vec());
            }
            "zip" => {
                match depot_core::api_helpers::stream_multipart_field_to_blob(
                    &mut field,
                    blobs.as_ref(),
                )
                .await
                {
                    Ok(r) => zip_blob = Some((r.blob_id, r.blake3, r.sha256, r.size)),
                    Err(e) => return Err(e),
                }
            }
            _ => {
                let _ = field.bytes().await;
            }
        }
    }

    let module_name = match module_name {
        Some(n) => n,
        None => return Err(DepotError::BadRequest("missing 'module' field".into())),
    };
    let version = match version {
        Some(v) => v,
        None => return Err(DepotError::BadRequest("missing 'version' field".into())),
    };

    let store = GolangStore {
        repo: &target_repo,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &target_config.store,
        updater: &state.updater,
    };

    // Store each provided file type.
    if let Some(data) = info_data {
        store
            .put_module_file(&module_name, &version, "info", &data)
            .await
            .inspect_err(|e| tracing::error!(error = %e, "golang upload info failed"))?;
    }
    if let Some(data) = mod_data {
        store
            .put_module_file(&module_name, &version, "mod", &data)
            .await
            .inspect_err(|e| tracing::error!(error = %e, "golang upload mod failed"))?;
    }
    if let Some((blob_id, blake3, sha256, size)) = zip_blob {
        store
            .commit_module_file(
                &module_name,
                &version,
                "zip",
                &depot_core::repo::BlobInfo {
                    blob_id,
                    blake3_hash: blake3,
                    sha256_hash: sha256,
                    size,
                },
            )
            .await
            .inspect_err(|e| tracing::error!(error = %e, "golang upload zip failed"))?;
    }

    Ok(StatusCode::OK.into_response())
}

// =============================================================================
// /repository/{repo}/ dispatch for Golang repos
// =============================================================================

pub async fn try_handle_repository_path(
    state: &FormatState,
    config: &RepoConfig,
    path: &str,
) -> Option<Response> {
    // Parse the GOPROXY path
    let (module_encoded, action) = parse_goproxy_path(path)?;
    let module = golang::decode_module_path(&module_encoded);

    Some(match config.repo_type() {
        RepoType::Hosted => handle_hosted(state, &config.name, &module, &action).await,
        RepoType::Cache => handle_cache(state, config, &module, &module_encoded, &action).await,
        RepoType::Proxy => handle_proxy(state, config, &module, &action).await,
    })
}

// =============================================================================
// GET /golang/{repo}/{*path} — GOPROXY protocol handler
// =============================================================================

pub async fn golang_get(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, path)): Path<(String, String)>,
) -> Result<Response, DepotError> {
    let config =
        depot_core::api_helpers::validate_format_repo(&state, &repo_name, ArtifactFormat::Golang)
            .await?;

    state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await?;

    // Parse the path to extract module and action.
    // Possible patterns:
    //   {module}/@v/list
    //   {module}/@v/{version}.info
    //   {module}/@v/{version}.mod
    //   {module}/@v/{version}.zip
    //   {module}/@latest
    let (module_encoded, action) = match parse_goproxy_path(&path) {
        Some(v) => v,
        None => {
            return Err(DepotError::NotFound("invalid Go module path".into()));
        }
    };
    let module = golang::decode_module_path(&module_encoded);

    Ok(match config.repo_type() {
        RepoType::Hosted => handle_hosted(&state, &repo_name, &module, &action).await,
        RepoType::Cache => handle_cache(&state, &config, &module, &module_encoded, &action).await,
        RepoType::Proxy => handle_proxy(&state, &config, &module, &action).await,
    })
}

/// Parsed action from a GOPROXY path.
enum GoAction {
    List,
    Latest,
    Info(String),
    Mod(String),
    Zip(String),
}

/// Parse a GOPROXY URL path into (encoded_module, action).
fn parse_goproxy_path(path: &str) -> Option<(String, GoAction)> {
    // Try @latest first
    if let Some(module) = path.strip_suffix("/@latest") {
        if !module.is_empty() {
            return Some((module.to_string(), GoAction::Latest));
        }
    }

    // Try @v/ patterns
    if let Some((module, rest)) = path.split_once("/@v/") {
        if module.is_empty() {
            return None;
        }
        if rest == "list" {
            return Some((module.to_string(), GoAction::List));
        }
        if let Some(ver) = rest.strip_suffix(".info") {
            return Some((module.to_string(), GoAction::Info(ver.to_string())));
        }
        if let Some(ver) = rest.strip_suffix(".mod") {
            return Some((module.to_string(), GoAction::Mod(ver.to_string())));
        }
        if let Some(ver) = rest.strip_suffix(".zip") {
            return Some((module.to_string(), GoAction::Zip(ver.to_string())));
        }
    }

    None
}

// --- Hosted ---

async fn handle_hosted(
    state: &FormatState,
    repo_name: &str,
    module: &str,
    action: &GoAction,
) -> Response {
    let (blobs, blob_config) =
        match depot_core::api_helpers::resolve_repo_blob_store(state, repo_name).await {
            Ok(b) => b,
            Err(e) => return e.into_response(),
        };
    let store = GolangStore {
        repo: repo_name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &blob_config.store,
        updater: &state.updater,
    };

    match action {
        GoAction::List => match store.list_versions(module).await {
            Ok(versions) => {
                let body = versions.join("\n");
                (StatusCode::OK, [("content-type", "text/plain")], body).into_response()
            }
            Err(e) => e.into_response(),
        },
        GoAction::Latest => match store.get_latest(module).await {
            Ok(Some(version)) => serve_module_file(&store, module, &version, "info").await,
            Ok(None) => DepotError::NotFound("no versions found".into()).into_response(),
            Err(e) => e.into_response(),
        },
        GoAction::Info(version) => serve_module_file(&store, module, version, "info").await,
        GoAction::Mod(version) => serve_module_file(&store, module, version, "mod").await,
        GoAction::Zip(version) => serve_module_file(&store, module, version, "zip").await,
    }
}

async fn serve_module_file(
    store: &GolangStore<'_>,
    module: &str,
    version: &str,
    file_type: &str,
) -> Response {
    match store.get_module_file(module, version, file_type).await {
        Ok(Some((reader, size, _record))) => {
            let content_type = match file_type {
                "info" => "application/json",
                "mod" => "text/plain",
                "zip" => "application/zip",
                _ => "application/octet-stream",
            };
            let stream = ReaderStream::with_capacity(reader, 256 * 1024);
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                content_type
                    .parse()
                    .unwrap_or(header::HeaderValue::from_static("application/octet-stream")),
            );
            headers.insert(header::CONTENT_LENGTH, size.into());
            (StatusCode::OK, headers, Body::from_stream(stream)).into_response()
        }
        Ok(None) => DepotError::NotFound("not found".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

// --- Cache ---

async fn handle_cache(
    state: &FormatState,
    config: &RepoConfig,
    module: &str,
    module_encoded: &str,
    action: &GoAction,
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
    let store = GolangStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    match action {
        GoAction::List => {
            // Try upstream first, fall back to local cache.
            match golang::fetch_upstream_file(
                &state.http,
                upstream_url,
                module,
                "@v/list",
                upstream_auth.as_ref(),
            )
            .await
            {
                Ok(Some(data)) => {
                    let body = String::from_utf8_lossy(&data).to_string();
                    (StatusCode::OK, [("content-type", "text/plain")], body).into_response()
                }
                Ok(None) | Err(_) => {
                    // Fall back to local cache.
                    match store.list_versions(module).await {
                        Ok(versions) => {
                            let body = versions.join("\n");
                            (StatusCode::OK, [("content-type", "text/plain")], body).into_response()
                        }
                        Err(e) => e.into_response(),
                    }
                }
            }
        }
        GoAction::Latest => {
            let path_suffix = "@latest";
            match golang::fetch_upstream_file(
                &state.http,
                upstream_url,
                module,
                path_suffix,
                upstream_auth.as_ref(),
            )
            .await
            {
                Ok(Some(data)) => {
                    (StatusCode::OK, [("content-type", "application/json")], data).into_response()
                }
                Ok(None) | Err(_) => {
                    // Fall back to local.
                    match store.get_latest(module).await {
                        Ok(Some(version)) => {
                            serve_module_file(&store, module, &version, "info").await
                        }
                        _ => DepotError::NotFound("not found".into()).into_response(),
                    }
                }
            }
        }
        GoAction::Info(version) => {
            cache_fetch_and_store(
                state,
                config,
                &store,
                module,
                module_encoded,
                version,
                "info",
                upstream_url,
                upstream_auth.as_ref(),
                &blobs,
            )
            .await
        }
        GoAction::Mod(version) => {
            cache_fetch_and_store(
                state,
                config,
                &store,
                module,
                module_encoded,
                version,
                "mod",
                upstream_url,
                upstream_auth.as_ref(),
                &blobs,
            )
            .await
        }
        GoAction::Zip(version) => {
            cache_fetch_and_store(
                state,
                config,
                &store,
                module,
                module_encoded,
                version,
                "zip",
                upstream_url,
                upstream_auth.as_ref(),
                &blobs,
            )
            .await
        }
    }
}
#[allow(clippy::too_many_arguments)]
async fn cache_fetch_and_store(
    state: &FormatState,
    _config: &RepoConfig,
    store: &GolangStore<'_>,
    module: &str,
    _module_encoded: &str,
    version: &str,
    file_type: &str,
    upstream_url: &str,
    upstream_auth: Option<&UpstreamAuth>,
    _blobs: &Arc<dyn depot_core::store::blob::BlobStore>,
) -> Response {
    // Check local cache first.
    if let Ok(Some((reader, size, _record))) =
        store.get_module_file(module, version, file_type).await
    {
        let content_type = match file_type {
            "info" => "application/json",
            "mod" => "text/plain",
            "zip" => "application/zip",
            _ => "application/octet-stream",
        };
        let stream = ReaderStream::with_capacity(reader, 256 * 1024);
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::CONTENT_TYPE,
            content_type
                .parse()
                .unwrap_or(header::HeaderValue::from_static("application/octet-stream")),
        );
        headers.insert(header::CONTENT_LENGTH, size.into());
        return (StatusCode::OK, headers, Body::from_stream(stream)).into_response();
    }

    // Fetch from upstream.
    let path_suffix = format!("@v/{version}.{file_type}");
    match golang::fetch_upstream_file(
        &state.http,
        upstream_url,
        module,
        &path_suffix,
        upstream_auth,
    )
    .await
    {
        Ok(Some(data)) => {
            // Cache it locally.
            let _ = store
                .put_module_file(module, version, file_type, &data)
                .await;

            let content_type = match file_type {
                "info" => "application/json",
                "mod" => "text/plain",
                "zip" => "application/zip",
                _ => "application/octet-stream",
            };
            (StatusCode::OK, [(header::CONTENT_TYPE, content_type)], data).into_response()
        }
        Ok(None) => DepotError::NotFound("not found upstream".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

// --- Proxy ---

async fn handle_proxy(
    state: &FormatState,
    config: &RepoConfig,
    module: &str,
    action: &GoAction,
) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return DepotError::Internal("expected proxy repo kind".into()).into_response();
    };

    for member_name in members {
        let mn = member_name.clone();
        let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
            Ok(Some(c)) if c.format() == ArtifactFormat::Golang => c,
            _ => continue,
        };

        let module_encoded = golang::encode_module_path(module);
        let resp = match member_config.repo_type() {
            RepoType::Hosted => handle_hosted(state, member_name, module, action).await,
            RepoType::Cache => {
                handle_cache(state, &member_config, module, &module_encoded, action).await
            }
            RepoType::Proxy => continue, // Don't recurse into nested proxies.
        };

        if resp.status() == StatusCode::OK {
            return resp;
        }
    }

    DepotError::NotFound("not found in any member".into()).into_response()
}
