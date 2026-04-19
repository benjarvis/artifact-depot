// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! YUM HTTP API implementation.
//!
//! Handles RPM upload, metadata serving (with on-demand rebuild),
//! snapshot creation, and public key serving for hosted, cache,
//! and proxy repos.

use axum::{
    body::Body,
    http::{header, HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
};

use sha2::Digest;

use depot_core::api_helpers;
use depot_core::auth::AuthenticatedUser;
use depot_core::error::DepotError;
use depot_core::format_state::FormatState;
use depot_core::service;
use depot_core::store::kv::{ArtifactFormat, RepoConfig, RepoKind, RepoType};

use crate::store::{self as yum, YumStore};

fn yum_store_from_config<'a>(
    config: &'a RepoConfig,
    state: &'a FormatState,
    blobs: &'a dyn depot_core::store::blob::BlobStore,
) -> YumStore<'a> {
    YumStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs,
        store: &config.store,
        updater: &state.updater,
        repodata_depth: config.format_config.repodata_depth(),
    }
}

#[derive(serde::Deserialize)]
pub struct CreateSnapshotRequest {
    pub name: String,
}

// =============================================================================
// /repository/{repo}/... dispatch for Yum repos (writes)
// =============================================================================

/// Handle a write request under `/repository/{repo}/<path>` for a Yum repo.
/// Returns `None` if the (method, path) combination is not a Yum write.
pub async fn try_handle_repository_write(
    state: &FormatState,
    user: &AuthenticatedUser,
    config: &RepoConfig,
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: Body,
) -> Option<Response> {
    if method == Method::POST && path == "upload" {
        return Some(handle_upload(state, user, config, method, headers, body).await);
    }
    if method == Method::POST && path == "snapshots" {
        return Some(handle_create_snapshot(state, config, body).await);
    }
    None
}

async fn handle_upload(
    state: &FormatState,
    user: &AuthenticatedUser,
    config: &RepoConfig,
    method: &Method,
    headers: &HeaderMap,
    body: Body,
) -> Response {
    let (_target_repo, target_config, blobs) =
        match api_helpers::upload_preamble(state, &user.0, &config.name, ArtifactFormat::Yum).await
        {
            Ok(v) => v,
            Err(e) => return e.into_response(),
        };

    let mut multipart = match api_helpers::multipart_from_body(method, headers, body).await {
        Ok(m) => m,
        Err(e) => return e,
    };

    let mut blob_info: Option<(String, String, String, String, u64)> = None; // (filename, blob_id, blake3, sha256, size)
    let mut directory = String::new();

    while let Ok(Some(mut field)) = multipart.next_field().await {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "file" => {
                match api_helpers::stream_multipart_field_to_blob(&mut field, blobs.as_ref()).await
                {
                    Ok(r) => {
                        let filename = r.filename.unwrap_or_else(|| "package.rpm".to_string());
                        blob_info = Some((filename, r.blob_id, r.blake3, r.sha256, r.size));
                    }
                    Err(e) => return e.into_response(),
                }
            }
            "directory" => {
                directory = field.text().await.unwrap_or_default();
            }
            _ => {
                let _ = field.bytes().await;
            }
        }
    }

    let (filename, blob_id, blake3, sha256, size) = match blob_info {
        Some(info) => info,
        None => return DepotError::BadRequest("missing 'file' field".into()).into_response(),
    };

    let store = yum_store_from_config(&target_config, state, blobs.as_ref());

    match store
        .commit_package(&blob_id, &blake3, &sha256, size, &filename, &directory)
        .await
    {
        Ok(_) => {
            api_helpers::propagate_staleness_to_parents(
                state,
                &config.name,
                ArtifactFormat::Yum,
                "_yum/metadata_stale",
            )
            .await;
            StatusCode::OK.into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "yum upload failed");
            e.into_response()
        }
    }
}

async fn handle_create_snapshot(state: &FormatState, config: &RepoConfig, body: Body) -> Response {
    let bytes = match api_helpers::body_to_bytes(body, usize::MAX).await {
        Ok(b) => b,
        Err(e) => return e,
    };
    let req: CreateSnapshotRequest = match serde_json::from_slice(&bytes) {
        Ok(r) => r,
        Err(e) => {
            return DepotError::BadRequest(format!("invalid JSON body: {e}")).into_response();
        }
    };

    if config.format() != ArtifactFormat::Yum {
        return DepotError::BadRequest("repo is not a Yum repo".into()).into_response();
    }

    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = yum_store_from_config(config, state, blobs.as_ref());

    // Ensure metadata is fresh before snapshotting
    let is_stale = store.is_metadata_stale().await.unwrap_or(false);
    if is_stale {
        let signing_key = store.get_signing_key().await.unwrap_or(None);
        if let Err(e) = store.rebuild_metadata(signing_key.as_deref()).await {
            tracing::error!(repo = config.name, error = %e, "metadata rebuild failed before snapshot");
            return e.into_response();
        }
    }

    let signing_key = store.get_signing_key().await.unwrap_or(None);
    match store
        .create_snapshot(&req.name, signing_key.as_deref())
        .await
    {
        Ok(_) => StatusCode::CREATED.into_response(),
        Err(e) => e.into_response(),
    }
}

// =============================================================================
// /repository/{repo}/ dispatch for Yum repos
// =============================================================================

pub async fn try_handle_repository_path(
    state: &FormatState,
    config: &RepoConfig,
    path: &str,
    query: Option<&str>,
) -> Option<Response> {
    let _ = query;
    // repodata/repomd.xml.key → PGP public key (served as application/pgp-keys).
    // Matches either the root form or the depth-prefixed form (see is_repodata_path).
    if path.ends_with("/repodata/repomd.xml.key") || path == "repodata/repomd.xml.key" {
        let blobs = match state.blob_store(&config.store).await {
            Ok(b) => b,
            Err(e) => return Some(e.into_response()),
        };
        let store = yum_store_from_config(config, state, blobs.as_ref());
        return Some(match store.get_public_key().await {
            Ok(Some(key)) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/pgp-keys")],
                key,
            )
                .into_response(),
            Ok(None) => DepotError::NotFound("no public key configured".into()).into_response(),
            Err(e) => e.into_response(),
        });
    }

    // repodata/* or {prefix}/repodata/* → metadata
    // Matches: "repodata/foo" (depth=0) or "p1/7/x86_64/repodata/foo" (depth>0)
    if is_repodata_path(path) {
        return Some(match config.repo_type() {
            RepoType::Hosted => serve_metadata(state, config, path).await,
            RepoType::Cache => cache_serve_metadata(state, config, path).await,
            RepoType::Proxy => proxy_serve_metadata(state, config, path).await,
        });
    }

    // snapshots/{snapshot}/repodata/* → snapshot metadata
    if let Some(rest) = path.strip_prefix("snapshots/") {
        if rest.contains("/repodata/") {
            let meta_path = format!("snapshots/{rest}");
            return Some(serve_snapshot_metadata(state, config, &meta_path).await);
        }
    }

    // All other paths: fall through to the generic artifact handler.
    // This ensures HEAD, GET, and content negotiation work uniformly.
    // The /yum/{repo}/{*path} routes handle Yum-specific GET downloads.
    None
}

/// Check if a path is a repodata path (root or prefixed).
fn is_repodata_path(path: &str) -> bool {
    if path.starts_with("repodata/") {
        return true;
    }
    // Match {prefix}/repodata/{file} but not just "{prefix}/repodata/"
    if let Some((_, rest)) = path.split_once("/repodata/") {
        return !rest.is_empty();
    }
    false
}

// =============================================================================
// Hosted metadata serving (with on-demand rebuild)
// =============================================================================

async fn serve_metadata(state: &FormatState, config: &RepoConfig, meta_path: &str) -> Response {
    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = yum_store_from_config(config, state, blobs.as_ref());

    let is_stale = store.is_metadata_stale().await.unwrap_or(false);
    if is_stale {
        let signing_key = store.get_signing_key().await.unwrap_or(None);
        if let Err(e) = store.rebuild_metadata(signing_key.as_deref()).await {
            tracing::error!(repo = config.name, error = %e, "yum metadata rebuild failed");
            return e.into_response();
        }
    }

    match store.get_metadata(meta_path).await {
        Ok(Some(data)) => {
            let content_type = metadata_content_type(meta_path);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                Body::from(data),
            )
                .into_response()
        }
        Ok(None) => DepotError::NotFound("metadata not found".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Serve snapshot metadata (no rebuild needed — snapshots are frozen).
async fn serve_snapshot_metadata(
    state: &FormatState,
    config: &RepoConfig,
    meta_path: &str,
) -> Response {
    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = yum_store_from_config(config, state, blobs.as_ref());

    match store.get_metadata(meta_path).await {
        Ok(Some(data)) => {
            let content_type = metadata_content_type(meta_path);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                Body::from(data),
            )
                .into_response()
        }
        Ok(None) => DepotError::NotFound("snapshot metadata not found".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

fn metadata_content_type(meta_path: &str) -> &'static str {
    if meta_path.ends_with(".gz") {
        "application/gzip"
    } else if meta_path.ends_with(".asc") {
        "application/pgp-signature"
    } else if meta_path.ends_with(".xml") {
        "application/xml"
    } else {
        "application/octet-stream"
    }
}

// =============================================================================
// Cache metadata serving
// =============================================================================

async fn cache_serve_metadata(
    state: &FormatState,
    config: &RepoConfig,
    meta_path: &str,
) -> Response {
    let RepoKind::Cache {
        ref upstream_url,
        cache_ttl_secs,
        ref upstream_auth,
        ..
    } = config.kind
    else {
        return DepotError::Internal("expected cache repo kind".into()).into_response();
    };
    let ttl = cache_ttl_secs;

    let blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let store = yum_store_from_config(config, state, blobs.as_ref());

    // Check if cached repomd.xml is fresh (TTL-based).
    let config_name = config.name.clone();
    let cached_repomd =
        service::get_artifact(state.kv.as_ref(), &config_name, "repodata/repomd.xml")
            .await
            .ok()
            .flatten();

    let cache_fresh = if let Some(ref rec) = cached_repomd {
        let now = depot_core::repo::now_utc();
        (now - rec.updated_at).num_seconds() < ttl as i64
    } else {
        false
    };

    // Content-addressed files (.gz) are always fresh if they exist — only
    // repomd.xml needs TTL checking. If the repomd.xml cache is fresh, serve
    // whatever is requested directly.
    if cache_fresh {
        if let Ok(Some(data)) = store.get_metadata(meta_path).await {
            let content_type = metadata_content_type(meta_path);
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                Body::from(data),
            )
                .into_response();
        }
    }

    // Fetch upstream repomd.xml to discover all metadata files.
    let base = upstream_url.trim_end_matches('/');
    let repomd_url = format!("{base}/repodata/repomd.xml");

    let upstream_start = std::time::Instant::now();
    match state.http.get(&repomd_url).send().await {
        Ok(resp) if resp.status().is_success() => {
            depot_core::repo::emit_upstream_event(
                "GET",
                &repomd_url,
                resp.status().as_u16(),
                upstream_start.elapsed(),
                0,
                resp.content_length().unwrap_or(0),
                "upstream.yum.metadata",
            );
            let repomd_data = match resp.bytes().await {
                Ok(b) => b.to_vec(),
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read upstream repomd.xml");
                    if let Ok(Some(data)) = store.get_metadata(meta_path).await {
                        return (
                            StatusCode::OK,
                            [(header::CONTENT_TYPE, metadata_content_type(meta_path))],
                            Body::from(data),
                        )
                            .into_response();
                    }
                    return DepotError::Upstream("upstream error".into()).into_response();
                }
            };

            // Store repomd.xml only — individual metadata files (primary.xml.gz,
            // filelists.xml.gz, etc.) are fetched lazily on cache miss below.
            let _ = store.store_cached_repomd(&repomd_data).await;

            // Propagate stale to parent proxy repos.
            let _ = store.set_metadata_stale().await;
            api_helpers::propagate_staleness_to_parents(
                state,
                &config_name,
                ArtifactFormat::Yum,
                "_yum/metadata_stale",
            )
            .await;

            // Fall through to serve (with lazy fetch on miss).
        }
        _ => {
            // Upstream unavailable — will serve stale cache below.
        }
    }

    // Serve from local cache, fetching from upstream on miss.
    // For .asc, treat empty response as a miss (cache repo has no signing key,
    // need to fetch the upstream's .asc instead).
    match store.get_metadata(meta_path).await {
        Ok(Some(data)) if !data.is_empty() => {
            let content_type = metadata_content_type(meta_path);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                Body::from(data),
            )
                .into_response()
        }
        Ok(_) => {
            // Lazy fetch: individual metadata files not yet cached.
            let base = upstream_url.trim_end_matches('/');
            let url = format!("{base}/{meta_path}");
            let mut req = state.http.get(&url);
            if let Some(auth) = upstream_auth {
                req = req.basic_auth(&auth.username, auth.password.as_deref());
            }
            match req.send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(bytes) = resp.bytes().await {
                        let _ = store.store_cached_metadata(meta_path, &bytes).await;
                        let content_type = metadata_content_type(meta_path);
                        return (
                            StatusCode::OK,
                            [(header::CONTENT_TYPE, content_type)],
                            Body::from(bytes.to_vec()),
                        )
                            .into_response();
                    }
                    DepotError::NotFound("metadata not found".into()).into_response()
                }
                _ => DepotError::NotFound("metadata not found".into()).into_response(),
            }
        }
        Err(e) => e.into_response(),
    }
}

// =============================================================================
// Proxy metadata serving (merged from members)
// =============================================================================

/// Proxy metadata serving: cached merge from all members.
///
/// Uses the same stale-check + rebuild pattern as hosted repos. Merged
/// metadata is stored as real content-addressed artifacts on the proxy repo.
async fn proxy_serve_metadata(
    state: &FormatState,
    config: &RepoConfig,
    meta_path: &str,
) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return DepotError::Internal("expected proxy repo kind".into()).into_response();
    };

    let proxy_blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let proxy_store = yum_store_from_config(config, state, proxy_blobs.as_ref());

    let is_stale = proxy_store.is_metadata_stale().await.unwrap_or(true);
    let needs_rebuild = is_stale
        || proxy_store
            .get_metadata("repodata/repomd.xml")
            .await
            .ok()
            .flatten()
            .is_none();
    if needs_rebuild {
        if let Err(e) = rebuild_proxy_yum_metadata(state, &proxy_store, members).await {
            tracing::error!(repo = config.name, error = %e, "proxy metadata rebuild failed");
            return e.into_response();
        }
    }

    match proxy_store.get_metadata(meta_path).await {
        Ok(Some(data)) => {
            let content_type = metadata_content_type(meta_path);
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, content_type)],
                Body::from(data),
            )
                .into_response()
        }
        Ok(None) => DepotError::NotFound("metadata not found".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Rebuild merged YUM proxy metadata from all members.
///
/// Collects primary/filelists/other XML from all members, merges them,
/// generates new content-addressed .gz files and repomd.xml.
async fn rebuild_proxy_yum_metadata(
    state: &FormatState,
    proxy_store: &yum::YumStore<'_>,
    members: &[String],
) -> Result<(), DepotError> {
    proxy_store.clear_stale_flag_pub().await?;

    let mut all_primary = Vec::new();
    let mut all_filelists = Vec::new();
    let mut all_other = Vec::new();

    // Ensure hosted members are fresh, collect metadata from all members.
    for member_name in members {
        let mn = member_name.clone();
        let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
            Ok(Some(c)) if c.format() == ArtifactFormat::Yum => c,
            _ => continue,
        };
        let mb = match state.blob_store(&member_config.store).await {
            Ok(b) => b,
            Err(_) => continue,
        };
        let member_store = yum_store_from_config(&member_config, state, mb.as_ref());

        if member_config.repo_type() == RepoType::Hosted {
            let is_stale = member_store.is_metadata_stale().await.unwrap_or(false);
            if is_stale {
                let signing_key = member_store.get_signing_key().await.unwrap_or(None);
                let _ = member_store.rebuild_metadata(signing_key.as_deref()).await;
            }
        }

        // Get repomd.xml to discover the content-addressed filenames.
        let repomd = match member_store.get_metadata("repodata/repomd.xml").await {
            Ok(Some(data)) => String::from_utf8_lossy(&data).to_string(),
            _ => continue,
        };
        let locations = yum::parse_repomd_locations(&repomd);

        // Fetch and decompress each metadata file.
        for loc in &locations {
            let data = match member_store.get_metadata(loc).await {
                Ok(Some(d)) => d,
                _ => continue,
            };
            // Decompress .gz to get raw XML.
            let xml = if loc.ends_with(".gz") {
                let mut decoder = flate2::read::GzDecoder::new(data.as_slice());
                let mut s = String::new();
                if std::io::Read::read_to_string(&mut decoder, &mut s).is_ok() {
                    s
                } else {
                    continue;
                }
            } else {
                String::from_utf8_lossy(&data).to_string()
            };

            if loc.contains("primary") {
                all_primary.push(xml);
            } else if loc.contains("filelists") {
                all_filelists.push(xml);
            } else if loc.contains("other") {
                all_other.push(xml);
            }
        }
    }

    // Merge XML from all members.
    let merged_primary = yum::merge_primary_xml(&all_primary);
    let merged_filelists = yum::merge_filelists_xml(&all_filelists);
    let merged_other = yum::merge_other_xml(&all_other);

    // Compress merged files.
    let primary_gz = yum::gzip_compress(merged_primary.as_bytes());
    let filelists_gz = yum::gzip_compress(merged_filelists.as_bytes());
    let other_gz = yum::gzip_compress(merged_other.as_bytes());

    // Compute checksums.
    let hex = |data: &[u8]| -> String {
        sha2::Sha256::digest(data)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    let primary_gz_sha = hex(&primary_gz);
    let filelists_gz_sha = hex(&filelists_gz);
    let other_gz_sha = hex(&other_gz);
    let primary_open_sha = hex(merged_primary.as_bytes());
    let filelists_open_sha = hex(merged_filelists.as_bytes());
    let other_open_sha = hex(merged_other.as_bytes());

    let primary_filename = format!("{primary_gz_sha}-primary.xml.gz");
    let filelists_filename = format!("{filelists_gz_sha}-filelists.xml.gz");
    let other_filename = format!("{other_gz_sha}-other.xml.gz");

    // Store content-addressed files first (immutable).
    proxy_store
        .store_metadata_file_pub(&format!("repodata/{primary_filename}"), &primary_gz)
        .await?;
    proxy_store
        .store_metadata_file_pub(&format!("repodata/{filelists_filename}"), &filelists_gz)
        .await?;
    proxy_store
        .store_metadata_file_pub(&format!("repodata/{other_filename}"), &other_gz)
        .await?;

    // Generate repomd.xml with merged checksums.
    let now = depot_core::repo::now_utc();
    let repomd = yum::generate_repomd_xml(
        now.timestamp(),
        &yum::RepoDataEntry {
            filename: &primary_filename,
            checksum: &primary_gz_sha,
            size: primary_gz.len() as u64,
            open_checksum: &primary_open_sha,
            open_size: merged_primary.len() as u64,
        },
        &yum::RepoDataEntry {
            filename: &filelists_filename,
            checksum: &filelists_gz_sha,
            size: filelists_gz.len() as u64,
            open_checksum: &filelists_open_sha,
            open_size: merged_filelists.len() as u64,
        },
        &yum::RepoDataEntry {
            filename: &other_filename,
            checksum: &other_gz_sha,
            size: other_gz.len() as u64,
            open_checksum: &other_open_sha,
            open_size: merged_other.len() as u64,
        },
        now.timestamp(),
    );

    // Store repomd.xml last (commit point).
    proxy_store.store_repomd_pub(repomd.as_bytes()).await?;

    Ok(())
}

// =============================================================================
// Repodata rebuild after depth change
// =============================================================================

/// Delete all existing repodata and trigger a rebuild at the new depth.
/// Called as a background task after `repodata_depth` is changed on a repo.
pub async fn rebuild_repodata_for_depth_change(
    state: &FormatState,
    config: &RepoConfig,
) -> Result<(), DepotError> {
    let blobs = state.blob_store(&config.store).await?;
    let store = yum_store_from_config(config, state, blobs.as_ref());

    let deleted = store.delete_all_repodata().await?;
    tracing::info!(
        repo = config.name,
        deleted,
        new_depth = config.format_config.repodata_depth(),
        "deleted old repodata, triggering rebuild at new depth",
    );

    // Set stale flag to trigger lazy rebuild at new depth on next access.
    store.set_metadata_stale().await?;

    // Also eagerly rebuild now so metadata is immediately available.
    let signing_key = store.get_signing_key().await.unwrap_or(None);
    store.rebuild_metadata(signing_key.as_deref()).await?;

    Ok(())
}
