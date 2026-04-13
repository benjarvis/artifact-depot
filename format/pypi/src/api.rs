// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! PyPI HTTP API implementation.
//!
//! Implements PEP 503 Simple Repository API and the legacy upload API for
//! hosted, cache, and proxy repositories.

use axum::{
    body::Body,
    extract::{Multipart, Path, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response},
    Extension,
};
use std::collections::HashSet;
use tokio_util::io::ReaderStream;

use depot_core::store_from_config_fn;

use crate::store::{self as pypi, normalize_name, parse_simple_html, PypiStore};
use depot_core::auth::AuthenticatedUser;
use depot_core::error::{DepotError, Retryability};
use depot_core::format_state::FormatState;
use depot_core::repo::apply_upstream_auth;
use depot_core::service;
use depot_core::store::kv::{
    ArtifactFormat, ArtifactRecord, Capability, PypiProjectIndex, PypiProjectLink, RepoConfig,
    RepoKind, RepoType, CURRENT_RECORD_VERSION,
};

store_from_config_fn!(pypi_store_from_config, PypiStore);

// =============================================================================
// POST /pypi/{repo}/ — twine upload
// =============================================================================

pub async fn upload(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(repo_name): Path<String>,
    mut multipart: Multipart,
) -> Result<Response, DepotError> {
    let (target_repo, target_config, blobs) =
        depot_core::api_helpers::upload_preamble(&state, &user.0, &repo_name, ArtifactFormat::PyPI)
            .await?;

    let mut name = None;
    let mut version = None;
    let mut filetype = None;
    let mut sha256_digest = None;
    let mut requires_python = None;
    let mut summary = None;
    let mut blob_info: Option<(String, String, String, String, u64)> = None; // (filename, blob_id, blake3, sha256, size)

    while let Ok(Some(mut field)) = multipart.next_field().await {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "name" => {
                name = field.text().await.ok();
            }
            "version" => {
                version = field.text().await.ok();
            }
            "filetype" => {
                filetype = field.text().await.ok();
            }
            "sha256_digest" => {
                sha256_digest = field.text().await.ok();
            }
            "requires_python" => {
                requires_python = field.text().await.ok();
            }
            "summary" => {
                summary = field.text().await.ok();
            }
            "content" => {
                let r = depot_core::api_helpers::stream_multipart_field_to_blob(
                    &mut field,
                    blobs.as_ref(),
                )
                .await?;
                let filename = r.filename.unwrap_or_else(|| "unknown".to_string());
                blob_info = Some((filename, r.blob_id, r.blake3, r.sha256, r.size));
            }
            // Ignore :action and other fields.
            _ => {
                let _ = field.bytes().await;
            }
        }
    }

    let name = name.ok_or_else(|| DepotError::BadRequest("missing 'name' field".into()))?;
    let version =
        version.ok_or_else(|| DepotError::BadRequest("missing 'version' field".into()))?;
    let (filename, blob_id, blake3, sha256, size) =
        blob_info.ok_or_else(|| DepotError::BadRequest("missing 'content' field".into()))?;
    let filetype = filetype.unwrap_or_else(|| pypi::normalize_name("unknown"));

    let store = PypiStore {
        repo: &target_repo,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &target_config.store,
        updater: &state.updater,
    };

    store
        .commit_package(
            &pypi::PypiUploadMeta {
                name: &name,
                version: &version,
                filename: &filename,
                filetype: &filetype,
                expected_sha256: sha256_digest.as_deref(),
                requires_python: requires_python.as_deref(),
                summary: summary.as_deref(),
            },
            &depot_core::repo::BlobInfo {
                blob_id,
                blake3_hash: blake3,
                sha256_hash: sha256,
                size,
            },
        )
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "pypi upload failed");
            e
        })?;

    let _ = store.set_stale_flag().await;
    depot_core::api_helpers::propagate_staleness_to_parents(
        &state,
        &repo_name,
        ArtifactFormat::PyPI,
        "_pypi/metadata_stale",
    )
    .await;

    Ok(StatusCode::OK.into_response())
}

// =============================================================================
// /repository/{repo}/ dispatch for PyPI repos
// =============================================================================

pub async fn try_handle_repository_path(
    state: &FormatState,
    config: &RepoConfig,
    path: &str,
) -> Option<Response> {
    // simple/ → project list
    if path == "simple" || path == "simple/" {
        return Some(match config.repo_type() {
            RepoType::Hosted => hosted_simple_index(state, &config.name).await,
            RepoType::Cache => cache_simple_index(state, config).await,
            RepoType::Proxy => proxy_simple_index(state, config).await,
        });
    }

    // simple/{project}/ → project file list
    if let Some(rest) = path.strip_prefix("simple/") {
        let project = rest.trim_end_matches('/');
        if !project.is_empty() && !project.contains('/') {
            return Some(match config.repo_type() {
                RepoType::Hosted => hosted_simple_project(state, &config.name, project).await,
                RepoType::Cache => cache_simple_project(state, config, project).await,
                RepoType::Proxy => proxy_simple_project(state, config, project).await,
            });
        }
    }

    // packages/{name}/{version}/{filename} → package download
    if let Some(rest) = path.strip_prefix("packages/") {
        if let Some((name, remainder)) = rest.split_once('/') {
            if let Some((version, filename)) = remainder.split_once('/') {
                return Some(match config.repo_type() {
                    RepoType::Hosted => {
                        hosted_download(state, &config.name, name, version, filename).await
                    }
                    RepoType::Cache => cache_download(state, config, name, version, filename).await,
                    RepoType::Proxy => proxy_download(state, config, name, version, filename).await,
                });
            }
        }
    }

    None
}

// =============================================================================
// GET /pypi/{repo}/simple/ — PEP 503 project list
// =============================================================================

pub async fn simple_index(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path(repo_name): Path<String>,
) -> Result<Response, DepotError> {
    let config =
        depot_core::api_helpers::validate_format_repo(&state, &repo_name, ArtifactFormat::PyPI)
            .await?;
    state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await?;

    Ok(match config.repo_type() {
        RepoType::Hosted => hosted_simple_index(&state, &repo_name).await,
        RepoType::Cache => cache_simple_index(&state, &config).await,
        RepoType::Proxy => proxy_simple_index(&state, &config).await,
    })
}

async fn hosted_simple_index(state: &FormatState, repo_name: &str) -> Response {
    let (blobs, blob_config) =
        match depot_core::api_helpers::resolve_repo_blob_store(state, repo_name).await {
            Ok(b) => b,
            Err(e) => return e.into_response(),
        };
    let store_name = blob_config.store.clone();
    let store = PypiStore {
        repo: repo_name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &store_name,
        updater: &state.updater,
    };

    match store.list_projects().await {
        Ok(projects) => {
            let mut html = String::from("<!DOCTYPE html>\n<html><body>\n");
            for p in &projects {
                html.push_str(&format!("<a href=\"{p}/\">{p}</a>\n"));
            }
            html.push_str("</body></html>\n");
            Html(html).into_response()
        }
        Err(e) => e.into_response(),
    }
}

async fn cache_simple_index(state: &FormatState, config: &RepoConfig) -> Response {
    let RepoKind::Cache {
        ref upstream_url,
        ref upstream_auth,
        ..
    } = config.kind
    else {
        return DepotError::Internal("expected cache repo kind".into()).into_response();
    };

    // For the full index, proxy to upstream.
    let url = format!("{}/", upstream_url.trim_end_matches('/'));
    match apply_upstream_auth(
        state.http.get(&url).header("Accept", "text/html"),
        upstream_auth.as_ref(),
    )
    .send()
    .await
    {
        Ok(resp) if resp.status().is_success() => {
            match resp.text().await {
                Ok(html) => {
                    // Rewrite hrefs to point through our cache.
                    // Upstream links are relative like `<a href="project/">project</a>`
                    // which already works for our simple/ endpoint.
                    Html(html).into_response()
                }
                Err(e) => {
                    // Fall back to locally cached projects.
                    tracing::warn!(error = %e, "failed to read upstream index, falling back to cache");
                    hosted_simple_index(state, &config.name).await
                }
            }
        }
        Ok(_) | Err(_) => {
            // Upstream unavailable — serve stale cache.
            tracing::warn!(
                repo = config.name,
                "upstream unavailable, serving cached index"
            );
            hosted_simple_index(state, &config.name).await
        }
    }
}

async fn proxy_simple_index(state: &FormatState, config: &RepoConfig) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return DepotError::Internal("expected proxy repo kind".into()).into_response();
    };

    let proxy_blobs = match state.blob_store(&config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    let proxy_store = pypi_store_from_config(config, state, proxy_blobs.as_ref());

    let is_stale = proxy_store.is_metadata_stale().await.unwrap_or(true);
    let needs_rebuild = is_stale
        || proxy_store
            .get_simple_index()
            .await
            .ok()
            .flatten()
            .is_none();

    if needs_rebuild {
        let _ = proxy_store.clear_stale_flag().await;

        let mut all_projects = HashSet::new();
        for member_name in members {
            let (blobs, member_blob_config) =
                match depot_core::api_helpers::resolve_repo_blob_store(state, member_name).await {
                    Ok(b) => b,
                    Err(_) => continue,
                };
            let store = PypiStore {
                repo: member_name,
                kv: state.kv.as_ref(),
                blobs: blobs.as_ref(),
                store: &member_blob_config.store,
                updater: &state.updater,
            };
            if let Ok(projects) = store.list_projects().await {
                for p in projects {
                    all_projects.insert(p);
                }
            }
        }

        let mut projects: Vec<String> = all_projects.into_iter().collect();
        projects.sort();

        let mut html = String::from("<!DOCTYPE html>\n<html><body>\n");
        for p in &projects {
            html.push_str(&format!("<a href=\"{p}/\">{p}</a>\n"));
        }
        html.push_str("</body></html>\n");

        let _ = proxy_store.store_simple_index(&html).await;
        return Html(html).into_response();
    }

    // Serve from cache.
    match proxy_store.get_simple_index().await {
        Ok(Some(html)) => Html(html).into_response(),
        _ => DepotError::NotFound("index not found".into()).into_response(),
    }
}

// =============================================================================
// GET /pypi/{repo}/simple/{project}/ — PEP 503 project file list
// =============================================================================

pub async fn simple_project(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, project)): Path<(String, String)>,
) -> Result<Response, DepotError> {
    let config =
        depot_core::api_helpers::validate_format_repo(&state, &repo_name, ArtifactFormat::PyPI)
            .await?;
    state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await?;

    Ok(match config.repo_type() {
        RepoType::Hosted => hosted_simple_project(&state, &repo_name, &project).await,
        RepoType::Cache => cache_simple_project(&state, &config, &project).await,
        RepoType::Proxy => proxy_simple_project(&state, &config, &project).await,
    })
}

fn render_project_files(_repo_name: &str, files: &[(String, ArtifactRecord)]) -> String {
    let mut html = String::from("<!DOCTYPE html>\n<html><body>\n");
    for (path, record) in files {
        let (filename, sha256, name, version, requires_python) = match &record.kind {
            depot_core::store::kv::ArtifactKind::PypiPackage { pypi, .. } => (
                pypi.filename.as_str(),
                pypi.sha256.as_str(),
                pypi.name.as_str(),
                pypi.version.as_str(),
                pypi.requires_python.as_deref(),
            ),
            _ => (path.rsplit('/').next().unwrap_or(path), "", "", "", None),
        };

        let href = format!("../../packages/{name}/{version}/{filename}#sha256={sha256}");

        let rp_attr = match requires_python {
            Some(rp) if !rp.is_empty() => {
                let escaped = rp
                    .replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;")
                    .replace('"', "&quot;");
                format!(" data-requires-python=\"{escaped}\"")
            }
            _ => String::new(),
        };

        html.push_str(&format!("<a href=\"{href}\"{rp_attr}>{filename}</a>\n"));
    }
    html.push_str("</body></html>\n");
    html
}

async fn hosted_simple_project(state: &FormatState, repo_name: &str, project: &str) -> Response {
    let (blobs, blob_config) =
        match depot_core::api_helpers::resolve_repo_blob_store(state, repo_name).await {
            Ok(b) => b,
            Err(e) => return e.into_response(),
        };
    let store_name = blob_config.store.clone();
    let store = PypiStore {
        repo: repo_name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &store_name,
        updater: &state.updater,
    };

    match store.list_project_files(project).await {
        Ok(files) => {
            if files.is_empty() {
                return DepotError::NotFound("project not found".into()).into_response();
            }
            Html(render_project_files(repo_name, &files)).into_response()
        }
        Err(e) => e.into_response(),
    }
}

async fn cache_simple_project(state: &FormatState, config: &RepoConfig, project: &str) -> Response {
    let RepoKind::Cache {
        ref upstream_url,
        cache_ttl_secs,
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
    let ttl = cache_ttl_secs;
    let norm = normalize_name(project);
    let index_path = format!("_index/{norm}");

    let store = PypiStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };

    // Check cached index page.
    let cn = config.name.clone();
    let ip = index_path.clone();
    let cached_index = service::get_artifact(state.kv.as_ref(), &cn, &ip)
        .await
        .ok()
        .flatten();
    let cache_fresh = if let Some(ref rec) = cached_index {
        if ttl == 0 {
            true
        } else {
            let now = depot_core::repo::now_utc();
            let age = (now - rec.updated_at).num_seconds().max(0) as u64;
            age < ttl
        }
    } else {
        false
    };

    if cache_fresh {
        // Serve from the project index blob.
        if let Ok(Some(index)) = store.get_project_index(project).await {
            return Html(render_index_links(&norm, &index.links)).into_response();
        }
    }

    // Fetch from upstream.
    match pypi::fetch_upstream_project_page(
        &state.http,
        upstream_url,
        project,
        upstream_auth.as_ref(),
    )
    .await
    {
        Ok(Some(html)) => {
            let links = parse_simple_html(&html);
            // Build project index from parsed links and store as a single record.
            let project_links = build_project_links(upstream_url, &norm, &links);
            cache_project_links(&store, &norm, &project_links).await;

            Html(render_index_links(&norm, &project_links)).into_response()
        }
        Ok(None) => DepotError::NotFound("project not found upstream".into()).into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "upstream fetch failed, serving stale cache");
            // Serve stale cache from project index.
            if let Ok(Some(index)) = store.get_project_index(project).await {
                return Html(render_index_links(&norm, &index.links)).into_response();
            }
            DepotError::Upstream(format!("upstream error: {e}")).into_response()
        }
    }
}

/// Build `PypiProjectLink` entries from parsed HTML links, resolving relative URLs.
fn build_project_links(
    upstream_base: &str,
    norm_name: &str,
    links: &[pypi::PackageLink],
) -> Vec<PypiProjectLink> {
    links
        .iter()
        .map(|link| {
            let version = pypi::extract_version_from_filename(norm_name, &link.filename);
            let upstream_url = resolve_link_url(upstream_base, norm_name, &link.url);
            PypiProjectLink {
                filename: link.filename.clone(),
                version,
                upstream_url,
                sha256: link.sha256.clone(),
                requires_python: link.requires_python.clone(),
            }
        })
        .collect()
}

/// Cache project link metadata from upstream as a single project index record.
async fn cache_project_links(store: &PypiStore<'_>, norm_name: &str, links: &[PypiProjectLink]) {
    let index = PypiProjectIndex {
        links: links.to_vec(),
    };
    if let Err(e) = store.store_project_index(norm_name, &index).await {
        tracing::warn!(error = %e, project = norm_name, "failed to cache project index");
    }
}

/// Render HTML from project index links (used for cache repos).
fn render_index_links(norm_name: &str, links: &[PypiProjectLink]) -> String {
    let mut html = String::from("<!DOCTYPE html>\n<html><body>\n");
    for link in links {
        let href = format!(
            "../../packages/{norm_name}/{}/{}",
            link.version, link.filename
        );
        let hash_frag = link
            .sha256
            .as_ref()
            .map(|s| format!("#sha256={s}"))
            .unwrap_or_default();
        let rp_attr = link
            .requires_python
            .as_ref()
            .map(|rp| {
                let escaped = rp
                    .replace('&', "&amp;")
                    .replace('<', "&lt;")
                    .replace('>', "&gt;")
                    .replace('"', "&quot;");
                format!(" data-requires-python=\"{escaped}\"")
            })
            .unwrap_or_default();
        html.push_str(&format!(
            "<a href=\"{href}{hash_frag}\"{rp_attr}>{}</a>\n",
            link.filename
        ));
    }
    html.push_str("</body></html>\n");
    html
}

/// Resolve a potentially relative link URL against the upstream base.
fn resolve_link_url(upstream_base: &str, _project: &str, link_url: &str) -> String {
    if link_url.starts_with("http://") || link_url.starts_with("https://") {
        return link_url.to_string();
    }
    // Relative URL — resolve against upstream base.
    let base = upstream_base.trim_end_matches('/');
    if link_url.starts_with('/') {
        // Absolute path — combine with scheme+host from base.
        if let Some((scheme, after_scheme)) = base.split_once("://") {
            let host = after_scheme
                .split_once('/')
                .map_or(after_scheme, |(h, _)| h);
            let origin = format!("{scheme}://{host}");
            return format!("{origin}{link_url}");
        }
        format!("{base}{link_url}")
    } else {
        // Relative to the project page.
        format!("{base}/{link_url}")
    }
}

async fn proxy_simple_project(state: &FormatState, config: &RepoConfig, project: &str) -> Response {
    let RepoKind::Proxy { ref members, .. } = config.kind else {
        return DepotError::Internal("expected proxy repo kind".into()).into_response();
    };
    let mut all_files: Vec<(String, ArtifactRecord)> = Vec::new();
    let mut seen_filenames = HashSet::new();

    for member_name in members {
        let mn = member_name.clone();
        let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
            Ok(Some(c)) if c.format() == ArtifactFormat::PyPI => c,
            _ => continue,
        };

        let member_blobs = match state.blob_store(&member_config.store).await {
            Ok(b) => b,
            Err(_) => continue,
        };

        match member_config.repo_type() {
            RepoType::Hosted => {
                let store = PypiStore {
                    repo: member_name,
                    kv: state.kv.as_ref(),
                    blobs: member_blobs.as_ref(),
                    store: &member_config.store,
                    updater: &state.updater,
                };
                if let Ok(files) = store.list_project_files(project).await {
                    for (path, record) in files {
                        let fname = match &record.kind {
                            depot_core::store::kv::ArtifactKind::PypiPackage { pypi, .. } => {
                                pypi.filename.clone()
                            }
                            _ => String::new(),
                        };
                        if seen_filenames.insert(fname) {
                            all_files.push((path, record));
                        }
                    }
                }
            }
            RepoType::Cache => {
                // For cache members, try to fetch their project page.
                let sub_config = member_config;
                let RepoKind::Cache {
                    ref upstream_url,
                    ref upstream_auth,
                    ..
                } = sub_config.kind
                else {
                    continue;
                };
                let cache_store = PypiStore {
                    repo: member_name,
                    kv: state.kv.as_ref(),
                    blobs: member_blobs.as_ref(),
                    store: &sub_config.store,
                    updater: &state.updater,
                };
                let norm = normalize_name(project);
                if let Ok(Some(html)) = pypi::fetch_upstream_project_page(
                    &state.http,
                    upstream_url,
                    project,
                    upstream_auth.as_ref(),
                )
                .await
                {
                    let links = parse_simple_html(&html);
                    let project_links = build_project_links(upstream_url, &norm, &links);
                    cache_project_links(&cache_store, &norm, &project_links).await;
                }
                // Render from project index + any already-cached real artifacts.
                if let Ok(Some(index)) = cache_store.get_project_index(&norm).await {
                    for link in &index.links {
                        if seen_filenames.insert(link.filename.clone()) {
                            // Synthesize a minimal ArtifactRecord for rendering.
                            let path = pypi::package_path(&norm, &link.version, &link.filename);
                            let now = depot_core::repo::now_utc();
                            let rec = ArtifactRecord {
                                schema_version: CURRENT_RECORD_VERSION,
                                id: String::new(),
                                size: 0,
                                content_type: "application/octet-stream".to_string(),
                                kind: depot_core::store::kv::ArtifactKind::PypiPackage {
                                    pypi: depot_core::store::kv::PypiPackageMeta {
                                        name: norm.clone(),
                                        version: link.version.clone(),
                                        filename: link.filename.clone(),
                                        filetype: String::new(),
                                        sha256: link.sha256.clone().unwrap_or_default(),
                                        requires_python: link.requires_python.clone(),
                                        summary: None,
                                        upstream_url: Some(link.upstream_url.clone()),
                                    },
                                },
                                blob_id: None,
                                content_hash: None,
                                etag: None,
                                created_at: now,
                                updated_at: now,
                                last_accessed_at: now,
                                path: String::new(),
                                internal: false,
                            };
                            all_files.push((path, rec));
                        }
                    }
                }
            }
            RepoType::Proxy => {
                // Nested proxy — just list local artifacts.
                let store = PypiStore {
                    repo: member_name,
                    kv: state.kv.as_ref(),
                    blobs: member_blobs.as_ref(),
                    store: &member_config.store,
                    updater: &state.updater,
                };
                if let Ok(files) = store.list_project_files(project).await {
                    for (path, record) in files {
                        let fname = match &record.kind {
                            depot_core::store::kv::ArtifactKind::PypiPackage { pypi, .. } => {
                                pypi.filename.clone()
                            }
                            _ => String::new(),
                        };
                        if seen_filenames.insert(fname) {
                            all_files.push((path, record));
                        }
                    }
                }
            }
        }
    }

    if all_files.is_empty() {
        return DepotError::NotFound("project not found".into()).into_response();
    }

    // Render using the proxy repo name for download URLs.
    Html(render_project_files(&config.name, &all_files)).into_response()
}

// =============================================================================
// GET /pypi/{repo}/packages/{name}/{version}/{filename} — download
// =============================================================================

pub async fn download_package(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Path((repo_name, name, version, filename)): Path<(String, String, String, String)>,
) -> Result<Response, DepotError> {
    let config =
        depot_core::api_helpers::validate_format_repo(&state, &repo_name, ArtifactFormat::PyPI)
            .await?;
    state
        .auth
        .check_permission(&user.0, &repo_name, Capability::Read)
        .await?;

    Ok(match config.repo_type() {
        RepoType::Hosted => hosted_download(&state, &repo_name, &name, &version, &filename).await,
        RepoType::Cache => cache_download(&state, &config, &name, &version, &filename).await,
        RepoType::Proxy => proxy_download(&state, &config, &name, &version, &filename).await,
    })
}

/// Build a streaming response for a PyPI package blob.
fn pypi_stream_response(
    reader: depot_core::store::blob::BlobReader,
    size: u64,
    filename: &str,
) -> Response {
    let content_type = if filename.ends_with(".whl") {
        "application/zip"
    } else if filename.ends_with(".tar.gz") {
        "application/gzip"
    } else {
        "application/octet-stream"
    };
    let stream = ReaderStream::with_capacity(reader, 256 * 1024);
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        content_type
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
    let store = PypiStore {
        repo: repo_name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &store_name,
        updater: &state.updater,
    };

    match store.get_package(name, version, filename).await {
        Ok(Some((reader, size, _record))) => pypi_stream_response(reader, size, filename),
        Ok(None) => DepotError::NotFound("package not found".into()).into_response(),
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
    let store = PypiStore {
        repo: &config.name,
        kv: state.kv.as_ref(),
        blobs: blobs.as_ref(),
        store: &config.store,
        updater: &state.updater,
    };
    let norm = normalize_name(name);
    let key = format!("pypi:{}:{}/{}/{}", config.name, name, version, filename);

    loop {
        // Check if we have the actual blob cached at _packages/{name}/{version}/{filename}.
        let path = pypi::package_path(name, version, filename);
        let cn = config.name.clone();
        let p = path.clone();
        if let Ok(Some(record)) = service::get_artifact(state.kv.as_ref(), &cn, &p).await {
            if let Some(bid) = record.blob_id.as_deref() {
                if !bid.is_empty() {
                    if let Ok(Some((reader, size))) = blobs.open_read(bid).await {
                        return pypi_stream_response(reader, size, filename);
                    }
                }
            }
        }

        // Look up the link entry from the project index to get the upstream URL.
        let link = match store.get_project_index(&norm).await {
            Ok(Some(index)) => index.links.iter().find(|l| l.filename == filename).cloned(),
            _ => None,
        };

        // If we have no index entry, try fetching the project page from upstream first.
        let link = if link.is_none() {
            match pypi::fetch_upstream_project_page(
                &state.http,
                upstream_url,
                &norm,
                upstream_auth.as_ref(),
            )
            .await
            {
                Ok(Some(html)) => {
                    let parsed = parse_simple_html(&html);
                    let project_links = build_project_links(upstream_url, &norm, &parsed);
                    cache_project_links(&store, &norm, &project_links).await;
                    project_links.into_iter().find(|l| l.filename == filename)
                }
                _ => None,
            }
        } else {
            link
        };

        let link = match link {
            Some(l) => l,
            None => {
                return DepotError::NotFound("package not found".into()).into_response();
            }
        };

        match state.inflight.try_claim(&key) {
            Ok(_guard) => {
                // Re-check cache after claiming (another leader may have just finished).
                let cn = config.name.clone();
                let p = path.clone();
                if let Ok(Some(rec)) = service::get_artifact(state.kv.as_ref(), &cn, &p).await {
                    if let Some(bid) = rec.blob_id.as_deref() {
                        if !bid.is_empty() {
                            if let Ok(Some((reader, size))) = blobs.open_read(bid).await {
                                return pypi_stream_response(reader, size, filename);
                            }
                        }
                    }
                }

                let resp = match pypi::fetch_upstream_response(
                    &state.http,
                    &link.upstream_url,
                    upstream_auth.as_ref(),
                )
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(error = %e, "failed to fetch package from upstream");
                        return DepotError::Upstream(format!("upstream error: {e}"))
                            .into_response();
                    }
                };

                // Stream upstream response directly to blob store.
                let blob_id = uuid::Uuid::new_v4().to_string();
                let mut writer = match blobs.create_writer(&blob_id, None).await {
                    Ok(w) => w,
                    Err(e) => {
                        return DepotError::Storage(
                            Box::new(std::io::Error::other(e.to_string())),
                            Retryability::Transient,
                        )
                        .into_response();
                    }
                };
                // SHA-256 comes from index metadata, not recomputed from bytes.
                let sha256 = link.sha256.clone().unwrap_or_default();
                let (b3, _sha, sz) =
                    match depot_core::repo::fetch_upstream_to_writer(resp, writer.as_mut(), false)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = writer.abort().await;
                            return DepotError::Upstream(format!("upstream error: {e}"))
                                .into_response();
                        }
                    };
                if let Err(e) = writer.finish().await {
                    tracing::error!(error = %e, "failed to finish blob write");
                    return DepotError::Storage(
                        Box::new(std::io::Error::other(e.to_string())),
                        Retryability::Transient,
                    )
                    .into_response();
                }

                let cached_record = match store
                    .put_cached_package(
                        &blob_id,
                        &b3,
                        &sha256,
                        sz,
                        name,
                        version,
                        filename,
                        &link.upstream_url,
                        link.requires_python.as_deref(),
                    )
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!(error = %e, "failed to cache package locally");
                        return DepotError::Storage(
                            Box::new(std::io::Error::other(e.to_string())),
                            Retryability::Transient,
                        )
                        .into_response();
                    }
                };

                // Serve the committed blob.
                match blobs
                    .open_read(cached_record.blob_id.as_deref().unwrap_or(""))
                    .await
                {
                    Ok(Some((reader, size))) => {
                        return pypi_stream_response(reader, size, filename)
                    }
                    _ => {
                        return DepotError::DataIntegrity("blob read failed".into()).into_response()
                    }
                }
                // _guard drops here -> notify_waiters()
            }
            Err(notify) => {
                notify.notified().await;
                continue; // retry from cache
            }
        }
    }
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
            Ok(Some(c)) if c.format() == ArtifactFormat::PyPI => c,
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
                // Recursive proxy not supported — just check local.
                let resp = hosted_download(state, member_name, name, version, filename).await;
                if resp.status() == StatusCode::OK {
                    return resp;
                }
            }
        }
    }

    DepotError::NotFound("package not found in any member".into()).into_response()
}
