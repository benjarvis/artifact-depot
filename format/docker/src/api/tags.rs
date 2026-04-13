// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};

use depot_core::auth::{check_grants, AuthenticatedUser};
use depot_core::format_state::FormatState;
use depot_core::service;
use depot_core::store::kv::{ArtifactFormat, Capability, RepoKind};

use super::helpers::{
    check_docker_permission, docker_error, hosted_store, resolve_blob_store, validate_docker_repo,
};
use super::types::{CatalogResponse, DockerPaginationParams, TagListResponse};

pub async fn do_list_tags(
    state: &FormatState,
    username: &str,
    repo_name: &str,
    image: Option<&str>,
    pagination: &DockerPaginationParams,
    req_headers: &HeaderMap,
    uri_authority: Option<&str>,
) -> Response {
    if let Err(r) = check_docker_permission(
        state,
        username,
        repo_name,
        Capability::Read,
        req_headers,
        uri_authority,
    )
    .await
    {
        return r;
    }
    let config = match validate_docker_repo(state, repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    let display_name = match image {
        Some(img) => format!("{}/{}", repo_name, img),
        None => repo_name.to_string(),
    };

    let all_tags = match config.kind {
        RepoKind::Hosted | RepoKind::Cache { .. } => {
            let blobs = match resolve_blob_store(state, &config).await {
                Ok(b) => b,
                Err(r) => return r,
            };
            let store = hosted_store(state, &config.name, image, blobs.as_ref(), &config.store);
            match store.list_tags().await {
                Ok(tags) => tags,
                Err(e) => {
                    return docker_error(
                        "UNKNOWN",
                        &e.to_string(),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    )
                }
            }
        }
        RepoKind::Proxy { ref members, .. } => {
            let mut tag_set = std::collections::BTreeSet::new();
            for member_name in members {
                let mn = member_name.clone();
                let member_config = match service::get_repo(state.kv.as_ref(), &mn).await {
                    Ok(Some(c)) => c,
                    _ => continue,
                };
                let member_blobs = match resolve_blob_store(state, &member_config).await {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let store = hosted_store(
                    state,
                    member_name,
                    image,
                    member_blobs.as_ref(),
                    &member_config.store,
                );
                if let Ok(tags) = store.list_tags().await {
                    tag_set.extend(tags);
                }
            }
            tag_set.into_iter().collect::<Vec<_>>()
        }
    };

    // Apply Docker V2 cursor pagination: filter tags > last, take n.
    let filtered: Vec<String> = match &pagination.last {
        Some(last) => all_tags
            .into_iter()
            .filter(|t| t.as_str() > last.as_str())
            .collect(),
        None => all_tags,
    };

    let n = pagination.n.unwrap_or(filtered.len());
    let page: Vec<String> = filtered.iter().take(n).cloned().collect();
    let has_more = filtered.len() > n;

    let mut resp = Json(TagListResponse {
        name: display_name.clone(),
        tags: page.clone(),
    })
    .into_response();

    if has_more {
        if let Some(last_tag) = page.last() {
            let link_path = match image {
                Some(img) => format!("/v2/{}/{}/tags/list", repo_name, img),
                None => format!("/v2/{}/tags/list", repo_name),
            };
            let link_value = format!("<{}?n={}&last={}>; rel=\"next\"", link_path, n, last_tag);
            if let Ok(hv) = link_value.parse() {
                resp.headers_mut().insert("Link", hv);
            }
        }
    }

    resp
}

/// `GET /v2/_catalog`
pub async fn catalog(
    State(state): State<FormatState>,
    Extension(user): Extension<AuthenticatedUser>,
    Query(pagination): Query<DockerPaginationParams>,
) -> Response {
    let roles = match state.auth.resolve_user_roles(&user.0).await {
        Ok(Some((_, roles))) => roles,
        Ok(None) => {
            return Json(CatalogResponse {
                repositories: Vec::new(),
            })
            .into_response()
        }
        Err(_) => return docker_error("UNKNOWN", "auth error", StatusCode::INTERNAL_SERVER_ERROR),
    };

    match service::list_repos(state.kv.as_ref()).await {
        Ok(repos) => {
            let docker_repos: Vec<String> = repos
                .into_iter()
                .filter(|r| {
                    r.format() == ArtifactFormat::Docker
                        && check_grants(&roles, &r.name, Capability::Read).is_ok()
                })
                .map(|r| r.name)
                .collect();

            // Apply Docker V2 cursor pagination.
            let filtered: Vec<String> = match &pagination.last {
                Some(last) => docker_repos
                    .into_iter()
                    .filter(|r| r.as_str() > last.as_str())
                    .collect(),
                None => docker_repos,
            };
            let n = pagination.n.unwrap_or(filtered.len());
            let page: Vec<String> = filtered.iter().take(n).cloned().collect();
            let has_more = filtered.len() > n;

            let mut resp = Json(CatalogResponse {
                repositories: page.clone(),
            })
            .into_response();

            if has_more {
                if let Some(last_repo) = page.last() {
                    let link_value =
                        format!("</v2/_catalog?n={}&last={}>; rel=\"next\"", n, last_repo);
                    if let Ok(hv) = link_value.parse() {
                        resp.headers_mut().insert("Link", hv);
                    }
                }
            }

            resp
        }
        Err(e) => docker_error("UNKNOWN", &e.to_string(), StatusCode::INTERNAL_SERVER_ERROR),
    }
}
