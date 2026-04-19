// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! OCI Distribution Spec referrers API.
//!
//! `GET /v2/{name}/referrers/{digest}?artifactType=...` returns an OCI
//! image index listing all manifests that declared the given digest as
//! their `subject`. This is how clients discover SBOM attestations,
//! signatures, and other attached artifacts.

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use depot_core::format_state::FormatState;
use depot_core::service;
use depot_core::store::kv::Capability;

use super::helpers::{check_docker_permission, docker_error, validate_docker_repo};

#[derive(Debug, Deserialize)]
pub struct ReferrersQuery {
    #[serde(rename = "artifactType")]
    pub artifact_type: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReferrersResponse {
    schema_version: u32,
    media_type: &'static str,
    manifests: Vec<ReferrerDescriptor>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReferrerDescriptor {
    media_type: String,
    digest: String,
    size: u64,
    artifact_type: String,
}

#[allow(clippy::too_many_arguments)]
pub async fn do_get_referrers(
    state: &FormatState,
    username: &str,
    repo_name: &str,
    _image: Option<&str>,
    digest: &str,
    headers: &HeaderMap,
    uri_authority: Option<&str>,
    query: &ReferrersQuery,
) -> Response {
    if let Err(r) = check_docker_permission(
        state,
        username,
        repo_name,
        Capability::Read,
        headers,
        uri_authority,
    )
    .await
    {
        return r;
    }
    let _config = match validate_docker_repo(state, repo_name).await {
        Ok(c) => c,
        Err(r) => return r,
    };

    let referrers = match service::list_oci_referrers(state.kv.as_ref(), repo_name, digest).await {
        Ok(r) => r,
        Err(e) => {
            return docker_error("UNKNOWN", &e.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
        }
    };

    let mut descriptors: Vec<ReferrerDescriptor> = referrers
        .into_iter()
        .map(|r| ReferrerDescriptor {
            media_type: r.media_type,
            digest: r.referrer_digest,
            size: r.size,
            artifact_type: r.artifact_type,
        })
        .collect();

    if let Some(ref filter) = query.artifact_type {
        descriptors.retain(|d| &d.artifact_type == filter);
    }

    let resp = ReferrersResponse {
        schema_version: 2,
        media_type: "application/vnd.oci.image.index.v1+json",
        manifests: descriptors,
    };

    let body = match serde_json::to_vec(&resp) {
        Ok(b) => b,
        Err(e) => {
            return docker_error("UNKNOWN", &e.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
        }
    };

    (
        StatusCode::OK,
        [("Content-Type", "application/vnd.oci.image.index.v1+json")],
        body,
    )
        .into_response()
}
