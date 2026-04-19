// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Scan result and scanner health admin API.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

use depot_core::error::DepotError;
use depot_core::scanner::{ScanStatus, Severity, SeverityCounts};
use depot_core::service;
use depot_core::store::kv::ArtifactFormat;

use crate::server::infra::state::AppState;
use depot_core::auth::AuthenticatedUser;

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ScanResultResponse {
    pub scanner: String,
    pub blob_hash: String,
    pub scanner_version: String,
    pub vuln_db_version: String,
    #[schema(value_type = String, format = "date-time")]
    pub scanned_at: chrono::DateTime<chrono::Utc>,
    pub status: ScanStatus,
    pub severity_counts: SeverityCounts,
    pub highest_severity: Severity,
    pub report_blob_hash: String,
    pub duration_ms: u64,
    pub attempt: u32,
    pub last_error: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ScannerStatusResponse {
    pub configured: bool,
    pub scanners: Vec<String>,
    pub queue_depth: u64,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// Get scan result for a blob.
#[utoipa::path(
    get,
    path = "/api/v1/scans/{scanner}/{blob_hash}",
    params(
        ("scanner" = String, Path, description = "Scanner backend name"),
        ("blob_hash" = String, Path, description = "BLAKE3 content hash of the scanned blob"),
    ),
    responses(
        (status = 200, description = "Scan result", body = ScanResultResponse),
        (status = 404, description = "No scan result for this blob"),
    ),
    tag = "scans"
)]
pub async fn get_scan(
    State(state): State<AppState>,
    Extension(_user): Extension<AuthenticatedUser>,
    Path((scanner, blob_hash)): Path<(String, String)>,
) -> impl IntoResponse {
    match service::get_scan_result(state.repo.kv.as_ref(), &scanner, &blob_hash).await {
        Ok(Some(rec)) => Json(ScanResultResponse {
            scanner: rec.scanner,
            blob_hash: rec.blob_hash,
            scanner_version: rec.scanner_version,
            vuln_db_version: rec.vuln_db_version,
            scanned_at: rec.scanned_at,
            status: rec.status,
            severity_counts: rec.severity_counts,
            highest_severity: rec.highest_severity,
            report_blob_hash: rec.report_blob_hash,
            duration_ms: rec.duration_ms,
            attempt: rec.attempt,
            last_error: rec.last_error,
        })
        .into_response(),
        Ok(None) => DepotError::NotFound("no scan result".into()).into_response(),
        Err(e) => e.into_response(),
    }
}

/// Stream the full JSON report for a scan result.
#[utoipa::path(
    get,
    path = "/api/v1/scans/{scanner}/{blob_hash}/report",
    params(
        ("scanner" = String, Path, description = "Scanner backend name"),
        ("blob_hash" = String, Path, description = "BLAKE3 content hash"),
    ),
    responses(
        (status = 200, description = "Full JSON report"),
        (status = 404, description = "Scan result or report blob not found"),
    ),
    tag = "scans"
)]
pub async fn get_scan_report(
    State(state): State<AppState>,
    Extension(_user): Extension<AuthenticatedUser>,
    Path((scanner, blob_hash)): Path<(String, String)>,
) -> Response {
    let rec = match service::get_scan_result(state.repo.kv.as_ref(), &scanner, &blob_hash).await {
        Ok(Some(r)) => r,
        Ok(None) => return DepotError::NotFound("no scan result".into()).into_response(),
        Err(e) => return e.into_response(),
    };
    if rec.report_blob_hash.is_empty() {
        return DepotError::NotFound("report blob not available".into()).into_response();
    }
    serve_blob(&state, &rec.report_blob_hash, "application/json").await
}

/// Re-enqueue a blob for scanning.
#[utoipa::path(
    post,
    path = "/api/v1/scans/{scanner}/{blob_hash}/rescan",
    params(
        ("scanner" = String, Path, description = "Scanner backend name"),
        ("blob_hash" = String, Path, description = "BLAKE3 content hash"),
    ),
    responses(
        (status = 202, description = "Rescan enqueued"),
        (status = 404, description = "No previous scan result to rescan"),
    ),
    tag = "scans"
)]
pub async fn rescan(
    State(state): State<AppState>,
    Extension(_user): Extension<AuthenticatedUser>,
    Path((scanner, blob_hash)): Path<(String, String)>,
) -> impl IntoResponse {
    let rec = match service::get_scan_result(state.repo.kv.as_ref(), &scanner, &blob_hash).await {
        Ok(Some(r)) => r,
        Ok(None) => return DepotError::NotFound("no scan result to rescan".into()).into_response(),
        Err(e) => return e.into_response(),
    };
    let job = depot_core::scanner::ScanJob {
        schema_version: depot_core::store::kv::CURRENT_RECORD_VERSION,
        scanner: rec.scanner,
        blob_hash: rec.blob_hash,
        format: ArtifactFormat::Raw,
        repo: String::new(),
        enqueued_at: chrono::Utc::now(),
        attempt: 0,
    };
    if let Err(e) = service::enqueue_scan_job(state.repo.kv.as_ref(), &job).await {
        return e.into_response();
    }
    StatusCode::ACCEPTED.into_response()
}

/// Scanner backend health and queue depth.
#[utoipa::path(
    get,
    path = "/api/v1/system/scanner",
    responses(
        (status = 200, description = "Scanner status", body = ScannerStatusResponse),
    ),
    tag = "system"
)]
pub async fn scanner_status(
    State(state): State<AppState>,
    Extension(_user): Extension<AuthenticatedUser>,
) -> impl IntoResponse {
    let queue = service::peek_scan_queue(state.repo.kv.as_ref(), 0)
        .await
        .map(|q| q.len() as u64)
        .unwrap_or(0);
    Json(ScannerStatusResponse {
        configured: true,
        scanners: vec![],
        queue_depth: queue,
    })
}

// ---------------------------------------------------------------------------
// SBOM sidecar upload
// ---------------------------------------------------------------------------

/// Upload an SBOM for a specific artifact (sidecar attachment).
///
/// Accepts CycloneDX JSON (`application/vnd.cyclonedx+json`) or SPDX JSON
/// (`application/spdx+json`) in the request body. The SBOM is stored as a
/// content-addressed blob and linked to the artifact's content hash via a
/// [`SbomRecord`].
#[utoipa::path(
    put,
    path = "/api/v1/repositories/{repo}/sbom/{path}",
    params(
        ("repo" = String, Path, description = "Repository name"),
        ("path" = String, Path, description = "Artifact path"),
    ),
    request_body(content = String, content_type = "application/vnd.cyclonedx+json",
        description = "CycloneDX or SPDX JSON SBOM"),
    responses(
        (status = 200, description = "SBOM stored"),
        (status = 404, description = "Artifact not found"),
        (status = 400, description = "Unsupported SBOM format"),
    ),
    tag = "scans"
)]
pub async fn put_artifact_sbom(
    State(state): State<AppState>,
    Extension(_user): Extension<AuthenticatedUser>,
    axum::extract::Path((repo, path)): axum::extract::Path<(String, String)>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/vnd.cyclonedx+json");

    let sbom_format = match content_type {
        "application/vnd.cyclonedx+json" | "application/json" => {
            depot_core::store::kv::SbomFormat::CycloneDx
        }
        "application/spdx+json" => depot_core::store::kv::SbomFormat::Spdx,
        other => {
            return DepotError::BadRequest(format!("unsupported SBOM content-type: {other}"))
                .into_response()
        }
    };

    let artifact = match service::get_artifact(state.repo.kv.as_ref(), &repo, &path).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return DepotError::NotFound(format!("artifact {repo}/{path} not found"))
                .into_response()
        }
        Err(e) => return e.into_response(),
    };

    let target_hash = match &artifact.content_hash {
        Some(h) => h.clone(),
        None => {
            return DepotError::BadRequest("artifact has no content hash".into()).into_response()
        }
    };

    let repo_config = match service::get_repo(state.repo.kv.as_ref(), &repo).await {
        Ok(Some(c)) => c,
        Ok(None) => return DepotError::NotFound(format!("repo {repo} not found")).into_response(),
        Err(e) => return e.into_response(),
    };

    let sbom_hash = blake3::hash(&body).to_hex().to_string();
    let blob_id = uuid::Uuid::new_v4().to_string();
    let blob_store = match state.repo.blob_store(&repo_config.store).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    if let Err(e) = blob_store.put(&blob_id, &body).await {
        return e.into_response();
    }
    let now = chrono::Utc::now();
    let blob_rec = depot_core::store::kv::BlobRecord {
        schema_version: depot_core::store::kv::CURRENT_RECORD_VERSION,
        blob_id,
        hash: sbom_hash.clone(),
        size: body.len() as u64,
        created_at: now,
        store: repo_config.store.clone(),
    };
    if let Err(e) =
        service::put_dedup_record(state.repo.kv.as_ref(), &repo_config.store, &blob_rec).await
    {
        return e.into_response();
    }

    let sbom_rec = depot_core::store::kv::SbomRecord {
        schema_version: depot_core::store::kv::CURRENT_RECORD_VERSION,
        target_blob_hash: target_hash,
        sbom_blob_hash: sbom_hash,
        source: depot_core::store::kv::SbomSource::Producer {
            origin: "sidecar_upload".into(),
        },
        format: sbom_format,
        created_at: now,
    };
    if let Err(e) = service::put_sbom_record(state.repo.kv.as_ref(), &sbom_rec).await {
        return e.into_response();
    }

    StatusCode::OK.into_response()
}

/// Retrieve the SBOM for an artifact.
#[utoipa::path(
    get,
    path = "/api/v1/repositories/{repo}/sbom/{path}",
    params(
        ("repo" = String, Path, description = "Repository name"),
        ("path" = String, Path, description = "Artifact path"),
    ),
    responses(
        (status = 200, description = "SBOM bytes"),
        (status = 404, description = "No SBOM for this artifact"),
    ),
    tag = "scans"
)]
pub async fn get_artifact_sbom(
    State(state): State<AppState>,
    Extension(_user): Extension<AuthenticatedUser>,
    axum::extract::Path((repo, path)): axum::extract::Path<(String, String)>,
) -> Response {
    let artifact = match service::get_artifact(state.repo.kv.as_ref(), &repo, &path).await {
        Ok(Some(a)) => a,
        Ok(None) => {
            return DepotError::NotFound(format!("artifact {repo}/{path} not found"))
                .into_response()
        }
        Err(e) => return e.into_response(),
    };
    let target_hash = match &artifact.content_hash {
        Some(h) => h.as_str(),
        None => return DepotError::NotFound("artifact has no content hash".into()).into_response(),
    };
    let sbom_rec = match service::get_sbom_record(state.repo.kv.as_ref(), target_hash).await {
        Ok(Some(r)) => r,
        Ok(None) => {
            return DepotError::NotFound("no SBOM for this artifact".into()).into_response()
        }
        Err(e) => return e.into_response(),
    };
    let ct = match sbom_rec.format {
        depot_core::store::kv::SbomFormat::CycloneDx => "application/vnd.cyclonedx+json",
        depot_core::store::kv::SbomFormat::Spdx => "application/spdx+json",
    };
    serve_blob(&state, &sbom_rec.sbom_blob_hash, ct).await
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Look up a report/SBOM blob by its BLAKE3 hash across all stores, then
/// stream it. Used for both report and SBOM retrieval.
async fn serve_blob(state: &AppState, blob_hash: &str, content_type: &str) -> Response {
    let stores = service::list_stores(state.repo.kv.as_ref()).await;
    let stores = match stores {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    for store_rec in &stores {
        if let Ok(Some(blob_rec)) =
            service::get_blob(state.repo.kv.as_ref(), &store_rec.name, blob_hash).await
        {
            let blob_store = match state.repo.blob_store(&store_rec.name).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            if let Ok(Some((reader, size))) = blob_store.open_read(&blob_rec.blob_id).await {
                let body = Body::from_stream(tokio_util::io::ReaderStream::new(reader));
                return Response::builder()
                    .status(StatusCode::OK)
                    .header("Content-Type", content_type)
                    .header("Content-Length", size)
                    .body(body)
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
            }
        }
    }
    DepotError::NotFound("report blob not found in any store".into()).into_response()
}
