// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Task type definitions shared between the service layer (which persists
//! tasks to KV) and the server layer (which tracks per-instance runtime
//! state — cancel tokens, progress channels — and exposes the REST API).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskProgress {
    pub total_repos: usize,
    pub completed_repos: usize,
    pub total_blobs: usize,
    pub checked_blobs: usize,
    pub failed_blobs: usize,
    pub bytes_read: u64,
    pub total_bytes: u64,
    pub total_artifacts: usize,
    pub checked_artifacts: usize,
    /// Current phase label (e.g. "Verifying blobs", "Checking repos").
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub phase: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskKind {
    Check,
    BlobGc,
    RepoClean { repo: String },
    RebuildDirEntries,
    BulkDelete { repo: String, prefix: String },
    RepoClone { source: String, target: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CheckFinding {
    pub repo: Option<String>,
    pub path: String,
    pub expected_hash: String,
    pub actual_hash: Option<String>,
    pub error: Option<String>,
    /// Whether this finding was auto-fixed (only in non-dry-run mode).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fixed: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct CheckResult {
    pub total_blobs: usize,
    pub passed: usize,
    pub mismatched: usize,
    pub missing: usize,
    pub errors: usize,
    #[serde(default)]
    pub findings: Vec<CheckFinding>,
    pub bytes_read: u64,
    /// Whether this check ran in dry-run mode (no fixes applied).
    #[serde(default)]
    pub dry_run: bool,
    /// Number of findings that were auto-fixed.
    #[serde(default)]
    pub fixed: usize,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct GcResult {
    pub drained_repos: u64,
    pub cleaned_upload_sessions: u64,
    pub scanned_artifacts: u64,
    pub expired_artifacts: u64,
    pub docker_deleted: u64,
    pub scanned_blobs: u64,
    pub deleted_dedup_refs: u64,
    pub orphaned_blobs_deleted: u64,
    pub orphaned_blob_bytes_deleted: u64,
    pub elapsed_ns: u64,
    /// Whether this GC ran in dry-run mode (no deletions performed).
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RepoCleanResult {
    pub repo: String,
    pub scanned_artifacts: u64,
    pub expired_artifacts: u64,
    pub docker_deleted: u64,
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RebuildDirEntriesResult {
    pub repos: usize,
    pub entries_updated: u64,
    pub stores_updated: usize,
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RepoCloneResult {
    pub source: String,
    pub target: String,
    pub copied_artifacts: u64,
    pub copied_entries: u64,
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct BulkDeleteResult {
    pub repo: String,
    pub prefix: String,
    pub deleted_artifacts: u64,
    pub deleted_bytes: u64,
    pub elapsed_ns: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskResult {
    Check(CheckResult),
    BlobGc(GcResult),
    RepoClean(RepoCleanResult),
    RebuildDirEntries(RebuildDirEntriesResult),
    BulkDelete(BulkDeleteResult),
    RepoClone(RepoCloneResult),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct TaskLogEntry {
    #[cfg_attr(feature = "openapi", schema(value_type = String, format = "date-time"))]
    pub timestamp: DateTime<Utc>,
    pub message: String,
}
