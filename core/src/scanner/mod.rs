// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Pluggable vulnerability scanner abstraction.
//!
//! Scanners consume a blob (or a pre-existing SBOM) and produce a
//! [`ScanOutcome`] describing discovered vulnerabilities. Backends implement
//! the [`Scanner`] trait; Trivy is the first backend and lives in the
//! `depot-scanner-trivy` crate.
//!
//! Scan results dedup naturally by BLAKE3 content hash: two artifacts in
//! different repositories with identical bytes share a single scan record.

use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error;
use crate::store::blob::BlobReader;
use crate::store::kv::ArtifactFormat;

pub mod queue;
pub mod registry;

pub use queue::{ScanJob, ScannerQueueHandle};
pub use registry::ScannerRegistry;

/// Input to a single scan operation.
///
/// Callers supply either a filesystem path (preferred when the blob store is
/// local) or a streaming reader (for object-storage backends). A pre-existing
/// SBOM, when present, is preferred by the scanner over analysing the raw
/// blob bytes.
pub struct ScanRequest<'a> {
    /// BLAKE3 hex hash of the target blob.
    pub blob_hash: &'a str,
    /// Local filesystem path when `BlobStore::blob_path` returns `Some`.
    pub blob_path: Option<PathBuf>,
    /// Streaming reader for the blob when a local path is unavailable.
    pub blob_reader: Option<BlobReader>,
    /// Artifact format hint; lets backends choose an appropriate scan mode.
    pub format: ArtifactFormat,
    /// HTTP content-type of the blob (e.g. `application/vnd.oci.image.manifest.v1+json`).
    pub content_type: String,
    /// Optional pre-existing SBOM bytes (CycloneDX or SPDX JSON).
    pub sbom: Option<BlobReader>,
    /// Deadline for the whole scan operation.
    pub timeout: Duration,
}

/// Outcome of a completed scan operation.
///
/// Contains both the structured summary (severity counts, status, version
/// metadata) and the raw artifact bytes (report JSON, optional generated
/// SBOM). The worker is responsible for writing those bytes to the blob
/// store and materialising a `ScanResultRecord` that references them by
/// content hash. Keeping blob storage out of the [`Scanner`] trait means
/// backends don't depend on `BlobStore` and stay unit-testable.
#[derive(Debug, Clone)]
pub struct ScanOutcome {
    pub scanner: String,
    pub scanner_version: String,
    /// Version identifier of the vulnerability database used for this scan.
    /// Used by the scheduler to detect stale results when the DB updates.
    pub vuln_db_version: String,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: ScanStatus,
    pub severity_counts: SeverityCounts,
    pub highest_severity: Severity,
    /// Wall-clock duration in milliseconds.
    pub duration_ms: u64,
    /// Byte blobs the worker must persist.
    pub artifacts: ScanArtifacts,
}

/// Raw bytes produced during a scan. The worker persists these to the blob
/// store and records their BLAKE3 hashes in the final [`crate::store::kv::ScanResultRecord`].
#[derive(Debug, Clone, Default)]
pub struct ScanArtifacts {
    /// Full JSON report as written by the scanner. Empty when
    /// [`ScanOutcome::status`] is [`ScanStatus::Error`] and the scanner
    /// produced no output.
    pub report_json: Vec<u8>,
    /// SBOM bytes generated as a byproduct of scanning, when the scanner
    /// was invoked in a mode that emits one.
    pub sbom_bytes: Option<Vec<u8>>,
    /// Format of [`ScanArtifacts::sbom_bytes`], or `None` when no SBOM was
    /// produced.
    pub sbom_format: Option<GeneratedSbomFormat>,
}

/// SBOM format produced by a scanner. Kept separate from
/// [`crate::store::kv::SbomFormat`] so the scanner module doesn't pull in
/// the KV record types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneratedSbomFormat {
    CycloneDx,
    Spdx,
}

/// Terminal status of a scan operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum ScanStatus {
    /// Scan completed and all results are trustworthy.
    Ok,
    /// Scan completed but some results may be missing (e.g. one layer failed).
    Partial,
    /// Scan failed entirely; see `last_error` for details.
    Error,
}

/// CVE severity levels. Ordering: `Unknown` < `Low` < `Medium` < `High` < `Critical`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Unknown,
    Low,
    Medium,
    High,
    Critical,
}

impl Severity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::Unknown => "unknown",
            Severity::Low => "low",
            Severity::Medium => "medium",
            Severity::High => "high",
            Severity::Critical => "critical",
        }
    }
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Count of vulnerabilities at each severity, for cheap listings.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct SeverityCounts {
    pub critical: u32,
    pub high: u32,
    pub medium: u32,
    pub low: u32,
    pub unknown: u32,
}

impl SeverityCounts {
    pub fn total(&self) -> u32 {
        self.critical + self.high + self.medium + self.low + self.unknown
    }

    /// Return the highest severity with a non-zero count, or [`Severity::Unknown`]
    /// when no vulnerabilities were found.
    pub fn highest(&self) -> Severity {
        if self.critical > 0 {
            Severity::Critical
        } else if self.high > 0 {
            Severity::High
        } else if self.medium > 0 {
            Severity::Medium
        } else if self.low > 0 {
            Severity::Low
        } else {
            Severity::Unknown
        }
    }
}

/// Scanner backend liveness and configuration report.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct ScannerHealth {
    pub name: String,
    pub scanner_version: String,
    pub vuln_db_version: String,
    pub endpoint: Option<String>,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Pluggable vulnerability scanner. Backends are registered at startup in
/// a [`ScannerRegistry`] and dispatched by name.
#[async_trait::async_trait]
pub trait Scanner: Send + Sync {
    /// Stable name used in KV records and config (e.g. `"trivy"`).
    fn name(&self) -> &'static str;

    /// Whether this scanner can handle a given artifact format. The worker
    /// skips jobs whose format isn't supported.
    fn supports(&self, format: ArtifactFormat) -> bool;

    /// Probe the backend and return its current health and DB version.
    async fn health_check(&self) -> error::Result<ScannerHealth>;

    /// Return the backend's current vulnerability database identifier. The
    /// scheduler uses this to decide when existing scan records are stale.
    async fn current_db_version(&self) -> error::Result<String>;

    /// Execute a scan and return the outcome. Implementations MUST honour
    /// [`ScanRequest::timeout`] and cancel any spawned processes on drop.
    async fn scan(&self, req: ScanRequest<'_>) -> error::Result<ScanOutcome>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_ordering() {
        assert!(Severity::Critical > Severity::High);
        assert!(Severity::High > Severity::Medium);
        assert!(Severity::Medium > Severity::Low);
        assert!(Severity::Low > Severity::Unknown);
    }

    #[test]
    fn severity_counts_highest() {
        let mut c = SeverityCounts::default();
        assert_eq!(c.highest(), Severity::Unknown);
        c.low = 3;
        assert_eq!(c.highest(), Severity::Low);
        c.critical = 1;
        assert_eq!(c.highest(), Severity::Critical);
    }

    #[test]
    fn severity_counts_total() {
        let c = SeverityCounts {
            critical: 1,
            high: 2,
            medium: 3,
            low: 4,
            unknown: 5,
        };
        assert_eq!(c.total(), 15);
    }

    #[test]
    fn severity_display() {
        assert_eq!(Severity::Critical.to_string(), "critical");
    }
}
