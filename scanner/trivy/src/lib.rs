// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Trivy-backed [`Scanner`] implementation.
//!
//! The scanner invokes the local `trivy` CLI as a stateless client, with
//! `--server <URL>` pointing at a user-operated Trivy server that holds the
//! vulnerability database. Depot manages no DB or cache itself.
//!
//! Format support in this version:
//! - **Docker / OCI**: `trivy image --input <tarball>` against the raw blob.
//! - **SBOM-driven** (any format): `trivy sbom <path>` when the caller
//!   supplies a producer-provided SBOM on [`ScanRequest::sbom`].
//!
//! APT / Yum direct archive scanning requires `.deb`/`.rpm` extraction and
//! is slated for a subsequent milestone; [`TrivyScanner::supports`] reflects
//! that.

#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::missing_panics_doc
    )
)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tracing::{debug, warn};

use depot_core::error::{DepotError, Result};
use depot_core::scanner::{
    ScanArtifacts, ScanOutcome, ScanRequest, ScanStatus, Scanner, ScannerHealth,
};
use depot_core::store::kv::ArtifactFormat;

pub mod config;
pub mod report;
mod subprocess;

pub use config::TrivyConfig;
pub use report::{TrivyReport, TrivyVersion};

/// Pluggable Trivy scanner backend.
pub struct TrivyScanner {
    config: TrivyConfig,
    semaphore: Arc<Semaphore>,
}

impl TrivyScanner {
    pub fn new(config: TrivyConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent.max(1)));
        Self { config, semaphore }
    }

    pub fn config(&self) -> &TrivyConfig {
        &self.config
    }

    /// Run `trivy --server <URL> version --format json` and parse the reply.
    async fn query_version(&self) -> Result<TrivyVersion> {
        let binary = self.config.binary();
        let url = self.config.server_url.clone();
        let args = ["--server", &url, "version", "--format", "json"];
        let out = subprocess::run(&binary, &args, self.config.timeout).await?;
        if !out.success() {
            return Err(DepotError::Internal(format!(
                "trivy version failed: exit={:?} stderr={}",
                out.exit_code,
                out.stderr_str()
            )));
        }
        serde_json::from_slice::<TrivyVersion>(&out.stdout)
            .map_err(|e| DepotError::Internal(format!("trivy version parse error: {e}")))
    }

    /// Stream `blob_reader` into a unique file under the workdir. The
    /// returned [`StagedBlob`] deletes the file when dropped.
    async fn stage_blob_to_tempfile(
        &self,
        mut reader: depot_core::store::blob::BlobReader,
        suffix: &str,
    ) -> Result<StagedBlob> {
        let filename = format!("depot-trivy-{}{}", uuid_v4(), suffix);
        let path = self.config.workdir.join(&filename);
        let mut file = tokio::fs::File::create(&path).await.map_err(|e| {
            DepotError::Internal(format!(
                "failed to create workdir file {}: {e}",
                path.display()
            ))
        })?;
        tokio::io::copy(&mut reader, &mut file)
            .await
            .map_err(|e| DepotError::Internal(format!("staging blob failed: {e}")))?;
        file.flush()
            .await
            .map_err(|e| DepotError::Internal(format!("flushing staged blob: {e}")))?;
        drop(file);
        Ok(StagedBlob { path: Some(path) })
    }

    /// Run a scan using the provided subcommand + target path. Returns the
    /// raw stdout report bytes and the parsed [`TrivyReport`].
    async fn invoke_trivy(
        &self,
        subcommand: &str,
        target: &Path,
        extra_args: &[&str],
    ) -> Result<(Vec<u8>, TrivyReport)> {
        let binary = self.config.binary();
        let target_str = target.to_string_lossy().to_string();
        let url = self.config.server_url.clone();

        let mut args: Vec<&str> = vec!["--server", &url, subcommand, "--format", "json", "--quiet"];
        args.extend_from_slice(extra_args);
        args.push(&target_str);

        let out = subprocess::run(&binary, &args, self.config.timeout).await?;
        // Trivy exits non-zero when findings exceed a configured threshold
        // (not our case here — we don't pass `--exit-code`). Any non-zero
        // exit therefore indicates a real failure.
        if !out.success() {
            return Err(DepotError::Internal(format!(
                "trivy {subcommand} failed: exit={:?} stderr={}",
                out.exit_code,
                out.stderr_str()
            )));
        }
        let report = TrivyReport::parse(&out.stdout).map_err(|e| {
            DepotError::Internal(format!("trivy {subcommand} report parse error: {e}"))
        })?;
        Ok((out.stdout, report))
    }
}

#[async_trait::async_trait]
impl Scanner for TrivyScanner {
    fn name(&self) -> &'static str {
        "trivy"
    }

    fn supports(&self, format: ArtifactFormat) -> bool {
        // SBOM-driven scans are handled in `scan` by inspecting
        // `ScanRequest::sbom`, so any format is scannable when an SBOM is
        // attached. The `supports` predicate answers the blob-only
        // question: can this scanner scan raw blob bytes for this format?
        matches!(format, ArtifactFormat::Docker)
    }

    async fn health_check(&self) -> Result<ScannerHealth> {
        match self.query_version().await {
            Ok(v) => {
                let db_version = v
                    .vulnerability_db
                    .as_ref()
                    .map(|db| db.version_string())
                    .unwrap_or_else(|| "unknown".to_string());
                Ok(ScannerHealth {
                    name: self.name().to_string(),
                    scanner_version: v.version,
                    vuln_db_version: db_version,
                    endpoint: Some(self.config.server_url.clone()),
                    healthy: true,
                    message: None,
                })
            }
            Err(e) => Ok(ScannerHealth {
                name: self.name().to_string(),
                scanner_version: String::new(),
                vuln_db_version: String::new(),
                endpoint: Some(self.config.server_url.clone()),
                healthy: false,
                message: Some(e.to_string()),
            }),
        }
    }

    async fn current_db_version(&self) -> Result<String> {
        let v = self.query_version().await?;
        Ok(v.vulnerability_db
            .as_ref()
            .map(|db| db.version_string())
            .unwrap_or_else(|| "unknown".to_string()))
    }

    async fn scan(&self, req: ScanRequest<'_>) -> Result<ScanOutcome> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| DepotError::Internal(format!("scanner semaphore closed: {e}")))?;
        let started_at = Utc::now();

        // Destructure up front so we can move owned readers into staging
        // without fighting partial-move checks on `req` later.
        let ScanRequest {
            blob_hash: _,
            blob_path,
            blob_reader,
            format,
            content_type: _,
            sbom,
            timeout: _,
        } = req;

        // Producer-supplied SBOM wins regardless of format.
        let (subcommand, path_source, extra_args): (&str, PathSource, Vec<&str>) =
            if let Some(sbom_reader) = sbom {
                let staged = self
                    .stage_blob_to_tempfile(sbom_reader, ".sbom.json")
                    .await?;
                ("sbom", PathSource::Staged(staged), Vec::new())
            } else {
                match format {
                    ArtifactFormat::Docker => {
                        let source = self
                            .resolve_blob_path(blob_path, blob_reader, ".tar")
                            .await?;
                        ("image", source, vec!["--input"])
                    }
                    other => {
                        warn!(format = %other, "TrivyScanner: format not implemented");
                        drop(permit);
                        return Err(DepotError::NotAllowed(format!(
                            "trivy backend does not yet scan {other} artifacts directly; \
                            attach an SBOM or enable a supported format"
                        )));
                    }
                }
            };

        // For `trivy image --input`, the path follows the `--input` flag.
        // For `trivy sbom`, the path is positional.
        let target = path_source.path().to_path_buf();
        let (report_json, report) = self.invoke_trivy(subcommand, &target, &extra_args).await?;
        let finished_at = Utc::now();
        let duration_ms = (finished_at - started_at).num_milliseconds().max(0) as u64;

        // Derive DB version: prefer an explicit server-reported version,
        // otherwise fall back to the data-source fingerprint from the
        // report itself. A dedicated probe would issue a second
        // subprocess; for batch scanning we want to avoid that overhead
        // on every scan, so we only probe when the report is thin.
        let vuln_db_version = if report.data_source_fingerprint().is_empty() {
            match self.query_version().await {
                Ok(v) => v
                    .vulnerability_db
                    .as_ref()
                    .map(|db| db.version_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                Err(_) => "unknown".to_string(),
            }
        } else {
            report.data_source_fingerprint()
        };

        let scanner_version = match self.query_version().await {
            Ok(v) => v.version,
            Err(_) => String::new(),
        };

        let counts = report.severity_counts();
        let highest = counts.highest();
        debug!(
            total = counts.total(),
            critical = counts.critical,
            high = counts.high,
            "trivy scan complete"
        );
        drop(permit);

        Ok(ScanOutcome {
            scanner: self.name().to_string(),
            scanner_version,
            vuln_db_version,
            started_at,
            finished_at,
            status: ScanStatus::Ok,
            severity_counts: counts,
            highest_severity: highest,
            duration_ms,
            artifacts: ScanArtifacts {
                report_json,
                // SBOM generation as a byproduct is a follow-up — the
                // current invocation asks for the vuln report only.
                sbom_bytes: None,
                sbom_format: None,
            },
        })
    }
}

impl TrivyScanner {
    async fn resolve_blob_path(
        &self,
        blob_path: Option<PathBuf>,
        blob_reader: Option<depot_core::store::blob::BlobReader>,
        suffix: &str,
    ) -> Result<PathSource> {
        if let Some(p) = blob_path {
            return Ok(PathSource::Borrowed(p));
        }
        // S3-backed stores have no filesystem path. Stream the blob to a
        // temp file under `workdir` for the duration of the scan.
        let reader = blob_reader.ok_or_else(|| {
            DepotError::BadRequest("ScanRequest must supply either blob_path or blob_reader".into())
        })?;
        let staged = self.stage_blob_to_tempfile(reader, suffix).await?;
        Ok(PathSource::Staged(staged))
    }
}

/// Origin of the target path passed to the Trivy subprocess. Files created
/// on the fly are deleted when [`PathSource`] drops; borrowed paths from
/// the blob store are left alone.
enum PathSource {
    Borrowed(PathBuf),
    Staged(StagedBlob),
}

impl PathSource {
    fn path(&self) -> &Path {
        match self {
            PathSource::Borrowed(p) => p.as_path(),
            PathSource::Staged(s) => s.path(),
        }
    }
}

/// A temporary file created by the scanner for a single invocation. The
/// file is unlinked when the value drops so cancelled scans don't leak.
struct StagedBlob {
    path: Option<PathBuf>,
}

impl StagedBlob {
    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .unwrap_or_else(|| Path::new("/dev/null"))
    }
}

impl Drop for StagedBlob {
    fn drop(&mut self) {
        if let Some(p) = self.path.take() {
            if let Err(e) = std::fs::remove_file(&p) {
                // Best-effort cleanup; log and move on.
                tracing::debug!(path = %p.display(), error = %e, "staged blob cleanup failed");
            }
        }
    }
}

/// Ultra-minimal UUID-ish suffix to avoid bringing the `uuid` crate in for
/// one-off filename generation. 16 bytes of randomness hex-encoded.
fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    // Mix in process id for extra entropy across concurrent scans.
    let pid = std::process::id();
    format!("{nanos:032x}{pid:08x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supports_docker_only_without_sbom() {
        let s = TrivyScanner::new(TrivyConfig::new("http://localhost:4954"));
        assert!(s.supports(ArtifactFormat::Docker));
        assert!(!s.supports(ArtifactFormat::Apt));
        assert!(!s.supports(ArtifactFormat::Yum));
        assert!(!s.supports(ArtifactFormat::PyPI));
    }

    #[test]
    fn name_is_trivy() {
        let s = TrivyScanner::new(TrivyConfig::new("http://localhost:4954"));
        assert_eq!(s.name(), "trivy");
    }

    #[test]
    fn concurrency_defaults_sensibly() {
        let cfg = TrivyConfig::new("http://localhost:4954");
        assert_eq!(cfg.max_concurrent, TrivyConfig::DEFAULT_MAX_CONCURRENT);
        assert_eq!(
            cfg.timeout,
            std::time::Duration::from_secs(TrivyConfig::DEFAULT_TIMEOUT_SECS)
        );
    }

    #[tokio::test]
    async fn scan_rejects_unsupported_format_without_sbom() {
        // No trivy binary is required: the unsupported-format branch short-
        // circuits before any subprocess invocation.
        let s = TrivyScanner::new(TrivyConfig::new("http://unreachable:4954"));
        let req = ScanRequest {
            blob_hash: "deadbeef",
            blob_path: Some(PathBuf::from("/nonexistent.tar")),
            blob_reader: None,
            format: ArtifactFormat::PyPI,
            content_type: String::new(),
            sbom: None,
            timeout: std::time::Duration::from_secs(10),
        };
        let err = s.scan(req).await.unwrap_err();
        assert!(matches!(err, DepotError::NotAllowed(_)));
        assert!(err.to_string().to_lowercase().contains("trivy"));
    }

    #[tokio::test]
    async fn scan_requires_path_or_reader() {
        let s = TrivyScanner::new(TrivyConfig::new("http://unreachable:4954"));
        let req = ScanRequest {
            blob_hash: "deadbeef",
            blob_path: None,
            blob_reader: None,
            format: ArtifactFormat::Docker,
            content_type: String::new(),
            sbom: None,
            timeout: std::time::Duration::from_secs(10),
        };
        let err = s.scan(req).await.unwrap_err();
        assert!(matches!(err, DepotError::BadRequest(_)));
    }
}
