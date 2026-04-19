// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Parser for Trivy's JSON scan-result output.
//!
//! Trivy writes a single top-level JSON document to stdout when invoked with
//! `--format json`. The schema is documented at
//! <https://aquasecurity.github.io/trivy/v0.60/docs/configuration/reporting/>
//! and uses PascalCase field names throughout.
//!
//! This module deserialises the subset of fields Depot needs today
//! (severity counts, DB version, artifact metadata). Extra fields in the
//! wire format are tolerated and ignored.

use chrono::{DateTime, Utc};
use serde::Deserialize;

use depot_core::scanner::{Severity, SeverityCounts};

/// Top-level Trivy report document.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyReport {
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub artifact_name: String,
    #[serde(default)]
    pub artifact_type: String,
    #[serde(default)]
    pub metadata: TrivyMetadata,
    #[serde(default)]
    pub results: Vec<TrivyResult>,
}

/// Artifact metadata — `Metadata` in Trivy JSON.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyMetadata {
    // Trivy emits `OS` with both letters uppercase; PascalCase would only
    // produce `Os`, so we rename explicitly.
    #[serde(default, rename = "OS")]
    pub os: Option<TrivyOs>,
    #[serde(default, rename = "ImageID")]
    pub image_id: Option<String>,
    #[serde(default)]
    pub repo_tags: Vec<String>,
    #[serde(default)]
    pub repo_digests: Vec<String>,
}

/// Operating system descriptor under `Metadata.OS`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyOs {
    #[serde(default)]
    pub family: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    #[serde(rename = "EOSL")]
    pub eosl: bool,
}

/// Per-target result block — one per scanned artefact class (os-pkgs,
/// lang-pkgs, etc.).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyResult {
    #[serde(default)]
    pub target: String,
    #[serde(default)]
    pub class: String,
    #[serde(default, rename = "Type")]
    pub type_: String,
    #[serde(default)]
    pub vulnerabilities: Vec<TrivyVulnerability>,
    #[serde(default)]
    pub data_source: Option<TrivyDataSource>,
}

/// `DataSource` block present on each result — names the feed the findings
/// came from (e.g. Alpine Secdb, GHSA, NVD) and, on recent Trivy versions,
/// an `UpdatedAt` timestamp we can use as a DB-version fingerprint.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyDataSource {
    #[serde(default, rename = "ID")]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "URL")]
    pub url: String,
}

/// Single vulnerability record.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyVulnerability {
    #[serde(default, rename = "VulnerabilityID")]
    pub vulnerability_id: String,
    #[serde(default)]
    pub pkg_name: String,
    #[serde(default)]
    pub installed_version: Option<String>,
    #[serde(default)]
    pub fixed_version: Option<String>,
    #[serde(default)]
    pub severity: TrivySeverity,
    #[serde(default)]
    pub title: String,
}

/// Trivy uses an uppercase severity vocabulary. Mapped to Depot's [`Severity`]
/// via [`From`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum TrivySeverity {
    Critical,
    High,
    Medium,
    Low,
    #[default]
    Unknown,
}

impl From<TrivySeverity> for Severity {
    fn from(s: TrivySeverity) -> Self {
        match s {
            TrivySeverity::Critical => Severity::Critical,
            TrivySeverity::High => Severity::High,
            TrivySeverity::Medium => Severity::Medium,
            TrivySeverity::Low => Severity::Low,
            TrivySeverity::Unknown => Severity::Unknown,
        }
    }
}

impl TrivyReport {
    /// Parse a `trivy ... --format json` stdout byte buffer.
    pub fn parse(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Aggregate severity counts across every result block.
    pub fn severity_counts(&self) -> SeverityCounts {
        let mut counts = SeverityCounts::default();
        for result in &self.results {
            for v in &result.vulnerabilities {
                match v.severity {
                    TrivySeverity::Critical => counts.critical += 1,
                    TrivySeverity::High => counts.high += 1,
                    TrivySeverity::Medium => counts.medium += 1,
                    TrivySeverity::Low => counts.low += 1,
                    TrivySeverity::Unknown => counts.unknown += 1,
                }
            }
        }
        counts
    }

    /// Total count of vulnerabilities in the report.
    pub fn total_vulnerabilities(&self) -> usize {
        self.results.iter().map(|r| r.vulnerabilities.len()).sum()
    }

    /// A DB-version fingerprint suitable for stale-scan detection.
    ///
    /// Trivy doesn't expose its DB version through the scan JSON in a
    /// first-class field, so we fall back to a deterministic compound of
    /// the data-source IDs that contributed findings. Callers that need a
    /// precise version probe [`crate::TrivyScanner::current_db_version`]
    /// against the server directly.
    pub fn data_source_fingerprint(&self) -> String {
        let mut ids: Vec<&str> = self
            .results
            .iter()
            .filter_map(|r| r.data_source.as_ref())
            .map(|d| d.id.as_str())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids.join(",")
    }
}

/// Top-level response shape of `trivy version --format json`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyVersion {
    #[serde(default)]
    pub version: String,
    // Trivy's field is `VulnerabilityDB` — PascalCase would produce
    // `VulnerabilityDb`, so we rename explicitly.
    #[serde(default, rename = "VulnerabilityDB")]
    pub vulnerability_db: Option<TrivyDbMetadata>,
}

/// Metadata block for the currently-loaded vulnerability database.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TrivyDbMetadata {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub next_update: Option<DateTime<Utc>>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

impl TrivyDbMetadata {
    /// Build a stable version string for use as [`ScanResultRecord::vuln_db_version`].
    ///
    /// `ScanResultRecord` lives in `depot_core::store::kv::ScanResultRecord`.
    pub fn version_string(&self) -> String {
        match self.updated_at {
            Some(ts) => format!("v{}@{}", self.version, ts.to_rfc3339()),
            None => format!("v{}", self.version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal report with a single critical finding (Trivy image mode shape).
    const IMAGE_REPORT: &str = r#"{
  "SchemaVersion": 2,
  "CreatedAt": "2026-04-15T10:30:00Z",
  "ArtifactName": "alpine:3.7",
  "ArtifactType": "container_image",
  "Metadata": {
    "OS": { "Family": "alpine", "Name": "3.7.3", "EOSL": true },
    "ImageID": "sha256:abc",
    "RepoTags": ["alpine:3.7"],
    "RepoDigests": ["alpine@sha256:def"]
  },
  "Results": [
    {
      "Target": "alpine:3.7 (alpine 3.7.3)",
      "Class": "os-pkgs",
      "Type": "alpine",
      "DataSource": { "ID": "alpine", "Name": "Alpine Secdb", "URL": "https://secdb.alpinelinux.org/" },
      "Vulnerabilities": [
        {
          "VulnerabilityID": "CVE-2024-99999",
          "PkgName": "openssl",
          "InstalledVersion": "1.0.2",
          "FixedVersion": "1.0.2k",
          "Severity": "CRITICAL",
          "Title": "OpenSSL issue"
        },
        {
          "VulnerabilityID": "CVE-2023-11111",
          "PkgName": "openssl",
          "InstalledVersion": "1.0.2",
          "Severity": "HIGH",
          "Title": "Another issue"
        }
      ]
    }
  ]
}"#;

    /// Report with empty findings — Trivy omits the `Vulnerabilities` key.
    const EMPTY_REPORT: &str = r#"{
  "SchemaVersion": 2,
  "CreatedAt": "2026-04-15T10:30:00Z",
  "ArtifactName": "scratch:latest",
  "ArtifactType": "container_image",
  "Metadata": {},
  "Results": []
}"#;

    const SBOM_REPORT: &str = r#"{
  "SchemaVersion": 2,
  "CreatedAt": "2026-04-15T10:30:00Z",
  "ArtifactName": "./sbom.json",
  "ArtifactType": "cyclonedx",
  "Metadata": {},
  "Results": [
    {
      "Target": "app",
      "Class": "lang-pkgs",
      "Type": "rust-binary",
      "DataSource": { "ID": "ghsa", "Name": "GitHub Advisories", "URL": "https://github.com/advisories" },
      "Vulnerabilities": [
        {
          "VulnerabilityID": "GHSA-aaaa",
          "PkgName": "tokio",
          "InstalledVersion": "1.0.0",
          "Severity": "MEDIUM"
        }
      ]
    }
  ]
}"#;

    const VERSION_OUTPUT: &str = r#"{
  "Version": "0.60.0",
  "VulnerabilityDB": {
    "Version": 2,
    "NextUpdate": "2026-04-16T00:00:00Z",
    "UpdatedAt": "2026-04-15T00:00:00Z"
  }
}"#;

    #[test]
    fn image_report_parses() {
        let r = TrivyReport::parse(IMAGE_REPORT.as_bytes()).unwrap();
        assert_eq!(r.schema_version, 2);
        assert_eq!(r.artifact_name, "alpine:3.7");
        assert_eq!(r.artifact_type, "container_image");
        assert_eq!(r.results.len(), 1);
        assert_eq!(r.results[0].vulnerabilities.len(), 2);
        assert_eq!(r.total_vulnerabilities(), 2);
        let os = r.metadata.os.as_ref().unwrap();
        assert_eq!(os.family, "alpine");
        assert!(os.eosl);
    }

    #[test]
    fn severity_counts_aggregate() {
        let r = TrivyReport::parse(IMAGE_REPORT.as_bytes()).unwrap();
        let counts = r.severity_counts();
        assert_eq!(counts.critical, 1);
        assert_eq!(counts.high, 1);
        assert_eq!(counts.total(), 2);
        assert_eq!(counts.highest(), Severity::Critical);
    }

    #[test]
    fn empty_report_has_no_findings() {
        let r = TrivyReport::parse(EMPTY_REPORT.as_bytes()).unwrap();
        assert!(r.results.is_empty());
        assert_eq!(r.severity_counts().total(), 0);
        assert_eq!(r.severity_counts().highest(), Severity::Unknown);
    }

    #[test]
    fn sbom_report_parses() {
        let r = TrivyReport::parse(SBOM_REPORT.as_bytes()).unwrap();
        assert_eq!(r.artifact_type, "cyclonedx");
        assert_eq!(r.severity_counts().medium, 1);
    }

    #[test]
    fn data_source_fingerprint_deduplicates() {
        let r = TrivyReport::parse(IMAGE_REPORT.as_bytes()).unwrap();
        assert_eq!(r.data_source_fingerprint(), "alpine");
    }

    #[test]
    fn unknown_severity_tolerated() {
        // A report where Trivy reports a severity we don't enumerate.
        let body = r#"{"SchemaVersion":2,"CreatedAt":"2026-04-15T10:30:00Z",
            "ArtifactName":"x","ArtifactType":"x","Metadata":{},
            "Results":[{"Target":"t","Class":"c","Type":"t",
              "Vulnerabilities":[{"VulnerabilityID":"x","PkgName":"p","Severity":"UNKNOWN"}]}]}"#;
        let r = TrivyReport::parse(body.as_bytes()).unwrap();
        assert_eq!(r.severity_counts().unknown, 1);
    }

    #[test]
    fn extra_fields_ignored() {
        // Newer Trivy versions add fields we don't know about; parse must tolerate.
        let body = r#"{"SchemaVersion":2,"CreatedAt":"2026-04-15T10:30:00Z",
            "ArtifactName":"x","ArtifactType":"x","Metadata":{},
            "Results":[], "FutureField": "ignored"}"#;
        let r = TrivyReport::parse(body.as_bytes()).unwrap();
        assert_eq!(r.artifact_name, "x");
    }

    #[test]
    fn version_payload_parses() {
        let v: TrivyVersion = serde_json::from_str(VERSION_OUTPUT).unwrap();
        assert_eq!(v.version, "0.60.0");
        let db = v.vulnerability_db.unwrap();
        assert_eq!(db.version, 2);
        assert!(db.version_string().starts_with("v2@2026-04-15"));
    }

    #[test]
    fn version_string_without_timestamp() {
        let db = TrivyDbMetadata {
            version: 42,
            next_update: None,
            updated_at: None,
        };
        assert_eq!(db.version_string(), "v42");
    }
}
