// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AptPackageMeta {
    pub package: String,
    pub version: String,
    pub architecture: String,
    /// Which distribution this package belongs to.
    #[serde(default)]
    pub distribution: String,
    pub component: String,
    pub maintainer: String,
    pub depends: String,
    pub section: String,
    pub priority: String,
    pub installed_size: String,
    pub description: String,
    pub sha256: String,
    pub control: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PypiPackageMeta {
    pub name: String,
    pub version: String,
    pub filename: String,
    pub filetype: String,
    pub sha256: String,
    pub requires_python: Option<String>,
    pub summary: Option<String>,
    pub upstream_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoModuleMeta {
    pub module: String,
    pub version: String,
    pub file_type: String,
    pub sha256: String,
    pub upstream_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelmChartMeta {
    pub name: String,
    pub version: String,
    pub app_version: String,
    pub description: String,
    pub api_version: String,
    pub chart_type: String,
    pub keywords: Vec<String>,
    pub home: String,
    pub sources: Vec<String>,
    pub icon: String,
    pub deprecated: bool,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrateVersionMeta {
    pub name: String,
    pub vers: String,
    pub deps: Vec<CrateDep>,
    pub cksum: String,
    /// JSON-encoded features map (stored as string for msgpack compatibility).
    pub features: String,
    /// JSON-encoded features2 map (optional, stored as string).
    pub features2: Option<String>,
    pub yanked: bool,
    pub links: Option<String>,
    pub rust_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrateDep {
    pub name: String,
    pub req: String,
    pub features: Vec<String>,
    pub optional: bool,
    pub default_features: bool,
    pub target: Option<String>,
    pub kind: String,
    pub registry: Option<String>,
    pub package: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpmPackageMeta {
    pub name: String,
    pub epoch: u32,
    pub version: String,
    pub release: String,
    pub arch: String,
    pub summary: String,
    pub description: String,
    pub url: String,
    pub license: String,
    pub vendor: String,
    pub group: String,
    pub buildhost: String,
    pub buildtime: u64,
    pub packager: String,
    pub installed_size: u64,
    pub archive_size: u64,
    pub sha256: String,
    pub requires: Vec<RpmDep>,
    pub provides: Vec<RpmDep>,
    pub conflicts: Vec<RpmDep>,
    pub obsoletes: Vec<RpmDep>,
    #[serde(default)]
    pub files: Vec<RpmFileEntry>,
    #[serde(default)]
    pub changelogs: Vec<RpmChangelog>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpmFileEntry {
    pub path: String,
    pub file_type: String, // "file", "dir", or "ghost"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpmChangelog {
    pub author: String,
    pub timestamp: u64,
    pub text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpmDep {
    pub name: String,
    pub flags: String,
    pub epoch: String,
    pub ver: String,
    pub rel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NpmPackageMeta {
    pub name: String,
    pub version: String,
    pub filename: String,
    pub sha1: String,
    pub sha512: String,
    pub upstream_url: Option<String>,
}

/// A cached PyPI project index containing all parsed links from the upstream
/// PEP 503 simple project page.  Stored as a JSON blob referenced by the
/// `_index/{project}` artifact record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PypiProjectIndex {
    pub links: Vec<PypiProjectLink>,
}

/// A single link entry within a cached PyPI project index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PypiProjectLink {
    pub filename: String,
    pub version: String,
    pub upstream_url: String,
    pub sha256: Option<String>,
    pub requires_python: Option<String>,
}
