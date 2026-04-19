// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Runtime configuration for [`crate::TrivyScanner`].

use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the Trivy scanner backend.
///
/// The deployment model Depot targets in v1 is: an operator runs their own
/// `trivy server` somewhere and tells Depot the URL. Depot bundles the Trivy
/// CLI locally as a stateless client and invokes it with `--server <url>`
/// per scan. No vulnerability database lives on the Depot host.
#[derive(Debug, Clone)]
pub struct TrivyConfig {
    /// Path to the `trivy` executable. When `None`, Depot looks up `trivy`
    /// on `$PATH`.
    pub binary_path: Option<PathBuf>,
    /// URL of the user-operated Trivy server (`trivy server --listen ...`).
    /// Scans are forwarded to this endpoint via `trivy --server <URL>`.
    pub server_url: String,
    /// Maximum concurrent scan subprocesses. A semaphore caps fan-out so
    /// scanning doesn't starve the rest of the instance.
    pub max_concurrent: usize,
    /// Per-scan wall-clock deadline. Exceeded scans are killed.
    pub timeout: Duration,
    /// Scratch directory for staging blobs streamed from object storage.
    /// Each scan materialises at most one file here and removes it on drop.
    pub workdir: PathBuf,
}

impl TrivyConfig {
    pub const DEFAULT_MAX_CONCURRENT: usize = 4;
    pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

    /// Minimal builder for tests and examples.
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            binary_path: None,
            server_url: server_url.into(),
            max_concurrent: Self::DEFAULT_MAX_CONCURRENT,
            timeout: Duration::from_secs(Self::DEFAULT_TIMEOUT_SECS),
            workdir: std::env::temp_dir(),
        }
    }

    /// Resolve the configured binary path, defaulting to `"trivy"` on `$PATH`.
    pub fn binary(&self) -> PathBuf {
        self.binary_path
            .clone()
            .unwrap_or_else(|| PathBuf::from("trivy"))
    }
}
