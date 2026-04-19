// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Enqueue interface for scan jobs.
//!
//! Ingest paths push a [`ScanJob`] through a [`ScannerQueueHandle`] right after
//! an artifact commits. The handle is cheaply cloneable and resolves to either
//! a real KV-backed enqueuer (in production) or a no-op (in tests).
//!
//! Enqueue is best-effort: errors are logged via the concrete enqueuer and
//! never propagate back to the caller, so a failed scan enqueue cannot fail
//! an ingest. The background reconciler sweeps for missed blobs as a
//! durability backstop.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::store::kv::{ArtifactFormat, KvStore};

/// A single entry on the scan queue.
///
/// Stored in `TABLE_SCAN_QUEUE` with a timestamp-ordered sort key; drained by
/// the scanner worker loop and removed on completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanJob {
    #[serde(default)]
    pub schema_version: u32,
    /// Which backend should handle this job (e.g. `"trivy"`).
    pub scanner: String,
    /// BLAKE3 hex hash of the target blob.
    pub blob_hash: String,
    /// Artifact format hint for the scanner.
    pub format: ArtifactFormat,
    /// Repository the triggering artifact lived in. Lets the worker re-check
    /// whether scanning is still enabled on that repo before running.
    pub repo: String,
    pub enqueued_at: DateTime<Utc>,
    /// Retry counter for transient failures. Starts at 0.
    #[serde(default)]
    pub attempt: u32,
}

/// Thin trait backing a [`ScannerQueueHandle`].
///
/// Production deployments use a KV-backed implementation that persists jobs
/// in `TABLE_SCAN_QUEUE`. Tests use [`NoopScanEnqueuer`].
#[async_trait::async_trait]
pub trait ScanEnqueuer: Send + Sync {
    /// Enqueue `job`. Errors MUST be logged inside the implementation; this
    /// function is intentionally infallible at the interface level so callers
    /// on the ingest hot path don't have to handle scan-queue failures.
    async fn enqueue(&self, job: ScanJob);
}

/// No-op enqueuer used when scanning is disabled or in tests.
pub struct NoopScanEnqueuer;

#[async_trait::async_trait]
impl ScanEnqueuer for NoopScanEnqueuer {
    async fn enqueue(&self, _job: ScanJob) {}
}

/// KV-backed enqueuer: writes the job into `TABLE_SCAN_QUEUE` via
/// [`crate::service::enqueue_scan_job`]. Errors are logged — this
/// intentionally cannot fail the caller.
pub struct KvBackedScanEnqueuer {
    kv: Arc<dyn KvStore>,
}

impl KvBackedScanEnqueuer {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }
}

#[async_trait::async_trait]
impl ScanEnqueuer for KvBackedScanEnqueuer {
    async fn enqueue(&self, job: ScanJob) {
        if let Err(e) = crate::service::enqueue_scan_job(self.kv.as_ref(), &job).await {
            warn!(
                scanner = %job.scanner,
                blob_hash = %job.blob_hash,
                error = %e,
                "scan enqueue failed; reconciler will retry on next sweep"
            );
        }
    }
}

/// Cheaply cloneable handle for pushing jobs onto the scan queue.
#[derive(Clone)]
pub struct ScannerQueueHandle {
    inner: Arc<dyn ScanEnqueuer>,
}

impl ScannerQueueHandle {
    pub fn new(inner: Arc<dyn ScanEnqueuer>) -> Self {
        Self { inner }
    }

    /// Handle that silently drops every enqueue. Useful in tests and in
    /// deployments where no scanner backend is configured.
    pub fn noop() -> Self {
        Self::new(Arc::new(NoopScanEnqueuer))
    }

    /// Handle backed by a KV store. Every enqueue writes a row to
    /// `TABLE_SCAN_QUEUE` for the scan worker to drain.
    pub fn kv_backed(kv: Arc<dyn KvStore>) -> Self {
        Self::new(Arc::new(KvBackedScanEnqueuer::new(kv)))
    }

    pub async fn enqueue(&self, job: ScanJob) {
        self.inner.enqueue(job).await;
    }
}

impl Default for ScannerQueueHandle {
    fn default() -> Self {
        Self::noop()
    }
}
