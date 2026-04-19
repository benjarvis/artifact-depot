// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Scanner worker loop.
//!
//! A single cluster-wide holder of [`LEASE_SCANNER`] drains the scan queue
//! in FIFO order. For each job it resolves the target blob, dispatches to
//! the named [`Scanner`] backend in the registry, writes a
//! [`ScanResultRecord`], and removes the queue entry. Jobs whose scanner
//! isn't currently registered are left on the queue.
//!
//! Report and SBOM blob persistence is intentionally minimal in this
//! revision — the rollup record (severity counts + status + backend
//! metadata) lands, but the full JSON report is dropped. A follow-up will
//! write report bytes as dedup-eligible blobs and populate
//! [`ScanResultRecord::report_blob_hash`].

pub mod reconciler;
pub mod scheduler;

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use depot_core::scanner::{
    ScanJob, ScanRequest, ScanStatus, Scanner, ScannerRegistry, SeverityCounts,
};
use depot_core::service;
use depot_core::store::kv::{KvStore, ScanResultRecord};
use depot_core::store_registry::StoreRegistry;

use crate::server::worker::cluster::{release_lease, try_acquire_lease, LeaseGuard, LEASE_SCANNER};

/// How often the worker polls for new jobs while holding the lease.
pub const SCANNER_TICK_SECS: u64 = 30;

/// Lease TTL. Longer than the tick so a single slow scan doesn't expire
/// the lease while we're still working.
pub const SCANNER_LEASE_TTL_SECS: u64 = 600;

/// Per-tick drain batch size.
pub const SCANNER_BATCH_SIZE: usize = 32;

/// Background scanner worker. Runs forever until `cancel` fires.
///
/// The worker is a no-op when `registry` is empty — useful for deployments
/// that haven't configured a scanner backend yet.
pub async fn run_scan_worker(
    kv: Arc<dyn KvStore>,
    stores: Arc<StoreRegistry>,
    registry: ScannerRegistry,
    instance_id: String,
    cancel: CancellationToken,
) {
    if registry.is_empty() {
        debug!("scanner worker started with empty registry; exiting");
        return;
    }

    let lease_holder = format!("{instance_id}:scanner");
    let mut interval = tokio::time::interval(Duration::from_secs(SCANNER_TICK_SECS));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                info!("scanner worker shutting down");
                return;
            }
        }

        match try_acquire_lease(
            kv.as_ref(),
            LEASE_SCANNER,
            &lease_holder,
            SCANNER_LEASE_TTL_SECS,
        )
        .await
        {
            Ok(true) => {
                debug!("acquired scanner lease");
            }
            Ok(false) => continue,
            Err(e) => {
                warn!(error = %e, "failed to acquire scanner lease");
                continue;
            }
        }

        // RAII so the lease is dropped on any panic / early return.
        let _guard = LeaseGuard::new(kv.clone(), LEASE_SCANNER, &lease_holder);

        if let Err(e) = drain_once(kv.as_ref(), &stores, &registry, &cancel).await {
            warn!(error = %e, "scanner drain pass failed");
        }

        let _ = release_lease(kv.as_ref(), LEASE_SCANNER, &lease_holder).await;
    }
}

/// Drain up to [`SCANNER_BATCH_SIZE`] pending jobs. Called while the lease
/// is held.
async fn drain_once(
    kv: &dyn KvStore,
    stores: &Arc<StoreRegistry>,
    registry: &ScannerRegistry,
    cancel: &CancellationToken,
) -> anyhow::Result<()> {
    let batch = service::peek_scan_queue(kv, SCANNER_BATCH_SIZE).await?;
    if batch.is_empty() {
        return Ok(());
    }
    info!(count = batch.len(), "scanning batch");

    for (sort_key, job) in batch {
        if cancel.is_cancelled() {
            return Ok(());
        }
        process_job(kv, stores, registry, &sort_key, &job).await;
    }
    Ok(())
}

async fn process_job(
    kv: &dyn KvStore,
    stores: &Arc<StoreRegistry>,
    registry: &ScannerRegistry,
    sort_key: &str,
    job: &ScanJob,
) {
    let scanner = match registry.get(&job.scanner) {
        Some(s) => s,
        None => {
            debug!(scanner = %job.scanner, "backend not registered; leaving job queued");
            return;
        }
    };

    if !scanner.supports(job.format) && job.scanner != "trivy" {
        // "trivy" is special-cased: it may handle format via SBOM even when
        // `supports()` returns false. Other backends must declare support.
        warn!(scanner = %job.scanner, format = %job.format, "scanner does not support format");
        let _ = service::delete_scan_job(kv, sort_key).await;
        return;
    }

    // Resolve the blob bytes + blob store for persisting the report.
    let repo_config = match service::get_repo(kv, &job.repo).await {
        Ok(Some(c)) => c,
        Ok(None) => {
            warn!(repo = %job.repo, "repo not found, dropping scan job");
            let _ = service::delete_scan_job(kv, sort_key).await;
            return;
        }
        Err(e) => {
            warn!(repo = %job.repo, error = %e, "repo lookup failed");
            return;
        }
    };

    let outcome = match run_scan(kv, stores, scanner.as_ref(), job).await {
        Ok(o) => o,
        Err(e) => {
            warn!(blob = %job.blob_hash, error = %e, "scan failed");
            record_failure(kv, job, &e.to_string()).await;
            let _ = service::delete_scan_job(kv, sort_key).await;
            return;
        }
    };

    // Persist report JSON as a content-addressed blob for retrieval via the
    // admin API. Empty reports (error scans) get an empty hash.
    let report_blob_hash = if outcome.artifacts.report_json.is_empty() {
        String::new()
    } else {
        persist_blob(
            kv,
            stores,
            &repo_config.store,
            &outcome.artifacts.report_json,
        )
        .await
        .unwrap_or_default()
    };

    let record = ScanResultRecord {
        schema_version: 0,
        scanner: outcome.scanner,
        blob_hash: job.blob_hash.clone(),
        scanner_version: outcome.scanner_version,
        vuln_db_version: outcome.vuln_db_version,
        scanned_at: outcome.finished_at,
        status: outcome.status,
        severity_counts: outcome.severity_counts,
        highest_severity: outcome.highest_severity,
        report_blob_hash,
        sbom_blob_hash: None,
        duration_ms: outcome.duration_ms,
        attempt: job.attempt + 1,
        last_error: None,
    };
    if let Err(e) = service::put_scan_result(kv, &record).await {
        warn!(blob = %job.blob_hash, error = %e, "put_scan_result failed");
        return;
    }

    if let Err(e) = service::delete_scan_job(kv, sort_key).await {
        warn!(blob = %job.blob_hash, error = %e, "failed to remove queue entry");
    }
}

/// Write raw bytes to the blob store as a content-addressed blob (BLAKE3 dedup).
/// Returns the BLAKE3 hash, or an empty string on failure (logged internally).
async fn persist_blob(
    kv: &dyn KvStore,
    stores: &Arc<StoreRegistry>,
    store_name: &str,
    data: &[u8],
) -> Option<String> {
    let hash = blake3::hash(data).to_hex().to_string();
    let blob_id = uuid::Uuid::new_v4().to_string();
    let blob_store =
        match crate::server::infra::store_registry::resolve_blob_store(stores, store_name).await {
            Ok(bs) => bs,
            Err(e) => {
                warn!(error = %e, "blob store lookup failed for report persistence");
                return None;
            }
        };
    if let Err(e) = blob_store.put(&blob_id, data).await {
        warn!(error = %e, "failed to write report blob");
        return None;
    }
    let now = chrono::Utc::now();
    let blob_rec = depot_core::store::kv::BlobRecord {
        schema_version: depot_core::store::kv::CURRENT_RECORD_VERSION,
        blob_id,
        hash: hash.clone(),
        size: data.len() as u64,
        created_at: now,
        store: store_name.to_string(),
    };
    if let Err(e) = service::put_dedup_record(kv, store_name, &blob_rec).await {
        warn!(error = %e, "failed to write report dedup record");
    }
    Some(hash)
}

async fn run_scan(
    kv: &dyn KvStore,
    stores: &Arc<StoreRegistry>,
    scanner: &dyn Scanner,
    job: &ScanJob,
) -> anyhow::Result<depot_core::scanner::ScanOutcome> {
    let repo_config = service::get_repo(kv, &job.repo)
        .await?
        .ok_or_else(|| anyhow::anyhow!("repo '{}' not found", job.repo))?;
    let blob_record = service::get_blob(kv, &repo_config.store, &job.blob_hash)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!(
                "blob {} missing in store {}",
                job.blob_hash,
                repo_config.store
            )
        })?;
    let blob_store =
        crate::server::infra::store_registry::resolve_blob_store(stores, &repo_config.store)
            .await
            .map_err(|e| anyhow::anyhow!("blob store lookup failed: {e}"))?;

    let blob_path = blob_store.blob_path(&blob_record.blob_id);
    let blob_reader = if blob_path.is_some() {
        None
    } else {
        match blob_store.open_read(&blob_record.blob_id).await? {
            Some((r, _size)) => Some(r),
            None => anyhow::bail!("blob bytes missing for {}", blob_record.blob_id),
        }
    };

    let req = ScanRequest {
        blob_hash: &job.blob_hash,
        blob_path,
        blob_reader,
        format: job.format,
        content_type: String::new(),
        sbom: None,
        timeout: Duration::from_secs(300),
    };
    Ok(scanner.scan(req).await?)
}

/// Persist a failed-scan marker so repeated failures are visible but don't
/// keep re-queuing forever.
async fn record_failure(kv: &dyn KvStore, job: &ScanJob, error: &str) {
    let record = ScanResultRecord {
        schema_version: 0,
        scanner: job.scanner.clone(),
        blob_hash: job.blob_hash.clone(),
        scanner_version: String::new(),
        vuln_db_version: String::new(),
        scanned_at: Utc::now(),
        status: ScanStatus::Error,
        severity_counts: SeverityCounts::default(),
        highest_severity: depot_core::scanner::Severity::Unknown,
        report_blob_hash: String::new(),
        sbom_blob_hash: None,
        duration_ms: 0,
        attempt: job.attempt + 1,
        last_error: Some(error.to_string()),
    };
    if let Err(e) = service::put_scan_result(kv, &record).await {
        warn!(blob = %job.blob_hash, error = %e, "failed to record scan failure");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use depot_core::scanner::{ScanArtifacts, ScanOutcome, ScanRequest, ScannerHealth, Severity};
    use depot_core::store::kv::{
        ArtifactFormat as AF, BlobRecord, FormatConfig, RepoConfig, RepoKind, StoreKind,
        StoreRecord, CURRENT_RECORD_VERSION,
    };
    use depot_kv_redb::sharded_redb::ShardedRedbKvStore;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct MockScanner {
        name: &'static str,
        call_count: AtomicU32,
    }

    #[async_trait]
    impl Scanner for MockScanner {
        fn name(&self) -> &'static str {
            self.name
        }

        fn supports(&self, _format: AF) -> bool {
            true
        }

        async fn health_check(&self) -> depot_core::error::Result<ScannerHealth> {
            Ok(ScannerHealth {
                name: self.name.to_string(),
                scanner_version: "mock-1".into(),
                vuln_db_version: "mock-db".into(),
                endpoint: None,
                healthy: true,
                message: None,
            })
        }

        async fn current_db_version(&self) -> depot_core::error::Result<String> {
            Ok("mock-db".into())
        }

        async fn scan(&self, _req: ScanRequest<'_>) -> depot_core::error::Result<ScanOutcome> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            let now = Utc::now();
            Ok(ScanOutcome {
                scanner: self.name.to_string(),
                scanner_version: "mock-1".into(),
                vuln_db_version: "mock-db".into(),
                started_at: now,
                finished_at: now,
                status: ScanStatus::Ok,
                severity_counts: SeverityCounts {
                    critical: 1,
                    ..Default::default()
                },
                highest_severity: Severity::Critical,
                duration_ms: 5,
                artifacts: ScanArtifacts::default(),
            })
        }
    }

    async fn setup() -> (
        Arc<dyn KvStore>,
        Arc<StoreRegistry>,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
        let kv_dir = tempfile::TempDir::new().unwrap();
        let blob_dir = tempfile::TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(
            ShardedRedbKvStore::open(&kv_dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );

        // Register a file blob store.
        service::put_store(
            kv.as_ref(),
            &StoreRecord {
                schema_version: CURRENT_RECORD_VERSION,
                name: "default".into(),
                kind: StoreKind::File {
                    root: blob_dir.path().to_string_lossy().into_owned(),
                    sync: false,
                    io_size: 1024,
                    direct_io: false,
                },
                created_at: Utc::now(),
            },
        )
        .await
        .unwrap();

        let stores = Arc::new(StoreRegistry::new());
        let blob_store =
            depot_blob_file::FileBlobStore::new(blob_dir.path(), false, 1_048_576, false).unwrap();
        stores.add("default", Arc::new(blob_store)).await;

        // Create a repo + a dummy blob.
        service::put_repo(
            kv.as_ref(),
            &RepoConfig {
                schema_version: CURRENT_RECORD_VERSION,
                name: "testrepo".into(),
                kind: RepoKind::Hosted,
                format_config: FormatConfig::Docker {
                    listen: None,
                    cleanup_untagged_manifests: None,
                },
                store: "default".into(),
                created_at: Utc::now(),
                cleanup_max_unaccessed_days: None,
                cleanup_max_age_days: None,
                deleting: false,
                scan_enabled: true,
            },
        )
        .await
        .unwrap();

        (kv, stores, kv_dir, blob_dir)
    }

    async fn stage_blob(kv: &dyn KvStore, stores: &Arc<StoreRegistry>, hash: &str) {
        let blob_id = uuid::Uuid::new_v4().to_string();
        let bs = crate::server::infra::store_registry::resolve_blob_store(stores, "default")
            .await
            .unwrap();
        bs.put(&blob_id, b"fake-blob-bytes").await.unwrap();
        service::put_dedup_record(
            kv,
            "default",
            &BlobRecord {
                schema_version: CURRENT_RECORD_VERSION,
                blob_id,
                hash: hash.into(),
                size: 16,
                created_at: Utc::now(),
                store: "default".into(),
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn drain_processes_queued_job() {
        let (kv, stores, _kd, _bd) = setup().await;
        stage_blob(kv.as_ref(), &stores, "blobhash123").await;

        let job = ScanJob {
            schema_version: CURRENT_RECORD_VERSION,
            scanner: "mock".into(),
            blob_hash: "blobhash123".into(),
            format: AF::Docker,
            repo: "testrepo".into(),
            enqueued_at: Utc::now(),
            attempt: 0,
        };
        service::enqueue_scan_job(kv.as_ref(), &job).await.unwrap();

        let scanner = Arc::new(MockScanner {
            name: "mock",
            call_count: AtomicU32::new(0),
        });
        let registry = ScannerRegistry::from_backends(vec![scanner.clone() as Arc<dyn Scanner>]);

        let cancel = CancellationToken::new();
        drain_once(kv.as_ref(), &stores, &registry, &cancel)
            .await
            .unwrap();

        assert_eq!(scanner.call_count.load(Ordering::SeqCst), 1);
        let result = service::get_scan_result(kv.as_ref(), "mock", "blobhash123")
            .await
            .unwrap()
            .expect("scan result present");
        assert_eq!(result.severity_counts.critical, 1);
        assert_eq!(result.status, ScanStatus::Ok);

        // Queue should be empty after drain.
        let remaining = service::peek_scan_queue(kv.as_ref(), 10).await.unwrap();
        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn drain_without_backend_leaves_job() {
        let (kv, stores, _kd, _bd) = setup().await;
        stage_blob(kv.as_ref(), &stores, "blobhash456").await;

        let job = ScanJob {
            schema_version: CURRENT_RECORD_VERSION,
            scanner: "mock".into(),
            blob_hash: "blobhash456".into(),
            format: AF::Docker,
            repo: "testrepo".into(),
            enqueued_at: Utc::now(),
            attempt: 0,
        };
        service::enqueue_scan_job(kv.as_ref(), &job).await.unwrap();

        // Empty registry — no backend for "mock".
        let registry = ScannerRegistry::empty();
        let cancel = CancellationToken::new();
        drain_once(kv.as_ref(), &stores, &registry, &cancel)
            .await
            .unwrap();

        let remaining = service::peek_scan_queue(kv.as_ref(), 10).await.unwrap();
        assert_eq!(remaining.len(), 1, "job should remain when backend absent");
    }
}
