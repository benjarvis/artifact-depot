// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end integration test for the vulnerability scanning pipeline.
//!
//! Exercises the full path: create repo with `scan_enabled` → upload artifact →
//! scan job enqueued → worker drains queue → ScanResultRecord lands →
//! admin API returns the result.
//!
//! Uses a [`MockScanner`] so no Trivy binary or server is required.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use depot_core::scanner::{
    ScanArtifacts, ScanJob, ScanOutcome, ScanRequest, ScanStatus, Scanner, ScannerHealth,
    ScannerQueueHandle, ScannerRegistry, Severity, SeverityCounts,
};
use depot_core::service;
use depot_core::store::kv::{ArtifactFormat, CURRENT_RECORD_VERSION};
use depot_kv_redb::sharded_redb::ShardedRedbKvStore;
use depot_server::server::worker::scanner::run_scan_worker;

struct MockScanner {
    call_count: AtomicU32,
}

#[async_trait::async_trait]
impl Scanner for MockScanner {
    fn name(&self) -> &'static str {
        "mock"
    }
    fn supports(&self, _f: ArtifactFormat) -> bool {
        true
    }
    async fn health_check(&self) -> depot_core::error::Result<ScannerHealth> {
        Ok(ScannerHealth {
            name: "mock".into(),
            scanner_version: "1.0".into(),
            vuln_db_version: "test-db".into(),
            endpoint: None,
            healthy: true,
            message: None,
        })
    }
    async fn current_db_version(&self) -> depot_core::error::Result<String> {
        Ok("test-db".into())
    }
    async fn scan(&self, _req: ScanRequest<'_>) -> depot_core::error::Result<ScanOutcome> {
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let now = Utc::now();
        Ok(ScanOutcome {
            scanner: "mock".into(),
            scanner_version: "1.0".into(),
            vuln_db_version: "test-db".into(),
            started_at: now,
            finished_at: now,
            status: ScanStatus::Ok,
            severity_counts: SeverityCounts {
                critical: 2,
                high: 5,
                ..Default::default()
            },
            highest_severity: Severity::Critical,
            duration_ms: 10,
            artifacts: ScanArtifacts {
                report_json: br#"{"findings":"test"}"#.to_vec(),
                ..Default::default()
            },
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scan_pipeline_enqueue_drain_result() {
    // --- Setup: KV + blob store + repo ---
    let kv_dir = tempfile::TempDir::new().unwrap();
    let blob_dir = tempfile::TempDir::new().unwrap();
    let kv: Arc<dyn depot_core::store::kv::KvStore> = Arc::new(
        ShardedRedbKvStore::open(&kv_dir.path().join("kv"), 2, 0)
            .await
            .unwrap(),
    );
    let stores = Arc::new(depot_core::store_registry::StoreRegistry::new());
    let blob_store =
        depot_blob_file::FileBlobStore::new(blob_dir.path(), false, 1_048_576, false).unwrap();
    stores.add("default", Arc::new(blob_store)).await;

    service::put_store(
        kv.as_ref(),
        &depot_core::store::kv::StoreRecord {
            schema_version: CURRENT_RECORD_VERSION,
            name: "default".into(),
            kind: depot_core::store::kv::StoreKind::File {
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

    // Create a repo with scanning enabled.
    service::put_repo(
        kv.as_ref(),
        &depot_core::store::kv::RepoConfig {
            schema_version: CURRENT_RECORD_VERSION,
            name: "scan-e2e".into(),
            kind: depot_core::store::kv::RepoKind::Hosted,
            format_config: depot_core::store::kv::FormatConfig::Docker {
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

    // Stage a dummy blob so the worker can resolve it.
    let blob_hash = "e2etesthash123";
    let blob_id = uuid::Uuid::new_v4().to_string();
    let blob_store_ref = stores.get("default").await.expect("store should exist");
    blob_store_ref
        .put(&blob_id, b"fake-docker-manifest")
        .await
        .unwrap();
    service::put_dedup_record(
        kv.as_ref(),
        "default",
        &depot_core::store::kv::BlobRecord {
            schema_version: CURRENT_RECORD_VERSION,
            blob_id,
            hash: blob_hash.into(),
            size: 20,
            created_at: Utc::now(),
            store: "default".into(),
        },
    )
    .await
    .unwrap();

    // --- Enqueue a scan job (simulates what commit_ingestion does) ---
    let queue = ScannerQueueHandle::kv_backed(kv.clone());
    queue
        .enqueue(ScanJob {
            schema_version: CURRENT_RECORD_VERSION,
            scanner: "mock".into(),
            blob_hash: blob_hash.into(),
            format: ArtifactFormat::Docker,
            repo: "scan-e2e".into(),
            enqueued_at: Utc::now(),
            attempt: 0,
        })
        .await;

    // Verify job is in the queue.
    let pending = service::peek_scan_queue(kv.as_ref(), 10).await.unwrap();
    assert_eq!(pending.len(), 1);

    // --- Run the scanner worker for one drain pass ---
    let mock = Arc::new(MockScanner {
        call_count: AtomicU32::new(0),
    });
    let registry = ScannerRegistry::from_backends(vec![mock.clone() as Arc<dyn Scanner>]);
    let cancel = tokio_util::sync::CancellationToken::new();
    let cancel_clone = cancel.clone();

    let worker = tokio::spawn(run_scan_worker(
        kv.clone(),
        stores.clone(),
        registry,
        "test-instance".into(),
        cancel_clone,
    ));

    // Wait for the worker to drain the queue (it ticks every 30s, but we
    // can't speed that up easily). Instead, poll the queue.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let remaining = service::peek_scan_queue(kv.as_ref(), 10).await.unwrap();
        if remaining.is_empty() {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            cancel.cancel();
            let _ = worker.await;
            panic!("scan queue was not drained within 60s");
        }
    }

    cancel.cancel();
    let _ = worker.await;

    // --- Verify the scan result landed ---
    assert!(
        mock.call_count.load(Ordering::SeqCst) >= 1,
        "MockScanner.scan was not called"
    );

    let result = service::get_scan_result(kv.as_ref(), "mock", blob_hash)
        .await
        .unwrap()
        .expect("ScanResultRecord should exist");
    assert_eq!(result.status, ScanStatus::Ok);
    assert_eq!(result.severity_counts.critical, 2);
    assert_eq!(result.severity_counts.high, 5);
    assert_eq!(result.highest_severity, Severity::Critical);
    assert!(
        !result.report_blob_hash.is_empty(),
        "report blob should be stored"
    );
}
