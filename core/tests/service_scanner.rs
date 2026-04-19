// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the scanner service helpers against a real redb
//! backend.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::{TimeZone, Utc};

use depot_core::scanner::{ScanJob, ScanStatus, Severity, SeverityCounts};
use depot_core::service;
use depot_core::store::kv::{
    ArtifactFormat, OciReferrerRecord, SbomFormat, SbomRecord, SbomSource, ScanResultRecord,
    CURRENT_RECORD_VERSION,
};
use depot_kv_redb::sharded_redb::ShardedRedbKvStore;

async fn test_kv() -> (ShardedRedbKvStore, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let kv = ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
        .await
        .unwrap();
    (kv, dir)
}

fn sample_scan_result(hash: &str) -> ScanResultRecord {
    ScanResultRecord {
        schema_version: 0,
        scanner: "trivy".into(),
        blob_hash: hash.into(),
        scanner_version: "0.60.0".into(),
        vuln_db_version: "v2@2026-04-15".into(),
        scanned_at: Utc::now(),
        status: ScanStatus::Ok,
        severity_counts: SeverityCounts {
            critical: 1,
            ..Default::default()
        },
        highest_severity: Severity::Critical,
        report_blob_hash: "report-hash".into(),
        sbom_blob_hash: None,
        duration_ms: 100,
        attempt: 1,
        last_error: None,
    }
}

#[tokio::test]
async fn scan_result_roundtrip() {
    let (kv, _d) = test_kv().await;
    let rec = sample_scan_result("abc");
    service::put_scan_result(&kv, &rec).await.unwrap();
    let fetched = service::get_scan_result(&kv, "trivy", "abc")
        .await
        .unwrap()
        .expect("present");
    assert_eq!(fetched.scanner, "trivy");
    assert_eq!(fetched.schema_version, CURRENT_RECORD_VERSION);
    assert_eq!(fetched.severity_counts.critical, 1);
}

#[tokio::test]
async fn scan_result_delete() {
    let (kv, _d) = test_kv().await;
    service::put_scan_result(&kv, &sample_scan_result("xyz"))
        .await
        .unwrap();
    service::delete_scan_result(&kv, "trivy", "xyz")
        .await
        .unwrap();
    assert!(service::get_scan_result(&kv, "trivy", "xyz")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn sbom_record_roundtrip() {
    let (kv, _d) = test_kv().await;
    let rec = SbomRecord {
        schema_version: 0,
        target_blob_hash: "target".into(),
        sbom_blob_hash: "sbom".into(),
        source: SbomSource::Producer {
            origin: "oci_referrer".into(),
        },
        format: SbomFormat::CycloneDx,
        created_at: Utc::now(),
    };
    service::put_sbom_record(&kv, &rec).await.unwrap();
    let got = service::get_sbom_record(&kv, "target")
        .await
        .unwrap()
        .expect("present");
    assert!(matches!(got.source, SbomSource::Producer { .. }));
    service::delete_sbom_record(&kv, "target").await.unwrap();
    assert!(service::get_sbom_record(&kv, "target")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn scan_queue_fifo_ordering() {
    let (kv, _d) = test_kv().await;
    for (i, hash) in ["a", "b", "c"].iter().enumerate() {
        let job = ScanJob {
            schema_version: 0,
            scanner: "trivy".into(),
            blob_hash: (*hash).into(),
            format: ArtifactFormat::Docker,
            repo: "myrepo".into(),
            enqueued_at: Utc
                .timestamp_millis_opt(1000 + i as i64 * 10)
                .single()
                .unwrap(),
            attempt: 0,
        };
        service::enqueue_scan_job(&kv, &job).await.unwrap();
    }
    let peeked = service::peek_scan_queue(&kv, 10).await.unwrap();
    assert_eq!(peeked.len(), 3);
    assert_eq!(peeked[0].1.blob_hash, "a");
    assert_eq!(peeked[1].1.blob_hash, "b");
    assert_eq!(peeked[2].1.blob_hash, "c");
}

#[tokio::test]
async fn scan_queue_drain_cycle() {
    let (kv, _d) = test_kv().await;
    let job = ScanJob {
        schema_version: 0,
        scanner: "trivy".into(),
        blob_hash: "blob".into(),
        format: ArtifactFormat::Docker,
        repo: "myrepo".into(),
        enqueued_at: Utc.timestamp_millis_opt(1000).single().unwrap(),
        attempt: 0,
    };
    service::enqueue_scan_job(&kv, &job).await.unwrap();
    let peeked = service::peek_scan_queue(&kv, 10).await.unwrap();
    assert_eq!(peeked.len(), 1);
    service::delete_scan_job(&kv, &peeked[0].0).await.unwrap();
    assert!(service::peek_scan_queue(&kv, 10).await.unwrap().is_empty());
}

#[tokio::test]
async fn oci_referrer_list_filters_by_subject() {
    let (kv, _d) = test_kv().await;
    service::put_oci_referrer(
        &kv,
        "repo",
        &OciReferrerRecord {
            schema_version: 0,
            subject_digest: "sha256:a".into(),
            referrer_digest: "sha256:r1".into(),
            artifact_type: "application/vnd.cyclonedx+json".into(),
            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            size: 100,
            created_at: Utc::now(),
        },
    )
    .await
    .unwrap();
    service::put_oci_referrer(
        &kv,
        "repo",
        &OciReferrerRecord {
            schema_version: 0,
            subject_digest: "sha256:a".into(),
            referrer_digest: "sha256:r2".into(),
            artifact_type: "application/vnd.in-toto+json".into(),
            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            size: 200,
            created_at: Utc::now(),
        },
    )
    .await
    .unwrap();
    service::put_oci_referrer(
        &kv,
        "repo",
        &OciReferrerRecord {
            schema_version: 0,
            subject_digest: "sha256:b".into(),
            referrer_digest: "sha256:r3".into(),
            artifact_type: "application/vnd.cyclonedx+json".into(),
            media_type: "application/vnd.oci.image.manifest.v1+json".into(),
            size: 150,
            created_at: Utc::now(),
        },
    )
    .await
    .unwrap();
    let refs_a = service::list_oci_referrers(&kv, "repo", "sha256:a")
        .await
        .unwrap();
    assert_eq!(refs_a.len(), 2);
    let refs_b = service::list_oci_referrers(&kv, "repo", "sha256:b")
        .await
        .unwrap();
    assert_eq!(refs_b.len(), 1);
    assert_eq!(refs_b[0].size, 150);
}
