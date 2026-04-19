// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Service-layer helpers for scan results, SBOMs, and the scan queue.

use std::borrow::Cow;

use crate::error;
use crate::scanner::ScanJob;
use crate::store::keys;
use crate::store::kv::{
    KvStore, OciReferrerRecord, SbomRecord, ScanResultRecord, CURRENT_RECORD_VERSION,
};

use super::typed::{typed_get, typed_list, typed_put};

// ---------------------------------------------------------------------------
// Scan results (dedup by BLAKE3 across repos + scanners)
// ---------------------------------------------------------------------------

/// Write a scan result, stamping the current schema version.
pub async fn put_scan_result(kv: &dyn KvStore, rec: &ScanResultRecord) -> error::Result<()> {
    let mut rec = rec.clone();
    rec.schema_version = CURRENT_RECORD_VERSION;
    let (table, pk, sk) = keys::scan_result_key(&rec.scanner, &rec.blob_hash);
    typed_put(kv, table, pk, sk, &rec).await
}

/// Fetch a scan result by `(scanner, blob_hash)`.
pub async fn get_scan_result(
    kv: &dyn KvStore,
    scanner: &str,
    blob_hash: &str,
) -> error::Result<Option<ScanResultRecord>> {
    let (table, pk, sk) = keys::scan_result_key(scanner, blob_hash);
    typed_get(kv, table, pk, sk).await
}

/// Delete a scan result, e.g. when blob GC cascades to its owning blob.
pub async fn delete_scan_result(
    kv: &dyn KvStore,
    scanner: &str,
    blob_hash: &str,
) -> error::Result<()> {
    let (table, pk, sk) = keys::scan_result_key(scanner, blob_hash);
    kv.delete(table, pk, sk).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// SBOM records
// ---------------------------------------------------------------------------

pub async fn put_sbom_record(kv: &dyn KvStore, rec: &SbomRecord) -> error::Result<()> {
    let mut rec = rec.clone();
    rec.schema_version = CURRENT_RECORD_VERSION;
    let (table, pk, sk) = keys::sbom_key(&rec.target_blob_hash);
    typed_put(kv, table, pk, sk, &rec).await
}

pub async fn get_sbom_record(
    kv: &dyn KvStore,
    target_blob_hash: &str,
) -> error::Result<Option<SbomRecord>> {
    let (table, pk, sk) = keys::sbom_key(target_blob_hash);
    typed_get(kv, table, pk, sk).await
}

pub async fn delete_sbom_record(kv: &dyn KvStore, target_blob_hash: &str) -> error::Result<()> {
    let (table, pk, sk) = keys::sbom_key(target_blob_hash);
    kv.delete(table, pk, sk).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// OCI referrer records
// ---------------------------------------------------------------------------

pub async fn put_oci_referrer(
    kv: &dyn KvStore,
    repo: &str,
    rec: &OciReferrerRecord,
) -> error::Result<()> {
    let mut rec = rec.clone();
    rec.schema_version = CURRENT_RECORD_VERSION;
    let (table, pk, sk) = keys::oci_referrer_key(repo, &rec.subject_digest, &rec.referrer_digest);
    typed_put(kv, table, pk, sk, &rec).await
}

pub async fn list_oci_referrers(
    kv: &dyn KvStore,
    repo: &str,
    subject_digest: &str,
) -> error::Result<Vec<OciReferrerRecord>> {
    let pk = keys::oci_referrers_pk(repo, subject_digest);
    let sk_prefix = keys::oci_referrers_sk_prefix(subject_digest);
    typed_list(
        kv,
        keys::TABLE_OCI_REFERRERS,
        Cow::Owned(pk),
        Cow::Owned(sk_prefix),
        1000,
    )
    .await
}

// ---------------------------------------------------------------------------
// Scan queue
// ---------------------------------------------------------------------------

/// Enqueue a scan job. Best-effort — idempotent on (enqueued_at_millis, blob_hash)
/// because the sort key includes both. Callers typically ignore errors since
/// the reconciler will backfill.
pub async fn enqueue_scan_job(kv: &dyn KvStore, job: &ScanJob) -> error::Result<()> {
    let mut job = job.clone();
    job.schema_version = CURRENT_RECORD_VERSION;
    let enqueued_millis = job.enqueued_at.timestamp_millis();
    let (table, pk, sk) = keys::scan_queue_key(enqueued_millis, &job.blob_hash);
    typed_put(kv, table, pk, sk, &job).await
}

/// Peek the oldest `limit` queue entries without removing them.
///
/// The worker uses this in a drain loop: peek → scan → delete.
pub async fn peek_scan_queue(
    kv: &dyn KvStore,
    limit: usize,
) -> error::Result<Vec<(String, ScanJob)>> {
    let (table, pk, _sk) = keys::scan_queue_key(0, "");
    let result = kv.scan_prefix(table, pk, Cow::Borrowed(""), limit).await?;
    let mut out = Vec::with_capacity(result.items.len());
    for (sk, bytes) in result.items {
        let job: ScanJob = rmp_serde::from_slice(&bytes)?;
        out.push((sk, job));
    }
    Ok(out)
}

/// Remove a queue entry by its composite sort key (as returned from
/// [`peek_scan_queue`]).
pub async fn delete_scan_job(kv: &dyn KvStore, sort_key: &str) -> error::Result<()> {
    kv.delete(
        keys::TABLE_SCAN_QUEUE,
        Cow::Borrowed(keys::SCAN_QUEUE_PK),
        Cow::Borrowed(sort_key),
    )
    .await?;
    Ok(())
}
