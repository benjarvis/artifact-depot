// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Scan reconciler: one-shot backfill that enqueues blobs from scan-enabled
//! repos that lack a `ScanResultRecord`.

use tracing::{debug, info, warn};

use depot_core::scanner::ScanJob;
use depot_core::service;
use depot_core::store::keys;
use depot_core::store::kv::{KvStore, CURRENT_RECORD_VERSION};

const SHARD_BATCH_SIZE: usize = 1000;

/// One-shot reconciler. Call at startup or when a repo toggles `scan_enabled`.
/// Scans all blob records in stores used by scan-enabled repos and enqueues
/// any that lack a scan result under `scanner_name`.
pub async fn reconcile_missing_scans(kv: &dyn KvStore, scanner_name: &str) -> anyhow::Result<u64> {
    let repos = service::list_repos_raw(kv).await?;
    let enabled_repos: Vec<_> = repos.into_iter().filter(|r| r.scan_enabled).collect();
    if enabled_repos.is_empty() {
        debug!("reconciler: no scan-enabled repos, nothing to backfill");
        return Ok(0);
    }

    let mut stores_to_scan: Vec<String> = enabled_repos.iter().map(|r| r.store.clone()).collect();
    stores_to_scan.sort_unstable();
    stores_to_scan.dedup();

    let mut enqueued = 0u64;

    for store_name in &stores_to_scan {
        let repo_for_store = enabled_repos
            .iter()
            .find(|r| r.store == *store_name)
            .map(|r| r.name.clone())
            .unwrap_or_default();

        for shard in 0..keys::NUM_SHARDS {
            let pk = keys::blob_shard_pk(store_name, shard);
            let mut cursor: Option<String> = None;
            loop {
                let prefix = cursor.as_deref().unwrap_or("");
                let result = kv
                    .scan_prefix(
                        keys::TABLE_BLOBS,
                        std::borrow::Cow::Owned(pk.clone()),
                        std::borrow::Cow::Owned(prefix.to_string()),
                        SHARD_BATCH_SIZE,
                    )
                    .await?;
                for (sk, _bytes) in &result.items {
                    let blob_hash = sk;
                    let existing = service::get_scan_result(kv, scanner_name, blob_hash).await?;
                    if existing.is_some() {
                        continue;
                    }
                    let job = ScanJob {
                        schema_version: CURRENT_RECORD_VERSION,
                        scanner: scanner_name.to_string(),
                        blob_hash: blob_hash.clone(),
                        format: depot_core::store::kv::ArtifactFormat::Raw,
                        repo: repo_for_store.clone(),
                        enqueued_at: chrono::Utc::now(),
                        attempt: 0,
                    };
                    if let Err(e) = service::enqueue_scan_job(kv, &job).await {
                        warn!(blob = %blob_hash, error = %e, "reconciler enqueue failed");
                    } else {
                        enqueued += 1;
                    }
                }
                if result.done {
                    break;
                }
                if let Some((last_sk, _)) = result.items.last() {
                    cursor = Some(format!("{}\0", last_sk));
                } else {
                    break;
                }
            }
        }
    }

    if enqueued > 0 {
        info!(enqueued, "reconciler backfilled unscanned blobs");
    }
    Ok(enqueued)
}
