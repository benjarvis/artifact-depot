// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Stale-scan scheduler: re-queues scan results whose `vuln_db_version`
//! differs from the scanner's current DB version.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use depot_core::scanner::{ScanJob, Scanner, ScannerRegistry};
use depot_core::service;
use depot_core::store::keys;
use depot_core::store::kv::{ArtifactFormat, KvStore, CURRENT_RECORD_VERSION};

use crate::server::worker::cluster::{release_lease, try_acquire_lease, LEASE_SCAN_SCHEDULER};

const SCHEDULER_INTERVAL_SECS: u64 = 3600;
const SCHEDULER_LEASE_TTL_SECS: u64 = 7200;
const SWEEP_BATCH_SIZE: usize = 1000;

pub async fn run_stale_scan_scheduler(
    kv: Arc<dyn KvStore>,
    registry: ScannerRegistry,
    instance_id: String,
    cancel: CancellationToken,
) {
    if registry.is_empty() {
        debug!("stale-scan scheduler started with empty registry; exiting");
        return;
    }

    let lease_holder = format!("{instance_id}:scan_scheduler");
    let mut interval = tokio::time::interval(Duration::from_secs(SCHEDULER_INTERVAL_SECS));
    interval.tick().await;

    loop {
        tokio::select! {
            _ = interval.tick() => {}
            _ = cancel.cancelled() => {
                info!("stale-scan scheduler shutting down");
                return;
            }
        }

        match try_acquire_lease(
            kv.as_ref(),
            LEASE_SCAN_SCHEDULER,
            &lease_holder,
            SCHEDULER_LEASE_TTL_SECS,
        )
        .await
        {
            Ok(true) => {}
            Ok(false) => continue,
            Err(e) => {
                warn!(error = %e, "failed to acquire scan scheduler lease");
                continue;
            }
        }

        for scanner_name in registry.names() {
            let scanner = match registry.get(scanner_name) {
                Some(s) => s,
                None => continue,
            };
            if let Err(e) = sweep_stale(kv.as_ref(), scanner.as_ref()).await {
                warn!(scanner = scanner_name, error = %e, "stale-scan sweep failed");
            }
        }

        let _ = release_lease(kv.as_ref(), LEASE_SCAN_SCHEDULER, &lease_holder).await;
    }
}

async fn sweep_stale(kv: &dyn KvStore, scanner: &dyn Scanner) -> anyhow::Result<()> {
    let current_db = scanner.current_db_version().await?;
    let scanner_name = scanner.name();
    let mut requeued = 0u64;

    for shard in 0..keys::NUM_SHARDS {
        let pk = keys::scan_result_shard_pk(scanner_name, shard);
        let result = kv
            .scan_prefix(
                keys::TABLE_SCAN_RESULTS,
                std::borrow::Cow::Owned(pk),
                std::borrow::Cow::Borrowed(""),
                SWEEP_BATCH_SIZE,
            )
            .await?;
        for (_sk, bytes) in &result.items {
            let rec: depot_core::store::kv::ScanResultRecord = match rmp_serde::from_slice(bytes) {
                Ok(r) => r,
                Err(_) => continue,
            };
            if rec.vuln_db_version == current_db {
                continue;
            }
            let job = ScanJob {
                schema_version: CURRENT_RECORD_VERSION,
                scanner: scanner_name.to_string(),
                blob_hash: rec.blob_hash.clone(),
                format: ArtifactFormat::Raw,
                repo: String::new(),
                enqueued_at: chrono::Utc::now(),
                attempt: 0,
            };
            if let Err(e) = service::enqueue_scan_job(kv, &job).await {
                warn!(blob = %rec.blob_hash, error = %e, "failed to re-enqueue stale scan");
            } else {
                requeued += 1;
            }
        }
    }

    if requeued > 0 {
        info!(scanner = scanner_name, requeued, "stale scans re-enqueued");
    }
    Ok(())
}
