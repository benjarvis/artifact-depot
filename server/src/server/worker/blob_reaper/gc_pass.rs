// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

// ---------------------------------------------------------------------------
// GC pass
// ---------------------------------------------------------------------------

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bloomfilter::Bloom;
use futures::stream::StreamExt;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::server::infra::task::TaskProgress;
use depot_core::service;
use depot_core::store::keys;
use depot_core::store::kv::KvStore;
use depot_core::store_registry::StoreRegistry;
use depot_core::update::UpdateSender;

use super::bloom::{bloom_empty_like, bloom_union};
use super::docker_gc::docker_gc;
use super::repo_cleanup::expire_repo_artifacts;

// ---------------------------------------------------------------------------
// GC state (persists across passes in-memory)
// ---------------------------------------------------------------------------

pub(super) struct OrphanCandidate {
    pub(super) first_seen_pass: u64,
}

#[derive(Default)]
pub struct GcState {
    /// Orphan blob files on disk with no artifact reference.
    /// Key = (store_name, blob_id).
    pub(super) orphan_candidates: HashMap<(String, String), OrphanCandidate>,
    /// Monotonically increasing pass counter.
    pub(super) pass: u64,
}

impl GcState {
    pub fn new() -> Self {
        Self::default()
    }
}

pub struct GcStats {
    pub drained_repos: u64,
    pub cleaned_upload_sessions: u64,
    pub scanned_artifacts: u64,
    pub expired_artifacts: u64,
    pub docker_deleted: u64,
    pub scanned_blobs: u64,
    pub deleted_dedup_refs: u64,
    pub orphaned_blobs_deleted: u64,
    pub orphaned_blob_bytes_deleted: u64,
}

#[allow(clippy::too_many_arguments)]
pub async fn gc_pass(
    kv: Arc<dyn KvStore>,
    stores: &Arc<StoreRegistry>,
    state: &mut GcState,
    cancel: Option<&CancellationToken>,
    max_orphan_candidates: usize,
    progress_tx: Option<&watch::Sender<TaskProgress>>,
    dry_run: bool,
    updater: &UpdateSender,
) -> depot_core::error::Result<GcStats> {
    state.pass += 1;
    let current_pass = state.pass;

    // --- Phase: Drain repos marked for deletion ---
    // Loop until no deleting repos remain, so repos marked during a drain
    // pass are caught immediately.
    let mut drained_repos = 0u64;
    if let Some(tx) = progress_tx {
        tx.send_modify(|p| {
            p.phase = "Draining deleted repos".into();
        });
    }
    loop {
        if cancel.is_some_and(|c| c.is_cancelled()) {
            return Err(depot_core::error::DepotError::Internal(
                "GC pass cancelled".to_string(),
            ));
        }

        let deleting: Vec<_> = service::list_repos_raw(kv.as_ref())
            .await?
            .into_iter()
            .filter(|r| r.deleting)
            .collect();

        if deleting.is_empty() {
            break;
        }

        for repo in &deleting {
            tracing::info!(repo = %repo.name, "draining deleting repo");
            if let Err(e) = service::drain_deleting_repo(kv.as_ref(), &repo.name).await {
                tracing::error!(repo = %repo.name, "failed to drain deleting repo: {}", e);
            } else {
                drained_repos += 1;
            }
        }
    }

    // --- Phase: Clean up stale upload sessions ---
    const UPLOAD_SESSION_TTL: std::time::Duration = std::time::Duration::from_secs(3600);
    let cleaned_upload_sessions =
        match service::cleanup_upload_sessions(kv.as_ref(), stores, UPLOAD_SESSION_TTL).await {
            Ok(n) => n as u64,
            Err(e) => {
                tracing::error!("upload session cleanup failed: {}", e);
                0
            }
        };

    let repos = service::list_repos(kv.as_ref()).await?;
    let store_records = service::list_stores(kv.as_ref()).await?;

    // --- Pre-phase: Expire artifacts in Docker repos with cleanup policies ---
    // Run artifact expiration for Docker repos BEFORE Docker GC, so that
    // layers orphaned by manifest expiration are caught in the same pass.
    let docker_repos_with_policy: Vec<_> = repos
        .iter()
        .filter(|r| r.format() == depot_core::store::kv::ArtifactFormat::Docker)
        .filter(|r| r.cleanup_max_age_days.is_some() || r.cleanup_max_unaccessed_days.is_some())
        .collect();

    let mut pre_expired = 0u64;
    if !docker_repos_with_policy.is_empty() {
        if let Some(tx) = progress_tx {
            tx.send_modify(|p| {
                p.phase = "Docker pre-expiration".into();
                p.total_repos = docker_repos_with_policy.len();
                p.completed_repos = 0;
            });
        }
        for repo in &docker_repos_with_policy {
            let (_, _, expired) = expire_repo_artifacts(kv.clone(), repo, cancel, updater).await?;
            pre_expired += expired;
            if let Some(tx) = progress_tx {
                tx.send_modify(|p| p.completed_repos += 1);
            }
        }
    }

    if cancel.is_some_and(|c| c.is_cancelled()) {
        return Err(depot_core::error::DepotError::Internal(
            "GC pass cancelled".to_string(),
        ));
    }

    // --- Phase 0: Docker-specific GC (runs after pre-expiration) ---
    // Delete Docker layer blobs not referenced by any manifest, and
    // optionally delete manifests not referenced by any tag.
    let docker_repos: Vec<_> = repos
        .iter()
        .filter(|r| r.format() == depot_core::store::kv::ArtifactFormat::Docker)
        .collect();
    if let Some(tx) = progress_tx {
        tx.send_modify(|p| {
            p.phase = "Docker GC".into();
            p.total_repos = docker_repos.len();
            p.completed_repos = 0;
        });
    }
    let mut docker_deleted = 0u64;
    for repo in &docker_repos {
        docker_deleted += docker_gc(kv.clone(), stores, repo, dry_run, cancel, updater).await?;
        if let Some(tx) = progress_tx {
            tx.send_modify(|p| p.completed_repos += 1);
        }
    }

    if cancel.is_some_and(|c| c.is_cancelled()) {
        return Err(depot_core::error::DepotError::Internal(
            "GC pass cancelled".to_string(),
        ));
    }

    // --- Artifact scan: build blob_id bloom filter + expire old artifacts ---

    // Use per-repo directory stats (same source as the dashboard) for BF sizing.
    // Skip proxy repos to avoid double-counting their members.
    let mut estimated_total = 0usize;
    for repo in &repos {
        if matches!(repo.kind, depot_core::store::kv::RepoKind::Proxy { .. }) {
            continue;
        }
        if let Ok(stats) = service::get_repo_stats(kv.as_ref(), repo).await {
            estimated_total += stats.artifact_count as usize;
        }
    }
    let estimated_total = estimated_total.max(1000); // minimum for BF sizing

    if let Some(tx) = progress_tx {
        tx.send_modify(|p| {
            p.phase = "Scanning artifacts".into();
            p.total_repos = repos.len();
            p.completed_repos = 0;
            p.total_blobs = estimated_total;
            p.checked_blobs = 0;
        });
    }

    // Create a template BF with the right dimensions.
    let template = Arc::new(Bloom::new_for_fp_rate(estimated_total, 0.01));
    let mut combined = bloom_empty_like(&template);
    let mut scanned_artifacts = 0u64;
    let mut expired_artifacts = 0u64;
    let now = chrono::Utc::now();

    // Shared atomic counter so shard tasks can report progress without
    // holding the watch sender.  A 1-second ticker pushes the value into
    // the progress channel so the UI updates smoothly.
    let live_scanned = Arc::new(AtomicU64::new(0));

    // Spawn a background ticker that pushes progress every second.
    let ticker_done = CancellationToken::new();
    if let Some(tx) = progress_tx {
        let live = live_scanned.clone();
        let done = ticker_done.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            interval.tick().await; // skip first immediate tick
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let val = live.load(Ordering::Relaxed) as usize;
                        tx.send_modify(|p| p.checked_blobs = val);
                    }
                    _ = done.cancelled() => break,
                }
            }
        });
    }

    // Scan all artifact partitions: build bloom filter + expire old artifacts.
    for repo in &repos {
        // Compute expiration cutoffs from repo config.
        let max_age_cutoff = repo
            .cleanup_max_age_days
            .map(|d| now - chrono::Duration::days(d as i64));
        let max_unaccessed_cutoff = repo
            .cleanup_max_unaccessed_days
            .map(|d| now - chrono::Duration::days(d as i64));

        let repo_name = repo.name.clone();
        let store_name = repo.store.clone();
        let mut js = JoinSet::new();
        for shard in 0..keys::NUM_SHARDS {
            let kv = kv.clone();
            let tmpl = template.clone();
            let repo_name = repo_name.clone();
            let store_name = store_name.clone();
            let cancel = cancel.cloned();
            let updater = updater.clone();
            let live_scanned = live_scanned.clone();
            js.spawn(async move {
                let pk = keys::artifact_shard_pk(&repo_name, shard);
                let mut local_bf = bloom_empty_like(&tmpl);
                let mut local_scanned = 0u64;
                let mut local_expired = 0u64;

                let mut cursor: Option<String> = None;
                loop {
                    if cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
                        return Err(depot_core::error::DepotError::Internal(
                            "GC pass cancelled".to_string(),
                        ));
                    }

                    let start = match &cursor {
                        Some(c) => c.as_str(),
                        None => "",
                    };
                    let result = kv
                        .scan_range(
                            keys::TABLE_ARTIFACTS,
                            Cow::Borrowed(pk.as_str()),
                            Cow::Borrowed(start),
                            None,
                            1000,
                        )
                        .await?;

                    let mut expired_entries: Vec<(&str, u64)> = Vec::new();
                    for (sk, value) in &result.items {
                        let record = match rmp_serde::from_slice::<
                            depot_core::store::kv::ArtifactRecord,
                        >(value)
                        {
                            Ok(r) => r,
                            Err(_) => continue,
                        };

                        let expired = !record.internal
                            && (max_age_cutoff
                                .map(|c| record.created_at < c)
                                .unwrap_or(false)
                                || max_unaccessed_cutoff
                                    .map(|c| record.last_accessed_at < c)
                                    .unwrap_or(false));

                        if expired {
                            expired_entries.push((sk, record.size));
                        } else {
                            if let Some(ref blob_id) = record.blob_id {
                                local_bf.set(blob_id.as_bytes());
                            }
                            local_scanned += 1;
                        }
                    }
                    if !expired_entries.is_empty() && !dry_run {
                        let del_keys: Vec<(&str, &str)> = expired_entries
                            .iter()
                            .map(|(sk, _)| (pk.as_str(), *sk))
                            .collect();
                        kv.delete_batch(keys::TABLE_ARTIFACTS, &del_keys).await?;
                        local_expired += expired_entries.len() as u64;
                        let mut batch_bytes = 0i64;
                        for &(sk, size) in &expired_entries {
                            updater
                                .dir_changed(&repo_name, sk, -1, -(size as i64))
                                .await;
                            batch_bytes += size as i64;
                        }
                        updater
                            .store_changed(
                                &store_name,
                                -(expired_entries.len() as i64),
                                -batch_bytes,
                            )
                            .await;
                    }

                    // Update the shared counter so the progress ticker picks it up.
                    live_scanned.fetch_add(result.items.len() as u64, Ordering::Relaxed);

                    if result.done {
                        break;
                    }
                    if let Some((last_sk, _)) = result.items.last() {
                        cursor = Some(format!("{}\0", last_sk));
                    } else {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Ok::<_, depot_core::error::DepotError>((local_bf, local_scanned, local_expired))
            });
        }
        while let Some(res) = js.join_next().await {
            let (local_bf, local_scanned, local_expired) = res??;
            bloom_union(&mut combined, &local_bf);
            scanned_artifacts += local_scanned;
            expired_artifacts += local_expired;
        }

        if let Some(tx) = progress_tx {
            tx.send_modify(|p| p.completed_repos += 1);
        }
    }

    // Stop the progress ticker and do a final update.
    ticker_done.cancel();
    if let Some(tx) = progress_tx {
        tx.send_modify(|p| p.checked_blobs = live_scanned.load(Ordering::Relaxed) as usize);
    }

    // Include artifacts expired in the Docker pre-phase.
    expired_artifacts += pre_expired;

    if cancel.is_some_and(|c| c.is_cancelled()) {
        return Err(depot_core::error::DepotError::Internal(
            "GC pass cancelled".to_string(),
        ));
    }

    // --- Sweep: clean KV records + walk filesystem per store ---
    //
    // For each store we do two things in a single logical pass:
    //
    // 1. Scan KV BlobRecords.  Records whose blob_id is absent from the BF
    //    are deleted immediately.  This is safe: deleting a KV record only
    //    prevents future dedup — it does not touch the blob file on disk.
    //
    // 2. Walk the blob store filesystem.  Files whose blob_id is absent from
    //    the BF and were already candidates from a previous pass are deleted.
    //    Newly-unreferenced files become candidates.  Candidates whose blob_id
    //    now appears in the BF (artifact created since last pass) are cleared.

    // Sum existing store stats to get an estimated total_bytes for progress.
    let mut estimated_total_bytes = 0u64;
    for sr in &store_records {
        if let Ok(Some(stats)) = service::get_store_stats(kv.as_ref(), &sr.name).await {
            estimated_total_bytes += stats.total_bytes;
        }
    }

    if let Some(tx) = progress_tx {
        tx.send_modify(|p| {
            p.phase = "Sweeping blob records".into();
            p.total_repos = store_records.len();
            p.completed_repos = 0;
            p.total_blobs = 0;
            p.checked_blobs = 0;
            p.bytes_read = 0;
            p.total_bytes = estimated_total_bytes;
        });
    }

    let mut scanned_blobs = 0u64;
    let mut deleted_dedup_refs = 0u64;
    let mut orphaned_blobs_deleted = 0u64;
    let mut orphaned_blob_bytes_deleted = 0u64;

    // Prune orphan candidates whose blob_id is now referenced (artifact
    // created between passes).
    state
        .orphan_candidates
        .retain(|(_s, blob_id), _| !combined.check(blob_id.as_bytes()));

    for store_rec in &store_records {
        // --- KV BlobRecord cleanup (parallel across shards) ---
        let store_name = &store_rec.name;
        let mut js = JoinSet::new();
        for shard in 0..keys::NUM_SHARDS {
            let kv = kv.clone();
            let store_name = store_name.clone();
            let bf_bits = combined.bitmap().to_vec();
            let bf_num_bits = combined.number_of_bits();
            let bf_num_hashes = combined.number_of_hash_functions();
            let bf_sip_keys = combined.sip_keys();
            let cancel = cancel.cloned();
            js.spawn(async move {
                let bf = Bloom::from_existing(&bf_bits, bf_num_bits, bf_num_hashes, bf_sip_keys);
                let pk = keys::blob_shard_pk(&store_name, shard);
                let mut local_scanned = 0u64;
                let mut local_scanned_bytes = 0u64;
                let mut local_deleted = 0u64;
                let mut local_live_count = 0u64;
                let mut local_live_bytes = 0u64;

                let mut cursor: Option<String> = None;
                loop {
                    if cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
                        return Err(depot_core::error::DepotError::Internal(
                            "GC pass cancelled".to_string(),
                        ));
                    }

                    let start = match &cursor {
                        Some(c) => c.as_str(),
                        None => "",
                    };
                    let result = kv
                        .scan_range(
                            keys::TABLE_BLOBS,
                            Cow::Borrowed(pk.as_str()),
                            Cow::Borrowed(start),
                            None,
                            1000,
                        )
                        .await?;

                    let mut del_sks: Vec<&str> = Vec::new();
                    for (sk, value) in &result.items {
                        local_scanned += 1;
                        let record: depot_core::store::kv::BlobRecord =
                            match rmp_serde::from_slice(value) {
                                Ok(r) => r,
                                Err(_) => continue,
                            };

                        local_scanned_bytes += record.size;
                        if !bf.check(record.blob_id.as_bytes()) {
                            del_sks.push(sk);
                        } else {
                            local_live_count += 1;
                            local_live_bytes += record.size;
                        }
                    }

                    if !del_sks.is_empty() && !dry_run {
                        let del_keys: Vec<(&str, &str)> =
                            del_sks.iter().map(|sk| (pk.as_str(), *sk)).collect();
                        kv.delete_batch(keys::TABLE_BLOBS, &del_keys).await?;
                        local_deleted += del_sks.len() as u64;
                    }

                    if result.done {
                        break;
                    }
                    if let Some((last_sk, _)) = result.items.last() {
                        cursor = Some(format!("{}\0", last_sk));
                    } else {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                Ok::<_, depot_core::error::DepotError>((
                    local_scanned,
                    local_scanned_bytes,
                    local_deleted,
                    local_live_count,
                    local_live_bytes,
                ))
            });
        }
        let mut blob_shard_results = Vec::new();
        while let Some(res) = js.join_next().await {
            blob_shard_results.push(res??);
        }

        let mut store_live_count = 0u64;
        let mut store_live_bytes = 0u64;
        let mut store_scanned_bytes = 0u64;
        for (local_scanned, local_scanned_bytes, local_deleted, live_count, live_bytes) in
            blob_shard_results
        {
            scanned_blobs += local_scanned;
            store_scanned_bytes += local_scanned_bytes;
            deleted_dedup_refs += local_deleted;
            store_live_count += live_count;
            store_live_bytes += live_bytes;
        }

        // Persist per-store stats.
        let stats = depot_core::store::kv::StoreStatsRecord {
            blob_count: store_live_count,
            total_bytes: store_live_bytes,
            updated_at: chrono::Utc::now(),
        };
        let _ = service::put_store_stats(kv.as_ref(), store_name, &stats).await;

        if let Some(tx) = progress_tx {
            let bytes = store_scanned_bytes;
            tx.send_modify(|p| {
                p.completed_repos += 1;
                p.checked_blobs = scanned_blobs as usize;
                p.bytes_read += bytes;
            });
        }

        // --- Orphan blob cleanup (filesystem or S3) ---
        if let Some(tx) = progress_tx {
            tx.send_modify(|p| {
                p.phase = format!("Walking blob store '{}'", store_rec.name);
            });
        }
        let blob_store = match stores.get(&store_rec.name).await {
            Some(s) => s,
            None => continue,
        };

        let store_name = store_rec.name.clone();

        // Stream every blob in the store via the BlobStore trait and collect
        // those whose blob_id doesn't hit the live-blob bloom filter.
        let mut orphan_candidates: Vec<(String, u64)> = Vec::new();
        match blob_store.scan_all_blobs().await {
            Ok(mut stream) => {
                while let Some(item) = stream.next().await {
                    match item {
                        Ok((blob_id, size)) => {
                            if !combined.check(blob_id.as_bytes()) {
                                orphan_candidates.push((blob_id, size));
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                store = %store_name,
                                error = %e,
                                "blob store scan yielded error during orphan sweep"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    store = %store_name,
                    error = %e,
                    "blob store scan failed during orphan sweep"
                );
            }
        }

        // Apply two-pass grace period to orphan candidates.
        let mut cap_warned = false;
        for (blob_id, size) in orphan_candidates {
            let key = (store_name.clone(), blob_id.clone());

            match state.orphan_candidates.get(&key) {
                Some(c) if current_pass - c.first_seen_pass >= 1 => {
                    // Candidate from a previous pass — delete the orphan.
                    if !dry_run {
                        let _ = blob_store.delete(&blob_id).await;
                    }
                    orphaned_blobs_deleted += 1;
                    orphaned_blob_bytes_deleted += size;
                    state.orphan_candidates.remove(&key);
                }
                Some(_) => {
                    // Still in grace period — keep as candidate.
                }
                None => {
                    if state.orphan_candidates.len() >= max_orphan_candidates {
                        if !cap_warned {
                            tracing::warn!(
                                max_orphan_candidates,
                                "orphan candidate cap reached; skipping new candidates this pass"
                            );
                            cap_warned = true;
                        }
                    } else {
                        // New candidate — will be eligible for deletion next pass.
                        state.orphan_candidates.insert(
                            key,
                            OrphanCandidate {
                                first_seen_pass: current_pass,
                            },
                        );
                    }
                }
            }
        }
    }

    Ok(GcStats {
        drained_repos,
        cleaned_upload_sessions,
        scanned_artifacts,
        expired_artifacts,
        docker_deleted,
        scanned_blobs,
        deleted_dedup_refs,
        orphaned_blobs_deleted,
        orphaned_blob_bytes_deleted,
    })
}
