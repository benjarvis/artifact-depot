// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Combined atime + dir-entry + store-stats background update worker.
//!
//! Artifact read paths send `Atime` events for deferred last_accessed_at updates.
//! Artifact write paths send `DirDelta` and `StoreDelta` events via bounded mpsc
//! channels. The worker drains ALL pending dir-delta events and coalesces them
//! for maximum batching, then applies the coalesced batch in parallel via
//! `buffer_unordered`. Each delta uses `put_if_version` / `delete_if_version`
//! for optimistic concurrency.
//!
//! Draining all pending events before applying maximises coalescing — under
//! heavy ingestion, thousands of events for the same directory collapse into a
//! single delta. Parent propagation deltas feed back into a local buffer and
//! are coalesced with the next batch. Store deltas are coalesced by store name
//! and flushed once per second via versioned RMW.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use metrics::histogram;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use futures::stream::{self, StreamExt, TryStreamExt};

#[cfg(test)]
use depot_core::update::UpdateSender;
use depot_core::update::{parent_dir, AtimeEvent, DirDelta, StoreDelta, UpdateReceiver};

use crate::server::infra::task::{TaskId, TaskManager, TaskProgress};
use depot_core::service;
use depot_core::service::BG_SHARD_CONCURRENCY;
use depot_core::store::keys;
use depot_core::store::kv::RepoKind;
use depot_core::store::kv::{ArtifactRecord, KvStore, StoreStatsRecord, TreeEntry, TreeEntryKind};

// --- Worker loop ---

/// Batch size for flushing atime updates.
const ATIME_FLUSH_BATCH: usize = 50;

/// Run the combined update worker loop.
///
/// Dir deltas are drained fully and coalesced for maximum batching under
/// heavy ingestion, then applied in parallel via `buffer_unordered`.
/// Parent propagation deltas feed into a local buffer and are coalesced
/// with the next batch, preserving sibling aggregation.
///
/// Atime and store-stats processing remains inline (low volume).
pub async fn run_update_worker(
    receiver: UpdateReceiver,
    kv: Arc<dyn KvStore>,
    cancel: CancellationToken,
) {
    let mut dir_rx = receiver.dir_rx;
    let mut atime_rx = receiver.atime_rx;
    let mut store_rx = receiver.store_rx;

    // Stashed events from select! wake-ups (recv consumes one item).
    let mut stashed_dir: Option<DirDelta> = None;
    let mut stashed_atime: Option<AtimeEvent> = None;
    let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    stats_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Local buffer for parent-propagation deltas.
    let mut propagation_buf: DirDeltaMap = HashMap::new();
    // Pending store deltas that failed to flush (retried next cycle).
    let mut pending_store_deltas: HashMap<String, (i64, i64)> = HashMap::new();

    loop {
        // Process all available work.
        let mut did_work = true;
        while did_work {
            did_work = false;

            // Dir deltas — drain all available, merge with propagation buffer.
            let mut coalesced = drain_dir_deltas(&mut dir_rx);
            if let Some(ev) = stashed_dir.take() {
                merge_dir_delta(&mut coalesced, ev);
            }
            for (_, delta) in propagation_buf.drain() {
                merge_dir_delta(&mut coalesced, delta);
            }
            if !coalesced.is_empty() {
                did_work = true;
                let deltas: Vec<DirDelta> = coalesced.into_values().collect();
                let batch_size = deltas.len();
                histogram!("dir_flush_batch_size").record(batch_size as f64);
                let batch_result = async { apply_dir_batch(&kv, deltas).await }
                    .instrument(tracing::debug_span!("update_batch", batch_size))
                    .await;
                for parent in batch_result.parents {
                    merge_dir_delta(&mut propagation_buf, parent);
                }
                // Re-queue failed deltas — they'll be coalesced with the
                // next batch, reducing contention on hot directories.
                for retry in batch_result.retries {
                    merge_dir_delta(&mut propagation_buf, retry);
                }
            }

            // Atime events — drain all available (including stashed) and flush.
            let mut atime_events = drain_atime(&mut atime_rx);
            if let Some(ev) = stashed_atime.take() {
                atime_events
                    .entry((ev.repo, ev.path))
                    .and_modify(|ts| {
                        if ev.timestamp > *ts {
                            *ts = ev.timestamp;
                        }
                    })
                    .or_insert(ev.timestamp);
            }
            if !atime_events.is_empty() {
                did_work = true;
                flush_atime(kv.as_ref(), atime_events).await;
            }

            // Store stats — drain and flush inline so they aren't starved
            // by the biased select when dir events arrive continuously.
            let store_deltas = drain_store_deltas(&mut store_rx, &mut pending_store_deltas);
            if !store_deltas.is_empty() {
                did_work = true;
                flush_store_stats(kv.as_ref(), store_deltas, &mut pending_store_deltas).await;
            }
        }

        // Sleep until new work arrives.
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::info!("update worker shutting down, final flush");
                // Final drain.
                let mut coalesced = drain_dir_deltas(&mut dir_rx);
                if let Some(ev) = stashed_dir.take() {
                    merge_dir_delta(&mut coalesced, ev);
                }
                for (_, delta) in propagation_buf.drain() {
                    merge_dir_delta(&mut coalesced, delta);
                }
                // Apply remaining deltas, propagating upward until done.
                loop {
                    if coalesced.is_empty() {
                        break;
                    }
                    let mut next_coalesced: DirDeltaMap = HashMap::new();
                    for delta in coalesced.into_values() {
                        let dir_path = delta.dir_path.clone();
                        match apply_dir_delta(kv.as_ref(), &delta).await {
                            Ok(true) if !dir_path.is_empty() => {
                                merge_dir_delta(
                                    &mut next_coalesced,
                                    make_parent_delta(&delta),
                                );
                            }
                            _ => {}
                        }
                    }
                    coalesced = next_coalesced;
                }
                let atime_events = drain_atime(&mut atime_rx);
                if !atime_events.is_empty() {
                    flush_atime(kv.as_ref(), atime_events).await;
                }
                let store_deltas = drain_store_deltas(&mut store_rx, &mut pending_store_deltas);
                flush_store_stats(kv.as_ref(), store_deltas, &mut pending_store_deltas).await;
                return;
            }
            ev = dir_rx.recv() => {
                stashed_dir = ev;
            }
            ev = atime_rx.recv() => {
                stashed_atime = ev;
            }
            _ = stats_interval.tick() => {
                let store_deltas = drain_store_deltas(&mut store_rx, &mut pending_store_deltas);
                flush_store_stats(kv.as_ref(), store_deltas, &mut pending_store_deltas).await;
            }
        }
    }
}

/// Apply a batch of coalesced dir deltas concurrently using `buffer_unordered`.
/// Returns parent propagation deltas for entries that changed.
/// Result of applying a batch of dir deltas: parent propagation deltas
/// and any deltas that failed all retries (to be re-queued).
struct DirBatchResult {
    parents: Vec<DirDelta>,
    retries: Vec<DirDelta>,
}

async fn apply_dir_batch(kv: &Arc<dyn KvStore>, deltas: Vec<DirDelta>) -> DirBatchResult {
    enum Outcome {
        Parent(DirDelta),
        Retry(DirDelta),
    }

    let outcomes: Vec<Option<Outcome>> = stream::iter(deltas)
        .map(|delta| {
            let kv = Arc::clone(kv);
            async move {
                let dir_path = delta.dir_path.clone();
                match apply_dir_delta(kv.as_ref(), &delta).await {
                    Ok(true) if !dir_path.is_empty() => {
                        Some(Outcome::Parent(make_parent_delta(&delta)))
                    }
                    Ok(true) => None, // root applied successfully, no parent
                    Ok(false) => {
                        // Retries exhausted — re-queue so it gets coalesced
                        // with the next batch and retried with less contention.
                        Some(Outcome::Retry(delta))
                    }
                    Err(e) => {
                        tracing::debug!(dir_path, error = %e, "dir delta apply failed");
                        Some(Outcome::Retry(delta))
                    }
                }
            }
        })
        .buffer_unordered(BG_SHARD_CONCURRENCY)
        .collect()
        .await;

    let mut result = DirBatchResult {
        parents: Vec::new(),
        retries: Vec::new(),
    };
    for outcome in outcomes.into_iter().flatten() {
        match outcome {
            Outcome::Parent(d) => result.parents.push(d),
            Outcome::Retry(d) => result.retries.push(d),
        }
    }
    result
}

/// Build a parent propagation delta from a successfully applied child delta.
fn make_parent_delta(delta: &DirDelta) -> DirDelta {
    let parent = parent_dir(&delta.dir_path).to_string();
    let parent_pk = keys::sharded_pk(&delta.repo, keys::tree_parent(&parent));
    DirDelta {
        repo: delta.repo.clone(),
        pk: parent_pk,
        dir_path: parent,
        count_delta: delta.count_delta,
        bytes_delta: delta.bytes_delta,
        timestamp: delta.timestamp,
    }
}

type DirDeltaMap = HashMap<(String, String), DirDelta>;

/// Drain all available dir delta events from the channel, coalescing by (pk, dir_path).
fn drain_dir_deltas(rx: &mut mpsc::Receiver<DirDelta>) -> DirDeltaMap {
    let mut map: DirDeltaMap = HashMap::new();
    while let Ok(delta) = rx.try_recv() {
        merge_dir_delta(&mut map, delta);
    }
    map
}

/// Merge a single dir delta into a coalesced map.
fn merge_dir_delta(map: &mut DirDeltaMap, delta: DirDelta) {
    let key = (delta.pk.clone(), delta.dir_path.clone());
    map.entry(key)
        .and_modify(|existing| {
            existing.count_delta += delta.count_delta;
            existing.bytes_delta += delta.bytes_delta;
            if delta.timestamp > existing.timestamp {
                existing.timestamp = delta.timestamp;
            }
        })
        .or_insert(delta);
}

/// Drain all available store delta events from the channel, merging with any
/// pending (previously failed) deltas.
fn drain_store_deltas(
    rx: &mut mpsc::Receiver<StoreDelta>,
    pending: &mut HashMap<String, (i64, i64)>,
) -> Vec<(String, i64, i64)> {
    while let Ok(delta) = rx.try_recv() {
        let entry = pending.entry(delta.store).or_insert((0, 0));
        entry.0 += delta.count_delta;
        entry.1 += delta.bytes_delta;
    }
    pending
        .drain()
        .filter(|(_, (c, b))| *c != 0 || *b != 0)
        .map(|(store, (c, b))| (store, c, b))
        .collect()
}

type AtimeMap = HashMap<(String, String), DateTime<Utc>>;

/// Drain all available atime events from the channel, coalescing by (repo, path).
fn drain_atime(rx: &mut mpsc::Receiver<AtimeEvent>) -> AtimeMap {
    let mut map: AtimeMap = HashMap::new();
    while let Ok(event) = rx.try_recv() {
        map.entry((event.repo, event.path))
            .and_modify(|ts| {
                if event.timestamp > *ts {
                    *ts = event.timestamp;
                }
            })
            .or_insert(event.timestamp);
    }
    map
}

// --- Atime flush ---

#[tracing::instrument(level = "debug", skip_all, fields(count = pending.len()))]
async fn flush_atime(kv: &dyn KvStore, pending: AtimeMap) {
    let items: Vec<((String, String), DateTime<Utc>)> = pending.into_iter().collect();
    histogram!("atime_flush_batch_size").record(items.len() as f64);
    for chunk in items.chunks(ATIME_FLUSH_BATCH) {
        for ((repo, path), ts) in chunk {
            if let Err(e) = service::touch_artifact(kv, repo, path, *ts).await {
                tracing::debug!(repo, path, error = %e, "atime touch failed");
            }
        }
    }
}

// --- Store stats flush ---

/// Maximum retries for version conflicts when flushing store stats.
const STORE_STATS_FLUSH_MAX_RETRIES: usize = 3;

#[tracing::instrument(level = "debug", name = "flush_store_stats", skip_all)]
async fn flush_store_stats(
    kv: &dyn KvStore,
    deltas: Vec<(String, i64, i64)>,
    pending: &mut HashMap<String, (i64, i64)>,
) {
    for (store_name, count_delta, bytes_delta) in deltas {
        let mut applied = false;
        for _attempt in 0..STORE_STATS_FLUSH_MAX_RETRIES {
            let (table, pk, sk) = keys::store_stats_key(&store_name);
            let versioned = kv.get_versioned(table, pk, sk).await;

            let (existing, version): (Option<StoreStatsRecord>, Option<u64>) = match versioned {
                Ok(Some((bytes, v))) => match rmp_serde::from_slice(&bytes) {
                    Ok(rec) => (Some(rec), Some(v)),
                    Err(_) => (None, None),
                },
                Ok(None) => (None, None),
                Err(e) => {
                    tracing::debug!(store = store_name, error = %e, "store stats read failed");
                    break;
                }
            };

            let (old_count, old_bytes) = match &existing {
                Some(s) => (s.blob_count as i64, s.total_bytes as i64),
                None => (0, 0),
            };
            let new_count = (old_count + count_delta).max(0) as u64;
            let new_bytes_val = (old_bytes + bytes_delta).max(0) as u64;
            let new_stats = StoreStatsRecord {
                blob_count: new_count,
                total_bytes: new_bytes_val,
                updated_at: Utc::now(),
            };
            let Ok(new_bytes) = rmp_serde::to_vec(&new_stats) else {
                break;
            };
            let (table, pk, sk) = keys::store_stats_key(&store_name);
            match kv.put_if_version(table, pk, sk, &new_bytes, version).await {
                Ok(true) => {
                    applied = true;
                    break;
                }
                Ok(false) => continue, // version conflict, retry
                Err(e) => {
                    tracing::debug!(store = store_name, error = %e, "store stats write failed");
                    break;
                }
            }
        }
        if !applied {
            // Re-add deltas so they are retried on the next flush cycle.
            let entry = pending.entry(store_name).or_insert((0, 0));
            entry.0 += count_delta;
            entry.1 += bytes_delta;
        }
    }
}

// --- Dir delta application ---

/// Apply a count/bytes delta to a tree Dir entry using optimistic concurrency.
///
/// Returns `true` if the entry changed (created, updated, or deleted).
/// Returns `false` on version conflict — the caller should re-queue the
/// delta so it gets coalesced with the next batch, naturally reducing
/// contention on hot directories.
async fn apply_dir_delta(kv: &dyn KvStore, delta: &DirDelta) -> depot_core::error::Result<bool> {
    let sk = keys::tree_sort_key(&delta.dir_path);
    {
        let versioned = kv
            .get_versioned(
                keys::TABLE_DIR_ENTRIES,
                Cow::Borrowed(&delta.pk),
                Cow::Borrowed(&sk),
            )
            .await?;

        // Parse existing entry — must be a Dir variant (or absent).
        let (existing_count, existing_bytes, existing_modified, existing_accessed, version) =
            match versioned {
                Some((bytes, v)) => {
                    let te: TreeEntry = rmp_serde::from_slice(&bytes)?;
                    match te.kind {
                        TreeEntryKind::Dir {
                            artifact_count,
                            total_bytes,
                        } => (
                            artifact_count,
                            total_bytes,
                            te.last_modified_at,
                            te.last_accessed_at,
                            Some(v),
                        ),
                        TreeEntryKind::File { .. } => {
                            // Unexpected file entry where we expected a dir — skip.
                            return Ok(false);
                        }
                    }
                }
                None => (
                    0,
                    0,
                    DateTime::<Utc>::UNIX_EPOCH,
                    DateTime::<Utc>::UNIX_EPOCH,
                    None,
                ),
            };

        let is_new = version.is_none();

        let new_count = if is_new {
            if delta.count_delta <= 0 {
                return Ok(false);
            }
            delta.count_delta as u64
        } else {
            (existing_count as i64 + delta.count_delta).max(0) as u64
        };

        let new_bytes = if is_new {
            delta.bytes_delta.max(0) as u64
        } else {
            (existing_bytes as i64 + delta.bytes_delta).max(0) as u64
        };

        let new_modified = if is_new {
            delta.timestamp
        } else {
            existing_modified.max(delta.timestamp)
        };

        let new_accessed = if is_new {
            delta.timestamp
        } else {
            existing_accessed.max(delta.timestamp)
        };

        if new_count == 0 {
            if let Some(v) = version {
                if kv
                    .delete_if_version(
                        keys::TABLE_DIR_ENTRIES,
                        Cow::Borrowed(&delta.pk),
                        Cow::Borrowed(&sk),
                        v,
                    )
                    .await?
                {
                    return Ok(true);
                }
                return Ok(false); // version conflict — caller will re-queue
            }
            return Ok(false);
        }

        let new_entry = TreeEntry {
            kind: TreeEntryKind::Dir {
                artifact_count: new_count,
                total_bytes: new_bytes,
            },
            last_modified_at: new_modified,
            last_accessed_at: new_accessed,
        };
        let new_value = rmp_serde::to_vec(&new_entry)?;

        if kv
            .put_if_version(
                keys::TABLE_DIR_ENTRIES,
                Cow::Borrowed(&delta.pk),
                Cow::Borrowed(&sk),
                &new_value,
                version,
            )
            .await?
        {
            return Ok(true);
        }

        Ok(false) // version conflict — caller will re-queue
    }
}

// --- Full scan (used by rebuild-dir-entries task) ---

/// Recompute all dir entries for all repos across all shards, and update store stats.
/// Returns (repos_count, entries_updated, stores_updated).
#[tracing::instrument(level = "debug", name = "full_scan", skip_all)]
pub async fn full_scan(
    kv: &dyn KvStore,
    task_manager: &TaskManager,
    task_id: TaskId,
    progress_tx: &watch::Sender<TaskProgress>,
) -> depot_core::error::Result<(usize, u64, usize)> {
    let repos = service::list_repos(kv).await?;
    let start = std::time::Instant::now();
    let mut total_updated = 0u64;

    // Estimate total artifacts/bytes from existing store stats.
    let store_records = service::list_stores(kv).await.unwrap_or_default();
    let mut estimated_total_blobs = 0u64;
    let mut estimated_total_bytes = 0u64;
    for sr in &store_records {
        if let Ok(Some(stats)) = service::get_store_stats(kv, &sr.name).await {
            estimated_total_blobs += stats.blob_count;
            estimated_total_bytes += stats.total_bytes;
        }
    }

    // Estimate total artifacts from root dir entries (same as the dashboard).
    // Skip proxy repos to avoid double-counting members.
    let mut total_artifacts: usize = 0;
    for repo_config in &repos {
        if matches!(repo_config.kind, RepoKind::Proxy { .. }) {
            continue;
        }
        total_artifacts += service::get_repo_stats(kv, repo_config)
            .await
            .map(|s| s.artifact_count as usize)
            .unwrap_or(0);
    }

    progress_tx.send_modify(|p| {
        p.total_repos = repos.len();
        p.total_blobs = estimated_total_blobs as usize;
        p.total_bytes = estimated_total_bytes;
        p.total_artifacts = total_artifacts;
        p.phase = "Rebuilding tree".to_string();
    });
    task_manager
        .append_log(
            task_id,
            format!("Found {} repositories to scan", repos.len()),
        )
        .await;
    task_manager
        .update_summary(task_id, format!("Scanning 0/{} repos", repos.len()))
        .await;

    // Accumulate per-store totals: store_name -> (artifact_count, total_bytes)
    let mut store_totals: HashMap<String, (u64, u64)> = HashMap::new();

    for (i, repo) in repos.iter().enumerate() {
        progress_tx.send_modify(|p| {
            p.phase = format!("Scanning artifacts for {}", repo.name);
        });
        let repo_start = std::time::Instant::now();
        let (repo_entries, repo_artifacts, repo_bytes) =
            full_scan_repo(kv, &repo.name, progress_tx).await?;
        total_updated += repo_entries;

        let store_entry = store_totals.entry(repo.store.clone()).or_insert((0, 0));
        store_entry.0 += repo_artifacts;
        store_entry.1 += repo_bytes;

        let repo_elapsed = repo_start.elapsed();
        task_manager
            .append_log(
                task_id,
                format!(
                    "[{}/{}] {} -- {} artifacts, {} bytes, {} entries updated ({}ms)",
                    i + 1,
                    repos.len(),
                    repo.name,
                    repo_artifacts,
                    repo_bytes,
                    repo_entries,
                    repo_elapsed.as_millis(),
                ),
            )
            .await;

        progress_tx.send_modify(|p| {
            p.completed_repos += 1;
            p.checked_blobs += repo_artifacts as usize;
            p.bytes_read += repo_bytes;
        });
        task_manager
            .update_summary(
                task_id,
                format!(
                    "Scanning {}/{} repos ({} entries updated so far)",
                    i + 1,
                    repos.len(),
                    total_updated,
                ),
            )
            .await;
    }

    // Write store stats.
    for (store_name, (artifact_count, total_bytes)) in &store_totals {
        let stats = StoreStatsRecord {
            blob_count: *artifact_count,
            total_bytes: *total_bytes,
            updated_at: Utc::now(),
        };
        let _ = service::put_store_stats(kv, store_name, &stats).await;
        task_manager
            .append_log(
                task_id,
                format!(
                    "Store '{}': {} artifacts, {} bytes",
                    store_name, artifact_count, total_bytes,
                ),
            )
            .await;
    }

    let elapsed = start.elapsed();
    tracing::info!(
        repos = repos.len(),
        updated = total_updated,
        stores = store_totals.len(),
        elapsed_ns = elapsed.as_nanos() as u64,
        "dir entry full scan complete"
    );
    Ok((repos.len(), total_updated, store_totals.len()))
}

/// Per-directory stats: (artifact_count, total_bytes, last_modified, last_accessed).
type DirStats = HashMap<String, (u64, u64, DateTime<Utc>, DateTime<Utc>)>;

/// Result of scanning one artifact shard: dir stats + file tree entries.
struct ShardScanResult {
    dir_stats: DirStats,
    /// File tree entries keyed by artifact path.
    file_entries: Vec<(String, TreeEntry)>,
}

/// Full scan of one repo: rebuild the entire tree from artifacts.
/// Returns (entries_updated, repo_artifact_count, repo_total_bytes).
async fn full_scan_repo(
    kv: &dyn KvStore,
    repo: &str,
    progress_tx: &watch::Sender<TaskProgress>,
) -> depot_core::error::Result<(u64, u64, u64)> {
    // 1. Scan all artifact shards in parallel, merging and updating progress
    //    as each shard completes so the UI sees continuous movement.
    let mut dir_stats: DirStats = HashMap::new();
    let mut file_entries: Vec<(String, TreeEntry)> = Vec::new();

    let mut shard_stream = stream::iter(0..keys::NUM_SHARDS)
        .map(|shard| async move { scan_artifact_shard(kv, repo, shard, progress_tx).await })
        .buffer_unordered(BG_SHARD_CONCURRENCY);

    while let Some(result) = shard_stream.try_next().await? {
        for (dir_path, (count, bytes, modified, accessed)) in result.dir_stats {
            let entry = dir_stats.entry(dir_path).or_insert((
                0,
                0,
                DateTime::<Utc>::UNIX_EPOCH,
                DateTime::<Utc>::UNIX_EPOCH,
            ));
            entry.0 += count;
            entry.1 += bytes;
            if modified > entry.2 {
                entry.2 = modified;
            }
            if accessed > entry.3 {
                entry.3 = accessed;
            }
        }
        file_entries.extend(result.file_entries);
    }

    // 3. Build the set of expected tree entries keyed by (pk, sk) -> serialized value.
    let mut expected: HashMap<(String, String), Vec<u8>> = HashMap::new();

    // Dir entries.
    for (dir_path, &(count, bytes, modified, accessed)) in &dir_stats {
        let te = TreeEntry {
            kind: TreeEntryKind::Dir {
                artifact_count: count,
                total_bytes: bytes,
            },
            last_modified_at: modified,
            last_accessed_at: accessed,
        };
        let te_bytes = rmp_serde::to_vec(&te)?;
        let pk = keys::sharded_pk(repo, keys::tree_parent(dir_path));
        let sk = keys::tree_sort_key(dir_path);
        expected.insert((pk, sk), te_bytes);
    }

    // File entries.
    for (path, te) in &file_entries {
        let te_bytes = rmp_serde::to_vec(te)?;
        let pk = keys::sharded_pk(repo, keys::tree_parent(path));
        let sk = keys::tree_sort_key(path);
        expected.insert((pk, sk), te_bytes);
    }

    // 4. Scan existing tree entries and compute a diff against expected.
    //    Only delete stale entries and write new/changed ones, using batch
    //    APIs to amortise KV round-trips on large repos.
    progress_tx.send_modify(|p| {
        p.phase = format!("Scanning tree entries for {}", repo);
    });
    let mut updates = 0u64;
    let existing_shards: Vec<Vec<(String, String, Vec<u8>)>> = stream::iter(0..keys::NUM_SHARDS)
        .map(|shard| async move { scan_tree_shard(kv, repo, shard).await })
        .buffer_unordered(BG_SHARD_CONCURRENCY)
        .try_collect()
        .await?;

    // Build a set of existing (pk, sk) -> value for O(1) lookup.
    let mut existing: HashMap<(String, String), Vec<u8>> = HashMap::new();
    for shard_entries in existing_shards {
        for (pk, sk, value) in shard_entries {
            existing.insert((pk, sk), value);
        }
    }

    // Collect stale keys to delete (in existing but not in expected).
    let to_delete: Vec<(&str, &str)> = existing
        .keys()
        .filter(|k| !expected.contains_key(*k))
        .map(|(pk, sk)| (pk.as_str(), sk.as_str()))
        .collect();

    // Collect entries to write (new or changed value).
    let to_write: Vec<(&str, &str, &[u8])> = expected
        .iter()
        .filter(|(k, v)| existing.get(*k) != Some(*v))
        .map(|((pk, sk), v)| (pk.as_str(), sk.as_str(), v.as_slice()))
        .collect();

    // 5. Apply deletes and writes in batches.
    const BATCH_SIZE: usize = 500;
    let total_writes = to_delete.len() + to_write.len();

    if total_writes > 0 {
        progress_tx.send_modify(|p| {
            p.phase = format!(
                "Writing entries for {} ({} deletes, {} writes)",
                repo,
                to_delete.len(),
                to_write.len(),
            );
        });
    }

    for chunk in to_delete.chunks(BATCH_SIZE) {
        kv.delete_batch(keys::TABLE_DIR_ENTRIES, chunk).await?;
        updates += chunk.len() as u64;
        progress_tx.send_modify(|p| {
            p.checked_blobs += chunk.len();
        });
    }

    for chunk in to_write.chunks(BATCH_SIZE) {
        kv.put_batch(keys::TABLE_DIR_ENTRIES, chunk).await?;
        updates += chunk.len() as u64;
        progress_tx.send_modify(|p| {
            p.checked_blobs += chunk.len();
        });
    }

    let (repo_count, repo_bytes) = dir_stats
        .get("")
        .map(|&(count, bytes, _, _)| (count, bytes))
        .unwrap_or((0, 0));
    Ok((updates, repo_count, repo_bytes))
}

/// Scan all artifacts in one shard, returning dir stats and file tree entries.
/// Updates `progress_tx.checked_artifacts` at most once per 100ms so the UI
/// stays responsive even for very large shards.
async fn scan_artifact_shard(
    kv: &dyn KvStore,
    repo: &str,
    shard: u16,
    progress_tx: &watch::Sender<TaskProgress>,
) -> depot_core::error::Result<ShardScanResult> {
    use std::time::{Duration, Instant};

    let pk = keys::artifact_shard_pk(repo, shard);
    let mut dir_stats: DirStats = HashMap::new();
    let mut file_entries: Vec<(String, TreeEntry)> = Vec::new();
    let mut last_progress = Instant::now();
    let mut unreported = 0usize;
    const PROGRESS_INTERVAL: Duration = Duration::from_millis(100);

    let mut cursor: Option<String> = None;
    loop {
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

        for (raw_sk, value) in &result.items {
            let record: ArtifactRecord = rmp_serde::from_slice(value)?;
            let sk = keys::normalize_path(raw_sk);

            // Collect file tree entry.
            file_entries.push((
                sk.to_string(),
                TreeEntry {
                    kind: TreeEntryKind::File {
                        size: record.size,
                        content_type: record.content_type.clone(),
                        artifact_kind: Box::new(record.kind.clone()),
                        blob_id: record.blob_id.clone(),
                        content_hash: record.content_hash.clone(),
                        created_at: record.created_at,
                    },
                    last_modified_at: record.updated_at,
                    last_accessed_at: record.last_accessed_at,
                },
            ));

            // Accumulate dir stats for each ancestor.
            for dir in ancestor_dirs(&sk) {
                let entry = dir_stats.entry(dir.to_string()).or_insert((
                    0,
                    0,
                    DateTime::<Utc>::UNIX_EPOCH,
                    DateTime::<Utc>::UNIX_EPOCH,
                ));
                entry.0 += 1;
                entry.1 += record.size;
                if record.updated_at > entry.2 {
                    entry.2 = record.updated_at;
                }
                if record.last_accessed_at > entry.3 {
                    entry.3 = record.last_accessed_at;
                }
            }
        }

        unreported += result.items.len();
        if unreported > 0 && last_progress.elapsed() >= PROGRESS_INTERVAL {
            let n = unreported;
            unreported = 0;
            last_progress = Instant::now();
            progress_tx.send_modify(|p| p.checked_artifacts += n);
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

    // Flush any remaining unreported items.
    if unreported > 0 {
        progress_tx.send_modify(|p| p.checked_artifacts += unreported);
    }

    Ok(ShardScanResult {
        dir_stats,
        file_entries,
    })
}

/// Scan all tree entries in one shard of a repo, returning (pk, sk, value) triples.
async fn scan_tree_shard(
    kv: &dyn KvStore,
    repo: &str,
    shard: u16,
) -> depot_core::error::Result<Vec<(String, String, Vec<u8>)>> {
    let tree_pk = keys::tree_shard_pk(repo, shard);
    let mut entries = Vec::new();
    let mut scan_cursor: Option<String> = None;
    loop {
        let start = match &scan_cursor {
            Some(c) => c.as_str(),
            None => "",
        };
        let result = kv
            .scan_range(
                keys::TABLE_DIR_ENTRIES,
                Cow::Borrowed(tree_pk.as_str()),
                Cow::Borrowed(start),
                None,
                1000,
            )
            .await?;

        for (sk, value) in &result.items {
            entries.push((tree_pk.clone(), sk.clone(), value.clone()));
        }

        if result.done {
            break;
        }
        if let Some((last_sk, _)) = result.items.last() {
            scan_cursor = Some(format!("{}\0", last_sk));
        } else {
            break;
        }
    }

    Ok(entries)
}

/// Return all ancestor directory paths for an artifact path (used by full_scan).
/// For `"a/b/c/file.txt"` returns `["a/b/c", "a/b", "a", ""]`.
fn ancestor_dirs(path: &str) -> Vec<&str> {
    let mut dirs = Vec::new();
    let mut remaining = path;
    while let Some(pos) = remaining.rfind('/') {
        remaining = remaining.split_at(pos).0;
        dirs.push(remaining);
    }
    // Always include root
    dirs.push("");
    dirs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_dir_nested() {
        assert_eq!(parent_dir("a/b/c/file.txt"), "a/b/c");
    }

    #[test]
    fn parent_dir_single_component() {
        assert_eq!(parent_dir("file.txt"), "");
    }

    #[test]
    fn parent_dir_two_components() {
        assert_eq!(parent_dir("dir/file.txt"), "dir");
    }

    #[test]
    fn ancestor_dirs_basic() {
        let dirs = ancestor_dirs("a/b/c/file.txt");
        assert_eq!(dirs, vec!["a/b/c", "a/b", "a", ""]);
    }

    #[test]
    fn ancestor_dirs_single_component() {
        let dirs = ancestor_dirs("file.txt");
        assert_eq!(dirs, vec![""]);
    }

    // --- Helper to build a DirDelta ---

    fn make_delta(repo: &str, dir_path: &str, count: i64, bytes: i64) -> DirDelta {
        let pk = keys::sharded_pk(repo, keys::tree_parent(dir_path));
        DirDelta {
            repo: repo.to_string(),
            pk,
            dir_path: dir_path.to_string(),
            count_delta: count,
            bytes_delta: bytes,
            timestamp: Utc::now(),
        }
    }

    // --- Dir delta coalescing tests ---

    #[test]
    fn dir_delta_coalescing() {
        let mut map: DirDeltaMap = HashMap::new();
        merge_dir_delta(&mut map, make_delta("repo", "a/b", 1, 100));
        merge_dir_delta(&mut map, make_delta("repo", "a/b", 1, 200)); // merge
        merge_dir_delta(&mut map, make_delta("repo", "a", 1, 50));
        assert_eq!(map.len(), 2); // only 2 unique keys

        let d_ab = &make_delta("repo", "a/b", 0, 0);
        let key = (d_ab.pk.clone(), d_ab.dir_path.clone());
        let item = &map[&key];
        assert_eq!(item.count_delta, 2); // 1 + 1
        assert_eq!(item.bytes_delta, 300); // 100 + 200
    }

    #[tokio::test]
    async fn dir_channel_drain_coalesces() {
        let (tx, mut rx) = mpsc::channel(100);
        tx.send(make_delta("repo", "a/b", 1, 100)).await.unwrap();
        tx.send(make_delta("repo", "a/b", 1, 200)).await.unwrap();
        tx.send(make_delta("repo", "a", 1, 50)).await.unwrap();

        let coalesced = drain_dir_deltas(&mut rx);
        assert_eq!(coalesced.len(), 2);

        let d_ab = &make_delta("repo", "a/b", 0, 0);
        let key = (d_ab.pk.clone(), d_ab.dir_path.clone());
        let item = &coalesced[&key];
        assert_eq!(item.count_delta, 2);
        assert_eq!(item.bytes_delta, 300);
    }

    async fn test_kv() -> (Arc<dyn KvStore>, tempfile::TempDir) {
        let dir = crate::server::infra::test_support::test_tempdir();
        let kv: Arc<dyn KvStore> = Arc::new(
            depot_kv_redb::sharded_redb::ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );
        (kv, dir)
    }

    /// Write a test artifact record to both the artifact table and the browse tree.
    async fn write_test_artifact(kv: &dyn KvStore, repo: &str, path: &str, size: u64) {
        let now = Utc::now();
        let pk = keys::sharded_pk(repo, path);
        let record = depot_core::store::kv::ArtifactRecord {
            schema_version: depot_core::store::kv::CURRENT_RECORD_VERSION,
            id: String::new(),
            size,
            content_type: "text/plain".to_string(),
            created_at: now,
            updated_at: now,
            last_accessed_at: now,
            path: path.to_string(),
            blob_id: None,
            content_hash: None,
            etag: None,
            kind: depot_core::store::kv::ArtifactKind::Raw,
            internal: false,
        };
        let data = rmp_serde::to_vec(&record).unwrap();
        kv.put(
            keys::TABLE_ARTIFACTS,
            Cow::Borrowed(pk.as_str()),
            Cow::Borrowed(path),
            &data,
        )
        .await
        .unwrap();

        // Also write the tree file entry for browse/atime.
        let te = TreeEntry {
            kind: TreeEntryKind::File {
                size,
                content_type: "text/plain".to_string(),
                artifact_kind: Box::new(depot_core::store::kv::ArtifactKind::Raw),
                blob_id: None,
                content_hash: None,
                created_at: now,
            },
            last_modified_at: now,
            last_accessed_at: now,
        };
        let te_data = rmp_serde::to_vec(&te).unwrap();
        let (_, te_pk, te_sk) = keys::tree_entry_key(repo, path);
        kv.put(keys::TABLE_DIR_ENTRIES, te_pk, te_sk, &te_data)
            .await
            .unwrap();
    }

    /// Read a tree Dir entry back from KV and return (artifact_count, total_bytes).
    async fn read_dir_entry(kv: &dyn KvStore, repo: &str, path: &str) -> Option<(u64, u64)> {
        let (_, pk, sk) = keys::tree_entry_key(repo, path);
        let bytes = kv.get(keys::TABLE_DIR_ENTRIES, pk, sk).await.unwrap()?;
        let te: TreeEntry = rmp_serde::from_slice(&bytes).unwrap();
        match te.kind {
            TreeEntryKind::Dir {
                artifact_count,
                total_bytes,
            } => Some((artifact_count, total_bytes)),
            _ => None,
        }
    }

    // --- apply_dir_delta tests ---

    #[tokio::test]
    async fn apply_delta_creates_entry() {
        let (kv, _dir) = test_kv().await;

        let delta = make_delta("myrepo", "sub", 2, 300);
        let changed = apply_dir_delta(kv.as_ref(), &delta).await.unwrap();
        assert!(changed, "should report change on first create");

        let (count, bytes) = read_dir_entry(kv.as_ref(), "myrepo", "sub")
            .await
            .expect("dir entry should exist");
        assert_eq!(count, 2);
        assert_eq!(bytes, 300);
    }

    #[tokio::test]
    async fn apply_delta_increments_then_decrements() {
        let (kv, _dir) = test_kv().await;

        // Create with +2, +300
        apply_dir_delta(kv.as_ref(), &make_delta("myrepo", "sub", 2, 300))
            .await
            .unwrap();

        // Increment with +1, +100
        apply_dir_delta(kv.as_ref(), &make_delta("myrepo", "sub", 1, 100))
            .await
            .unwrap();
        let (count, bytes) = read_dir_entry(kv.as_ref(), "myrepo", "sub").await.unwrap();
        assert_eq!(count, 3);
        assert_eq!(bytes, 400);

        // Decrement with -1, -100
        apply_dir_delta(kv.as_ref(), &make_delta("myrepo", "sub", -1, -100))
            .await
            .unwrap();
        let (count, bytes) = read_dir_entry(kv.as_ref(), "myrepo", "sub").await.unwrap();
        assert_eq!(count, 2);
        assert_eq!(bytes, 300);
    }

    #[tokio::test]
    async fn apply_delta_removes_at_zero() {
        let (kv, _dir) = test_kv().await;

        // Create then remove all.
        apply_dir_delta(kv.as_ref(), &make_delta("myrepo", "sub", 1, 100))
            .await
            .unwrap();
        let changed = apply_dir_delta(kv.as_ref(), &make_delta("myrepo", "sub", -1, -100))
            .await
            .unwrap();
        assert!(changed, "should report change on delete");

        assert!(
            read_dir_entry(kv.as_ref(), "myrepo", "sub").await.is_none(),
            "dir entry should be deleted at count 0"
        );
    }

    #[tokio::test]
    async fn apply_delta_negative_on_missing_is_noop() {
        let (kv, _dir) = test_kv().await;

        let changed = apply_dir_delta(kv.as_ref(), &make_delta("myrepo", "gone", -1, -100))
            .await
            .unwrap();
        assert!(!changed, "negative delta on missing entry should be no-op");
    }

    #[tokio::test]
    async fn apply_delta_propagates_upward() {
        let (kv, _dir) = test_kv().await;

        // Simulate upward propagation: delta at "a/b", then same delta at "a", then at "".
        // Each level uses tree_entry_key(repo, path) = (hash(parent), name).
        let delta_ab = make_delta("myrepo", "a/b", 1, 500);
        apply_dir_delta(kv.as_ref(), &delta_ab).await.unwrap();

        let delta_a = make_delta("myrepo", "a", 1, 500);
        apply_dir_delta(kv.as_ref(), &delta_a).await.unwrap();

        let delta_root = make_delta("myrepo", "", 1, 500);
        apply_dir_delta(kv.as_ref(), &delta_root).await.unwrap();

        // Check all three levels have the same totals.
        for dir_path in &["a/b", "a", ""] {
            let (count, bytes) = read_dir_entry(kv.as_ref(), "myrepo", dir_path)
                .await
                .unwrap_or_else(|| panic!("dir entry for '{}' should exist", dir_path));
            assert_eq!(count, 1, "count at '{}'", dir_path);
            assert_eq!(bytes, 500, "bytes at '{}'", dir_path);
        }
    }

    #[tokio::test]
    async fn flush_atime_touches_artifacts() {
        let (kv, _dir) = test_kv().await;

        // Write an artifact
        write_test_artifact(kv.as_ref(), "repo", "file.txt", 10).await;

        // Flush an atime update far in the future (must be >60s past last_accessed_at
        // due to debounce in touch_artifact).
        let ts = Utc::now() + chrono::Duration::hours(1);
        let mut pending = HashMap::new();
        pending.insert(("repo".to_string(), "file.txt".to_string()), ts);
        flush_atime(kv.as_ref(), pending).await;

        // Read the tree file entry and check the timestamp was updated.
        let (_, pk, sk) = keys::tree_entry_key("repo", "file.txt");
        let data = kv
            .get(keys::TABLE_DIR_ENTRIES, pk, sk)
            .await
            .unwrap()
            .expect("tree file entry should exist");
        let te: TreeEntry = rmp_serde::from_slice(&data).unwrap();
        assert_eq!(te.last_accessed_at, ts);
    }

    #[tokio::test]
    async fn worker_loop_processes_events_and_shuts_down() {
        let (kv, _dir) = test_kv().await;

        // Write an artifact so atime touch has something to update.
        write_test_artifact(kv.as_ref(), "repo", "file.txt", 10).await;

        let (sender, receiver) = UpdateSender::new(100);
        let cancel = CancellationToken::new();

        let kv_clone = kv.clone();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(async move {
            run_update_worker(receiver, kv_clone, cancel_clone).await;
        });

        // Send an atime event far in the future (>60s debounce).
        let ts = Utc::now() + chrono::Duration::hours(1);
        sender.touch("repo", "file.txt", ts);

        // Send a dir changed event (artifact created: +1, +10 bytes).
        sender.dir_changed("repo", "file.txt", 1, 10).await;

        // Give the worker a moment to process.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Cancel and wait for shutdown.
        cancel.cancel();
        handle.await.unwrap();

        // Verify atime was updated on the tree file entry.
        let (_, pk, sk) = keys::tree_entry_key("repo", "file.txt");
        let data = kv
            .get(keys::TABLE_DIR_ENTRIES, pk, sk)
            .await
            .unwrap()
            .expect("tree file entry should exist");
        let te: TreeEntry = rmp_serde::from_slice(&data).unwrap();
        assert_eq!(te.last_accessed_at, ts);
    }

    #[tokio::test]
    async fn update_sender_touch_and_dir_changed() {
        let (sender, _rx) = UpdateSender::new(100);
        let ts = DateTime::from_timestamp(1000, 0).unwrap();

        // Should not panic when sending events.
        sender.touch("repo", "path", ts);
        sender.dir_changed("repo", "a/b/c.txt", 1, 100).await;
    }

    #[test]
    fn update_sender_noop_does_not_panic() {
        let sender = UpdateSender::noop();
        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        sender.touch("repo", "path", ts);
        // Note: noop dir_changed would need to be called in async context.
        // Just verify construction doesn't panic.
    }

    #[tokio::test]
    async fn full_scan_repo_builds_entries() {
        let (kv, _dir) = test_kv().await;

        // Register a repo so full_scan can find it.
        let repo = depot_core::store::kv::RepoConfig {
            schema_version: depot_core::store::kv::CURRENT_RECORD_VERSION,
            name: "scan-repo".to_string(),
            kind: depot_core::store::kv::RepoKind::Hosted,
            format_config: depot_core::store::kv::FormatConfig::Raw {
                content_disposition: None,
            },
            store: "default".to_string(),
            created_at: Utc::now(),
            cleanup_max_unaccessed_days: None,
            cleanup_max_age_days: None,
            deleting: false,
        };
        service::put_repo(kv.as_ref(), &repo).await.unwrap();

        // Write an artifact (shard determined by path hash).
        write_test_artifact(kv.as_ref(), "scan-repo", "dir/sub/file.txt", 500).await;

        // Run full scan for the repo.
        let (ptx, _prx) = watch::channel(TaskProgress::default());
        let (updated, repo_count, repo_bytes) = full_scan_repo(kv.as_ref(), "scan-repo", &ptx)
            .await
            .unwrap();
        assert!(updated > 0, "should have written dir entries");
        assert_eq!(repo_count, 1);
        assert_eq!(repo_bytes, 500);

        // Check dir entries were created in the tree.
        assert!(
            read_dir_entry(kv.as_ref(), "scan-repo", "").await.is_some(),
            "root dir entry should exist"
        );
        assert!(
            read_dir_entry(kv.as_ref(), "scan-repo", "dir/sub")
                .await
                .is_some(),
            "dir/sub entry should exist"
        );

        // Run again — unchanged entries are skipped by the diff.
        let (updated2, count2, bytes2) = full_scan_repo(kv.as_ref(), "scan-repo", &ptx)
            .await
            .unwrap();
        // Diff-based: nothing changed, so no writes needed.
        assert_eq!(updated2, 0, "unchanged entries should be skipped");
        assert_eq!(count2, 1);
        assert_eq!(bytes2, 500);
    }

    #[tokio::test]
    async fn full_scan_cleans_stale_entries() {
        let (kv, _dir) = test_kv().await;

        // Register repo.
        let repo = depot_core::store::kv::RepoConfig {
            schema_version: depot_core::store::kv::CURRENT_RECORD_VERSION,
            name: "stale-repo".to_string(),
            kind: depot_core::store::kv::RepoKind::Hosted,
            format_config: depot_core::store::kv::FormatConfig::Raw {
                content_disposition: None,
            },
            store: "default".to_string(),
            created_at: Utc::now(),
            cleanup_max_unaccessed_days: None,
            cleanup_max_age_days: None,
            deleting: false,
        };
        service::put_repo(kv.as_ref(), &repo).await.unwrap();

        // Write a stale tree dir entry with no corresponding artifacts.
        let (_, pk, sk) = keys::tree_entry_key("stale-repo", "orphan/dir");
        let entry = TreeEntry {
            kind: TreeEntryKind::Dir {
                artifact_count: 10,
                total_bytes: 1000,
            },
            last_modified_at: Utc::now(),
            last_accessed_at: Utc::now(),
        };
        let data = rmp_serde::to_vec(&entry).unwrap();
        kv.put(keys::TABLE_DIR_ENTRIES, pk, sk, &data)
            .await
            .unwrap();

        // Full scan should delete the stale entry.
        let (ptx, _prx) = watch::channel(TaskProgress::default());
        let (updated, _, _) = full_scan_repo(kv.as_ref(), "stale-repo", &ptx)
            .await
            .unwrap();
        assert!(updated > 0, "should have deleted stale entry");

        assert!(
            read_dir_entry(kv.as_ref(), "stale-repo", "orphan/dir")
                .await
                .is_none(),
            "stale dir entry should be deleted"
        );
    }
}
