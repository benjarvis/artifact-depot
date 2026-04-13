// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Periodic KV state scanner.
//!
//! Polls `TABLE_REPOS`, store-stats records, and `TABLE_TASKS` (plus their
//! event log tails) every [`Settings::state_scan_interval_ms`] and emits
//! domain events on the local [`EventBus`] whenever the KV values diverge
//! from the scanner's own last-seen snapshot. This is how every instance
//! learns about changes to cross-instance state without a pub/sub layer.
//!
//! The scanner holds its own diff state independent of `ModelHandle` so
//! in-band events from CRUD handlers (e.g. `RepoCreated` published when a
//! user posts `/api/v1/repositories`) are not double-counted here.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::server::api::repositories::RepoResponse;
use crate::server::api::stores::StoreResponse;
use crate::server::config::settings::SettingsHandle;
use crate::server::infra::event_bus::{EventBus, ModelEvent, Topic};
use crate::server::infra::task::{TaskInfo, TaskLogEntry};
use depot_core::error;
use depot_core::service;
use depot_core::service::task as task_svc;
use depot_core::store::kv::{KvStore, TaskRecord};
use depot_core::task::TaskStatus;

const TASK_EVENT_TAIL_BATCH: usize = 1000;

#[derive(Default)]
struct ScannerState {
    /// name -> (artifact_count, total_bytes)
    repo_stats: HashMap<String, (u64, u64)>,
    /// Set of repo names last observed in KV.
    repo_set: HashSet<String>,
    /// name -> (blob_count, total_bytes)
    store_stats: HashMap<String, (u64, u64)>,
    /// Set of store names last observed in KV.
    store_set: HashSet<String>,
    /// task UUID -> digest of fields that trigger TaskUpdated.
    task_digests: HashMap<String, TaskDigest>,
    /// task UUID -> highest event seq folded into the last emission.
    task_event_seq: HashMap<String, u64>,
}

#[derive(Clone, PartialEq, Eq)]
struct TaskDigest {
    status: TaskStatus,
    updated_at: chrono::DateTime<chrono::Utc>,
    event_seq: u64,
}

impl TaskDigest {
    fn of(record: &TaskRecord) -> Self {
        Self {
            status: record.status,
            updated_at: record.updated_at,
            event_seq: record.event_seq,
        }
    }
}

pub async fn run_state_scanner(
    kv: Arc<dyn KvStore>,
    event_bus: Arc<EventBus>,
    settings: Arc<SettingsHandle>,
    cancel: CancellationToken,
) {
    let mut state = ScannerState::default();
    let mut backoff = Duration::from_millis(500);
    let max_backoff = Duration::from_secs(30);

    loop {
        let interval_ms = settings.load().state_scan_interval_ms.max(50);
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
        }

        match scan_once(&kv, &event_bus, &mut state).await {
            Ok(()) => {
                backoff = Duration::from_millis(500);
            }
            Err(e) => {
                tracing::warn!(error = %e, "state scanner tick failed, backing off");
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }

    tracing::info!("state scanner shutting down");
}

async fn scan_once(
    kv: &Arc<dyn KvStore>,
    event_bus: &Arc<EventBus>,
    state: &mut ScannerState,
) -> error::Result<()> {
    scan_repos(kv, event_bus, state).await?;
    scan_stores(kv, event_bus, state).await?;
    scan_tasks(kv, event_bus, state).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Repo stats
// ---------------------------------------------------------------------------

async fn scan_repos(
    kv: &Arc<dyn KvStore>,
    event_bus: &Arc<EventBus>,
    state: &mut ScannerState,
) -> error::Result<()> {
    let configs = service::list_repos(kv.as_ref()).await?;
    let mut seen = HashSet::with_capacity(configs.len());

    for repo in &configs {
        seen.insert(repo.name.clone());
        // Membership: newly-appearing repos produce RepoCreated.
        if !state.repo_set.contains(&repo.name) {
            let resp = RepoResponse::from(repo.clone());
            event_bus.publish(Topic::Repos, ModelEvent::RepoCreated { repo: resp });
        }
    }

    // Membership: repos gone from KV produce RepoDeleted.
    for name in state.repo_set.difference(&seen) {
        event_bus.publish(Topic::Repos, ModelEvent::RepoDeleted { name: name.clone() });
    }

    // Stats: read root dir entries and compare to last-seen.
    for repo in &configs {
        let stats = match service::get_repo_stats(kv.as_ref(), repo).await {
            Ok(s) => s,
            Err(e) => {
                tracing::debug!(repo = %repo.name, error = %e, "failed to read repo stats");
                continue;
            }
        };
        let key = (stats.artifact_count, stats.total_bytes);
        if state.repo_stats.get(&repo.name) != Some(&key) {
            event_bus.publish(
                Topic::Repos,
                ModelEvent::RepoStatsUpdated {
                    name: repo.name.clone(),
                    artifact_count: stats.artifact_count,
                    total_bytes: stats.total_bytes,
                },
            );
            state.repo_stats.insert(repo.name.clone(), key);
        }
    }

    // Prune entries for repos that no longer exist.
    state.repo_stats.retain(|k, _| seen.contains(k));
    state.repo_set = seen;
    Ok(())
}

// ---------------------------------------------------------------------------
// Store stats
// ---------------------------------------------------------------------------

async fn scan_stores(
    kv: &Arc<dyn KvStore>,
    event_bus: &Arc<EventBus>,
    state: &mut ScannerState,
) -> error::Result<()> {
    let records = service::list_stores(kv.as_ref()).await?;
    let mut seen = HashSet::with_capacity(records.len());

    for sr in &records {
        seen.insert(sr.name.clone());
        if !state.store_set.contains(&sr.name) {
            let resp = StoreResponse::from(sr.clone());
            event_bus.publish(Topic::Stores, ModelEvent::StoreCreated { store: resp });
        }
    }

    for name in state.store_set.difference(&seen) {
        event_bus.publish(
            Topic::Stores,
            ModelEvent::StoreDeleted { name: name.clone() },
        );
    }

    for sr in &records {
        match service::get_store_stats(kv.as_ref(), &sr.name).await {
            Ok(Some(stats)) => {
                let key = (stats.blob_count, stats.total_bytes);
                if state.store_stats.get(&sr.name) != Some(&key) {
                    event_bus.publish(
                        Topic::Stores,
                        ModelEvent::StoreStatsUpdated {
                            name: sr.name.clone(),
                            blob_count: stats.blob_count,
                            total_bytes: stats.total_bytes,
                        },
                    );
                    state.store_stats.insert(sr.name.clone(), key);
                }
            }
            Ok(None) => {
                state.store_stats.remove(&sr.name);
            }
            Err(e) => {
                tracing::debug!(store = %sr.name, error = %e, "failed to read store stats");
            }
        }
    }

    state.store_stats.retain(|k, _| seen.contains(k));
    state.store_set = seen;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

async fn scan_tasks(
    kv: &Arc<dyn KvStore>,
    event_bus: &Arc<EventBus>,
    state: &mut ScannerState,
) -> error::Result<()> {
    let records = task_svc::list_tasks(kv.as_ref()).await?;
    let mut seen = HashSet::with_capacity(records.len());

    for record in &records {
        if record.deleting {
            continue;
        }
        seen.insert(record.id.clone());
        let digest = TaskDigest::of(record);
        let prev = state.task_digests.get(&record.id);

        if prev.is_none() {
            // New task — build a TaskInfo with full log tail.
            let info = build_task_info(kv, record, 0).await;
            event_bus.publish(Topic::Tasks, ModelEvent::TaskCreated { task: info });
            state.task_digests.insert(record.id.clone(), digest);
            state
                .task_event_seq
                .insert(record.id.clone(), record.event_seq);
        } else if prev != Some(&digest) {
            // Changed — emit TaskUpdated with any newly appended events folded in.
            let last_seen_seq = state.task_event_seq.get(&record.id).copied().unwrap_or(0);
            let info = build_task_info(kv, record, last_seen_seq).await;
            event_bus.publish(Topic::Tasks, ModelEvent::TaskUpdated { task: info });
            state.task_digests.insert(record.id.clone(), digest);
            state
                .task_event_seq
                .insert(record.id.clone(), record.event_seq);
        }
    }

    // Emit TaskDeleted for tasks that were previously tracked but are now
    // absent (or tombstoned via `deleting=true`).
    let previously_tracked: Vec<String> = state.task_digests.keys().cloned().collect();
    for id in previously_tracked {
        if !seen.contains(&id) {
            event_bus.publish(Topic::Tasks, ModelEvent::TaskDeleted { id: id.clone() });
            state.task_digests.remove(&id);
            state.task_event_seq.remove(&id);
        }
    }

    Ok(())
}

/// Fold the task's current summary and newly-appended events into a
/// [`TaskInfo`] ready for publication on the event bus.
async fn build_task_info(
    kv: &Arc<dyn KvStore>,
    record: &TaskRecord,
    last_seen_seq: u64,
) -> TaskInfo {
    let mut info = TaskInfo::from_record(record);
    if record.event_seq > last_seen_seq {
        let from = if last_seen_seq == 0 {
            0
        } else {
            last_seen_seq + 1
        };
        match task_svc::list_task_events_since(kv.as_ref(), &record.id, from, TASK_EVENT_TAIL_BATCH)
            .await
        {
            Ok(events) => {
                info.log = events
                    .into_iter()
                    .map(|e| TaskLogEntry {
                        timestamp: e.timestamp,
                        message: e.message,
                    })
                    .collect();
            }
            Err(e) => {
                tracing::debug!(task = %record.id, error = %e, "failed to tail task events");
            }
        }
    }
    info
}
