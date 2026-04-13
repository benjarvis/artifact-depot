// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cleanup helpers: repo deletion draining, upload session TTL, and task
//! reaping (deleting-flag sweeps, orphan detection, TTL-based expiry).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use metrics::histogram;
use tokio_util::sync::CancellationToken;

use crate::server::config::settings::SettingsHandle;
use crate::server::worker::cluster;
use depot_core::error;
use depot_core::service;
use depot_core::service::task as task_svc;
use depot_core::store::kv::KvStore;
use depot_core::store_registry::StoreRegistry;
use depot_core::task::TaskStatus;

#[tracing::instrument(level = "debug", skip_all)]
pub async fn run_cleanup_tick(
    kv: &Arc<dyn KvStore>,
    stores: &Arc<StoreRegistry>,
    settings: &Arc<SettingsHandle>,
) -> error::Result<()> {
    let tick_start = std::time::Instant::now();

    // Drain repos marked for deletion.
    drain_deleting_repos(kv).await;

    // Clean up Docker upload sessions not updated in the last hour.
    const UPLOAD_SESSION_TTL: std::time::Duration = std::time::Duration::from_secs(3600);
    if let Err(e) = service::cleanup_upload_sessions(kv.as_ref(), stores, UPLOAD_SESSION_TTL).await
    {
        tracing::error!("upload session cleanup failed: {}", e);
    }

    // Reap tasks: orphan detection, TTL, and drain deleting=true tasks.
    reap_tasks(kv, settings).await;

    histogram!("cleanup_tick_duration_seconds").record(tick_start.elapsed().as_secs_f64());
    Ok(())
}

/// Background loop that periodically runs `run_cleanup_tick`.
pub async fn run_cleanup_loop(
    kv: Arc<dyn KvStore>,
    stores: Arc<StoreRegistry>,
    settings: Arc<SettingsHandle>,
    cancel: CancellationToken,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval.tick().await; // skip immediate tick

    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(e) = run_cleanup_tick(&kv, &stores, &settings).await {
                    tracing::warn!(error = %e, "cleanup tick failed");
                }
            }
            _ = cancel.cancelled() => {
                tracing::info!("cleanup loop shutting down");
                return;
            }
        }
    }
}

/// Find repos marked `deleting = true` and drain their artifacts, dir entries,
/// then delete the repo config. Idempotent — safe to call on every tick.
async fn drain_deleting_repos(kv: &Arc<dyn KvStore>) {
    let all_repos = match service::list_repos_raw(kv.as_ref()).await {
        Ok(repos) => repos,
        Err(e) => {
            tracing::error!("failed to list repos for deletion drain: {}", e);
            return;
        }
    };

    for repo in all_repos {
        if !repo.deleting {
            continue;
        }
        tracing::info!(repo = %repo.name, "draining deleting repo");
        if let Err(e) = service::drain_deleting_repo(kv.as_ref(), &repo.name).await {
            tracing::error!(repo = %repo.name, "failed to drain deleting repo: {}", e);
        }
    }
}

/// Three-phase task reap:
/// 1. Drain any task marked `deleting=true` (event partition + summary).
/// 2. Mark Running/Pending tasks whose owner has gone missing as Failed.
/// 3. Mark completed / failed / cancelled tasks older than the retention
///    window as `deleting=true` so the next tick finishes the reap.
async fn reap_tasks(kv: &Arc<dyn KvStore>, settings: &Arc<SettingsHandle>) {
    let tasks = match task_svc::list_tasks(kv.as_ref()).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "task reap: list_tasks failed");
            return;
        }
    };

    // Build the set of live instance IDs once per tick for orphan detection.
    let live_instances: HashSet<String> = match cluster::list_live_instances(kv.as_ref()).await {
        Ok(v) => v.into_iter().map(|r| r.instance_id).collect(),
        Err(e) => {
            tracing::warn!(error = %e, "task reap: list_live_instances failed");
            HashSet::new()
        }
    };

    let retention_secs = settings.load().task_retention_secs as i64;
    let now = Utc::now();

    for task in tasks {
        if task.deleting {
            // Phase 1: finish the drain.
            if let Err(e) = task_svc::drain_task_events(kv.as_ref(), &task.id).await {
                tracing::warn!(task = %task.id, error = %e, "task reap: drain events failed");
                continue;
            }
            if let Err(e) = task_svc::delete_task_summary(kv.as_ref(), &task.id).await {
                tracing::warn!(task = %task.id, error = %e, "task reap: delete summary failed");
            }
            continue;
        }

        // Phase 2: orphan detection. Skip when we have no live-instance list
        // (e.g. on KV errors) to avoid wrongly reaping everyone.
        if !live_instances.is_empty()
            && matches!(task.status, TaskStatus::Running | TaskStatus::Pending)
            && !task.owner_instance_id.is_empty()
            && !live_instances.contains(&task.owner_instance_id)
        {
            let mut updated = task.clone();
            updated.status = TaskStatus::Failed;
            updated.completed_at = Some(now);
            updated.updated_at = now;
            updated.error = Some(format!(
                "owner instance {} is no longer live",
                task.owner_instance_id
            ));
            if let Err(e) = task_svc::put_task_summary(kv.as_ref(), &updated).await {
                tracing::warn!(task = %task.id, error = %e, "task reap: mark orphan failed");
            }
            continue;
        }

        // Phase 3: TTL on terminal tasks.
        if matches!(
            task.status,
            TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
        ) {
            if let Some(completed_at) = task.completed_at {
                let age = (now - completed_at).num_seconds();
                if age >= retention_secs {
                    let mut updated = task.clone();
                    updated.deleting = true;
                    updated.updated_at = now;
                    if let Err(e) = task_svc::put_task_summary(kv.as_ref(), &updated).await {
                        tracing::warn!(task = %task.id, error = %e, "task reap: ttl mark failed");
                    }
                }
            }
        }
    }
}

/// Extract image prefix from a path like "myimage/_manifests/sha256:abc".
/// Returns None for flat paths like "_manifests/sha256:abc".
pub fn extract_image_prefix(path: &str) -> Option<&str> {
    let idx = path.find("/_manifests/")?;
    let prefix = path.get(..idx)?;
    if !prefix.is_empty() && !prefix.starts_with('_') {
        Some(prefix)
    } else {
        None
    }
}
