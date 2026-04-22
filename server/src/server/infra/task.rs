// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! KV-backed task manager.
//!
//! Every task is persisted as a [`TaskRecord`] in `TABLE_TASKS`; every log
//! message is appended as a [`TaskEventRecord`] to `TABLE_TASK_EVENTS`. The
//! only in-memory state tracked here is the per-task **runtime ownership**
//! (cancel tokens, progress watch channels, event-seq counter) for tasks
//! executing on *this* instance. Cross-instance visibility is delivered by
//! the state scanner (a separate worker).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use depot_core::service::task as task_svc;
use depot_core::store::kv::{KvStore, TaskRecord};

pub use depot_core::task::{
    BulkDeleteResult, CheckFinding, CheckResult, GcResult, RebuildDirEntriesResult,
    RepoCleanResult, RepoCloneResult, TaskKind, TaskLogEntry, TaskProgress, TaskResult, TaskStatus,
};

/// Soft cap on log entries returned by `get()` — matches the in-memory cap
/// used before KV persistence was introduced.
const MAX_LOG_ENTRIES: usize = 1000;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(pub Uuid);

impl TaskId {
    pub fn as_str(&self) -> String {
        self.0.to_string()
    }
}

impl serde::Serialize for TaskId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0.to_string())
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, serde::Serialize, utoipa::ToSchema)]
pub struct TaskInfo {
    #[schema(value_type = String)]
    pub id: TaskId,
    pub kind: TaskKind,
    pub status: TaskStatus,
    #[schema(value_type = String, format = "date-time")]
    pub created_at: DateTime<Utc>,
    #[schema(value_type = Option<String>, format = "date-time")]
    pub started_at: Option<DateTime<Utc>>,
    #[schema(value_type = Option<String>, format = "date-time")]
    pub completed_at: Option<DateTime<Utc>>,
    pub progress: TaskProgress,
    pub result: Option<TaskResult>,
    pub error: Option<String>,
    pub summary: String,
    pub log: Vec<TaskLogEntry>,
}

impl TaskInfo {
    pub fn from_record(record: &TaskRecord) -> Self {
        let uuid = Uuid::parse_str(&record.id).unwrap_or_else(|_| Uuid::nil());
        Self {
            id: TaskId(uuid),
            kind: record.kind.clone(),
            status: record.status,
            created_at: record.created_at,
            started_at: record.started_at,
            completed_at: record.completed_at,
            progress: record.progress.clone(),
            result: record.result.clone(),
            error: record.error.clone(),
            summary: record.summary.clone(),
            log: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Runtime ownership — in-memory, only for tasks executing on this instance.
// ---------------------------------------------------------------------------

struct OwnedTask {
    progress_tx: watch::Sender<TaskProgress>,
    /// Keep the receiver alive so that `progress_tx.send()` doesn't fail.
    _progress_rx: watch::Receiver<TaskProgress>,
    cancel: CancellationToken,
    /// Monotonic seq counter for append_task_event calls.
    event_seq: Arc<AtomicU64>,
}

// ---------------------------------------------------------------------------
// TaskManager
// ---------------------------------------------------------------------------

pub struct TaskManager {
    kv: Arc<dyn KvStore>,
    instance_id: String,
    owned: Mutex<HashMap<TaskId, OwnedTask>>,
}

impl TaskManager {
    /// Create a new KV-backed task manager.
    ///
    /// Lifecycle changes (create, mark_running, mark_terminal, delete,
    /// update_summary) only write to KV — they do not publish events. The
    /// state scanner is the sole publisher of task events; HTTP handlers
    /// can call `BackgroundServices::scan_trigger.notify_one()` after a KV
    /// write to wake the scanner immediately for snappy UX.
    pub fn new(kv: Arc<dyn KvStore>, instance_id: String) -> Self {
        Self {
            kv,
            instance_id,
            owned: Mutex::new(HashMap::new()),
        }
    }

    async fn put_record(&self, record: &TaskRecord) {
        if let Err(e) = task_svc::put_task_summary(self.kv.as_ref(), record).await {
            tracing::warn!(task = %record.id, error = %e, "failed to persist task summary");
        }
    }

    async fn get_record(&self, id: TaskId) -> Option<TaskRecord> {
        match task_svc::get_task_summary(self.kv.as_ref(), &id.as_str()).await {
            Ok(rec) => rec,
            Err(e) => {
                tracing::warn!(task = %id, error = %e, "failed to read task summary");
                None
            }
        }
    }

    /// Create a new task in `Pending` state. Returns the task ID, a progress
    /// sender, and a cancellation token.
    pub async fn create(
        &self,
        kind: TaskKind,
    ) -> (TaskId, watch::Sender<TaskProgress>, CancellationToken) {
        let id = TaskId(Uuid::new_v4());
        let progress = TaskProgress::default();
        let (progress_tx, progress_rx) = watch::channel(progress.clone());
        let cancel = CancellationToken::new();
        let event_seq = Arc::new(AtomicU64::new(0));

        let now = Utc::now();
        let record = TaskRecord {
            schema_version: 0,
            id: id.as_str(),
            kind,
            status: TaskStatus::Pending,
            created_at: now,
            started_at: None,
            completed_at: None,
            updated_at: now,
            progress,
            result: None,
            error: None,
            summary: String::new(),
            owner_instance_id: self.instance_id.clone(),
            deleting: false,
            event_seq: 0,
        };
        self.put_record(&record).await;

        {
            let mut owned = self.owned.lock().await;
            owned.insert(
                id,
                OwnedTask {
                    progress_tx: progress_tx.clone(),
                    _progress_rx: progress_rx,
                    cancel: cancel.clone(),
                    event_seq,
                },
            );
        }

        (id, progress_tx, cancel)
    }

    /// Transition task to `Running` and spawn the progress bridge that
    /// persists progress updates to KV roughly every 250ms.
    pub async fn mark_running(&self, id: TaskId) {
        let Some(mut record) = self.get_record(id).await else {
            return;
        };
        record.status = TaskStatus::Running;
        record.started_at = Some(Utc::now());
        record.updated_at = Utc::now();
        self.put_record(&record).await;

        // Spawn the progress bridge.
        let owned_snapshot = {
            let owned = self.owned.lock().await;
            owned.get(&id).map(|o| {
                (
                    o.progress_tx.subscribe(),
                    o.cancel.clone(),
                    o.event_seq.clone(),
                )
            })
        };
        if let Some((mut rx, cancel, event_seq)) = owned_snapshot {
            let kv = self.kv.clone();
            let task_id_str = id.as_str();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        changed = rx.changed() => {
                            if changed.is_err() {
                                break;
                            }
                            let progress = rx.borrow_and_update().clone();
                            // Versioned RMW: re-read with a version stamp
                            // and CAS on write. If mark_completed / failed /
                            // cancelled raced between our read and write,
                            // put_if_version returns false and we silently
                            // drop this tick — the terminal state has
                            // already been persisted and must not be
                            // overwritten by a stale Running snapshot.
                            if let Ok(Some((mut rec, version))) =
                                task_svc::get_task_summary_versioned(kv.as_ref(), &task_id_str)
                                    .await
                            {
                                if matches!(
                                    rec.status,
                                    TaskStatus::Running | TaskStatus::Pending
                                ) {
                                    rec.progress = progress;
                                    rec.updated_at = Utc::now();
                                    rec.event_seq = event_seq.load(Ordering::Relaxed);
                                    let _ = task_svc::put_task_summary_if_unchanged(
                                        kv.as_ref(),
                                        &rec,
                                        version,
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                    // Throttle progress writes, but let cancel interrupt the wait.
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => break,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
                    }
                }
            });
        }
    }

    /// Transition task to `Completed` with a result.
    pub async fn mark_completed(&self, id: TaskId, result: TaskResult) {
        self.mark_terminal(id, TaskStatus::Completed, Some(result), None)
            .await;
    }

    /// Transition task to `Failed` with an error message.
    pub async fn mark_failed(&self, id: TaskId, error: String) {
        self.mark_terminal(id, TaskStatus::Failed, None, Some(error))
            .await;
    }

    /// Mark a task as cancelled (called after the task observes the token).
    pub async fn mark_cancelled(&self, id: TaskId) {
        self.mark_terminal(id, TaskStatus::Cancelled, None, None)
            .await;
    }

    async fn mark_terminal(
        &self,
        id: TaskId,
        status: TaskStatus,
        result: Option<TaskResult>,
        error: Option<String>,
    ) {
        let Some(mut record) = self.get_record(id).await else {
            return;
        };
        record.status = status;
        record.completed_at = Some(Utc::now());
        record.updated_at = Utc::now();
        record.result = result;
        record.error = error;

        // Snapshot live progress + final event_seq from the owned state.
        let mut owned = self.owned.lock().await;
        if let Some(owned_task) = owned.get(&id) {
            record.progress = owned_task.progress_tx.borrow().clone();
            record.event_seq = owned_task.event_seq.load(Ordering::Relaxed);
            owned_task.cancel.cancel();
        }
        owned.remove(&id);
        drop(owned);

        self.put_record(&record).await;
    }

    /// Fire the cancellation token for a running/pending task. Returns true
    /// if the task was running/pending on this instance.
    pub async fn cancel(&self, id: TaskId) -> bool {
        let owned = self.owned.lock().await;
        if let Some(o) = owned.get(&id) {
            o.cancel.cancel();
            return true;
        }
        false
    }

    /// Append a log entry: writes a `TaskEventRecord` and bumps the
    /// in-memory seq counter. The `TaskRecord.event_seq` hint is updated on
    /// the next progress-bridge tick.
    pub async fn append_log(&self, id: TaskId, message: String) {
        let seq = {
            let owned = self.owned.lock().await;
            owned
                .get(&id)
                .map(|o| o.event_seq.fetch_add(1, Ordering::Relaxed))
        };
        if let Some(seq) = seq {
            if let Err(e) =
                task_svc::append_task_event(self.kv.as_ref(), &id.as_str(), seq, message).await
            {
                tracing::warn!(task = %id, seq, error = %e, "failed to append task event");
            }
        }
    }

    /// Update the summary string on a task.
    pub async fn update_summary(&self, id: TaskId, summary: String) {
        let Some(mut record) = self.get_record(id).await else {
            return;
        };
        record.summary = summary;
        record.updated_at = Utc::now();
        self.put_record(&record).await;
    }

    /// Read a task's current state from KV, including its log tail.
    pub async fn get(&self, id: TaskId) -> Option<TaskInfo> {
        let record = self.get_record(id).await?;
        let mut info = TaskInfo::from_record(&record);
        if record.event_seq > 0 {
            match task_svc::list_task_events_since(self.kv.as_ref(), &record.id, 0, MAX_LOG_ENTRIES)
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
                    tracing::warn!(task = %id, error = %e, "failed to read task events");
                }
            }
        }
        Some(info)
    }

    /// List all persisted tasks, without log entries (callers that need the
    /// log call `get(id)` for the specific task).
    pub async fn list(&self) -> Vec<TaskInfo> {
        match task_svc::list_tasks(self.kv.as_ref()).await {
            Ok(records) => records
                .iter()
                .filter(|r| !r.deleting)
                .map(TaskInfo::from_record)
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e, "failed to list tasks");
                Vec::new()
            }
        }
    }

    /// Delete a task and its event log. If the task is running on this
    /// instance, fires its cancel token first.
    ///
    /// The record is marked `deleting=true` before the event drain so a
    /// concurrent scanner (in a later PR) can observe the tombstone. Under
    /// the cleanup sweep path the drain will move from synchronous here to
    /// asynchronous in the sweep worker.
    pub async fn delete(&self, id: TaskId) -> bool {
        let Some(mut record) = self.get_record(id).await else {
            return false;
        };
        record.deleting = true;
        record.updated_at = Utc::now();
        self.put_record(&record).await;

        // If running locally, fire cancel token.
        {
            let owned = self.owned.lock().await;
            if let Some(o) = owned.get(&id) {
                o.cancel.cancel();
            }
        }

        // Drain associated events, then remove the summary.
        let id_str = id.as_str();
        if let Err(e) = task_svc::drain_task_events(self.kv.as_ref(), &id_str).await {
            tracing::warn!(task = %id, error = %e, "failed to drain task events during delete");
        }
        if let Err(e) = task_svc::delete_task_summary(self.kv.as_ref(), &id_str).await {
            tracing::warn!(task = %id, error = %e, "failed to delete task summary");
            return false;
        }

        true
    }

    async fn any_running_of(&self, pred: impl Fn(&TaskRecord) -> bool) -> bool {
        match task_svc::list_tasks(self.kv.as_ref()).await {
            Ok(records) => records.iter().any(|r| {
                !r.deleting
                    && matches!(r.status, TaskStatus::Running | TaskStatus::Pending)
                    && pred(r)
            }),
            Err(_) => false,
        }
    }

    pub async fn has_running_check(&self) -> bool {
        self.any_running_of(|r| r.kind == TaskKind::Check).await
    }

    pub async fn has_running_gc(&self) -> bool {
        self.any_running_of(|r| r.kind == TaskKind::BlobGc).await
    }

    pub async fn has_running_rebuild_dir_entries(&self) -> bool {
        self.any_running_of(|r| r.kind == TaskKind::RebuildDirEntries)
            .await
    }

    pub async fn has_running_repo_clean(&self, repo: &str) -> bool {
        self.any_running_of(|r| matches!(&r.kind, TaskKind::RepoClean { repo: rr } if rr == repo))
            .await
    }

    pub async fn has_running_repo_clone(&self, source: &str, target: &str) -> bool {
        self.any_running_of(|r| {
            matches!(
                &r.kind,
                TaskKind::RepoClone { source: s, target: t } if s == source && t == target
            )
        })
        .await
    }

    pub async fn has_running_bulk_delete(&self, repo: &str, prefix: &str) -> bool {
        self.any_running_of(|r| {
            matches!(
                &r.kind,
                TaskKind::BulkDelete { repo: rr, prefix: pp } if rr == repo && pp == prefix
            )
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use depot_kv_redb::sharded_redb::ShardedRedbKvStore;
    use tempfile::TempDir;

    async fn test_mgr() -> (TaskManager, TempDir) {
        let dir = TempDir::new().unwrap();
        let kv: Arc<dyn KvStore> = Arc::new(
            ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );
        let mgr = TaskManager::new(kv, "test-inst".to_string());
        (mgr, dir)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_lifecycle() {
        let (mgr, _dir) = test_mgr().await;

        let (id, _tx, _cancel) = mgr.create(TaskKind::Check).await;
        let info = mgr.get(id).await.unwrap();
        assert_eq!(info.status, TaskStatus::Pending);
        assert_eq!(info.kind, TaskKind::Check);

        mgr.mark_running(id).await;
        let info = mgr.get(id).await.unwrap();
        assert_eq!(info.status, TaskStatus::Running);
        assert!(info.started_at.is_some());

        let result = TaskResult::Check(CheckResult::default());
        mgr.mark_completed(id, result).await;
        let info = mgr.get(id).await.unwrap();
        assert_eq!(info.status, TaskStatus::Completed);
        assert!(info.completed_at.is_some());
        assert!(info.result.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_cancel() {
        let (mgr, _dir) = test_mgr().await;
        let (id, _tx, cancel) = mgr.create(TaskKind::Check).await;
        mgr.mark_running(id).await;

        assert!(mgr.cancel(id).await);
        assert!(cancel.is_cancelled());

        mgr.mark_cancelled(id).await;
        let info = mgr.get(id).await.unwrap();
        assert_eq!(info.status, TaskStatus::Cancelled);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn task_delete_sets_deleting_flag() {
        let (mgr, _dir) = test_mgr().await;
        let (id, _tx, _cancel) = mgr.create(TaskKind::Check).await;
        mgr.mark_running(id).await;
        mgr.mark_failed(id, "oops".to_string()).await;

        assert!(mgr.delete(id).await);
        // list() hides deleting tasks even before the sweep has run.
        assert!(mgr.list().await.is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn has_running_filters_by_kind() {
        let (mgr, _dir) = test_mgr().await;
        assert!(!mgr.has_running_check().await);

        let (check_id, _tx, _c) = mgr.create(TaskKind::Check).await;
        assert!(mgr.has_running_check().await);
        assert!(!mgr.has_running_gc().await);

        mgr.mark_running(check_id).await;
        assert!(mgr.has_running_check().await);

        mgr.mark_completed(check_id, TaskResult::Check(CheckResult::default()))
            .await;
        assert!(!mgr.has_running_check().await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn append_log_persists_events() {
        let (mgr, _dir) = test_mgr().await;
        let (id, _tx, _cancel) = mgr.create(TaskKind::Check).await;
        mgr.mark_running(id).await;

        mgr.append_log(id, "step 1".into()).await;
        mgr.append_log(id, "step 2".into()).await;
        // Manually bump the event_seq hint on the record so get() tails logs.
        // (The progress bridge does this lazily; trigger it by completing.)
        mgr.mark_completed(id, TaskResult::Check(CheckResult::default()))
            .await;
        let info = mgr.get(id).await.unwrap();
        assert_eq!(info.log.len(), 2);
        assert_eq!(info.log[0].message, "step 1");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn progress_bridge_does_not_overwrite_terminal_state() {
        // Reproduces the race where a worker fires a burst of progress
        // updates and then calls mark_completed. The bridge's RMW must
        // not clobber the Completed status with a stale Running snapshot.
        let (mgr, _dir) = test_mgr().await;
        let (id, tx, _cancel) = mgr.create(TaskKind::Check).await;
        mgr.mark_running(id).await;

        // Fire many progress updates so the bridge is actively RMW'ing.
        for i in 0..50 {
            tx.send_modify(|p| {
                p.checked_blobs = i;
            });
        }

        mgr.mark_completed(id, TaskResult::Check(CheckResult::default()))
            .await;

        // Give the bridge several wake-ups to attempt a stale write.
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let info = mgr.get(id).await.unwrap();
        assert_eq!(info.status, TaskStatus::Completed);
        assert!(info.completed_at.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn has_running_repo_clean_filters_by_repo() {
        let (mgr, _dir) = test_mgr().await;
        let (id, _tx, _c) = mgr
            .create(TaskKind::RepoClean {
                repo: "my-repo".into(),
            })
            .await;
        assert!(mgr.has_running_repo_clean("my-repo").await);
        assert!(!mgr.has_running_repo_clean("other-repo").await);
        assert!(!mgr.has_running_check().await);

        mgr.mark_completed(
            id,
            TaskResult::RepoClean(RepoCleanResult {
                repo: "my-repo".into(),
                ..Default::default()
            }),
        )
        .await;
        assert!(!mgr.has_running_repo_clean("my-repo").await);
    }
}
