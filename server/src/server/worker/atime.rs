// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Deferred atime (last_accessed_at) update worker.
//!
//! Read paths send lightweight `AtimeEvent` messages via a bounded channel
//! instead of updating the KV store inline. A background worker drains the
//! channel on a 5-second interval, deduplicates events, and flushes batches
//! of `touch_artifact` calls that update tree file entries.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use metrics::{counter, histogram};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use depot_core::service;
use depot_core::store::kv::KvStore;

/// A pending atime update.
struct AtimeEvent {
    repo: String,
    path: String,
    timestamp: DateTime<Utc>,
}

/// Opaque receiver handle for the atime worker.
pub struct AtimeReceiver(mpsc::Receiver<AtimeEvent>);

/// Cheaply cloneable sender handle for atime events.
///
/// Callers use `send()` which calls `try_send` on the bounded channel.
/// If the channel is full the event is silently dropped (atime is best-effort).
#[derive(Clone)]
pub struct AtimeSender {
    tx: mpsc::Sender<AtimeEvent>,
}

impl AtimeSender {
    /// Create a new sender/receiver pair with the given channel capacity.
    pub fn new(capacity: usize) -> (Self, AtimeReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, AtimeReceiver(rx))
    }

    /// Create a no-op sender whose `send()` silently discards events.
    /// Useful for tests and code paths that don't need atime tracking.
    pub fn noop() -> Self {
        let (tx, _rx) = mpsc::channel(1);
        // _rx is dropped, so try_send will always return Err(Closed) — which we ignore.
        Self { tx }
    }

    /// Queue an atime update. Best-effort: silently drops on full or closed channel.
    pub fn send(&self, repo: &str, path: &str, now: DateTime<Utc>) {
        let event = AtimeEvent {
            repo: repo.to_string(),
            path: path.to_string(),
            timestamp: now,
        };
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(event) {
            tracing::warn!("atime channel full, dropping event");
            counter!("atime_events_dropped_total").increment(1);
        }
        // Closed channel is silently ignored (shutdown or noop sender).
    }
}

/// Batch size for flushing atime updates inside a single transaction.
const FLUSH_BATCH_SIZE: usize = 50;

/// Run the atime worker loop. Drains pending events every 5 seconds,
/// deduplicates by (repo, path) keeping the latest timestamp, and flushes
/// in batches via `touch_artifact`.
///
/// Performs one final drain+flush after cancellation before returning.
pub async fn run_atime_worker(
    receiver: AtimeReceiver,
    kv: std::sync::Arc<dyn KvStore>,
    cancel: CancellationToken,
) {
    let mut rx = receiver.0;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    interval.tick().await; // first tick is immediate, skip it

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let pending = drain_and_dedup(&mut rx);
                if !pending.is_empty() {
                    flush_all(kv.as_ref(), pending).await;
                }
            }
            _ = cancel.cancelled() => {
                tracing::info!("atime worker shutting down, final flush");
                let pending = drain_and_dedup(&mut rx);
                if !pending.is_empty() {
                    flush_all(kv.as_ref(), pending).await;
                }
                return;
            }
        }
    }
}

/// Drain all available events from the receiver and deduplicate by (repo, path),
/// keeping the latest timestamp for each key.
fn drain_and_dedup(
    rx: &mut mpsc::Receiver<AtimeEvent>,
) -> HashMap<(String, String), DateTime<Utc>> {
    let mut map = HashMap::new();
    while let Ok(event) = rx.try_recv() {
        let key = (event.repo, event.path);
        map.entry(key)
            .and_modify(|ts: &mut DateTime<Utc>| {
                if event.timestamp > *ts {
                    *ts = event.timestamp;
                }
            })
            .or_insert(event.timestamp);
    }
    map
}

/// Flush deduped atime updates in batches of FLUSH_BATCH_SIZE.
async fn flush_all(kv: &dyn KvStore, pending: HashMap<(String, String), DateTime<Utc>>) {
    let items: Vec<((String, String), DateTime<Utc>)> = pending.into_iter().collect();
    histogram!("atime_flush_batch_size").record(items.len() as f64);
    for chunk in items.chunks(FLUSH_BATCH_SIZE) {
        let batch: Vec<(String, String, DateTime<Utc>)> = chunk
            .iter()
            .map(|((r, p), ts)| (r.clone(), p.clone(), *ts))
            .collect();
        let result: Result<(), depot_core::error::DepotError> = async {
            for (repo, path, ts) in &batch {
                if let Err(e) = service::touch_artifact(kv, repo, path, *ts).await {
                    tracing::debug!(repo, path, error = %e, "atime touch failed");
                }
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            tracing::warn!(error = %e, "atime flush batch failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::repo::now_utc;
    use depot_core::service;
    use depot_core::store::kv::*;
    use depot_kv_redb::ShardedRedbKvStore;
    use std::sync::Arc;

    use depot_core::store::keys;
    use depot_core::store::kv::TreeEntry;

    /// Read tree file entry's last_accessed_at.
    async fn tree_atime(kv: &dyn KvStore, repo: &str, path: &str) -> chrono::DateTime<chrono::Utc> {
        let (_, pk, sk) = keys::tree_entry_key(repo, path);
        let bytes = kv
            .get(keys::TABLE_DIR_ENTRIES, pk, sk)
            .await
            .unwrap()
            .expect("tree entry should exist");
        let te: TreeEntry = rmp_serde::from_slice(&bytes).unwrap();
        te.last_accessed_at
    }

    /// Helper: create a KV store with a repo and artifact for atime testing.
    async fn setup_kv_with_artifact(
        repo: &str,
        path: &str,
    ) -> (Arc<ShardedRedbKvStore>, tempfile::TempDir) {
        let dir = crate::server::infra::test_support::test_tempdir();
        let kv = Arc::new(
            ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );

        let now = now_utc();
        let config = RepoConfig {
            schema_version: CURRENT_RECORD_VERSION,
            name: repo.to_string(),
            kind: RepoKind::Hosted,
            format_config: FormatConfig::Raw {
                content_disposition: None,
            },
            store: "default".to_string(),
            created_at: now,
            cleanup_max_unaccessed_days: None,
            cleanup_max_age_days: None,
            deleting: false,
        };
        service::put_repo(kv.as_ref(), &config).await.unwrap();

        let old_ts = now - chrono::Duration::days(30);
        let record = ArtifactRecord {
            schema_version: CURRENT_RECORD_VERSION,
            id: String::new(),
            size: 0,
            content_type: "application/octet-stream".to_string(),
            created_at: old_ts,
            updated_at: old_ts,
            last_accessed_at: old_ts,
            path: String::new(),
            kind: ArtifactKind::Raw,
            internal: false,
            blob_id: None,
            content_hash: None,
            etag: None,
        };
        service::put_artifact(kv.as_ref(), repo, path, &record)
            .await
            .unwrap();

        (kv, dir)
    }

    #[test]
    fn test_noop_sender_does_not_panic() {
        let sender = AtimeSender::noop();
        let now = chrono::Utc::now();
        // Multiple sends on a noop sender must not panic.
        for _ in 0..10 {
            sender.send("repo", "path", now);
        }
    }

    #[test]
    fn test_sender_channel_full_drops_event() {
        let (sender, mut receiver) = AtimeSender::new(1);
        let now = chrono::Utc::now();

        // First send should succeed (fills the channel).
        sender.send("repo", "path1", now);
        // Second send should be dropped (channel full).
        sender.send("repo", "path2", now);

        // Only one event should be receivable.
        assert!(receiver.0.try_recv().is_ok());
        assert!(receiver.0.try_recv().is_err());
    }

    #[test]
    fn test_drain_and_dedup_keeps_latest_timestamp() {
        let (sender, mut receiver) = AtimeSender::new(16);
        let t1 = DateTime::from_timestamp(1000, 0).unwrap();
        let t2 = DateTime::from_timestamp(2000, 0).unwrap();
        let t3 = DateTime::from_timestamp(3000, 0).unwrap();

        // Send 3 events for the same key with increasing timestamps.
        sender.send("repo", "path", t1);
        sender.send("repo", "path", t3);
        sender.send("repo", "path", t2);

        let map = drain_and_dedup(&mut receiver.0);
        assert_eq!(map.len(), 1);
        assert_eq!(
            map[&("repo".to_string(), "path".to_string())],
            t3,
            "should keep the latest timestamp"
        );
    }

    #[test]
    fn test_drain_and_dedup_multiple_keys() {
        let (sender, mut receiver) = AtimeSender::new(16);
        let t1 = DateTime::from_timestamp(1000, 0).unwrap();
        let t2 = DateTime::from_timestamp(2000, 0).unwrap();
        let t3 = DateTime::from_timestamp(3000, 0).unwrap();

        sender.send("repo-a", "path1", t1);
        sender.send("repo-b", "path2", t2);
        sender.send("repo-a", "path3", t3);

        let map = drain_and_dedup(&mut receiver.0);
        assert_eq!(map.len(), 3);
        assert_eq!(map[&("repo-a".to_string(), "path1".to_string())], t1);
        assert_eq!(map[&("repo-b".to_string(), "path2".to_string())], t2);
        assert_eq!(map[&("repo-a".to_string(), "path3".to_string())], t3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_worker_flushes_on_cancel() {
        let (kv, _dir) = setup_kv_with_artifact("atime-repo", "file.txt").await;

        let (sender, receiver) = AtimeSender::new(64);
        let cancel = CancellationToken::new();

        let worker = tokio::spawn({
            let kv = kv.clone() as Arc<dyn KvStore>;
            let cancel = cancel.clone();
            async move {
                run_atime_worker(receiver, kv, cancel).await;
            }
        });

        let new_ts = now_utc();
        sender.send("atime-repo", "file.txt", new_ts);

        // Give the sender a moment to deliver, then cancel.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
        worker.await.unwrap();

        // Verify the tree file entry's last_accessed_at was updated.
        assert_eq!(
            tree_atime(kv.as_ref(), "atime-repo", "file.txt").await,
            new_ts
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_worker_flush_nonexistent_artifact() {
        let dir = crate::server::infra::test_support::test_tempdir();
        let kv = Arc::new(
            ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        ) as Arc<dyn KvStore>;

        let (sender, receiver) = AtimeSender::new(64);
        let cancel = CancellationToken::new();

        let worker = tokio::spawn({
            let kv = kv.clone();
            let cancel = cancel.clone();
            async move {
                run_atime_worker(receiver, kv, cancel).await;
            }
        });

        // Send event for nonexistent artifact — should not crash.
        sender.send("no-such-repo", "no-such-path", now_utc());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
        worker.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_flush_batching_over_50() {
        let dir = crate::server::infra::test_support::test_tempdir();
        let kv = Arc::new(
            ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );

        // Create repo + 60 artifacts.
        let now = now_utc();
        let config = RepoConfig {
            schema_version: CURRENT_RECORD_VERSION,
            name: "batch-repo".to_string(),
            kind: RepoKind::Hosted,
            format_config: FormatConfig::Raw {
                content_disposition: None,
            },
            store: "default".to_string(),
            created_at: now,
            cleanup_max_unaccessed_days: None,
            cleanup_max_age_days: None,
            deleting: false,
        };
        service::put_repo(kv.as_ref(), &config).await.unwrap();

        let old_ts = now - chrono::Duration::days(30);
        for i in 0..60 {
            let record = ArtifactRecord {
                schema_version: CURRENT_RECORD_VERSION,
                id: String::new(),
                size: 0,
                content_type: "application/octet-stream".to_string(),
                created_at: old_ts,
                updated_at: old_ts,
                last_accessed_at: old_ts,
                path: String::new(),
                kind: ArtifactKind::Raw,
                internal: false,
                blob_id: None,
                content_hash: None,
                etag: None,
            };
            service::put_artifact(
                kv.as_ref(),
                "batch-repo",
                &format!("file{i:03}.txt"),
                &record,
            )
            .await
            .unwrap();
        }

        let (sender, receiver) = AtimeSender::new(256);
        let cancel = CancellationToken::new();
        let new_ts = now_utc();

        // Send 60 events (exceeds FLUSH_BATCH_SIZE=50).
        for i in 0..60 {
            sender.send("batch-repo", &format!("file{i:03}.txt"), new_ts);
        }

        let worker = tokio::spawn({
            let kv = kv.clone() as Arc<dyn KvStore>;
            let cancel = cancel.clone();
            async move {
                run_atime_worker(receiver, kv, cancel).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
        worker.await.unwrap();

        // Verify all 60 tree file entries were updated.
        for i in 0..60 {
            let atime = tree_atime(kv.as_ref(), "batch-repo", &format!("file{i:03}.txt")).await;
            assert_eq!(atime, new_ts, "file{i:03}.txt should be updated");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_dedup_across_multiple_keys_preserves_all() {
        let dir = crate::server::infra::test_support::test_tempdir();
        let kv = Arc::new(
            ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
                .await
                .unwrap(),
        );

        let now = now_utc();
        let config = RepoConfig {
            schema_version: CURRENT_RECORD_VERSION,
            name: "dedup-repo".to_string(),
            kind: RepoKind::Hosted,
            format_config: FormatConfig::Raw {
                content_disposition: None,
            },
            store: "default".to_string(),
            created_at: now,
            cleanup_max_unaccessed_days: None,
            cleanup_max_age_days: None,
            deleting: false,
        };
        service::put_repo(kv.as_ref(), &config).await.unwrap();

        let old_ts = now - chrono::Duration::days(30);
        for i in 0..5 {
            let record = ArtifactRecord {
                schema_version: CURRENT_RECORD_VERSION,
                id: String::new(),
                size: 0,
                content_type: "application/octet-stream".to_string(),
                created_at: old_ts,
                updated_at: old_ts,
                last_accessed_at: old_ts,
                path: String::new(),
                kind: ArtifactKind::Raw,
                internal: false,
                blob_id: None,
                content_hash: None,
                etag: None,
            };
            service::put_artifact(kv.as_ref(), "dedup-repo", &format!("file{i}.txt"), &record)
                .await
                .unwrap();
        }

        let (sender, receiver) = AtimeSender::new(256);
        let cancel = CancellationToken::new();

        let new_ts = now_utc();
        // Send duplicates for each key — only latest should survive dedup.
        for i in 0..5 {
            let earlier = new_ts - chrono::Duration::hours(1);
            sender.send("dedup-repo", &format!("file{i}.txt"), earlier);
            sender.send("dedup-repo", &format!("file{i}.txt"), new_ts);
        }

        let worker = tokio::spawn({
            let kv = kv.clone() as Arc<dyn KvStore>;
            let cancel = cancel.clone();
            async move {
                run_atime_worker(receiver, kv, cancel).await;
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancel.cancel();
        worker.await.unwrap();

        for i in 0..5 {
            let atime = tree_atime(kv.as_ref(), "dedup-repo", &format!("file{i}.txt")).await;
            assert_eq!(atime, new_ts, "file{i}.txt should have latest timestamp");
        }
    }
}
