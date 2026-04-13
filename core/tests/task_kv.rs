// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the KV-backed task service layer.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use chrono::Utc;
use depot_core::service::task::{
    append_task_event, delete_task_summary, drain_task_events, get_task_summary,
    list_task_events_since, list_tasks, put_task_summary,
};
use depot_core::store::kv::{TaskRecord, CURRENT_RECORD_VERSION};
use depot_core::task::{TaskKind, TaskStatus};
use depot_kv_redb::sharded_redb::ShardedRedbKvStore;

async fn test_kv() -> (ShardedRedbKvStore, tempfile::TempDir) {
    let dir = tempfile::TempDir::new().unwrap();
    let kv = ShardedRedbKvStore::open(&dir.path().join("kv"), 2, 0)
        .await
        .unwrap();
    (kv, dir)
}

fn sample_record(id: &str) -> TaskRecord {
    TaskRecord {
        schema_version: 0, // put_task_summary stamps CURRENT_RECORD_VERSION.
        id: id.to_string(),
        kind: TaskKind::Check,
        status: TaskStatus::Pending,
        created_at: Utc::now(),
        started_at: None,
        completed_at: None,
        updated_at: Utc::now(),
        progress: Default::default(),
        result: None,
        error: None,
        summary: String::new(),
        owner_instance_id: "inst-a".to_string(),
        deleting: false,
        event_seq: 0,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn put_get_list_round_trip() {
    let (kv, _dir) = test_kv().await;
    put_task_summary(&kv, &sample_record("t1")).await.unwrap();
    put_task_summary(&kv, &sample_record("t2")).await.unwrap();

    let got = get_task_summary(&kv, "t1").await.unwrap().unwrap();
    assert_eq!(got.id, "t1");
    assert_eq!(got.schema_version, CURRENT_RECORD_VERSION);

    let all = list_tasks(&kv).await.unwrap();
    assert_eq!(all.len(), 2);

    assert!(delete_task_summary(&kv, "t1").await.unwrap());
    assert!(get_task_summary(&kv, "t1").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn append_and_tail_events() {
    let (kv, _dir) = test_kv().await;
    for seq in 0..5 {
        let wrote = append_task_event(&kv, "t1", seq, format!("msg-{seq}"))
            .await
            .unwrap();
        assert!(wrote);
    }
    // from_seq=0 returns all entries (seq 0..=4).
    let all = list_task_events_since(&kv, "t1", 0, 10).await.unwrap();
    assert_eq!(all.len(), 5);
    assert_eq!(all[0].seq, 0);

    // from_seq=3 returns entries 3..=4.
    let tail = list_task_events_since(&kv, "t1", 3, 10).await.unwrap();
    assert_eq!(tail.len(), 2);
    assert_eq!(tail[0].seq, 3);

    // Duplicate seq is a no-op (put_if_absent guard).
    let dup = append_task_event(&kv, "t1", 2, "dup".into()).await.unwrap();
    assert!(!dup);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drain_clears_event_partition_across_batches() {
    let (kv, _dir) = test_kv().await;
    for seq in 0..1200 {
        append_task_event(&kv, "t1", seq, "x".into()).await.unwrap();
    }
    drain_task_events(&kv, "t1").await.unwrap();
    let remaining = list_task_events_since(&kv, "t1", 0, 10).await.unwrap();
    assert!(remaining.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn schema_version_stamped_on_write() {
    let (kv, _dir) = test_kv().await;
    let mut rec = sample_record("t1");
    rec.schema_version = 0;
    put_task_summary(&kv, &rec).await.unwrap();
    let got = get_task_summary(&kv, "t1").await.unwrap().unwrap();
    assert_eq!(got.schema_version, CURRENT_RECORD_VERSION);
}
