// SPDX-FileCopyrightText: 2026 Artifact Depot Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Task persistence: KV-backed task summaries and append-only event logs.
//!
//! Each task writes a [`TaskRecord`] to [`keys::TABLE_TASKS`] (unsharded, one
//! record per task UUID) and zero or more [`TaskEventRecord`]s to
//! [`keys::TABLE_TASK_EVENTS`] (per-task partition, seq-numbered sort keys).
//! A periodic state scanner on every instance polls these tables so all
//! instances see the same task state regardless of who started the task.

use std::borrow::Cow;

use chrono::Utc;

use crate::error;
use crate::service::scan::SCAN_BATCH_SIZE;
use crate::service::typed::{typed_list, typed_put, typed_put_if_absent};
use crate::store::keys;
use crate::store::kv::{KvStore, TaskEventRecord, TaskRecord, CURRENT_RECORD_VERSION};

/// Unconditionally write a task summary record.
pub async fn put_task_summary(kv: &dyn KvStore, record: &TaskRecord) -> error::Result<()> {
    let mut record = record.clone();
    record.schema_version = CURRENT_RECORD_VERSION;
    let (table, pk, sk) = keys::task_key(&record.id);
    typed_put(kv, table, pk, sk, &record).await
}

/// Read a task summary by ID.
pub async fn get_task_summary(kv: &dyn KvStore, id: &str) -> error::Result<Option<TaskRecord>> {
    let (table, pk, sk) = keys::task_key(id);
    match kv.get(table, pk, sk).await? {
        Some(bytes) => Ok(Some(rmp_serde::from_slice(&bytes)?)),
        None => Ok(None),
    }
}

/// Read a task summary along with its KV version stamp, for callers that
/// will follow up with [`put_task_summary_if_unchanged`].
pub async fn get_task_summary_versioned(
    kv: &dyn KvStore,
    id: &str,
) -> error::Result<Option<(TaskRecord, u64)>> {
    let (table, pk, sk) = keys::task_key(id);
    match kv.get_versioned(table, pk, sk).await? {
        Some((bytes, version)) => Ok(Some((rmp_serde::from_slice(&bytes)?, version))),
        None => Ok(None),
    }
}

/// CAS write of a task summary: succeeds only if the record's KV version
/// still matches `expected_version`. Use this for progress-bridge style
/// RMW updates so a concurrent terminal transition written by
/// `mark_completed` / `mark_failed` / `mark_cancelled` is not overwritten
/// by a stale `Running` snapshot.
///
/// Returns `Ok(true)` on a successful CAS write, `Ok(false)` if the
/// version no longer matches.
pub async fn put_task_summary_if_unchanged(
    kv: &dyn KvStore,
    record: &TaskRecord,
    expected_version: u64,
) -> error::Result<bool> {
    let mut record = record.clone();
    record.schema_version = CURRENT_RECORD_VERSION;
    let (table, pk, sk) = keys::task_key(&record.id);
    let bytes = rmp_serde::to_vec_named(&record)?;
    kv.put_if_version(table, pk, sk, &bytes, Some(expected_version))
        .await
}

/// List all persisted task summaries.
pub async fn list_tasks(kv: &dyn KvStore) -> error::Result<Vec<TaskRecord>> {
    typed_list(
        kv,
        keys::TABLE_TASKS,
        Cow::Borrowed(keys::SINGLE_PK),
        Cow::Borrowed(""),
        10_000,
    )
    .await
}

/// Delete a task summary record. Caller must first drain any associated
/// event records via [`drain_task_events`]. Returns `true` if a record was
/// removed, `false` if the key did not exist.
pub async fn delete_task_summary(kv: &dyn KvStore, id: &str) -> error::Result<bool> {
    let (table, pk, sk) = keys::task_key(id);
    kv.delete(table, pk, sk).await
}

/// Append a single log entry to a task's event log. Uses `put_if_absent` so
/// a retried append with the same seq is a no-op rather than a spurious
/// overwrite.
///
/// Returns `Ok(true)` if the record was written, `Ok(false)` if a record at
/// that seq already existed (caller already appended at this seq).
pub async fn append_task_event(
    kv: &dyn KvStore,
    task_id: &str,
    seq: u64,
    message: String,
) -> error::Result<bool> {
    let record = TaskEventRecord {
        schema_version: CURRENT_RECORD_VERSION,
        task_id: task_id.to_string(),
        seq,
        timestamp: Utc::now(),
        message,
    };
    let (table, pk, sk) = keys::task_event_key(task_id, seq);
    typed_put_if_absent(kv, table, pk, sk, &record).await
}

/// List task events with seq `>= from_seq`, up to `limit` records. Pass
/// `from_seq = 0` to read from the start; pass `last_seen + 1` to tail only
/// new events after the last one you processed.
pub async fn list_task_events_since(
    kv: &dyn KvStore,
    task_id: &str,
    from_seq: u64,
    limit: usize,
) -> error::Result<Vec<TaskEventRecord>> {
    let start_sk = format!("{:020}", from_seq);
    let pk = keys::task_event_pk(task_id);
    let scan = kv
        .scan_range(
            keys::TABLE_TASK_EVENTS,
            Cow::Owned(pk),
            Cow::Owned(start_sk),
            None,
            limit,
        )
        .await?;
    let mut out = Vec::with_capacity(scan.items.len());
    for (_, bytes) in scan.items {
        out.push(rmp_serde::from_slice(&bytes)?);
    }
    Ok(out)
}

/// Delete all event records for a task, batching to avoid unbounded KV
/// round-trips.  Mirrors the `drain_deleting_repo` pattern.
pub async fn drain_task_events(kv: &dyn KvStore, task_id: &str) -> error::Result<()> {
    let pk = keys::task_event_pk(task_id);
    loop {
        let result = kv
            .scan_prefix(
                keys::TABLE_TASK_EVENTS,
                Cow::Borrowed(pk.as_str()),
                Cow::Borrowed(""),
                SCAN_BATCH_SIZE,
            )
            .await?;
        if result.items.is_empty() {
            break;
        }
        let del_keys: Vec<(&str, &str)> = result
            .items
            .iter()
            .map(|(sk, _)| (pk.as_str(), sk.as_str()))
            .collect();
        kv.delete_batch(keys::TABLE_TASK_EVENTS, &del_keys).await?;
        if result.done {
            break;
        }
    }
    Ok(())
}
